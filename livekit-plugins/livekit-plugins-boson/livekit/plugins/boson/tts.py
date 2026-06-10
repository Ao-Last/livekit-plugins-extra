from __future__ import annotations

import asyncio
import json
import os
import weakref
from dataclasses import dataclass, replace
from typing import Any, Literal

import aiohttp

from livekit.agents import (
    APIConnectionError,
    APIConnectOptions,
    APIStatusError,
    APITimeoutError,
    create_api_error_from_http,
    tts,
    utils,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS, NOT_GIVEN, NotGivenOr
from livekit.agents.utils import is_given

from .version import __version__

DEFAULT_BASE_URL = "https://api.boson.ai"
DEFAULT_MODEL = "higgs-audio-v3-tts"
DEFAULT_VOICE = "default"
NUM_CHANNELS = 1

ResponseFormat = Literal["mp3", "opus", "pcm", "wav", "aac", "flac"]


@dataclass
class _TTSOptions:
    model: str
    api_key: str
    voice: str | None
    response_format: ResponseFormat | str
    sample_rate: int
    ref_audio: str | None
    ref_text: str | None
    base_url: str

    @property
    def speech_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/v1/audio/speech"


class TTS(tts.TTS):
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
        voice: str | None = DEFAULT_VOICE,
        response_format: ResponseFormat | str = "pcm",
        sample_rate: int = 24000,
        ref_audio: str | None = None,
        ref_text: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Create a Boson AI Higgs Audio TTS instance.

        Args:
            api_key: Boson API key. Falls back to the ``BOSON_API_KEY`` environment variable.
            model: Boson TTS model ID. Defaults to ``higgs-audio-v3-tts``.
            voice: Preset voice name or registered Boson voice ID. Omitted from the
                request when ``ref_audio`` or ``ref_text`` is provided.
            response_format: Audio format for ``synthesize()``. Streaming always uses
                ``pcm`` because Boson streams raw PCM chunks.
            sample_rate: Sample rate used when decoding raw PCM output for LiveKit.
            ref_audio: Optional reference audio URL, data URI, or base64 payload for
                one-off voice cloning.
            ref_text: Optional transcript of ``ref_audio``.
            base_url: Boson API base URL.
            http_session: Existing aiohttp session to reuse.
        """
        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True),
            sample_rate=sample_rate,
            num_channels=NUM_CHANNELS,
        )

        api_key = api_key or os.environ.get("BOSON_API_KEY")
        if not api_key:
            raise ValueError(
                "Boson API key is required, either as argument or set "
                "BOSON_API_KEY environment variable"
            )

        self._opts = _TTSOptions(
            model=model,
            api_key=api_key,
            voice=voice,
            response_format=response_format,
            sample_rate=sample_rate,
            ref_audio=ref_audio,
            ref_text=ref_text,
            base_url=base_url,
        )
        self._session = http_session
        self._streams: weakref.WeakSet[SynthesizeStream] = weakref.WeakSet()

    @property
    def model(self) -> str:
        return self._opts.model

    @property
    def provider(self) -> str:
        return "Boson AI"

    def _ensure_session(self) -> aiohttp.ClientSession:
        if not self._session:
            self._session = utils.http_context.http_session()
        return self._session

    def update_options(
        self,
        *,
        model: NotGivenOr[str] = NOT_GIVEN,
        voice: NotGivenOr[str | None] = NOT_GIVEN,
        response_format: NotGivenOr[ResponseFormat | str] = NOT_GIVEN,
        sample_rate: NotGivenOr[int] = NOT_GIVEN,
        ref_audio: NotGivenOr[str | None] = NOT_GIVEN,
        ref_text: NotGivenOr[str | None] = NOT_GIVEN,
    ) -> None:
        """Update TTS options used by newly created streams."""
        if is_given(model):
            self._opts.model = model
        if is_given(voice):
            self._opts.voice = voice
        if is_given(response_format):
            self._opts.response_format = response_format
        if is_given(sample_rate):
            self._opts.sample_rate = sample_rate
            self._sample_rate = sample_rate
        if is_given(ref_audio):
            self._opts.ref_audio = ref_audio
        if is_given(ref_text):
            self._opts.ref_text = ref_text

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> ChunkedStream:
        return ChunkedStream(tts=self, input_text=text, conn_options=conn_options)

    def stream(
        self,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SynthesizeStream:
        stream = SynthesizeStream(tts=self, conn_options=conn_options)
        self._streams.add(stream)
        return stream

    async def aclose(self) -> None:
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()


class ChunkedStream(tts.ChunkedStream):
    def __init__(self, *, tts: TTS, input_text: str, conn_options: APIConnectOptions) -> None:
        super().__init__(tts=tts, input_text=input_text, conn_options=conn_options)
        self._tts: TTS = tts
        self._opts = replace(tts._opts)

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        try:
            async with self._tts._ensure_session().post(
                self._opts.speech_url,
                headers=_headers(self._opts),
                json=_speech_payload(
                    self._opts,
                    input_text=self._input_text,
                    stream=False,
                    response_format=self._opts.response_format,
                ),
                timeout=aiohttp.ClientTimeout(total=self._conn_options.timeout),
            ) as resp:
                await _raise_for_status(resp)

                output_emitter.initialize(
                    request_id=_request_id(resp),
                    sample_rate=self._opts.sample_rate,
                    num_channels=NUM_CHANNELS,
                    mime_type=_mime_type(self._opts.response_format),
                    stream=False,
                )

                async for chunk, _ in resp.content.iter_chunks():
                    if chunk:
                        output_emitter.push(chunk)

                output_emitter.flush()

        except asyncio.TimeoutError:
            raise APITimeoutError() from None
        except aiohttp.ClientResponseError as e:
            raise create_api_error_from_http(e.message, status=e.status) from None
        except APIStatusError:
            raise
        except Exception as e:
            raise APIConnectionError() from e


class SynthesizeStream(tts.SynthesizeStream):
    def __init__(self, *, tts: TTS, conn_options: APIConnectOptions) -> None:
        super().__init__(tts=tts, conn_options=conn_options)
        self._tts: TTS = tts
        self._opts = replace(tts._opts)

    async def _run(self, output_emitter: tts.AudioEmitter) -> None:
        output_emitter.initialize(
            request_id=utils.shortuuid(),
            sample_rate=self._opts.sample_rate,
            num_channels=NUM_CHANNELS,
            mime_type="audio/pcm",
            stream=True,
        )

        try:
            text_buffer = ""
            async for data in self._input_ch:
                if isinstance(data, str):
                    text_buffer += data
                elif isinstance(data, self._FlushSentinel):
                    text = text_buffer.strip()
                    if text:
                        await self._run_segment(text, output_emitter)
                    text_buffer = ""

        except asyncio.TimeoutError:
            raise APITimeoutError() from None
        except aiohttp.ClientResponseError as e:
            raise create_api_error_from_http(e.message, status=e.status) from None
        except APIStatusError:
            raise
        except Exception as e:
            raise APIConnectionError() from e

    async def _run_segment(self, text: str, output_emitter: tts.AudioEmitter) -> None:
        segment_id = utils.shortuuid()
        output_emitter.start_segment(segment_id=segment_id)

        self._mark_started()
        async with self._tts._ensure_session().post(
            self._opts.speech_url,
            headers=_headers(self._opts),
            json=_speech_payload(
                self._opts,
                input_text=text,
                stream=True,
                response_format="pcm",
            ),
            timeout=aiohttp.ClientTimeout(
                total=None,
                sock_connect=self._conn_options.timeout,
                sock_read=self._conn_options.timeout,
            ),
        ) as resp:
            await _raise_for_status(resp)

            async for chunk, _ in resp.content.iter_chunks():
                if chunk:
                    output_emitter.push(chunk)

        output_emitter.end_segment()


def _headers(opts: _TTSOptions) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {opts.api_key}",
        "Content-Type": "application/json",
        "X-LiveKit-Plugin": "livekit-plugins-boson",
        "X-LiveKit-Plugin-Version": __version__,
    }


def _speech_payload(
    opts: _TTSOptions,
    *,
    input_text: str,
    stream: bool,
    response_format: ResponseFormat | str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "input": input_text,
        "model": opts.model,
        "response_format": response_format,
        "stream": stream,
    }

    if opts.ref_audio is not None:
        payload["ref_audio"] = opts.ref_audio
    if opts.ref_text is not None:
        payload["ref_text"] = opts.ref_text

    if opts.ref_audio is None and opts.ref_text is None and opts.voice is not None:
        payload["voice"] = opts.voice

    return payload


async def _raise_for_status(resp: aiohttp.ClientResponse) -> None:
    if resp.status < 400:
        return

    body_text = await resp.text()
    body: object | None
    try:
        body = json.loads(body_text)
    except json.JSONDecodeError:
        body = body_text or None

    raise create_api_error_from_http(
        _error_message(body),
        status=resp.status,
        request_id=_request_id(resp),
        body=body,
    )


def _error_message(body: object | None) -> str:
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return error["message"]
        if isinstance(body.get("message"), str):
            return body["message"]
    if isinstance(body, str):
        return body
    return ""


def _request_id(resp: aiohttp.ClientResponse) -> str:
    return (
        resp.headers.get("x-request-id")
        or resp.headers.get("x-boson-request-id")
        or utils.shortuuid()
    )


def _mime_type(response_format: ResponseFormat | str) -> str:
    return {
        "mp3": "audio/mpeg",
        "opus": "audio/ogg",
        "pcm": "audio/pcm",
        "wav": "audio/wav",
        "aac": "audio/aac",
        "flac": "audio/flac",
    }.get(str(response_format), f"audio/{response_format}")
