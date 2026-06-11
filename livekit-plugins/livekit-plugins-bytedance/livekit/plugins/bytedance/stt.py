"""Volcengine BigModel streaming ASR WebSocket implementation.

Implements the binary WebSocket protocol for the optimized bidirectional
streaming endpoint:

    wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async

This module intentionally focuses on streaming ASR. It does not implement
Volcengine's batch/offline ASR APIs, and it does not target the
``bigmodel_nostream`` streaming-input endpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import json
import struct
import uuid
import weakref
from dataclasses import dataclass
from typing import Any, Literal

import aiohttp

from livekit import rtc
from livekit.agents import (
    APIConnectionError,
    APIConnectOptions,
    APIStatusError,
    LanguageCode,
    stt,
    utils,
)
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS, NOT_GIVEN, NotGivenOr, TimedString
from livekit.agents.utils import AudioBuffer, is_given

from .log import logger
from .tts import _is_retryable_code

DEFAULT_ASR_BASE_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
DEFAULT_ASR_RESOURCE_ID = "volc.seedasr.sauc.duration"

_SUCCESS_CODE = 20000000
_SUCCESS_PAYLOAD_CODES = {0, 1000, _SUCCESS_CODE}

# Message types (high nibble of byte 1)
_MSG_FULL_CLIENT_REQ = 0x1
_MSG_AUDIO_ONLY_REQ = 0x2
_MSG_FULL_SERVER_RESP = 0x9
_MSG_ERROR = 0xF

# Message type specific flags (low nibble of byte 1)
_FLAG_NO_SEQUENCE = 0x0
_FLAG_POS_SEQUENCE = 0x1
_FLAG_FINAL_NO_SEQUENCE = 0x2
_FLAG_NEG_SEQUENCE = 0x3

# Serialization/compression nibbles (byte 2)
_SER_NONE = 0x0
_SER_JSON = 0x1
_COMP_NONE = 0x0
_COMP_GZIP = 0x1

AudioFormat = Literal["pcm", "wav", "ogg", "mp3"]
AudioCodec = Literal["raw", "opus"]


def _build_header(
    *,
    msg_type: int,
    flags: int,
    serialization: int,
    compression: int,
) -> bytes:
    """Build the 4-byte protocol v1 header used by Volcengine ASR."""
    return bytes(
        [
            0x11,  # version=1, header_size=1 (4 bytes)
            (msg_type << 4) | flags,
            (serialization << 4) | compression,
            0x00,
        ]
    )


def _build_full_client_request(payload: dict[str, Any]) -> bytes:
    payload_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    compressed = gzip.compress(payload_bytes)
    return (
        _build_header(
            msg_type=_MSG_FULL_CLIENT_REQ,
            flags=_FLAG_NO_SEQUENCE,
            serialization=_SER_JSON,
            compression=_COMP_GZIP,
        )
        + struct.pack(">I", len(compressed))
        + compressed
    )


def _build_audio_only_request(audio: bytes, *, final: bool = False) -> bytes:
    compressed = gzip.compress(audio)
    flags = _FLAG_FINAL_NO_SEQUENCE if final else _FLAG_NO_SEQUENCE
    return (
        _build_header(
            msg_type=_MSG_AUDIO_ONLY_REQ,
            flags=flags,
            serialization=_SER_NONE,
            compression=_COMP_GZIP,
        )
        + struct.pack(">I", len(compressed))
        + compressed
    )


@dataclass
class _ASRFrame:
    msg_type: int
    flags: int
    payload: bytes
    sequence: int | None = None
    error_code: int = 0

    @property
    def is_final(self) -> bool:
        return self.flags in (_FLAG_FINAL_NO_SEQUENCE, _FLAG_NEG_SEQUENCE) or (
            self.sequence is not None and self.sequence < 0
        )


def _parse_server_frame(data: bytes) -> _ASRFrame:
    """Parse a Volcengine ASR server binary frame."""
    if len(data) < 4:
        raise ValueError("ASR frame is shorter than the 4-byte header")

    header_size = (data[0] & 0x0F) * 4
    if header_size < 4 or len(data) < header_size:
        raise ValueError("invalid ASR frame header size")

    msg_type = (data[1] >> 4) & 0x0F
    flags = data[1] & 0x0F
    compression = data[2] & 0x0F
    offset = header_size

    if msg_type == _MSG_ERROR:
        if len(data) < offset + 8:
            raise ValueError("ASR error frame is missing code or payload size")
        error_code = struct.unpack(">I", data[offset : offset + 4])[0]
        offset += 4
        payload_size = struct.unpack(">I", data[offset : offset + 4])[0]
        offset += 4
        payload = data[offset : offset + payload_size]
        if compression == _COMP_GZIP:
            payload = gzip.decompress(payload)
        return _ASRFrame(
            msg_type=msg_type,
            flags=flags,
            payload=payload,
            error_code=error_code,
        )

    sequence: int | None = None
    if flags in (_FLAG_POS_SEQUENCE, _FLAG_NEG_SEQUENCE):
        if len(data) < offset + 4:
            raise ValueError("ASR frame is missing sequence number")
        sequence = struct.unpack(">i", data[offset : offset + 4])[0]
        offset += 4

    if len(data) < offset + 4:
        raise ValueError("ASR frame is missing payload size")
    payload_size = struct.unpack(">I", data[offset : offset + 4])[0]
    offset += 4
    payload = data[offset : offset + payload_size]
    if compression == _COMP_GZIP:
        payload = gzip.decompress(payload)

    return _ASRFrame(
        msg_type=msg_type,
        flags=flags,
        sequence=sequence,
        payload=payload,
    )


def _decode_json_payload(payload: bytes) -> dict[str, Any]:
    if not payload:
        return {}
    return json.loads(payload.decode("utf-8"))


def _payload_status_code(payload: dict[str, Any]) -> int:
    for key in ("code", "status_code"):
        if key not in payload:
            continue
        try:
            return int(payload[key])
        except (TypeError, ValueError):
            return 0
    return 0


def _ms_to_seconds(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return 0.0


@dataclass
class _STTOptions:
    api_key: str | None
    app_key: str | None
    access_key: str | None
    resource_id: str
    base_url: str
    model_name: str
    language: LanguageCode
    audio_format: AudioFormat
    codec: AudioCodec
    sample_rate: int
    bits: int
    num_channels: int
    enable_interim_results: bool
    enable_nonstream: bool | None
    enable_itn: bool | None
    enable_punc: bool | None
    enable_ddc: bool | None
    show_utterances: bool | None
    result_type: str | None
    enable_speaker_info: bool | None
    ssd_version: str | None
    output_zh_variant: str | None
    show_speech_rate: bool | None
    show_volume: bool | None
    enable_lid: bool | None
    enable_emotion_detection: bool | None
    enable_gender_detection: bool | None
    enable_accelerate_text: bool | None
    accelerate_score: int | None
    vad_segment_duration: int | None
    end_window_size: int | None
    force_to_speech_time: int | None
    sensitive_words_filter: dict[str, Any] | str | None
    enable_poi_fc: bool | None
    enable_music_fc: bool | None
    corpus: dict[str, Any] | None
    user: dict[str, Any] | None
    audio_options: dict[str, Any]
    request_options: dict[str, Any]


class STT(stt.STT):
    """Volcengine BigModel streaming ASR client.

    The default endpoint is the optimized bidirectional streaming API
    ``/api/v3/sauc/bigmodel_async``. Authentication follows the current
    Volcengine console header contract with ``X-Api-Key``; legacy
    ``X-Api-App-Key``/``X-Api-Access-Key`` credentials are also accepted.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        app_key: str | None = None,
        access_key: str | None = None,
        resource_id: str = DEFAULT_ASR_RESOURCE_ID,
        base_url: str = DEFAULT_ASR_BASE_URL,
        model_name: str = "bigmodel",
        language: str = "zh-CN",
        audio_format: AudioFormat = "pcm",
        codec: AudioCodec = "raw",
        sample_rate: int = 16000,
        bits: int = 16,
        num_channels: int = 1,
        enable_interim_results: bool = True,
        enable_nonstream: bool | None = True,
        enable_itn: bool | None = None,
        enable_punc: bool | None = None,
        enable_ddc: bool | None = None,
        show_utterances: bool | None = True,
        result_type: str | None = None,
        enable_speaker_info: bool | None = None,
        ssd_version: str | None = None,
        output_zh_variant: str | None = None,
        show_speech_rate: bool | None = None,
        show_volume: bool | None = None,
        enable_lid: bool | None = None,
        enable_emotion_detection: bool | None = None,
        enable_gender_detection: bool | None = None,
        enable_accelerate_text: bool | None = None,
        accelerate_score: int | None = None,
        vad_segment_duration: int | None = None,
        end_window_size: int | None = None,
        force_to_speech_time: int | None = None,
        sensitive_words_filter: dict[str, Any] | str | None = None,
        enable_poi_fc: bool | None = None,
        enable_music_fc: bool | None = None,
        corpus: dict[str, Any] | None = None,
        user: dict[str, Any] | None = None,
        audio_options: dict[str, Any] | None = None,
        request_options: dict[str, Any] | None = None,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        if not api_key and not (app_key and access_key):
            raise ValueError(
                "Volcengine ASR requires api_key, or both app_key and access_key "
                "for legacy console authentication"
            )
        if sample_rate != 16000:
            raise ValueError(
                "Volcengine BigModel streaming ASR currently supports sample_rate=16000"
            )
        if bits != 16:
            raise ValueError("Volcengine BigModel streaming ASR currently supports bits=16")

        super().__init__(
            capabilities=stt.STTCapabilities(
                streaming=True,
                interim_results=enable_interim_results,
                diarization=bool(enable_speaker_info),
                aligned_transcript="word" if show_utterances else False,
                offline_recognize=False,
            )
        )

        self._opts = _STTOptions(
            api_key=api_key,
            app_key=app_key,
            access_key=access_key,
            resource_id=resource_id,
            base_url=base_url,
            model_name=model_name,
            language=LanguageCode(language),
            audio_format=audio_format,
            codec=codec,
            sample_rate=sample_rate,
            bits=bits,
            num_channels=num_channels,
            enable_interim_results=enable_interim_results,
            enable_nonstream=enable_nonstream,
            enable_itn=enable_itn,
            enable_punc=enable_punc,
            enable_ddc=enable_ddc,
            show_utterances=show_utterances,
            result_type=result_type,
            enable_speaker_info=enable_speaker_info,
            ssd_version=ssd_version,
            output_zh_variant=output_zh_variant,
            show_speech_rate=show_speech_rate,
            show_volume=show_volume,
            enable_lid=enable_lid,
            enable_emotion_detection=enable_emotion_detection,
            enable_gender_detection=enable_gender_detection,
            enable_accelerate_text=enable_accelerate_text,
            accelerate_score=accelerate_score,
            vad_segment_duration=vad_segment_duration,
            end_window_size=end_window_size,
            force_to_speech_time=force_to_speech_time,
            sensitive_words_filter=sensitive_words_filter,
            enable_poi_fc=enable_poi_fc,
            enable_music_fc=enable_music_fc,
            corpus=corpus,
            user=user,
            audio_options=audio_options or {},
            request_options=request_options or {},
        )
        self._session = http_session
        self._streams: weakref.WeakSet[SpeechStream] = weakref.WeakSet()
        self._ws_meta: dict[int, dict[str, str]] = {}

    @property
    def model(self) -> str:
        return self._opts.resource_id

    @property
    def provider(self) -> str:
        return "ByteDance / Volcengine"

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = utils.http_context.http_session()
        return self._session

    async def _connect_ws(
        self, *, request_id: str, timeout: float
    ) -> aiohttp.ClientWebSocketResponse:
        session = self._ensure_session()
        connect_id = str(uuid.uuid4())
        headers = {
            "X-Api-Resource-Id": self._opts.resource_id,
            "X-Api-Request-Id": request_id,
            "X-Api-Sequence": "-1",
            "X-Api-Connect-Id": connect_id,
        }
        if self._opts.api_key:
            headers["X-Api-Key"] = self._opts.api_key
        else:
            assert self._opts.app_key is not None and self._opts.access_key is not None
            headers["X-Api-App-Key"] = self._opts.app_key
            headers["X-Api-Access-Key"] = self._opts.access_key

        try:
            ws = await asyncio.wait_for(
                session.ws_connect(self._opts.base_url, headers=headers),
                timeout=timeout,
            )
        except asyncio.TimeoutError as e:
            raise APIConnectionError("timed out connecting to Volcengine ASR") from e
        except aiohttp.ClientError as e:
            raise APIConnectionError("failed to connect to Volcengine ASR") from e

        logid = ""
        with contextlib.suppress(AttributeError):
            logid = ws._response.headers.get("X-Tt-Logid", "") or ""
        self._ws_meta[id(ws)] = {"connect_id": connect_id, "logid": logid}
        return ws

    def _build_recognition_payload(self) -> dict[str, Any]:
        audio: dict[str, Any] = {
            "format": self._opts.audio_format,
            "codec": self._opts.codec,
            "rate": self._opts.sample_rate,
            "bits": self._opts.bits,
            "channel": self._opts.num_channels,
        }
        audio.update(self._opts.audio_options)

        request: dict[str, Any] = {"model_name": self._opts.model_name}
        option_map = {
            "enable_nonstream": self._opts.enable_nonstream,
            "enable_itn": self._opts.enable_itn,
            "enable_punc": self._opts.enable_punc,
            "enable_ddc": self._opts.enable_ddc,
            "show_utterances": self._opts.show_utterances,
            "result_type": self._opts.result_type,
            "enable_speaker_info": self._opts.enable_speaker_info,
            "ssd_version": self._opts.ssd_version,
            "output_zh_variant": self._opts.output_zh_variant,
            "show_speech_rate": self._opts.show_speech_rate,
            "show_volume": self._opts.show_volume,
            "enable_lid": self._opts.enable_lid,
            "enable_emotion_detection": self._opts.enable_emotion_detection,
            "enable_gender_detection": self._opts.enable_gender_detection,
            "enable_accelerate_text": self._opts.enable_accelerate_text,
            "accelerate_score": self._opts.accelerate_score,
            "vad_segment_duration": self._opts.vad_segment_duration,
            "end_window_size": self._opts.end_window_size,
            "force_to_speech_time": self._opts.force_to_speech_time,
            "sensitive_words_filter": self._opts.sensitive_words_filter,
            "enable_poi_fc": self._opts.enable_poi_fc,
            "enable_music_fc": self._opts.enable_music_fc,
            "corpus": self._opts.corpus,
        }
        request.update({key: value for key, value in option_map.items() if value is not None})
        request.update(self._opts.request_options)

        payload: dict[str, Any] = {
            "user": self._opts.user or {"uid": utils.shortuuid()},
            "audio": audio,
            "request": request,
        }
        return payload

    async def _recognize_impl(
        self,
        buffer: AudioBuffer,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> stt.SpeechEvent:
        stream = self.stream(language=language, conn_options=conn_options)
        final_text: list[str] = []
        request_id = ""
        try:
            frames = buffer if isinstance(buffer, list) else [buffer]
            for frame in frames:
                stream.push_frame(frame)
            stream.end_input()

            async for event in stream:
                if event.request_id:
                    request_id = event.request_id
                if event.type == stt.SpeechEventType.FINAL_TRANSCRIPT and event.alternatives:
                    final_text.append(event.alternatives[0].text)
        finally:
            await stream.aclose()

        resolved_language = LanguageCode(language) if is_given(language) else self._opts.language
        return stt.SpeechEvent(
            type=stt.SpeechEventType.FINAL_TRANSCRIPT,
            request_id=request_id,
            alternatives=[
                stt.SpeechData(
                    language=resolved_language,
                    text="".join(final_text),
                )
            ],
        )

    def stream(
        self,
        *,
        language: NotGivenOr[str] = NOT_GIVEN,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SpeechStream:
        resolved_language = LanguageCode(language) if is_given(language) else self._opts.language
        stream = SpeechStream(
            stt=self,
            conn_options=conn_options,
            language=resolved_language,
        )
        self._streams.add(stream)
        return stream

    async def aclose(self) -> None:
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()


class SpeechStream(stt.RecognizeStream):
    def __init__(
        self,
        *,
        stt: STT,
        conn_options: APIConnectOptions,
        language: LanguageCode,
    ) -> None:
        super().__init__(stt=stt, conn_options=conn_options, sample_rate=stt._opts.sample_rate)
        self._volc_stt = stt
        self._language = language
        self._request_id = str(uuid.uuid4())
        self._speaking = False
        self._last_interim_text = ""
        self._finalized_utterances: set[tuple[int, int, str]] = set()

    async def _run(self) -> None:
        closing_ws = False

        @utils.log_exceptions(logger=logger)
        async def send_task(ws: aiohttp.ClientWebSocketResponse) -> None:
            nonlocal closing_ws
            samples_per_chunk = self._volc_stt._opts.sample_rate // 5
            audio_bstream = utils.audio.AudioByteStream(
                sample_rate=self._volc_stt._opts.sample_rate,
                num_channels=self._volc_stt._opts.num_channels,
                samples_per_channel=samples_per_chunk,
            )

            async for data in self._input_ch:
                frames: list[rtc.AudioFrame] = []
                if isinstance(data, rtc.AudioFrame):
                    frames.extend(audio_bstream.write(data.data.tobytes()))
                elif isinstance(data, self._FlushSentinel):
                    frames.extend(audio_bstream.flush())

                for frame in frames:
                    await ws.send_bytes(_build_audio_only_request(frame.data.tobytes()))

            for frame in audio_bstream.flush():
                await ws.send_bytes(_build_audio_only_request(frame.data.tobytes()))

            await ws.send_bytes(_build_audio_only_request(b"", final=True))
            closing_ws = True

        @utils.log_exceptions(logger=logger)
        async def recv_task(ws: aiohttp.ClientWebSocketResponse) -> None:
            nonlocal closing_ws
            while True:
                msg = await ws.receive()
                if msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSING,
                ):
                    if closing_ws:
                        return
                    raise APIStatusError(message="Volcengine ASR WebSocket closed unexpectedly")

                if msg.type == aiohttp.WSMsgType.ERROR:
                    raise APIConnectionError("Volcengine ASR WebSocket error")

                if msg.type != aiohttp.WSMsgType.BINARY:
                    logger.warning("unexpected Volcengine ASR message type %s", msg.type)
                    continue

                frame = _parse_server_frame(msg.data)
                if frame.msg_type == _MSG_ERROR:
                    message = frame.payload.decode("utf-8", errors="replace")
                    raise APIStatusError(
                        message=message or "Volcengine ASR server error",
                        status_code=frame.error_code,
                        request_id=self._request_id,
                        body=message,
                        retryable=_is_retryable_code(frame.error_code),
                    )

                if frame.msg_type != _MSG_FULL_SERVER_RESP:
                    logger.warning("unexpected Volcengine ASR frame type %s", frame.msg_type)
                    continue

                payload = _decode_json_payload(frame.payload)
                self._process_payload(payload, is_final_frame=frame.is_final)
                if frame.is_final:
                    return

        ws: aiohttp.ClientWebSocketResponse | None = None
        try:
            ws = await self._volc_stt._connect_ws(
                request_id=self._request_id,
                timeout=self._conn_options.timeout,
            )
            await ws.send_bytes(
                _build_full_client_request(self._volc_stt._build_recognition_payload())
            )
            tasks = [
                asyncio.create_task(send_task(ws)),
                asyncio.create_task(recv_task(ws)),
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                await utils.aio.gracefully_cancel(*tasks)
        finally:
            if ws is not None:
                self._volc_stt._ws_meta.pop(id(ws), None)
                await ws.close()

    def _process_payload(self, payload: dict[str, Any], *, is_final_frame: bool) -> None:
        code = _payload_status_code(payload)
        if code not in _SUCCESS_PAYLOAD_CODES:
            message = str(payload.get("message") or payload.get("error") or "Volcengine ASR error")
            raise APIStatusError(
                message=message,
                status_code=code,
                request_id=self._request_id,
                body=payload,
                retryable=_is_retryable_code(code),
            )

        result = payload.get("result") or {}
        if not isinstance(result, dict):
            return

        utterances = result.get("utterances") or []
        emitted_final = False
        if isinstance(utterances, list):
            for utterance in utterances:
                if not isinstance(utterance, dict):
                    continue
                if self._process_utterance(utterance, is_final_frame=is_final_frame):
                    emitted_final = True

        if not utterances:
            text = str(result.get("text") or payload.get("text") or "")
            if text:
                if is_final_frame:
                    self._emit_transcript(stt.SpeechEventType.FINAL_TRANSCRIPT, text)
                    emitted_final = True
                elif (
                    self._volc_stt._opts.enable_interim_results and text != self._last_interim_text
                ):
                    self._last_interim_text = text
                    self._emit_transcript(stt.SpeechEventType.INTERIM_TRANSCRIPT, text)

        if emitted_final:
            self._emit_end_of_speech()

    def _process_utterance(self, utterance: dict[str, Any], *, is_final_frame: bool) -> bool:
        text = str(utterance.get("text") or "")
        if not text:
            return False

        start_ms = int(utterance.get("start_time") or 0)
        end_ms = int(utterance.get("end_time") or 0)
        is_definite = bool(utterance.get("definite")) or is_final_frame

        if is_definite:
            key = (start_ms, end_ms, text)
            if key in self._finalized_utterances:
                return False
            self._finalized_utterances.add(key)
            self._emit_transcript(
                stt.SpeechEventType.FINAL_TRANSCRIPT,
                text,
                start_time=_ms_to_seconds(start_ms),
                end_time=_ms_to_seconds(end_ms),
                speaker_id=self._speaker_id(utterance),
                words=self._words_from_utterance(utterance),
                metadata={"volcengine": utterance},
            )
            return True

        if self._volc_stt._opts.enable_interim_results and text != self._last_interim_text:
            self._last_interim_text = text
            self._emit_transcript(
                stt.SpeechEventType.INTERIM_TRANSCRIPT,
                text,
                start_time=_ms_to_seconds(start_ms),
                end_time=_ms_to_seconds(end_ms),
                speaker_id=self._speaker_id(utterance),
                words=self._words_from_utterance(utterance),
                metadata={"volcengine": utterance},
            )
        return False

    def _emit_transcript(
        self,
        event_type: stt.SpeechEventType,
        text: str,
        *,
        start_time: float = 0.0,
        end_time: float = 0.0,
        speaker_id: str | None = None,
        words: list[TimedString] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not self._speaking:
            self._speaking = True
            self._event_ch.send_nowait(stt.SpeechEvent(type=stt.SpeechEventType.START_OF_SPEECH))

        self._event_ch.send_nowait(
            stt.SpeechEvent(
                type=event_type,
                request_id=self._request_id,
                alternatives=[
                    stt.SpeechData(
                        language=self._language,
                        text=text,
                        start_time=start_time + self.start_time_offset,
                        end_time=end_time + self.start_time_offset,
                        speaker_id=speaker_id,
                        words=words,
                        metadata=metadata,
                    )
                ],
            )
        )

    def _emit_end_of_speech(self) -> None:
        if self._speaking:
            self._speaking = False
            self._event_ch.send_nowait(stt.SpeechEvent(type=stt.SpeechEventType.END_OF_SPEECH))

    def _words_from_utterance(self, utterance: dict[str, Any]) -> list[TimedString] | None:
        words = utterance.get("words") or []
        if not isinstance(words, list):
            return None

        timed_words: list[TimedString] = []
        for word in words:
            if not isinstance(word, dict):
                continue
            text = str(word.get("text") or word.get("word") or "")
            if not text:
                continue
            timed_words.append(
                TimedString(
                    text=text,
                    start_time=_ms_to_seconds(word.get("start_time")) + self.start_time_offset,
                    end_time=_ms_to_seconds(word.get("end_time")) + self.start_time_offset,
                    start_time_offset=self.start_time_offset,
                )
            )

        return timed_words or None

    @staticmethod
    def _speaker_id(utterance: dict[str, Any]) -> str | None:
        speaker_id = utterance.get("speaker_id") or utterance.get("speaker")
        return str(speaker_id) if speaker_id is not None else None
