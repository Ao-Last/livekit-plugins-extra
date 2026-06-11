"""Volcengine TTS V3 bidirectional streaming API implementation.

Implements the V3 protocol at wss://openspeech.bytedance.com/api/v3/tts/bidirection
which supports TTS 2.0 features such as voice instructions, cloned-voice models,
SSML, subtitles, and voice-tag parsing.

See: https://www.volcengine.com/docs/6561/1329505
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import struct
import time
import weakref
from dataclasses import dataclass
from typing import Any, Literal

import aiohttp

from livekit.agents import APIConnectOptions, APIError, APIStatusError, tts, utils
from livekit.agents.types import DEFAULT_API_CONNECT_OPTIONS

from .log import logger

# ---------------------------------------------------------------------------
# V3 binary protocol constants
# ---------------------------------------------------------------------------

# Client → server events
_EVT_START_CONNECTION = 1
_EVT_FINISH_CONNECTION = 2
_EVT_START_SESSION = 100
_EVT_CANCEL_SESSION = 101
_EVT_FINISH_SESSION = 102
_EVT_TASK_REQUEST = 200

# Server → client events
_EVT_CONNECTION_STARTED = 50
_EVT_CONNECTION_FAILED = 51
_EVT_CONNECTION_FINISHED = 52
_EVT_SESSION_STARTED = 150
_EVT_SESSION_CANCELED = 151
_EVT_SESSION_FINISHED = 152
_EVT_SESSION_FAILED = 153
_EVT_TTS_SENTENCE_START = 350
_EVT_TTS_SENTENCE_END = 351
_EVT_TTS_RESPONSE = 352  # audio data
_EVT_TTS_SUBTITLE = 353

# Message types (high nibble of byte 1)
_MSG_FULL_CLIENT_REQ = 0x1
_MSG_FULL_SERVER_RESP = 0x9
_MSG_AUDIO_RESP = 0xB
_MSG_ERROR = 0xF

_SUCCESS_CODE = 20000000


# ---------------------------------------------------------------------------
# V3 binary frame codec
# ---------------------------------------------------------------------------


def _build_client_frame(
    event: int,
    session_id: str | None = None,
    payload: dict | None = None,
) -> bytes:
    """Build a V3 client request binary frame with event.

    Connection-level events (StartConnection, FinishConnection) have no session_id.
    Session/data events include session_id_len + session_id before the payload.
    """
    # Header: version=1|headersize=1, msgtype=0001|flags=0100, serial=JSON|compress=none, reserved
    header = bytes([0x11, 0x14, 0x10, 0x00])
    event_bytes = struct.pack(">i", event)

    payload_bytes = json.dumps(payload or {}).encode("utf-8")
    payload_len = struct.pack(">I", len(payload_bytes))

    if session_id is not None:
        # Session-level frame: event + sid_len + sid + payload_len + payload
        sid_bytes = session_id.encode("utf-8")
        sid_len = struct.pack(">I", len(sid_bytes))
        return header + event_bytes + sid_len + sid_bytes + payload_len + payload_bytes
    else:
        # Connection-level frame: event + payload_len + payload
        return header + event_bytes + payload_len + payload_bytes


@dataclass
class _V3Frame:
    msg_type: int
    event: int
    session_id: str
    payload: bytes  # raw payload bytes (audio or JSON)
    is_audio: bool
    error_code: int = 0  # populated for _MSG_ERROR frames; 0 otherwise


def _parse_v3_frame(data: bytes) -> _V3Frame:
    """Parse a V3 server response binary frame."""
    msg_type = (data[1] >> 4) & 0xF
    flags = data[1] & 0xF
    # serialization = (data[2] >> 4) & 0xF
    # compression = data[2] & 0xF

    offset = 4  # skip 4-byte header

    event = 0
    session_id = ""

    if msg_type == _MSG_ERROR:
        # Error frame: bytes 4-7 = error code, bytes 8-11 = payload_len, bytes 12+ = payload
        error_code = 0
        payload = b""
        if len(data) >= 8:
            error_code = struct.unpack(">I", data[4:8])[0]
        if len(data) >= 12:
            payload_len = struct.unpack(">I", data[8:12])[0]
            payload = data[12 : 12 + payload_len]
        return _V3Frame(
            msg_type=msg_type,
            event=0,
            session_id="",
            payload=payload,
            is_audio=False,
            error_code=error_code,
        )

    has_event = (flags & 0x4) != 0

    if has_event and len(data) >= offset + 4:
        event = struct.unpack(">i", data[offset : offset + 4])[0]
        offset += 4

    # Connection events have connect_id; session/data events have session_id
    if has_event and len(data) >= offset + 4:
        id_len = struct.unpack(">I", data[offset : offset + 4])[0]
        offset += 4
        if id_len > 0 and len(data) >= offset + id_len:
            session_id = data[offset : offset + id_len].decode("utf-8", errors="replace")
            offset += id_len

    is_audio = msg_type == _MSG_AUDIO_RESP

    if is_audio:
        # Audio frame: remaining bytes after session_id are payload_len + audio_data
        if len(data) >= offset + 4:
            audio_len = struct.unpack(">I", data[offset : offset + 4])[0]
            offset += 4
            payload = data[offset : offset + audio_len]
        else:
            payload = b""
    else:
        # JSON frame: payload_len + JSON bytes
        if len(data) >= offset + 4:
            payload_len = struct.unpack(">I", data[offset : offset + 4])[0]
            offset += 4
            payload = data[offset : offset + payload_len]
        else:
            payload = b""

    return _V3Frame(
        msg_type=msg_type,
        event=event,
        session_id=session_id,
        payload=payload,
        is_audio=is_audio,
    )


def _is_retryable_code(code: int) -> bool:
    """Volcengine V3 codes: 45xxxxxx = client error (fatal), 55xxxxxx = server (retry)."""
    return not (40000000 <= code < 50000000)


def _parse_session_meta(payload: bytes) -> tuple[int, str]:
    """Extract (status_code, message) from a SessionFailed/SessionFinished payload."""
    raw = payload.decode("utf-8", errors="replace") if payload else ""
    try:
        meta = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        return 0, raw or "unknown"
    return int(meta.get("status_code", 0) or 0), str(meta.get("message", raw or "unknown"))


# ---------------------------------------------------------------------------
# TTS class
# ---------------------------------------------------------------------------

_MIME_TYPES = {
    "pcm": "audio/pcm",
    "mp3": "audio/mp3",
    "ogg_opus": "audio/ogg",
    "wav": "audio/wav",
}

AudioFormat = Literal["pcm", "mp3", "ogg_opus", "wav"]


class VolcengineV3TTS(tts.TTS):
    """Volcengine TTS using V3 bidirectional streaming API.

    Supports TTS 1.0 and TTS 2.0 models. The V3 API handles sentence splitting
    server-side, so LLM output tokens can be streamed directly.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        app_key: str | None = None,
        access_key: str | None = None,
        resource_id: str = "seed-tts-2.0",
        model: str | None = None,
        speaker: str = "zh_female_shuangkuaisisi_uranus_bigtts",
        base_url: str = "wss://openspeech.bytedance.com/api/v3/tts/bidirection",
        ssml: str | None = None,
        audio_format: AudioFormat = "pcm",
        sample_rate: int = 24000,
        bit_rate: int | None = None,
        speech_rate: int = 0,
        loudness_rate: int = 0,
        enable_subtitle: bool | None = None,
        disable_markdown_filter: bool | None = None,
        disable_emoji_filter: bool | None = None,
        enable_latex_tn: bool | None = None,
        latex_parser: str | None = None,
        explicit_language: str | None = None,
        explicit_dialect: str | None = None,
        aigc_watermark: bool | None = None,
        aigc_metadata: dict[str, Any] | None = None,
        cache_config: dict[str, Any] | None = None,
        post_process: dict[str, Any] | None = None,
        context_texts: list[str] | None = None,
        use_tag_parser: bool | None = None,
        require_usage_tokens: bool = False,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Create a Volcengine TTS V3 bidirectional streaming instance.

        Args:
            api_key: Volcengine API key for the current console auth flow.
            app_key: Legacy console app key. Used only when ``api_key`` is not provided.
            access_key: Legacy console access key. Used only when ``api_key`` is not provided.
            resource_id: Volcengine TTS resource ID, for example ``seed-tts-2.0`` or
                ``seed-icl-2.0``.
            model: Optional concrete model for cloned voices, for example
                ``seed-tts-2.0-standard`` or ``seed-tts-2.0-expressive``.
            speaker: Volcengine speaker ID.
            base_url: V3 bidirectional websocket URL.
            ssml: Optional SSML marker text.
            audio_format: Requested audio format.
            sample_rate: Output sample rate.
            bit_rate: Optional MP3 bit rate.
            speech_rate: Volcengine speech-rate control.
            loudness_rate: Volcengine loudness control.
            enable_subtitle: Enable word-level subtitle timestamps. Subtitle events are
                currently ignored by the LiveKit audio stream.
            disable_markdown_filter: Volcengine markdown parsing/filter option.
            disable_emoji_filter: Volcengine emoji parsing/filter option.
            enable_latex_tn: Enable LaTeX text normalization.
            latex_parser: Optional LaTeX parser version, for example ``v2``.
            explicit_language: Explicit spoken language, for example ``zh-cn`` or ``en``.
            explicit_dialect: Explicit dialect for supported speakers.
            aigc_watermark: Enable AIGC rhythm watermark.
            aigc_metadata: Optional AIGC metadata watermark payload.
            cache_config: Optional cache configuration payload.
            post_process: Optional post-processing payload, for example ``{"pitch": 0}``.
            context_texts: Optional TTS 2.0 style prompt/context hints.
            use_tag_parser: Enable voice-tag parser for supported cloned voices.
            require_usage_tokens: Request usage token/character accounting in responses.
            http_session: Existing aiohttp session to reuse.
        """
        if not api_key and not (app_key and access_key):
            raise ValueError(
                "Volcengine TTS V3 requires api_key, or both app_key and access_key "
                "for legacy console authentication"
            )

        super().__init__(
            capabilities=tts.TTSCapabilities(streaming=True),
            sample_rate=sample_rate,
            num_channels=1,
        )
        self._api_key = api_key
        self._app_key = app_key
        self._access_key = access_key
        self._resource_id = resource_id
        self._model = model
        self._speaker = speaker
        self._base_url = base_url
        self._ssml = ssml
        self._format = audio_format
        self._sample_rate = sample_rate
        self._bit_rate = bit_rate
        self._speech_rate = speech_rate
        self._loudness_rate = loudness_rate
        self._enable_subtitle = enable_subtitle
        self._disable_markdown_filter = disable_markdown_filter
        self._disable_emoji_filter = disable_emoji_filter
        self._enable_latex_tn = enable_latex_tn
        self._latex_parser = latex_parser
        self._explicit_language = explicit_language
        self._explicit_dialect = explicit_dialect
        self._aigc_watermark = aigc_watermark
        self._aigc_metadata = aigc_metadata
        self._cache_config = cache_config
        self._post_process = post_process
        self._context_texts = context_texts
        self._use_tag_parser = use_tag_parser
        self._require_usage_tokens = require_usage_tokens
        self._session = http_session

        self._pool = utils.ConnectionPool[aiohttp.ClientWebSocketResponse](
            connect_cb=self._connect_ws,
            close_cb=self._close_ws,
            max_session_duration=120,
            mark_refreshed_on_get=True,
        )
        # Per-ws observability metadata: connect_id (our trace ID) + logid
        # (Volcengine's server-side trace ID from X-Tt-Logid).  Keyed by
        # `id(ws)` because `ClientWebSocketResponse` is unhashable and the
        # pool returns us raw `ws` objects without any wrapper.  Cleared in
        # `_close_ws` when the pool evicts a connection.
        self._ws_meta: dict[int, dict[str, str]] = {}
        self._streams: weakref.WeakSet[SynthesizeStream] = weakref.WeakSet()

    @property
    def model(self) -> str:
        return self._resource_id

    @property
    def provider(self) -> str:
        return "ByteDance / Volcengine"

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = utils.http_context.http_session()
        return self._session

    def _ws_log_extra(
        self, ws: aiohttp.ClientWebSocketResponse, **overrides: object
    ) -> dict[str, object]:
        """Build a structured-log `extra={...}` dict for logs scoped to `ws`.

        Returns `{connect_id, logid, ...overrides}`.  Any caller can merge in
        extra context (e.g. `session_id`, numeric timings) via kwargs.  Missing
        metadata is represented as an empty string rather than omitted, so log
        consumers can rely on the keys being present.
        """
        meta = self._ws_meta.get(id(ws), {})
        return {
            "connect_id": meta.get("connect_id", ""),
            "logid": meta.get("logid", ""),
            **overrides,
        }

    async def _connect_ws(self, timeout: float) -> aiohttp.ClientWebSocketResponse:
        """Open WS and complete StartConnection handshake."""
        session = self._ensure_session()
        connect_id = utils.shortuuid()
        headers = {
            "X-Api-Resource-Id": self._resource_id,
            # Per-connection trace ID — makes it possible to cross-reference our
            # logs with Volcengine server-side logs if we need to chase an issue.
            "X-Api-Connect-Id": connect_id,
        }
        if self._api_key:
            headers["X-Api-Key"] = self._api_key
        else:
            assert self._app_key is not None and self._access_key is not None
            headers["X-Api-App-Key"] = self._app_key
            headers["X-Api-Access-Key"] = self._access_key
        if self._require_usage_tokens:
            headers["X-Control-Require-Usage-Tokens-Return"] = "*"
        # Deliberately NOT passing `heartbeat=`. aiohttp's WS heartbeat closes
        # the connection itself if a PONG isn't pulled from the receive queue
        # within heartbeat/2 seconds — and the queue is only drained while
        # someone is awaiting `ws.receive()`. While a WS sits idle in our
        # ConnectionPool between user turns, nobody reads it, PONGs pile up
        # unprocessed, and aiohttp self-kills the connection. See aio-libs/
        # aiohttp#7508 (and rejected fix PR #10544) for the upstream behavior,
        # and our issue #124 for the full writeup. Middlebox idle-drop (the
        # original reason heartbeat=5 was added in PR #112) is now handled
        # reactively via the `APIError → retry` path.
        ws = await asyncio.wait_for(
            session.ws_connect(self._base_url, headers=headers),
            timeout=timeout,
        )
        # Capture Volcengine's server-side trace ID from the handshake
        # response.  `X-Tt-Logid` is in the HTTP 101 Upgrade response
        # headers, reachable via `ws._response.headers`.  That attribute is
        # private in aiohttp but has been stable across 3.x.  Missing logid
        # (e.g. a stubbed ws in tests) is not fatal — we just log "" for it.
        logid = ""
        with contextlib.suppress(AttributeError):
            logid = ws._response.headers.get("X-Tt-Logid", "") or ""
        self._ws_meta[id(ws)] = {"connect_id": connect_id, "logid": logid}

        # Send StartConnection
        frame = _build_client_frame(_EVT_START_CONNECTION)
        await ws.send_bytes(frame)

        # Wait for ConnectionStarted
        resp_data = await asyncio.wait_for(ws.receive_bytes(), timeout=timeout)
        resp = _parse_v3_frame(resp_data)
        if resp.event != _EVT_CONNECTION_STARTED:
            await ws.close()
            self._ws_meta.pop(id(ws), None)
            raise ConnectionError(
                f"Expected ConnectionStarted({_EVT_CONNECTION_STARTED}), got event={resp.event}"
            )
        logger.info(
            "V3 WS connection established",
            extra={"connect_id": connect_id, "logid": logid},
        )
        return ws

    async def _close_ws(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Send FinishConnection and close WS."""
        try:
            if ws.closed:
                return
            try:
                frame = _build_client_frame(_EVT_FINISH_CONNECTION)
                await ws.send_bytes(frame)
                # Best-effort wait for ConnectionFinished
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(ws.receive_bytes(), timeout=3.0)
            except Exception:
                pass
            finally:
                await ws.close()
        finally:
            self._ws_meta.pop(id(ws), None)

    def _build_session_payload(self) -> dict:
        """Build the StartSession payload with TTS config."""
        audio_params: dict = {
            "format": self._format,
            "sample_rate": self._sample_rate,
        }
        if self._bit_rate is not None:
            audio_params["bit_rate"] = self._bit_rate
        if self._speech_rate != 0:
            audio_params["speech_rate"] = self._speech_rate
        if self._loudness_rate != 0:
            audio_params["loudness_rate"] = self._loudness_rate
        if self._enable_subtitle is not None:
            audio_params["enable_subtitle"] = self._enable_subtitle

        req_params: dict = {
            "speaker": self._speaker,
            "audio_params": audio_params,
        }
        if self._model:
            req_params["model"] = self._model
        if self._ssml:
            req_params["ssml"] = self._ssml

        additions: dict[str, Any] = {}
        if self._disable_markdown_filter is not None:
            additions["disable_markdown_filter"] = self._disable_markdown_filter
        if self._disable_emoji_filter is not None:
            additions["disable_emoji_filter"] = self._disable_emoji_filter
        if self._enable_latex_tn is not None:
            additions["enable_latex_tn"] = self._enable_latex_tn
        if self._latex_parser:
            additions["latex_parser"] = self._latex_parser
        if self._explicit_language:
            additions["explicit_language"] = self._explicit_language
        if self._explicit_dialect:
            additions["explicit_dialect"] = self._explicit_dialect
        if self._aigc_watermark is not None:
            additions["aigc_watermark"] = self._aigc_watermark
        if self._aigc_metadata:
            additions["aigc_metadata"] = self._aigc_metadata
        if self._cache_config:
            additions["cache_config"] = self._cache_config
        if self._post_process:
            additions["post_process"] = self._post_process
        if self._context_texts:
            additions["context_texts"] = self._context_texts
        if self._use_tag_parser is not None:
            additions["use_tag_parser"] = self._use_tag_parser
        if additions:
            req_params["additions"] = json.dumps(additions, ensure_ascii=False)

        payload: dict = {
            "user": {"uid": utils.shortuuid()},
            "event": _EVT_START_SESSION,
            "namespace": "BidirectionalTTS",
            "req_params": req_params,
        }
        return payload

    def synthesize(
        self,
        text: str,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ):
        raise NotImplementedError("VolcengineV3TTS only supports streaming synthesis")

    def stream(
        self,
        *,
        conn_options: APIConnectOptions = DEFAULT_API_CONNECT_OPTIONS,
    ) -> SynthesizeStream:
        stream = SynthesizeStream(
            tts=self,
            conn_options=conn_options,
            pool=self._pool,
        )
        self._streams.add(stream)
        return stream

    def prewarm(self) -> None:
        self._pool.prewarm()

    async def aclose(self) -> None:
        for stream in list(self._streams):
            await stream.aclose()
        self._streams.clear()
        await self._pool.aclose()


class SynthesizeStream(tts.SynthesizeStream):
    def __init__(
        self,
        *,
        tts: VolcengineV3TTS,
        conn_options: APIConnectOptions,
        pool: utils.ConnectionPool[aiohttp.ClientWebSocketResponse],
    ) -> None:
        super().__init__(tts=tts, conn_options=conn_options)
        self._v3_tts = tts
        self._pool = pool
        # Buffer the first token so it survives across _run() retries.
        self._buffered_first_token: str | None = None

    async def _run(self, emitter: tts.AudioEmitter) -> None:
        request_id = utils.shortuuid()
        session_id = utils.shortuuid()
        mime_type = _MIME_TYPES.get(self._v3_tts._format, "audio/pcm")

        emitter.initialize(
            request_id=request_id,
            sample_rate=self._v3_tts._sample_rate,
            num_channels=1,
            mime_type=mime_type,
            frame_size_ms=200,
            stream=True,
        )

        # Wait for the first real text token BEFORE acquiring a WS from the
        # pool.  If the LLM is slow to emit a token, a pre-acquired WS would
        # sit idle and (empirically) sometimes gets handed back as already
        # closed — see the `if ws.closed` check below.  Deferring the
        # acquisition keeps the pool free and cuts the idle window on whichever
        # WS we end up using.
        #
        # The first token is buffered in self._buffered_first_token so that
        # if _run() is retried (WS connect, StartSession handshake, or any
        # transient failure), the token is not lost and the retry produces
        # the complete utterance.
        if self._buffered_first_token is not None:
            first_token = self._buffered_first_token
            # LiveKit >=1.5 replays the stream's input buffer into a fresh
            # channel before retrying. Because this plugin also keeps a private
            # first-token buffer, discard the replayed copy of that first token
            # so it is not sent twice on retry.
            while self._input_ch.qsize() > 0:
                replayed = self._input_ch.recv_nowait()
                if isinstance(replayed, self._FlushSentinel):
                    continue
                if replayed != first_token:
                    logger.warning(
                        "unexpected replayed first token while retrying V3 TTS",
                        extra={"expected": first_token, "actual": replayed},
                    )
                break
        else:
            first_token = None
            wait_start = time.perf_counter()
            async for token in self._input_ch:
                if isinstance(token, self._FlushSentinel):
                    continue
                first_token = token
                break

            if first_token is None:
                return  # nothing to synthesize

            first_token_elapsed = time.perf_counter() - wait_start
            logger.info(
                "llm first token",
                extra={
                    "spent": round(first_token_elapsed, 4),
                    "session_id": session_id,
                },
            )
            self._buffered_first_token = first_token

        async with self._pool.connection(timeout=self._conn_options.timeout) as ws:
            # The pool occasionally hands out an already-closed WS (root cause
            # still under investigation — aiohttp heartbeat, server-side idle
            # policy, and unfinished prior sessions are all candidates).
            # Raise APIError so LiveKit's TTS retry loop gets a fresh WS on
            # the same turn; ConnectionPool drops this one on exception exit.
            if ws.closed:
                # Log with connect_id/logid so we can correlate with Volcengine
                # server logs and eventually pin down why the WS was closed
                # before first use.
                logger.warning(
                    "WS from pool is already closed; dropping and retrying",
                    extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
                )
                raise APIError("WS from pool is already closed")

            # --- Start session ---
            # Wrap the setup in APIError-producing try/except so any remaining
            # transport-layer failures (ws send/recv) also go through the
            # framework's retry loop.  Raw aiohttp/asyncio exceptions would
            # bypass it and kill the turn silently.
            session_payload = self._v3_tts._build_session_payload()
            start_frame = _build_client_frame(_EVT_START_SESSION, session_id, session_payload)
            try:
                await ws.send_bytes(start_frame)
            except (aiohttp.ClientError, ConnectionError) as e:
                raise APIError(f"WS send (StartSession) failed: {e}") from e

            # Wait for SessionStarted
            try:
                resp_data = await asyncio.wait_for(
                    ws.receive_bytes(), timeout=self._conn_options.timeout
                )
            except asyncio.TimeoutError as e:
                raise APIError("Timed out waiting for SessionStarted") from e
            except (aiohttp.ClientError, ConnectionError) as e:
                raise APIError(f"WS recv (SessionStarted) failed: {e}") from e
            resp = _parse_v3_frame(resp_data)
            if resp.event == _EVT_SESSION_FAILED:
                status_code, message = _parse_session_meta(resp.payload)
                raise APIStatusError(
                    f"SessionFailed (code={status_code}): {message}",
                    status_code=status_code,
                    request_id=session_id,
                    body=message,
                    retryable=_is_retryable_code(status_code),
                )
            if resp.event != _EVT_SESSION_STARTED:
                raise APIError(
                    f"Expected SessionStarted({_EVT_SESSION_STARTED}), got event={resp.event}"
                )

            logger.info(
                "V3 TTS session started",
                extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
            )
            emitter.start_segment(segment_id=utils.shortuuid())

            # Send the first token immediately — no idle gap.
            first_frame = _build_client_frame(
                _EVT_TASK_REQUEST,
                session_id,
                {"req_params": {"text": first_token}},
            )
            try:
                await ws.send_bytes(first_frame)
            except (aiohttp.ClientError, ConnectionError) as e:
                raise APIError(f"WS send (first TaskRequest) failed: {e}") from e

            # Shared state between send/recv tasks
            recv_error: APIError | None = None
            send_error: APIError | None = None
            recv_done = asyncio.Event()
            # Tracks whether we've pushed any audio to the emitter.  If we have
            # and then hit an error, we drain the emitter before raising *and*
            # mark the error non-retryable: the framework's retry loop would
            # regenerate the same audio and cause overlapping playback.
            audio_pushed = False

            async def _send_task() -> None:
                # A mid-stream send failure (WS killed by server idle-timeout,
                # network blip, etc.) must NOT cancel _recv_task via
                # asyncio.gather — recv may still have already-synthesized
                # audio in the WS buffer that we want to drain.  Catch send
                # errors, record them, and return cleanly; recv will either
                # finish normally or hit its own timeout.
                nonlocal send_error
                try:
                    async for token in self._input_ch:
                        if recv_done.is_set():
                            break
                        if isinstance(token, self._FlushSentinel):
                            continue  # V3 handles sentence splitting server-side
                        task_payload = {"req_params": {"text": token}}
                        frame = _build_client_frame(_EVT_TASK_REQUEST, session_id, task_payload)
                        try:
                            await ws.send_bytes(frame)
                        except (aiohttp.ClientError, ConnectionError) as e:
                            send_error = APIError(f"WS send failed mid-session: {e}")
                            logger.warning(
                                "ws send failed; letting recv drain",
                                extra=self._v3_tts._ws_log_extra(
                                    ws, session_id=session_id, err=str(e)
                                ),
                            )
                            return
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    # Let cancellation / interpreter shutdown propagate.
                    raise
                except Exception as e:
                    # We do NOT expect anything beyond the transport errors
                    # caught above; reaching this branch means a real bug
                    # (AttributeError, TypeError, ...).  Log a full traceback
                    # so the bug is visible, but still record as send_error
                    # so the outer task exits cleanly rather than killing
                    # `_recv_task` via asyncio.gather.
                    logger.exception(
                        "unexpected error in _send_task",
                        extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
                    )
                    send_error = APIError(f"send_task unexpected error: {e}")
                    return

                # Input ended — finish session (skip if recv already done/errored)
                if not recv_done.is_set():
                    finish_frame = _build_client_frame(_EVT_FINISH_SESSION, session_id, {})
                    try:
                        await ws.send_bytes(finish_frame)
                    except (aiohttp.ClientError, ConnectionError) as e:
                        # FinishSession couldn't be delivered; recv will time
                        # out or see the WS close.  Not fatal by itself.
                        send_error = APIError(f"WS send (FinishSession) failed: {e}")
                        logger.warning(
                            "ws send (FinishSession) failed; letting recv drain",
                            extra=self._v3_tts._ws_log_extra(ws, session_id=session_id, err=str(e)),
                        )

            async def _recv_task() -> None:
                nonlocal recv_error, audio_pushed
                is_first_audio = True
                start_time = time.perf_counter()
                resp_timeout = self._conn_options.timeout

                # The first token has already been sent before this task
                # starts, so we can apply the response timeout immediately.

                while True:
                    try:
                        data = await asyncio.wait_for(ws.receive_bytes(), timeout=resp_timeout)
                    except asyncio.TimeoutError:
                        recv_error = APIError("Timed out waiting for V3 TTS response")
                        break
                    except (ConnectionError, aiohttp.ClientError) as e:
                        recv_error = APIError(f"WS connection error: {e}")
                        break

                    frame = _parse_v3_frame(data)

                    if frame.event == _EVT_TTS_RESPONSE and frame.is_audio:
                        if is_first_audio:
                            elapsed = time.perf_counter() - start_time
                            logger.info(
                                "tts first response",
                                extra=self._v3_tts._ws_log_extra(
                                    ws,
                                    session_id=session_id,
                                    spent=round(elapsed, 4),
                                ),
                            )
                            is_first_audio = False
                        if frame.payload:
                            emitter.push(data=frame.payload)
                            audio_pushed = True

                    elif frame.event == _EVT_SESSION_FINISHED:
                        logger.info(
                            "V3 TTS session finished",
                            extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
                        )
                        break

                    elif frame.event == _EVT_SESSION_FAILED:
                        status_code, message = _parse_session_meta(frame.payload)
                        recv_error = APIStatusError(
                            f"V3 TTS session failed (code={status_code}): {message}",
                            status_code=status_code,
                            request_id=session_id,
                            body=message,
                            retryable=_is_retryable_code(status_code),
                        )
                        break

                    elif frame.event in (
                        _EVT_TTS_SENTENCE_START,
                        _EVT_TTS_SENTENCE_END,
                    ):
                        pass  # server-side sentence boundaries, no action needed

                    elif frame.msg_type == _MSG_ERROR:
                        error_msg = (
                            frame.payload.decode("utf-8", errors="replace")
                            if frame.payload
                            else "unknown"
                        )
                        recv_error = APIStatusError(
                            f"V3 protocol error (code={frame.error_code}): {error_msg}",
                            status_code=frame.error_code,
                            request_id=session_id,
                            body=error_msg,
                            retryable=_is_retryable_code(frame.error_code),
                        )
                        break

                recv_done.set()

            tasks = [
                asyncio.create_task(_send_task()),
                asyncio.create_task(_recv_task()),
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                await utils.aio.gracefully_cancel(*tasks)

            emitter.end_segment()
            logger.info(
                "tts end",
                extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
            )

            # Prefer surfacing the server-side error (recv_error) over the
            # local send failure — the server's status_code is the useful
            # signal for whether to retry.
            err = recv_error if recv_error is not None else send_error
            if err is not None:
                if audio_pushed:
                    # Drain the emitter so the audio we already collected
                    # reaches the consumer before the exception propagates.
                    # Without this, the framework's `finally: aclose()` would
                    # cancel the emitter's internal task mid-flight and the
                    # buffered frame would be discarded.
                    try:
                        emitter.end_input()
                        await emitter.join()
                    except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                        raise
                    except Exception:
                        # Drain is best-effort — we want to surface the
                        # original `err` regardless.  Log at debug so a real
                        # emitter bug is at least leaving a breadcrumb.
                        logger.debug(
                            "emitter drain failed during error path",
                            exc_info=True,
                            extra=self._v3_tts._ws_log_extra(ws, session_id=session_id),
                        )
                    # Also force non-retryable: a retry would re-synthesize
                    # the same text and the consumer would hear it twice.
                    if isinstance(err, APIStatusError):
                        err.retryable = False
                    else:
                        err = APIStatusError(
                            str(err),
                            status_code=0,
                            request_id=session_id,
                            body=str(err),
                            retryable=False,
                        )
                raise err

            # Only clear the first-token buffer after the session finished
            # cleanly.  If we cleared right after sending it (the old
            # behaviour), a mid-session failure would retry with nothing
            # buffered and the new `_run` would hang waiting on `_input_ch`
            # for a token that's already been consumed.
            self._buffered_first_token = None


TTS = VolcengineV3TTS
