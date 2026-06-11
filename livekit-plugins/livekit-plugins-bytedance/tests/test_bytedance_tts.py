"""Tests for the ByteDance/Volcengine V3 TTS client.

Philosophy: no network. A FakeWebSocket scripts server responses; a FakePool
hands those scripted sockets into `SynthesizeStream`. This lets us simulate
the high-frequency failure modes (slow LLM, zombie WS, mid-stream server
close, 45xxxxxx vs 55xxxxxx codes) deterministically.
"""

from __future__ import annotations

import asyncio
import json
import struct
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import Any

import aiohttp
import pytest

from livekit.agents import APIConnectOptions, APIStatusError
from livekit.plugins.bytedance import TTS, VolcengineV3TTS
from livekit.plugins.bytedance.tts import (
    _EVT_AUDIO_MUTED,
    _EVT_CONNECTION_STARTED,
    _EVT_SESSION_FAILED,
    _EVT_SESSION_FINISHED,
    _EVT_SESSION_STARTED,
    _EVT_START_CONNECTION,
    _EVT_START_SESSION,
    _EVT_TASK_REQUEST,
    _EVT_TTS_ENDED,
    _EVT_TTS_RESPONSE,
    _EVT_TTS_SUBTITLE,
    _EVT_USAGE_RESPONSE,
    _is_retryable_code,
    _parse_session_meta,
    _parse_v3_frame,
)

# ---------------------------------------------------------------------------
# Frame builders for server → client direction
# ---------------------------------------------------------------------------


def _server_event_frame(event: int, id_: str, payload: dict[str, Any]) -> bytes:
    """Full-server response (msg_type=0x9) with event + id + JSON payload."""
    # Byte 1 = 1001 0100 = 0x94 (full-server response, flags=with-event)
    header = bytes([0x11, 0x94, 0x10, 0x00])
    event_bytes = struct.pack(">i", event)
    id_bytes = id_.encode("utf-8")
    id_len = struct.pack(">I", len(id_bytes))
    payload_bytes = json.dumps(payload).encode("utf-8")
    payload_len = struct.pack(">I", len(payload_bytes))
    return header + event_bytes + id_len + id_bytes + payload_len + payload_bytes


def _connection_started_frame(connect_id: str = "fake-conn") -> bytes:
    return _server_event_frame(_EVT_CONNECTION_STARTED, connect_id, {})


def _session_started_frame(session_id: str) -> bytes:
    return _server_event_frame(_EVT_SESSION_STARTED, session_id, {})


def _session_finished_frame(
    session_id: str, *, status_code: int = 20000000, message: str = "ok"
) -> bytes:
    return _server_event_frame(
        _EVT_SESSION_FINISHED,
        session_id,
        {"status_code": status_code, "message": message},
    )


def _session_failed_frame(session_id: str, *, status_code: int, message: str) -> bytes:
    return _server_event_frame(
        _EVT_SESSION_FAILED,
        session_id,
        {"status_code": status_code, "message": message},
    )


def _audio_response_frame(session_id: str, audio: bytes) -> bytes:
    """Audio-only server response (msg_type=0xB, flags=with-event)."""
    # Byte 1 = 1011 0100 = 0xB4
    # Byte 2 = 0000 0000 = 0x00 (raw serialization, no compression)
    header = bytes([0x11, 0xB4, 0x00, 0x00])
    event_bytes = struct.pack(">i", _EVT_TTS_RESPONSE)
    sid_bytes = session_id.encode("utf-8")
    sid_len = struct.pack(">I", len(sid_bytes))
    audio_len = struct.pack(">I", len(audio))
    return header + event_bytes + sid_len + sid_bytes + audio_len + audio


def _error_frame(error_code: int, message: str = "") -> bytes:
    """Server error frame (msg_type=0xF)."""
    header = bytes([0x11, 0xF0, 0x10, 0x00])
    code_bytes = struct.pack(">I", error_code)
    msg_bytes = message.encode("utf-8")
    msg_len = struct.pack(">I", len(msg_bytes))
    return header + code_bytes + msg_len + msg_bytes


# ---------------------------------------------------------------------------
# Fake WebSocket
# ---------------------------------------------------------------------------


class _FakeHandshakeResponse:
    """Mimics aiohttp's ClientResponse so `ws._response.headers` lookups work."""

    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}


class FakeWebSocket:
    """Scripts server frames on receive_bytes(); records client sends."""

    def __init__(self, *, handshake_headers: dict[str, str] | None = None) -> None:
        self.sent: list[bytes] = []
        self._recv_q: asyncio.Queue[bytes | Exception] = asyncio.Queue()
        self._closed = False
        self.send_raises: Exception | None = None  # raise on next send_bytes
        # `_response` is an aiohttp private attribute on ClientWebSocketResponse
        # that exposes the HTTP 101 Upgrade response headers.  The plugin reads
        # `X-Tt-Logid` from there; the test harness mimics the shape.
        self._response = _FakeHandshakeResponse(handshake_headers)

    @property
    def closed(self) -> bool:
        return self._closed

    async def send_bytes(self, data: bytes) -> None:
        if self.send_raises is not None:
            exc, self.send_raises = self.send_raises, None
            raise exc
        if self._closed:
            raise aiohttp.ClientConnectionResetError("Cannot write to closing transport")
        self.sent.append(data)

    async def receive_bytes(self) -> bytes:
        item = await self._recv_q.get()
        if isinstance(item, Exception):
            raise item
        return item

    async def close(self) -> None:
        self._closed = True

    # -- test helpers --

    def queue(self, frame: bytes) -> None:
        self._recv_q.put_nowait(frame)

    def queue_exc(self, exc: Exception) -> None:
        self._recv_q.put_nowait(exc)

    def simulate_server_close(self) -> None:
        """Server closed the WS mid-stream."""
        self._closed = True
        self._recv_q.put_nowait(aiohttp.ClientConnectionResetError("server closed"))


# ---------------------------------------------------------------------------
# Fake pool — minimal stand-in for utils.ConnectionPool
# ---------------------------------------------------------------------------


class FakePool:
    def __init__(self, websockets: Sequence[FakeWebSocket]) -> None:
        self._websockets = list(websockets)
        self.acquired: list[FakeWebSocket] = []

    @asynccontextmanager
    async def connection(self, *, timeout: float):
        if not self._websockets:
            raise RuntimeError("fake pool exhausted")
        ws = self._websockets.pop(0)
        self.acquired.append(ws)
        yield ws


# ---------------------------------------------------------------------------
# Fake aiohttp ClientSession (for _connect_ws unit test)
# ---------------------------------------------------------------------------


class FakeClientSession:
    def __init__(self, ws: FakeWebSocket) -> None:
        self._ws = ws
        self.ws_connect_calls: list[tuple[str, dict[str, Any]]] = []

    async def ws_connect(self, url: str, **kwargs: Any) -> FakeWebSocket:
        self.ws_connect_calls.append((url, kwargs))
        return self._ws


# ---------------------------------------------------------------------------
# Audio helpers
# ---------------------------------------------------------------------------

# At 24kHz, 1 channel, int16 PCM, the AudioEmitter chunks at frame_size_ms=200.
# That is 24000 * 0.2 * 2 = 9600 bytes per frame; anything smaller is dropped
# on flush as an "incomplete frame".  Tests must therefore queue audio chunks
# of at least this size to observe output through the stream.
_PCM_BYTES_PER_FRAME = 24000 * 2 // 5  # 9600

_AUDIO_A = bytes([0x11, 0x22]) * (_PCM_BYTES_PER_FRAME // 2)  # 1 frame
_AUDIO_B = bytes([0x33, 0x44]) * (_PCM_BYTES_PER_FRAME // 2)  # 1 frame


# ---------------------------------------------------------------------------
# Common fixtures
# ---------------------------------------------------------------------------


def _make_tts(*, context_texts: list[str] | None = None) -> VolcengineV3TTS:
    return TTS(
        api_key="fake-api-key",
        resource_id="seed-tts-2.0",
        context_texts=context_texts,
    )


def _fast_conn_options(*, max_retry: int = 0, timeout: float = 2.0) -> APIConnectOptions:
    return APIConnectOptions(max_retry=max_retry, retry_interval=0.05, timeout=timeout)


async def _collect_audio(stream, *, timeout: float = 3.0) -> list[bytes]:
    """Iterate the stream and collect raw PCM bytes until it completes."""
    audio: list[bytes] = []

    async def _run() -> None:
        async for ev in stream:
            audio.append(bytes(ev.frame.data))

    await asyncio.wait_for(_run(), timeout=timeout)
    return audio


def _parse_session_id_from_start_session(frame: bytes) -> str:
    """Extract session_id from a StartSession client frame we captured."""
    # header(4) + event(4) + sid_len(4) + sid + payload_len(4) + payload
    assert struct.unpack(">i", frame[4:8])[0] == _EVT_START_SESSION
    sid_len = struct.unpack(">I", frame[8:12])[0]
    return frame[12 : 12 + sid_len].decode("utf-8")


def _extract_task_request_text(frame: bytes) -> str:
    """Extract the text from a TaskRequest client frame we captured."""
    assert struct.unpack(">i", frame[4:8])[0] == _EVT_TASK_REQUEST
    sid_len = struct.unpack(">I", frame[8:12])[0]
    payload_start = 12 + sid_len + 4  # skip sid + payload_len
    payload = frame[payload_start:].decode("utf-8")
    return json.loads(payload)["req_params"]["text"]


def _extract_start_session_payload(frame: bytes) -> dict[str, Any]:
    """Extract the JSON payload from a StartSession client frame."""
    assert struct.unpack(">i", frame[4:8])[0] == _EVT_START_SESSION
    sid_len = struct.unpack(">I", frame[8:12])[0]
    payload_len_start = 12 + sid_len
    payload_start = payload_len_start + 4
    payload_len = struct.unpack(">I", frame[payload_len_start:payload_start])[0]
    payload = frame[payload_start : payload_start + payload_len].decode("utf-8")
    return json.loads(payload)


# ---------------------------------------------------------------------------
# Pure unit tests — helpers
# ---------------------------------------------------------------------------


class TestProtocolHelpers:
    def test_tts_alias_exports_volcengine_v3_tts(self) -> None:
        assert TTS is VolcengineV3TTS

    def test_reference_protocol_event_codes(self) -> None:
        # Matches ByteDance's "TTS Websocket Bidirection protocols" helper.
        assert _EVT_USAGE_RESPONSE == 154
        assert _EVT_AUDIO_MUTED == 250
        assert _EVT_TTS_RESPONSE == 352
        assert _EVT_TTS_ENDED == 359
        assert _EVT_TTS_SUBTITLE == 364

    def test_is_retryable_code_client_error_not_retryable(self) -> None:
        assert _is_retryable_code(45000000) is False
        assert _is_retryable_code(45000001) is False
        assert _is_retryable_code(49999999) is False

    def test_is_retryable_code_server_error_retryable(self) -> None:
        assert _is_retryable_code(55000000) is True
        assert _is_retryable_code(55000001) is True

    def test_parse_session_meta_well_formed(self) -> None:
        payload = json.dumps({"status_code": 55000001, "message": "oops"}).encode("utf-8")
        code, message = _parse_session_meta(payload)
        assert code == 55000001
        assert message == "oops"

    def test_parse_session_meta_handles_invalid_json(self) -> None:
        code, message = _parse_session_meta(b"not-json")
        assert code == 0
        assert message == "not-json"

    def test_parse_session_meta_handles_empty(self) -> None:
        code, message = _parse_session_meta(b"")
        assert code == 0
        assert message == "unknown"

    def test_parse_v3_frame_error_code_roundtrip(self) -> None:
        raw = _error_frame(45000042, "bad request")
        frame = _parse_v3_frame(raw)
        assert frame.error_code == 45000042
        assert frame.payload == b"bad request"

    def test_parse_v3_frame_audio_roundtrip(self) -> None:
        audio = b"\x01\x02\x03PCM"
        raw = _audio_response_frame("sess-1", audio)
        frame = _parse_v3_frame(raw)
        assert frame.is_audio is True
        assert frame.event == _EVT_TTS_RESPONSE
        assert frame.session_id == "sess-1"
        assert frame.payload == audio

    def test_requires_current_or_legacy_credentials(self) -> None:
        with pytest.raises(ValueError, match="api_key"):
            TTS()

        # Legacy console auth is still accepted as documented by Volcengine.
        assert TTS(app_key="legacy-app", access_key="legacy-access").model == "seed-tts-2.0"


# ---------------------------------------------------------------------------
# _connect_ws: headers + heartbeat
# ---------------------------------------------------------------------------


async def test_connect_ws_sets_headers_without_heartbeat() -> None:
    tts = _make_tts()
    ws = FakeWebSocket(handshake_headers={"X-Tt-Logid": "vc-log-abc"})
    ws.queue(_connection_started_frame())
    fake_session = FakeClientSession(ws)
    tts._session = fake_session  # bypass utils.http_context

    result = await tts._connect_ws(timeout=1.0)

    assert result is ws
    assert len(fake_session.ws_connect_calls) == 1
    url, kwargs = fake_session.ws_connect_calls[0]
    assert url == "wss://openspeech.bytedance.com/api/v3/tts/bidirection"
    # heartbeat MUST NOT be set — see aio-libs/aiohttp#7508 and issue #124:
    # aiohttp's heartbeat self-kills the WS while it's idle in the pool
    # because nobody is actively calling ws.receive() to drain PONGs.
    assert "heartbeat" not in kwargs
    headers = kwargs["headers"]
    assert headers["X-Api-Key"] == "fake-api-key"
    assert "X-Api-App-Key" not in headers
    assert "X-Api-Access-Key" not in headers
    assert headers["X-Api-Resource-Id"] == "seed-tts-2.0"
    # Connect-Id should be populated (non-empty) and be unique per call.
    connect_id = headers["X-Api-Connect-Id"]
    assert connect_id

    # StartConnection should have been sent before returning.
    assert len(ws.sent) == 1
    assert struct.unpack(">i", ws.sent[0][4:8])[0] == _EVT_START_CONNECTION

    # Logid from the handshake response should now be retrievable for logging.
    extra = tts._ws_log_extra(ws)
    assert extra["connect_id"] == connect_id
    assert extra["logid"] == "vc-log-abc"


async def test_connect_ws_supports_legacy_console_auth_headers() -> None:
    tts = TTS(
        app_key="fake-app",
        access_key="fake-access",
        resource_id="seed-icl-2.0",
        require_usage_tokens=True,
    )
    ws = FakeWebSocket()
    ws.queue(_connection_started_frame())
    fake_session = FakeClientSession(ws)
    tts._session = fake_session

    await tts._connect_ws(timeout=1.0)

    headers = fake_session.ws_connect_calls[0][1]["headers"]
    assert headers["X-Api-App-Key"] == "fake-app"
    assert headers["X-Api-Access-Key"] == "fake-access"
    assert "X-Api-Key" not in headers
    assert headers["X-Api-Resource-Id"] == "seed-icl-2.0"
    assert headers["X-Control-Require-Usage-Tokens-Return"] == "*"


def test_start_session_payload_matches_bidirectional_tts_contract() -> None:
    tts = TTS(
        api_key="fake-api-key",
        resource_id="seed-icl-2.0",
        model="seed-tts-2.0-expressive",
        speaker="S_custom_cloned_voice",
        ssml="<speak>hello</speak>",
        audio_format="ogg_opus",
        sample_rate=48000,
        bit_rate=160000,
        speech_rate=20,
        loudness_rate=-10,
        enable_subtitle=True,
        disable_markdown_filter=True,
        disable_emoji_filter=True,
        enable_latex_tn=True,
        latex_parser="v2",
        explicit_language="zh-cn",
        explicit_dialect="zh_yueyu",
        aigc_watermark=True,
        aigc_metadata={"enable": True, "content_producer": "livekit"},
        cache_config={"text_type": 0, "use_cache": True},
        post_process={"pitch": 2},
        context_texts=["你可以用痛心的语气说话吗?"],
        use_tag_parser=True,
    )

    payload = tts._build_session_payload()

    assert payload["namespace"] == "BidirectionalTTS"
    req_params = payload["req_params"]
    assert req_params["model"] == "seed-tts-2.0-expressive"
    assert req_params["speaker"] == "S_custom_cloned_voice"
    assert req_params["ssml"] == "<speak>hello</speak>"
    assert req_params["audio_params"] == {
        "format": "ogg_opus",
        "sample_rate": 48000,
        "bit_rate": 160000,
        "speech_rate": 20,
        "loudness_rate": -10,
        "enable_subtitle": True,
    }
    assert json.loads(req_params["additions"]) == {
        "disable_markdown_filter": True,
        "disable_emoji_filter": True,
        "enable_latex_tn": True,
        "latex_parser": "v2",
        "explicit_language": "zh-cn",
        "explicit_dialect": "zh_yueyu",
        "aigc_watermark": True,
        "aigc_metadata": {"enable": True, "content_producer": "livekit"},
        "cache_config": {"text_type": 0, "use_cache": True},
        "post_process": {"pitch": 2},
        "context_texts": ["你可以用痛心的语气说话吗?"],
        "use_tag_parser": True,
    }


async def test_connect_ws_handles_missing_logid_header() -> None:
    """Volcengine might not always echo X-Tt-Logid; we should not crash."""
    tts = _make_tts()
    ws = FakeWebSocket(handshake_headers={})  # no X-Tt-Logid
    ws.queue(_connection_started_frame())
    tts._session = FakeClientSession(ws)

    await tts._connect_ws(timeout=1.0)

    extra = tts._ws_log_extra(ws)
    assert extra["logid"] == ""
    assert extra["connect_id"]  # still populated


async def test_close_ws_drops_metadata() -> None:
    """Metadata for an evicted WS must not leak (would grow unbounded)."""
    tts = _make_tts()
    ws = FakeWebSocket(handshake_headers={"X-Tt-Logid": "vc-log-xyz"})
    ws.queue(_connection_started_frame())
    tts._session = FakeClientSession(ws)
    await tts._connect_ws(timeout=1.0)
    assert id(ws) in tts._ws_meta

    # _close_ws would normally round-trip a FinishConnection; we short-circuit
    # by queueing a dummy response it can recv during its best-effort wait.
    ws.queue(b"")  # any bytes; the close path suppresses exceptions
    await tts._close_ws(ws)
    assert id(ws) not in tts._ws_meta


async def test_connect_ws_generates_unique_connect_ids() -> None:
    tts = _make_tts()
    seen: list[str] = []
    for _ in range(3):
        ws = FakeWebSocket()
        ws.queue(_connection_started_frame())
        fake_session = FakeClientSession(ws)
        tts._session = fake_session
        await tts._connect_ws(timeout=1.0)
        seen.append(fake_session.ws_connect_calls[0][1]["headers"]["X-Api-Connect-Id"])
    assert len(set(seen)) == 3


# ---------------------------------------------------------------------------
# Happy-path streaming
# ---------------------------------------------------------------------------


async def test_happy_path_streams_audio_and_finishes_cleanly() -> None:
    tts = _make_tts()
    ws = FakeWebSocket()
    tts._pool = FakePool([ws])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options())
    stream.push_text("你好")
    stream.push_text("世界")
    stream.end_input()

    # Scripted server flow: session starts, audio comes back, session finishes.
    # We queue the session_id-dependent responses after we see StartSession.
    async def drive() -> None:
        # Wait until StartSession is sent so we know the session_id.
        while len(ws.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws.sent[0])
        ws.queue(_session_started_frame(session_id))
        # Respond with audio for each TaskRequest and then the final marker.
        ws.queue(_audio_response_frame(session_id, _AUDIO_A))
        ws.queue(_audio_response_frame(session_id, _AUDIO_B))
        ws.queue(_session_finished_frame(session_id))

    drive_task = asyncio.create_task(drive())
    try:
        audio = await _collect_audio(stream)
    finally:
        await drive_task

    joined = b"".join(audio)
    assert _AUDIO_A in joined, "first audio chunk missing from stream output"
    assert _AUDIO_B in joined, "second audio chunk missing from stream output"

    # Verify the client frames: StartSession, TaskRequest("你好"),
    # TaskRequest("世界"), FinishSession.
    texts = [
        _extract_task_request_text(f)
        for f in ws.sent
        if struct.unpack(">i", f[4:8])[0] == _EVT_TASK_REQUEST
    ]
    assert texts == ["你好", "世界"]
    # The first-token buffer should have been cleared on clean completion.
    # Access through the underlying SynthesizeStream instance.
    # (stream is that instance itself.)
    assert stream._buffered_first_token is None  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# LLM slow / WS zombie scenarios
# ---------------------------------------------------------------------------


async def test_first_token_awaits_input_before_acquiring_ws() -> None:
    """Regression: the WS must NOT be acquired until we actually have text.

    Protects the fix that addresses the original bug where a slow LLM caused a
    pre-acquired WS to be closed by the server before we could send anything.
    """
    tts = _make_tts()
    ws = FakeWebSocket()
    pool = FakePool([ws])
    tts._pool = pool  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options())
    # Don't push anything for a while.
    await asyncio.sleep(0.1)
    assert len(pool.acquired) == 0, (
        "WS was acquired from the pool before the first token arrived; "
        "this is the original LLM-slow / zombie-WS bug."
    )

    # Now push a token; WS acquisition should happen, then fail because we
    # never script any server responses — we just care that acquisition did
    # happen, so we close the stream.
    stream.push_text("hi")
    stream.end_input()
    # Give the stream a moment to acquire.
    for _ in range(20):
        if pool.acquired:
            break
        await asyncio.sleep(0.02)
    assert len(pool.acquired) == 1

    await stream.aclose()


async def test_retry_reuses_buffered_first_token_after_ws_zombie() -> None:
    """If the first WS dies on StartSession send, retry must reuse the token.

    This is the "LLM took 30s to produce first_token; pooled WS had already
    been closed by server" case we hit in production.
    """
    tts = _make_tts()

    ws1 = FakeWebSocket()
    # WS1 is dead before we even try to use it — next send_bytes will reset.
    ws1.send_raises = aiohttp.ClientConnectionResetError("Cannot write to closing transport")

    ws2 = FakeWebSocket()
    tts._pool = FakePool([ws1, ws2])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=1))
    stream.push_text("只有一个token")
    stream.end_input()

    async def drive_ws2() -> None:
        # Wait for WS2's StartSession
        while len(ws2.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws2.sent[0])
        ws2.queue(_session_started_frame(session_id))
        ws2.queue(_audio_response_frame(session_id, _AUDIO_A))
        ws2.queue(_session_finished_frame(session_id))

    drive_task = asyncio.create_task(drive_ws2())
    try:
        audio = await _collect_audio(stream)
    finally:
        await drive_task

    assert _AUDIO_A in b"".join(audio), (
        "retry attempt produced no audio — buffered first-token may have been "
        "lost across the failed attempt"
    )
    # The TaskRequest on ws2 must contain the original token — proving
    # _buffered_first_token survived across the failed attempt.
    texts = [
        _extract_task_request_text(f)
        for f in ws2.sent
        if struct.unpack(">i", f[4:8])[0] == _EVT_TASK_REQUEST
    ]
    assert texts == ["只有一个token"]


async def test_zombie_ws_closed_before_use_triggers_retry() -> None:
    """If the pool hands out an already-closed WS, retry must use a fresh one.

    Guards the `if ws.closed: raise APIError(...)` path: AssertionError would
    escape LiveKit's retry family and kill the turn silently, leaving the user
    with no audio response.
    """
    tts = _make_tts()

    ws1 = FakeWebSocket()
    ws1._closed = True  # pool hands out an already-closed WS

    ws2 = FakeWebSocket()
    tts._pool = FakePool([ws1, ws2])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=1))
    stream.push_text("zombie ws test")
    stream.end_input()

    async def drive_ws2() -> None:
        while len(ws2.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws2.sent[0])
        ws2.queue(_session_started_frame(session_id))
        ws2.queue(_audio_response_frame(session_id, _AUDIO_A))
        ws2.queue(_session_finished_frame(session_id))

    drive_task = asyncio.create_task(drive_ws2())
    try:
        audio = await _collect_audio(stream)
    finally:
        await drive_task

    assert _AUDIO_A in b"".join(audio), "retry attempt produced no audio"
    # Zombie WS must never have been written to — the check runs before any send.
    assert len(ws1.sent) == 0
    # The retry on ws2 must carry the buffered first token.
    texts = [
        _extract_task_request_text(f)
        for f in ws2.sent
        if struct.unpack(">i", f[4:8])[0] == _EVT_TASK_REQUEST
    ]
    assert texts == ["zombie ws test"]


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


async def test_session_failed_client_error_is_non_retryable() -> None:
    tts = _make_tts()
    ws = FakeWebSocket()
    tts._pool = FakePool([ws])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=0))
    stream.push_text("hello")
    stream.end_input()

    async def drive() -> None:
        while len(ws.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws.sent[0])
        ws.queue(_session_failed_frame(session_id, status_code=45000001, message="bad params"))

    drive_task = asyncio.create_task(drive())
    try:
        with pytest.raises(APIStatusError) as exc_info:
            await _collect_audio(stream)
    finally:
        await drive_task

    assert exc_info.value.status_code == 45000001
    assert exc_info.value.retryable is False


async def test_session_failed_server_error_is_retryable() -> None:
    tts = _make_tts()
    ws = FakeWebSocket()
    tts._pool = FakePool([ws])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=0))
    stream.push_text("hello")
    stream.end_input()

    async def drive() -> None:
        while len(ws.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws.sent[0])
        ws.queue(_session_failed_frame(session_id, status_code=55000001, message="server busy"))

    drive_task = asyncio.create_task(drive())
    try:
        with pytest.raises(APIStatusError) as exc_info:
            await _collect_audio(stream)
    finally:
        await drive_task

    assert exc_info.value.status_code == 55000001
    assert exc_info.value.retryable is True


async def test_error_frame_status_code_surfaced() -> None:
    """A mid-stream Error frame (msg_type=0xF) should surface its error_code."""
    tts = _make_tts()
    ws = FakeWebSocket()
    tts._pool = FakePool([ws])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=0))
    stream.push_text("hello")
    stream.end_input()

    async def drive() -> None:
        while len(ws.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws.sent[0])
        ws.queue(_session_started_frame(session_id))
        ws.queue(_error_frame(45000042, "fatal"))

    drive_task = asyncio.create_task(drive())
    try:
        with pytest.raises(APIStatusError) as exc_info:
            await _collect_audio(stream)
    finally:
        await drive_task

    assert exc_info.value.status_code == 45000042
    assert exc_info.value.retryable is False


# ---------------------------------------------------------------------------
# Fault-tolerant send: recv drains after send fails
# ---------------------------------------------------------------------------


async def test_send_failure_does_not_cancel_recv_drain() -> None:
    """When mid-stream send fails, already-synthesized audio must still arrive.

    This protects the fix: `_send_task` must catch its own errors so
    `asyncio.gather` doesn't kill `_recv_task` while audio is still in the
    WS receive buffer.
    """
    tts = _make_tts()
    ws = FakeWebSocket()
    tts._pool = FakePool([ws])  # type: ignore[assignment]

    stream = tts.stream(conn_options=_fast_conn_options(max_retry=0))

    # Push initial token; we'll push a second one later to trigger send failure.
    stream.push_text("token-1")

    async def drive() -> None:
        # Wait for StartSession
        while len(ws.sent) < 1:
            await asyncio.sleep(0.01)
        session_id = _parse_session_id_from_start_session(ws.sent[0])
        ws.queue(_session_started_frame(session_id))
        # Server produces audio for token-1
        ws.queue(_audio_response_frame(session_id, _AUDIO_A))
        # Wait until we see the first TaskRequest sent
        while not any(struct.unpack(">i", f[4:8])[0] == _EVT_TASK_REQUEST for f in ws.sent):
            await asyncio.sleep(0.01)
        # Rig the next send to fail, then push a token to trigger it.
        ws.send_raises = aiohttp.ClientConnectionResetError("Cannot write to closing transport")
        stream.push_text("token-2-will-fail-to-send")
        stream.end_input()
        # Let recv drain: end with SessionFinished so recv exits naturally.
        ws.queue(_session_finished_frame(session_id))

    drive_task = asyncio.create_task(drive())
    try:
        # We expect the stream to raise — but the already-received audio
        # must have been delivered before the error.
        audio: list[bytes] = []
        with pytest.raises(Exception):  # noqa: B017 - error wrapping varies
            async for ev in stream:
                audio.append(bytes(ev.frame.data))
    finally:
        await drive_task

    joined = b"".join(audio)
    assert _AUDIO_A in joined, (
        "Audio synthesized before the send failure must still reach the "
        "consumer; otherwise the send_bytes exception is cancelling recv."
    )
