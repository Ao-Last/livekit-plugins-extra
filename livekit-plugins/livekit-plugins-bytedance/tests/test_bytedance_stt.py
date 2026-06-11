"""Tests for the ByteDance/Volcengine streaming ASR client.

The mocked protocol is Volcengine's WebSocket ASR v3 binary protocol for the
optimized bidirectional streaming endpoint:

    wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async

The pasted provider document did not include a publication/update date, so the
tests pin the endpoint path and frame contract rather than a dated revision.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import struct
from typing import Any

import aiohttp
import pytest

from livekit import rtc
from livekit.agents import APIConnectOptions, APIStatusError, stt
from livekit.plugins.bytedance import STT
from livekit.plugins.bytedance.stt import (
    _FLAG_FINAL_NO_SEQUENCE,
    _FLAG_NEG_SEQUENCE,
    _FLAG_NO_SEQUENCE,
    _FLAG_POS_SEQUENCE,
    _MSG_AUDIO_ONLY_REQ,
    _MSG_ERROR,
    _MSG_FULL_CLIENT_REQ,
    _MSG_FULL_SERVER_RESP,
    DEFAULT_ASR_BASE_URL,
    DEFAULT_ASR_RESOURCE_ID,
    _build_audio_only_request,
    _build_full_client_request,
    _is_retryable_code,
    _parse_server_frame,
)


def _server_response_frame(
    payload: dict[str, Any], *, sequence: int = 1, final: bool = False
) -> bytes:
    payload_bytes = gzip.compress(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    flags = _FLAG_NEG_SEQUENCE if final else _FLAG_POS_SEQUENCE
    seq = -abs(sequence) if final else abs(sequence)
    header = bytes([0x11, (_MSG_FULL_SERVER_RESP << 4) | flags, 0x11, 0x00])
    return header + struct.pack(">i", seq) + struct.pack(">I", len(payload_bytes)) + payload_bytes


def _server_error_frame(error_code: int, message: str) -> bytes:
    payload = message.encode("utf-8")
    header = bytes([0x11, (_MSG_ERROR << 4) | _FLAG_NO_SEQUENCE, 0x10, 0x00])
    return header + struct.pack(">I", error_code) + struct.pack(">I", len(payload)) + payload


def _decode_client_json_frame(frame: bytes) -> dict[str, Any]:
    assert frame[0] == 0x11
    assert frame[1] == (_MSG_FULL_CLIENT_REQ << 4) | _FLAG_NO_SEQUENCE
    assert frame[2] == 0x11
    payload_len = struct.unpack(">I", frame[4:8])[0]
    return json.loads(gzip.decompress(frame[8 : 8 + payload_len]).decode("utf-8"))


def _decode_client_audio_frame(frame: bytes) -> tuple[int, bytes]:
    assert frame[0] == 0x11
    assert frame[1] >> 4 == _MSG_AUDIO_ONLY_REQ
    assert frame[2] == 0x01
    payload_len = struct.unpack(">I", frame[4:8])[0]
    return frame[1] & 0x0F, gzip.decompress(frame[8 : 8 + payload_len])


class _FakeHandshakeResponse:
    def __init__(self, headers: dict[str, str] | None = None) -> None:
        self.headers = headers or {}


class FakeWebSocket:
    def __init__(self, *, handshake_headers: dict[str, str] | None = None) -> None:
        self.sent: list[bytes] = []
        self._recv_q: asyncio.Queue[aiohttp.WSMessage | Exception] = asyncio.Queue()
        self._closed = False
        self._response = _FakeHandshakeResponse(handshake_headers)

    @property
    def closed(self) -> bool:
        return self._closed

    async def send_bytes(self, data: bytes) -> None:
        if self._closed:
            raise aiohttp.ClientConnectionResetError("Cannot write to closing transport")
        self.sent.append(data)

    async def receive(self) -> aiohttp.WSMessage:
        item = await self._recv_q.get()
        if isinstance(item, Exception):
            raise item
        return item

    async def close(self) -> None:
        self._closed = True

    def queue_binary(self, data: bytes) -> None:
        self._recv_q.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, data, None))

    def queue_close(self) -> None:
        self._recv_q.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, None))


class FakeClientSession:
    def __init__(self, ws: FakeWebSocket) -> None:
        self._ws = ws
        self.ws_connect_calls: list[tuple[str, dict[str, Any]]] = []

    async def ws_connect(self, url: str, **kwargs: Any) -> FakeWebSocket:
        self.ws_connect_calls.append((url, kwargs))
        return self._ws


def _make_audio_frame(*, samples: int = 1600) -> rtc.AudioFrame:
    return rtc.AudioFrame(
        data=b"\x01\x02" * samples,
        sample_rate=16000,
        num_channels=1,
        samples_per_channel=samples,
    )


def _fast_conn_options() -> APIConnectOptions:
    return APIConnectOptions(max_retry=0, retry_interval=0.05, timeout=1.0)


async def _collect_events(stream, *, timeout: float = 3.0) -> list[stt.SpeechEvent]:
    events: list[stt.SpeechEvent] = []

    async def _run() -> None:
        async for event in stream:
            events.append(event)

    await asyncio.wait_for(_run(), timeout=timeout)
    return events


class TestProtocolHelpers:
    def test_retryable_code_helper_matches_volcengine_error_bands(self) -> None:
        assert _is_retryable_code(45000001) is False
        assert _is_retryable_code(45000151) is False
        assert _is_retryable_code(55000031) is True

    def test_full_client_request_is_json_gzip_v3_frame(self) -> None:
        raw = _build_full_client_request(
            {
                "user": {"uid": "u1"},
                "audio": {"format": "pcm", "rate": 16000},
                "request": {"model_name": "bigmodel"},
            }
        )

        payload = _decode_client_json_frame(raw)
        assert payload["user"]["uid"] == "u1"
        assert payload["audio"]["format"] == "pcm"
        assert payload["request"]["model_name"] == "bigmodel"

    def test_audio_only_request_marks_final_packet(self) -> None:
        normal = _build_audio_only_request(b"pcm-data")
        flags, audio = _decode_client_audio_frame(normal)
        assert flags == _FLAG_NO_SEQUENCE
        assert audio == b"pcm-data"

        final = _build_audio_only_request(b"", final=True)
        flags, audio = _decode_client_audio_frame(final)
        assert flags == _FLAG_FINAL_NO_SEQUENCE
        assert audio == b""

    def test_parse_server_response_roundtrip(self) -> None:
        raw = _server_response_frame(
            {
                "code": 1000,
                "result": {
                    "text": "你好",
                    "utterances": [{"text": "你好", "definite": True}],
                },
            },
            sequence=3,
            final=True,
        )

        frame = _parse_server_frame(raw)
        assert frame.msg_type == _MSG_FULL_SERVER_RESP
        assert frame.flags == _FLAG_NEG_SEQUENCE
        assert frame.sequence == -3
        assert frame.is_final is True
        assert json.loads(frame.payload.decode("utf-8"))["result"]["text"] == "你好"

    def test_parse_server_error_roundtrip(self) -> None:
        raw = _server_error_frame(45000001, "invalid params")
        frame = _parse_server_frame(raw)
        assert frame.msg_type == _MSG_ERROR
        assert frame.error_code == 45000001
        assert frame.payload == b"invalid params"


def test_requires_current_or_legacy_credentials() -> None:
    with pytest.raises(ValueError, match="api_key"):
        STT()

    legacy = STT(app_key="legacy-app", access_key="legacy-access")
    assert legacy.model == DEFAULT_ASR_RESOURCE_ID


def test_recognition_payload_matches_bigmodel_async_streaming_contract() -> None:
    asr = STT(
        api_key="fake-api-key",
        resource_id="volc.seedasr.sauc.concurrent",
        enable_itn=True,
        enable_punc=True,
        enable_speaker_info=True,
        ssd_version="200",
        corpus={"context": "LiveKit"},
        user={"uid": "tester"},
        request_options={"vad_segment_duration": 800},
    )

    payload = asr._build_recognition_payload()

    assert payload["user"] == {"uid": "tester"}
    assert payload["audio"] == {
        "format": "pcm",
        "codec": "raw",
        "rate": 16000,
        "bits": 16,
        "channel": 1,
    }
    assert "language" not in payload["audio"]
    assert payload["request"] == {
        "model_name": "bigmodel",
        "enable_nonstream": True,
        "enable_itn": True,
        "enable_punc": True,
        "show_utterances": True,
        "enable_speaker_info": True,
        "ssd_version": "200",
        "vad_segment_duration": 800,
        "corpus": {"context": "LiveKit"},
    }


async def test_connect_ws_sets_current_console_headers_without_heartbeat() -> None:
    asr = STT(api_key="fake-api-key")
    ws = FakeWebSocket(handshake_headers={"X-Tt-Logid": "vc-log-123"})
    fake_session = FakeClientSession(ws)
    asr._session = fake_session

    result = await asr._connect_ws(request_id="req-123", timeout=1.0)

    assert result is ws
    assert len(fake_session.ws_connect_calls) == 1
    url, kwargs = fake_session.ws_connect_calls[0]
    assert url == DEFAULT_ASR_BASE_URL
    assert "heartbeat" not in kwargs

    headers = kwargs["headers"]
    assert headers["X-Api-Key"] == "fake-api-key"
    assert "X-Api-App-Key" not in headers
    assert "X-Api-Access-Key" not in headers
    assert headers["X-Api-Resource-Id"] == DEFAULT_ASR_RESOURCE_ID
    assert headers["X-Api-Request-Id"] == "req-123"
    assert headers["X-Api-Sequence"] == "-1"
    assert headers["X-Api-Connect-Id"]
    assert asr._ws_meta[id(ws)]["logid"] == "vc-log-123"


async def test_connect_ws_supports_legacy_console_auth_headers() -> None:
    asr = STT(app_key="legacy-app", access_key="legacy-access")
    ws = FakeWebSocket()
    fake_session = FakeClientSession(ws)
    asr._session = fake_session

    await asr._connect_ws(request_id="req-456", timeout=1.0)

    headers = fake_session.ws_connect_calls[0][1]["headers"]
    assert headers["X-Api-App-Key"] == "legacy-app"
    assert headers["X-Api-Access-Key"] == "legacy-access"
    assert "X-Api-Key" not in headers


async def test_happy_path_streaming_emits_interim_and_final_events() -> None:
    asr = STT(api_key="fake-api-key")
    ws = FakeWebSocket()
    asr._session = FakeClientSession(ws)

    stream = asr.stream(conn_options=_fast_conn_options())
    stream.push_frame(_make_audio_frame())
    stream.end_input()

    ws.queue_binary(
        _server_response_frame(
            {
                "code": 1000,
                "result": {
                    "text": "你",
                    "utterances": [
                        {"text": "你", "start_time": 0, "end_time": 120, "definite": False}
                    ],
                },
            },
            sequence=1,
        )
    )
    ws.queue_binary(
        _server_response_frame(
            {
                "code": 1000,
                "result": {
                    "text": "你好。",
                    "utterances": [
                        {
                            "text": "你好。",
                            "start_time": 0,
                            "end_time": 600,
                            "definite": True,
                            "words": [
                                {"text": "你", "start_time": 0, "end_time": 240},
                                {"text": "好", "start_time": 240, "end_time": 480},
                            ],
                        }
                    ],
                },
                "audio_info": {"duration": 600},
            },
            sequence=2,
            final=True,
        )
    )

    events = await _collect_events(stream)

    event_types = [event.type for event in events]
    assert event_types == [
        stt.SpeechEventType.START_OF_SPEECH,
        stt.SpeechEventType.INTERIM_TRANSCRIPT,
        stt.SpeechEventType.FINAL_TRANSCRIPT,
        stt.SpeechEventType.END_OF_SPEECH,
    ]
    assert events[1].alternatives[0].text == "你"
    final_alt = events[2].alternatives[0]
    assert final_alt.text == "你好。"
    assert final_alt.start_time == pytest.approx(stream.start_time_offset)
    assert final_alt.end_time == pytest.approx(stream.start_time_offset + 0.6)
    assert final_alt.words is not None
    assert [str(word) for word in final_alt.words] == ["你", "好"]

    # Full client request, a flushed audio packet, and the final empty packet.
    assert len(ws.sent) == 3
    payload = _decode_client_json_frame(ws.sent[0])
    assert payload["request"]["model_name"] == "bigmodel"
    audio_flags, audio = _decode_client_audio_frame(ws.sent[1])
    assert audio_flags == _FLAG_NO_SEQUENCE
    assert audio == _make_audio_frame().data.tobytes()
    final_flags, final_audio = _decode_client_audio_frame(ws.sent[2])
    assert final_flags == _FLAG_FINAL_NO_SEQUENCE
    assert final_audio == b""


async def test_streaming_server_error_frame_raises_api_status_error() -> None:
    asr = STT(api_key="fake-api-key")
    ws = FakeWebSocket()
    asr._session = FakeClientSession(ws)

    stream = asr.stream(conn_options=_fast_conn_options())
    stream.push_frame(_make_audio_frame())
    stream.end_input()
    ws.queue_binary(_server_error_frame(45000001, "invalid params"))

    with pytest.raises(APIStatusError) as exc_info:
        await _collect_events(stream)

    assert exc_info.value.status_code == 45000001
    assert exc_info.value.retryable is False
