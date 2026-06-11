from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import aiohttp
import pytest
from aiohttp import web

from livekit.agents import APIStatusError
from livekit.agents.types import APIConnectOptions
from livekit.plugins.boson import tts as boson_tts

# Mock contract: Boson AI Higgs Audio TTS public OpenAPI contract observed from
# https://docs.boson.ai/openapi.json on 2026-06-10.
BOSON_OPENAPI_CONTRACT_DATE = "2026-06-10"
BOSON_SPEECH_PATH = "/v1/audio/speech"
BOSON_TTS_MODEL = "higgs-audio-v3-tts"
SAMPLE_RATE = 24000


@dataclass
class _SpeechRequest:
    path: str
    headers: dict[str, str]
    body: dict[str, Any]


@dataclass
class _MockBosonServer:
    base_url: str
    requests: list[_SpeechRequest]


def _pcm_bytes(samples_per_channel: int) -> bytes:
    return b"\x01\x00" * samples_per_channel


def _validate_speech_payload(body: dict[str, Any]) -> str | None:
    allowed_keys = {
        "input",
        "model",
        "voice",
        "response_format",
        "stream",
        "ref_audio",
        "ref_text",
    }
    unexpected = set(body) - allowed_keys
    if unexpected:
        return f"unexpected fields: {sorted(unexpected)}"

    if not isinstance(body.get("input"), str) or not body["input"].strip():
        return "input must be a non-empty string"
    if body.get("model") != BOSON_TTS_MODEL:
        return f"model must be {BOSON_TTS_MODEL}"
    if body.get("response_format") != "pcm":
        return "response_format must be pcm"
    if not isinstance(body.get("stream"), bool):
        return "stream must be a boolean"

    if ("ref_audio" in body or "ref_text" in body) and "voice" in body:
        return "voice must be omitted when reference audio/text is provided"

    return None


@pytest.fixture
async def mock_boson_server() -> AsyncIterator[_MockBosonServer]:
    requests: list[_SpeechRequest] = []

    async def handle_speech(request: web.Request) -> web.StreamResponse:
        if request.headers.get("Authorization") != "Bearer test-key":
            return web.json_response(
                {"error": {"message": "invalid Boson API key"}},
                status=401,
                headers={"x-request-id": "req-auth"},
            )

        body = await request.json()
        if not isinstance(body, dict):
            return web.json_response(
                {"error": {"message": "JSON body must be an object"}},
                status=400,
                headers={"x-request-id": "req-invalid-json"},
            )

        requests.append(
            _SpeechRequest(
                path=request.path,
                headers=dict(request.headers),
                body=body,
            )
        )

        validation_error = _validate_speech_payload(body)
        if validation_error is not None:
            return web.json_response(
                {"error": {"message": validation_error}},
                status=400,
                headers={"x-request-id": "req-invalid-payload"},
            )

        if body["input"] == "trigger provider error":
            return web.json_response(
                {"error": {"message": "Boson mock rejected input"}},
                status=400,
                headers={"x-request-id": "req-error"},
            )

        if body["stream"] is False:
            return web.Response(
                body=_pcm_bytes(SAMPLE_RATE // 5),
                headers={"x-request-id": "req-chunked"},
                content_type="application/octet-stream",
            )

        response = web.StreamResponse(
            status=200,
            headers={"x-request-id": "req-stream"},
        )
        await response.prepare(request)
        await response.write(_pcm_bytes(SAMPLE_RATE // 10))
        await response.write(_pcm_bytes(SAMPLE_RATE // 10))
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_post(BOSON_SPEECH_PATH, handle_speech)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()

    server = site._server
    assert server is not None
    sockets = server.sockets
    assert sockets is not None
    host, port = sockets[0].getsockname()[:2]

    try:
        yield _MockBosonServer(base_url=f"http://{host}:{port}", requests=requests)
    finally:
        await runner.cleanup()


def _assert_nonempty_audio(events: list[Any]) -> None:
    assert events
    assert events[-1].is_final
    assert sum(event.frame.duration for event in events) > 0
    assert {event.frame.sample_rate for event in events} == {SAMPLE_RATE}
    assert {event.frame.num_channels for event in events} == {1}


@pytest.mark.integration
async def test_synthesize_uses_boson_speech_api_contract(
    mock_boson_server: _MockBosonServer,
) -> None:
    assert BOSON_OPENAPI_CONTRACT_DATE == "2026-06-10"

    async with aiohttp.ClientSession() as http_session:
        client = boson_tts.TTS(
            api_key="test-key",
            base_url=mock_boson_server.base_url,
            http_session=http_session,
        )
        try:
            async with client.synthesize(
                "hello from livekit",
                conn_options=APIConnectOptions(max_retry=0, timeout=2),
            ) as stream:
                events = [event async for event in stream]
        finally:
            await client.aclose()

    _assert_nonempty_audio(events)
    assert {event.request_id for event in events} == {"req-chunked"}

    assert len(mock_boson_server.requests) == 1
    speech_request = mock_boson_server.requests[0]
    assert speech_request.path == BOSON_SPEECH_PATH
    assert speech_request.headers["Authorization"] == "Bearer test-key"
    assert speech_request.headers["X-LiveKit-Plugin"] == "livekit-plugins-boson"
    assert speech_request.body == {
        "input": "hello from livekit",
        "model": BOSON_TTS_MODEL,
        "response_format": "pcm",
        "stream": False,
        "voice": "default",
    }


@pytest.mark.integration
async def test_stream_uses_boson_pcm_streaming_contract(
    mock_boson_server: _MockBosonServer,
) -> None:
    async with aiohttp.ClientSession() as http_session:
        client = boson_tts.TTS(
            api_key="test-key",
            base_url=mock_boson_server.base_url,
            http_session=http_session,
        )
        try:
            async with client.stream(
                conn_options=APIConnectOptions(max_retry=0, timeout=2),
            ) as stream:
                stream.push_text("stream this text")
                stream.flush()
                stream.end_input()
                events = [event async for event in stream]
        finally:
            await client.aclose()

    _assert_nonempty_audio(events)

    assert len(mock_boson_server.requests) == 1
    speech_request = mock_boson_server.requests[0]
    assert speech_request.path == BOSON_SPEECH_PATH
    assert speech_request.body == {
        "input": "stream this text",
        "model": BOSON_TTS_MODEL,
        "response_format": "pcm",
        "stream": True,
        "voice": "default",
    }


@pytest.mark.integration
async def test_synthesize_maps_boson_error_response(
    mock_boson_server: _MockBosonServer,
) -> None:
    async with aiohttp.ClientSession() as http_session:
        client = boson_tts.TTS(
            api_key="test-key",
            base_url=mock_boson_server.base_url,
            http_session=http_session,
        )
        try:
            with pytest.raises(APIStatusError) as exc_info:
                async with client.synthesize(
                    "trigger provider error",
                    conn_options=APIConnectOptions(max_retry=0, timeout=2),
                ) as stream:
                    _ = [event async for event in stream]
        finally:
            await client.aclose()

    error = exc_info.value
    assert error.status_code == 400
    assert error.request_id == "req-error"
    assert "Boson mock rejected input" in error.message
    assert error.body == {"error": {"message": "Boson mock rejected input"}}

    assert len(mock_boson_server.requests) == 1
