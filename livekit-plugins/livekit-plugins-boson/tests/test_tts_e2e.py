from __future__ import annotations

import os
import wave
from pathlib import Path

import pytest

from livekit.agents import tts
from livekit.agents.types import APIConnectOptions
from livekit.plugins import boson

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not os.environ.get("BOSON_API_KEY"),
        reason="BOSON_API_KEY is required for Boson real e2e tests",
    ),
]

E2E_TEXT = "Hello from LiveKit Boson TTS end to end test."


def _assert_nonempty_audio(events: list[tts.SynthesizedAudio], *, sample_rate: int) -> None:
    assert events
    assert events[-1].is_final
    assert sum(event.frame.duration for event in events) > 0
    assert {event.frame.sample_rate for event in events} == {sample_rate}
    assert {event.frame.num_channels for event in events} == {1}


def _maybe_write_wav(
    events: list[tts.SynthesizedAudio],
    *,
    path: Path,
    sample_rate: int,
    num_channels: int,
) -> None:
    artifact_dir = os.environ.get("BOSON_E2E_ARTIFACT_DIR")
    if not artifact_dir:
        return

    output_path = Path(artifact_dir) / path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with wave.open(str(output_path), "wb") as wav_file:
        wav_file.setnchannels(num_channels)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        for event in events:
            wav_file.writeframes(event.frame.data.tobytes())


async def test_boson_synthesize_real_e2e() -> None:
    client = boson.TTS()
    try:
        async with client.synthesize(
            E2E_TEXT,
            conn_options=APIConnectOptions(max_retry=1, timeout=20),
        ) as stream:
            events = [event async for event in stream]
    finally:
        await client.aclose()

    _assert_nonempty_audio(events, sample_rate=client.sample_rate)
    _maybe_write_wav(
        events,
        path=Path("boson-synthesize-e2e.wav"),
        sample_rate=client.sample_rate,
        num_channels=client.num_channels,
    )


async def test_boson_stream_real_e2e() -> None:
    client = boson.TTS()
    try:
        async with client.stream(
            conn_options=APIConnectOptions(max_retry=1, timeout=20),
        ) as stream:
            stream.push_text(E2E_TEXT)
            stream.flush()
            stream.end_input()
            events = [event async for event in stream]
    finally:
        await client.aclose()

    _assert_nonempty_audio(events, sample_rate=client.sample_rate)
    _maybe_write_wav(
        events,
        path=Path("boson-stream-e2e.wav"),
        sample_rate=client.sample_rate,
        num_channels=client.num_channels,
    )
