from __future__ import annotations

import os
import wave
from pathlib import Path

import aiohttp
import pytest

from livekit.agents import tts
from livekit.agents.types import APIConnectOptions
from livekit.plugins import bytedance

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        not os.environ.get("VOLCENGINE_TTS_V3_API_KEY"),
        reason="VOLCENGINE_TTS_V3_API_KEY is required for Volcengine TTS V3 real e2e tests",
    ),
]

E2E_TEXT = "你好，这是 LiveKit Bytedance TTS V3 端到端测试。"
SAMPLE_RATE = 24000


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
    artifact_dir = os.environ.get("BYTEDANCE_E2E_ARTIFACT_DIR")
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


async def test_volcengine_tts_v3_stream_real_e2e() -> None:
    resource_id = os.environ.get("VOLCENGINE_TTS_V3_RESOURCE_ID", "seed-tts-2.0")
    speaker = os.environ.get("VOLCENGINE_TTS_V3_SPEAKER", "zh_female_vv_uranus_bigtts")

    async with aiohttp.ClientSession() as http_session:
        client = bytedance.TTS(
            api_key=os.environ["VOLCENGINE_TTS_V3_API_KEY"],
            resource_id=resource_id,
            speaker=speaker,
            audio_format="pcm",
            sample_rate=SAMPLE_RATE,
            http_session=http_session,
        )
        try:
            async with client.stream(
                conn_options=APIConnectOptions(max_retry=1, timeout=30),
            ) as stream:
                stream.push_text(E2E_TEXT)
                stream.end_input()
                events = [event async for event in stream]
        finally:
            await client.aclose()

    assert client.model == resource_id
    assert client.provider == "ByteDance / Volcengine"
    _assert_nonempty_audio(events, sample_rate=client.sample_rate)
    _maybe_write_wav(
        events,
        path=Path("volcengine-tts-v3-stream-e2e.wav"),
        sample_rate=client.sample_rate,
        num_channels=client.num_channels,
    )
