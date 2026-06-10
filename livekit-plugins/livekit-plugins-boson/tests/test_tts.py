import os

import pytest

from livekit.plugins.boson import tts as boson_tts


def test_tts_requires_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BOSON_API_KEY", raising=False)

    with pytest.raises(ValueError, match="BOSON_API_KEY"):
        boson_tts.TTS()


def test_tts_uses_environment_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOSON_API_KEY", "env-key")

    tts = boson_tts.TTS()

    assert tts._opts.api_key == "env-key"


def test_tts_properties() -> None:
    tts = boson_tts.TTS(api_key="test-key")

    assert tts.model == "higgs-audio-v3-tts"
    assert tts.provider == "Boson AI"
    assert tts.sample_rate == 24000
    assert tts.num_channels == 1


def test_speech_payload_uses_voice_by_default() -> None:
    tts = boson_tts.TTS(api_key="test-key", voice="default")

    payload = boson_tts._speech_payload(  # pyright: ignore[reportPrivateUsage]
        tts._opts,
        input_text="hello",
        stream=False,
        response_format="pcm",
    )

    assert payload == {
        "input": "hello",
        "model": "higgs-audio-v3-tts",
        "response_format": "pcm",
        "stream": False,
        "voice": "default",
    }


def test_speech_payload_omits_voice_for_reference_audio() -> None:
    tts = boson_tts.TTS(
        api_key="test-key",
        ref_audio="https://example.com/reference.wav",
        ref_text="reference transcript",
    )

    payload = boson_tts._speech_payload(  # pyright: ignore[reportPrivateUsage]
        tts._opts,
        input_text="hello",
        stream=True,
        response_format="pcm",
    )

    assert "voice" not in payload
    assert payload["ref_audio"] == "https://example.com/reference.wav"
    assert payload["ref_text"] == "reference transcript"
    assert payload["stream"] is True


def test_update_options_updates_sample_rate() -> None:
    tts = boson_tts.TTS(api_key="test-key")

    tts.update_options(sample_rate=48000, voice="voice_123")

    assert tts.sample_rate == 48000
    assert tts._opts.sample_rate == 48000
    assert tts._opts.voice == "voice_123"


def test_headers() -> None:
    tts = boson_tts.TTS(api_key="test-key")

    headers = boson_tts._headers(tts._opts)  # pyright: ignore[reportPrivateUsage]

    assert headers["Authorization"] == "Bearer test-key"
    assert headers["X-LiveKit-Plugin"] == "livekit-plugins-boson"


def test_mime_type() -> None:
    assert boson_tts._mime_type("pcm") == "audio/pcm"  # pyright: ignore[reportPrivateUsage]
    assert boson_tts._mime_type("mp3") == "audio/mpeg"  # pyright: ignore[reportPrivateUsage]


def test_does_not_mutate_environment() -> None:
    assert os.environ.get("BOSON_API_KEY") != "test-key"
