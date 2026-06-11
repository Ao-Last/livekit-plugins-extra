# Boson AI plugin for LiveKit Agents

[![PyPI](https://img.shields.io/pypi/v/livekit-plugins-boson.svg)](https://pypi.org/project/livekit-plugins-boson/)
[![CI](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml/badge.svg)](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml)

Community-maintained [LiveKit Agents](https://github.com/livekit/agents) plugin
for [Boson AI Higgs Audio v3 TTS](https://docs.boson.ai/models/higgs-audio-tts/overview).

This package is unofficial and is not currently maintained by Boson AI or
LiveKit.

## What It Supports

- `livekit.plugins.boson.TTS`
- Batch synthesis through `TTS.synthesize()`
- Streaming synthesis through `TTS.stream()`
- Boson preset or registered voices through `voice`
- One-off reference audio cloning through `ref_audio` and `ref_text`
- Raw PCM streaming for LiveKit's TTS pipeline

The plugin targets Boson's `POST /v1/audio/speech` endpoint and defaults to
the `higgs-audio-v3-tts` model.

## Links

- Boson model overview: <https://docs.boson.ai/models/higgs-audio-tts/overview>
- Boson speech API reference: <https://docs.boson.ai/api-reference/text-to-speech/create-speech>
- Boson OpenAPI schema: <https://docs.boson.ai/openapi.json>
- Source repository: <https://github.com/Ao-Last/livekit-plugins-extra>

## Installation

```bash
pip install livekit-plugins-boson
```

## Prerequisites

Set your Boson API key:

```bash
export BOSON_API_KEY=...
```

## Usage

Use the default Higgs Audio v3 TTS model and voice:

```python
from livekit.plugins import boson

tts = boson.TTS()
```

Use a preset or registered Boson voice:

```python
tts = boson.TTS(voice="default")
```

Use one-off reference audio cloning:

```python
tts = boson.TTS(
    ref_audio="https://example.com/reference.wav",
    ref_text="Transcript of the reference audio.",
)
```

Override the model or raw PCM sample rate if Boson exposes a compatible model:

```python
tts = boson.TTS(
    model="higgs-audio-v3-tts",
    sample_rate=24000,
)
```

Streaming synthesis uses Boson's raw PCM stream. Boson's docs currently require
`response_format: "pcm"` when `stream: true`; the plugin handles this for
`TTS.stream()`.

When using the plugin inside a normal LiveKit agent worker, LiveKit manages the
HTTP session. If you call the plugin from a standalone script or test, pass an
`aiohttp.ClientSession` explicitly or run inside LiveKit's HTTP context helper.

## Testing

The repository contains three layers of tests:

- Unit tests for option handling, request headers, and payload construction.
- Mock-server integration tests for Boson's speech API contract as observed on
  2026-06-10.
- Real e2e tests against Boson using `BOSON_API_KEY`.

Run the default suite:

```bash
uv run pytest
```

Run real e2e tests:

```bash
export BOSON_API_KEY=...
uv run pytest -m e2e
```

The real e2e suite verifies both the Boson speech API and the default
`higgs-audio-v3-tts` model path, but it is not an audio quality evaluation.

## License

Apache-2.0.
