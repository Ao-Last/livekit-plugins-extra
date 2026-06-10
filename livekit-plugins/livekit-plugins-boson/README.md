# Boson AI plugin for LiveKit Agents

Community-maintained LiveKit Agents plugin for Boson AI Higgs Audio TTS.

This package is unofficial and not currently maintained by Boson AI or LiveKit.

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

Streaming synthesis uses Boson's raw PCM stream and feeds it directly into the
LiveKit TTS pipeline.
