# LiveKit Plugins Extra

[![CI](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml/badge.svg)](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml)
[![PyPI - livekit-plugins-boson](https://img.shields.io/pypi/v/livekit-plugins-boson.svg)](https://pypi.org/project/livekit-plugins-boson/)

Community-maintained [LiveKit Agents](https://github.com/livekit/agents)
provider plugins that are not yet part of the upstream LiveKit Agents
repository.

The repo intentionally mirrors the upstream `livekit/agents` plugin layout so a
plugin can later be proposed upstream with minimal reshaping.

## Plugins

| Provider | Package | Import | Capability | Upstream docs | Status |
| --- | --- | --- | --- | --- | --- |
| Boson AI | [`livekit-plugins-boson`](https://pypi.org/project/livekit-plugins-boson/) | `livekit.plugins.boson` | TTS | [Higgs Audio v3 TTS](https://docs.boson.ai/models/higgs-audio-tts/overview), [API reference](https://docs.boson.ai/api-reference/text-to-speech/create-speech) | Published |

The Boson plugin targets Boson's `POST /v1/audio/speech` endpoint and defaults
to the `higgs-audio-v3-tts` model. See the Boson documentation for model
behavior, supported voices, reference-audio cloning, tags, streaming details,
and API field semantics.

## Quick Start

Install the Boson plugin:

```bash
pip install livekit-plugins-boson
```

Set your Boson API key:

```bash
export BOSON_API_KEY=...
```

Use it from a LiveKit Agents app:

```python
from livekit.plugins import boson

tts = boson.TTS()
```

Choose a preset or registered Boson voice:

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

Streaming synthesis uses Boson's raw PCM streaming response and feeds it into
the LiveKit TTS pipeline. Boson's docs currently require
`response_format: "pcm"` when `stream: true`.

## Links

- Boson model overview: <https://docs.boson.ai/models/higgs-audio-tts/overview>
- Boson speech API reference: <https://docs.boson.ai/api-reference/text-to-speech/create-speech>
- Boson OpenAPI schema: <https://docs.boson.ai/openapi.json>
- LiveKit Agents repository: <https://github.com/livekit/agents>
- PyPI package: <https://pypi.org/project/livekit-plugins-boson/>

## Repository Layout

```text
livekit-plugins-extra/
  livekit-plugins/
    livekit-plugins-boson/
      pyproject.toml
      README.md
      livekit/plugins/boson/
      tests/
```

Each provider package should be self-contained under
`livekit-plugins/livekit-plugins-<provider>/`.

## Development

Install workspace dependencies:

```bash
uv sync --all-extras --dev
```

Run the default checks:

```bash
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

The default test suite includes unit tests and local mock-server integration
tests. It does not require provider credentials.

Run real Boson end-to-end tests:

```bash
export BOSON_API_KEY=...
uv run pytest -m e2e
```

Real e2e tests are skipped when `BOSON_API_KEY` is not set. In GitHub Actions,
run the `e2e` workflow manually from the Actions tab; it reads `BOSON_API_KEY`
from the repository secrets and uploads generated WAV files as the
`boson-e2e-audio` artifact.

## Publishing

Packages are published to PyPI through GitHub Actions Trusted Publishing. The
current publisher configuration for `livekit-plugins-boson` is:

```text
Owner: Ao-Last
Repository: livekit-plugins-extra
Workflow: publish-pypi.yml
Environment: pypi
```

To publish a new version:

1. Update the plugin version, for example
   `livekit-plugins/livekit-plugins-boson/livekit/plugins/boson/version.py`.
2. Run the local checks and the real e2e test.
3. Push to `main`.
4. Run the `publish-pypi` workflow manually from the GitHub Actions tab and
   choose the package to publish.

PyPI versions are immutable. If `0.1.0` is already published, the next upload
must use a new version such as `0.1.1` or `0.2.0`.

## License

Apache-2.0. This package is unofficial and is not currently maintained by
Boson AI or LiveKit.
