# LiveKit Plugins Extra

[![CI](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml/badge.svg)](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml)
[![PyPI - livekit-plugins-boson](https://img.shields.io/pypi/v/livekit-plugins-boson.svg)](https://pypi.org/project/livekit-plugins-boson/)
[![PyPI - livekit-plugins-bytedance](https://img.shields.io/pypi/v/livekit-plugins-bytedance.svg)](https://pypi.org/project/livekit-plugins-bytedance/)

Community-maintained [LiveKit Agents](https://github.com/livekit/agents)
provider plugins that are not yet part of the upstream LiveKit Agents
repository.

The repo intentionally mirrors the upstream `livekit/agents` plugin layout so a
plugin can later be proposed upstream with minimal reshaping.

## Plugins

| Provider | Package | Import | Capability | Upstream docs | Status |
| --- | --- | --- | --- | --- | --- |
| Boson AI | [`livekit-plugins-boson`](https://pypi.org/project/livekit-plugins-boson/) | `livekit.plugins.boson` | TTS | [Higgs Audio v3 TTS](https://docs.boson.ai/models/higgs-audio-tts/overview), [API reference](https://docs.boson.ai/api-reference/text-to-speech/create-speech) | Published |
| ByteDance / Volcengine | `livekit-plugins-bytedance` | `livekit.plugins.bytedance` | Volcengine TTS V3 bidirectional streaming, BigModel streaming ASR | [TTS V3 bidirectional API](https://www.volcengine.com/docs/6561/1329505), [BigModel ASR WebSocket](https://www.volcengine.com/docs/6561/1354869) | In development |

The Boson plugin targets Boson's `POST /v1/audio/speech` endpoint and defaults
to the `higgs-audio-v3-tts` model. See the Boson documentation for model
behavior, supported voices, reference-audio cloning, tags, streaming details,
and API field semantics.

The ByteDance plugin currently targets two Volcengine streaming WebSocket APIs:

- TTS V3 bidirectional streaming:
  `wss://openspeech.bytedance.com/api/v3/tts/bidirection`
- BigModel ASR optimized bidirectional streaming:
  `wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async`

It does not yet implement Volcengine legacy TTS, batch/offline ASR,
`bigmodel_nostream`, the legacy non-optimized ASR path, LLM, realtime dialogue,
or other ByteDance AI APIs. TTS resource IDs follow Volcengine's documented
bidirectional TTS contract: `seed-tts-2.0` and `seed-icl-2.0`. ASR defaults to
the ASR 2.0 duration resource `volc.seedasr.sauc.duration`.

## Quick Start

Install the Boson plugin:

```bash
pip install livekit-plugins-boson
```

Install the ByteDance plugin:

```bash
pip install livekit-plugins-bytedance
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

Use Volcengine TTS V3 from a LiveKit Agents app:

```python
from livekit.plugins import bytedance

tts = bytedance.TTS(
    api_key="your-volcengine-api-key",
    resource_id="seed-tts-2.0",
)
```

Use Volcengine streaming ASR:

```python
from livekit.plugins import bytedance

stt = bytedance.STT(
    api_key="your-volcengine-api-key",
    resource_id="volc.seedasr.sauc.duration",
)
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
- Volcengine TTS V3 bidirectional API: <https://www.volcengine.com/docs/6561/1329505>
- Volcengine BigModel ASR WebSocket API: <https://www.volcengine.com/docs/6561/1354869>
- LiveKit Agents repository: <https://github.com/livekit/agents>
- PyPI package: <https://pypi.org/project/livekit-plugins-boson/>
- PyPI package: <https://pypi.org/project/livekit-plugins-bytedance/>

## Repository Layout

```text
livekit-plugins-extra/
  livekit-plugins/
    livekit-plugins-boson/
      pyproject.toml
      README.md
      livekit/plugins/boson/
      tests/
    livekit-plugins-bytedance/
      pyproject.toml
      README.md
      livekit/plugins/bytedance/
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

Run real Volcengine TTS V3 end-to-end tests after adding the test credentials:

```bash
export VOLCENGINE_TTS_V3_API_KEY=...
export VOLCENGINE_TTS_V3_RESOURCE_ID=seed-tts-2.0
uv run pytest -m e2e
```

Real e2e tests are skipped locally when their provider credentials are not set.
In GitHub Actions, run the `e2e` workflow manually from the Actions tab and
choose `boson`, `bytedance`, or `all`. The workflow reads provider credentials
from repository secrets and uploads generated WAV files as `boson-e2e-audio`
and/or `bytedance-e2e-audio` artifacts.

The ByteDance ASR implementation currently has hermetic protocol tests but no
real provider e2e fixture yet. Add a checked-in speech fixture or a gated sample
asset before enabling ASR e2e in CI.

## Publishing

Packages are published to PyPI through GitHub Actions Trusted Publishing. The
current publisher configuration for `livekit-plugins-boson` is:

```text
Owner: Ao-Last
Repository: livekit-plugins-extra
Workflow: publish-pypi.yml
Environment: pypi
```

The current `publish-pypi.yml` workflow accepts a package choice and can publish
either workspace package. That works if each PyPI project trusts the same
workflow and environment, but it is broader than necessary. Before publishing
additional packages, prefer a per-package publishing workflow:

| Package | Workflow | Environment |
| --- | --- | --- |
| `livekit-plugins-boson` | `publish-boson-pypi.yml` | `pypi-boson` |
| `livekit-plugins-bytedance` | `publish-bytedance-pypi.yml` | `pypi-bytedance` |

This keeps PyPI Trusted Publishing scoped to one package at a time, gives each
package its own approval/history surface, and avoids accidentally publishing
the wrong package through a workflow input. Do not rename or remove the current
workflow for an already-configured PyPI project until that project's Trusted
Publisher settings have been migrated.

To publish a new version:

1. Update the plugin version, for example
   `livekit-plugins/livekit-plugins-boson/livekit/plugins/boson/version.py` or
   `livekit-plugins/livekit-plugins-bytedance/livekit/plugins/bytedance/version.py`.
2. Run the local checks and the real e2e test.
3. Push to `main`.
4. Run the package-specific publish workflow manually from the GitHub Actions
   tab. If the repo is still using the generic `publish-pypi` workflow, choose
   the package to publish carefully.

PyPI versions are immutable. If `0.1.0` is already published, the next upload
must use a new version such as `0.1.1` or `0.2.0`.

## License

Apache-2.0. This package is unofficial and is not currently maintained by
Boson AI or LiveKit.
