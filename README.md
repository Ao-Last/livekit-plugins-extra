# LiveKit Plugins Extra

Community-maintained LiveKit Agents plugins that are not yet part of the upstream
LiveKit Agents repository.

This repository keeps the same high-level layout as `livekit/agents` so each
plugin can be proposed upstream with minimal reshaping:

```text
livekit-plugins/
  livekit-plugins-<provider>/
    pyproject.toml
    README.md
    livekit/plugins/<provider>/
```

## Plugins

| Package | Import | Status |
| --- | --- | --- |
| `livekit-plugins-boson` | `livekit.plugins.boson` | Boson AI Higgs Audio TTS |

## Development

Install the workspace dependencies:

```bash
uv sync --all-extras --dev
```

Run tests:

```bash
uv run pytest
```

Run formatting and linting:

```bash
uv run ruff format .
uv run ruff check .
```
