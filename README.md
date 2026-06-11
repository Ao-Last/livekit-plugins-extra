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

Run only the real provider end-to-end tests:

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
PyPI project must trust this GitHub workflow before the first publish:

```text
Owner: Ao-Last
Repository: livekit-plugins-extra
Workflow: publish-pypi.yml
Environment: pypi
```

After the PyPI trusted publisher is configured, run the `publish-pypi` workflow
manually from the GitHub Actions tab and choose the package to publish.

Run formatting and linting:

```bash
uv run ruff format .
uv run ruff check .
```
