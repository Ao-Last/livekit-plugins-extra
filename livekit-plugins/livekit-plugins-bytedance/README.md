# ByteDance plugin for LiveKit Agents

[![PyPI](https://img.shields.io/pypi/v/livekit-plugins-bytedance.svg)](https://pypi.org/project/livekit-plugins-bytedance/)
[![CI](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml/badge.svg)](https://github.com/Ao-Last/livekit-plugins-extra/actions/workflows/ci.yml)

Community-maintained [LiveKit Agents](https://github.com/livekit/agents)
plugin for ByteDance and Volcengine AI services.

This package is unofficial and is not currently maintained by ByteDance,
Volcengine, or LiveKit.

## Current Scope

`livekit-plugins-bytedance` is intentionally narrow today. The package name is
reserved for the broader ByteDance/Volcengine ecosystem, but version `0.1.x`
only implements:

| Service | API | LiveKit class | Status |
| --- | --- | --- | --- |
| Volcengine TTS V3 bidirectional streaming | `wss://openspeech.bytedance.com/api/v3/tts/bidirection` | `livekit.plugins.bytedance.TTS` | Supported |

The implemented TTS client follows Volcengine's TTS V3 bidirectional streaming
protocol as documented at:

- Volcengine TTS V3 bidirectional API: <https://www.volcengine.com/docs/6561/1329505>

## Explicitly Not Supported Yet

This package does not currently implement:

- Volcengine legacy TTS v1 (`/api/v1/tts/ws_binary`)
- Volcengine STT or BigModelSTT
- Doubao/Ark LLM APIs
- Volcengine realtime dialogue APIs
- ByteDance video, image, embedding, or moderation APIs
- Non-streaming LiveKit `TTS.synthesize()`

For those services, use a provider-specific package if one exists. The existing
third-party `livekit-plugins-volcengine` package is separate from this package
and uses the `livekit.plugins.volcengine` import namespace.

## Supported TTS Features

- Streaming synthesis through `TTS.stream()`
- Volcengine TTS V3 connection/session/task binary protocol
- `seed-tts-1.0` and `seed-tts-2.0` style `resource_id` values, when enabled
  for your Volcengine account
- `speaker`
- `audio_format`: `pcm`, `mp3`, or `ogg_opus`
- `sample_rate`
- `speech_rate`
- `loudness_rate`
- `emotion`
- TTS 2.0 `context_texts`
- Server-side sentence splitting
- LiveKit retry behavior for transient websocket failures before audio is
  emitted

The plugin sends credentials with Volcengine's V3 websocket headers:

- `X-Api-App-Key`
- `X-Api-Access-Key`
- `X-Api-Resource-Id`
- `X-Api-Connect-Id`

## Installation

```bash
pip install livekit-plugins-bytedance
```

## Credentials

Create or locate your Volcengine TTS V3 credentials in the Volcengine console,
then pass them to the plugin explicitly:

```python
from livekit.plugins import bytedance

tts = bytedance.TTS(
    app_key="your-app-key",
    access_key="your-access-key",
    resource_id="seed-tts-2.0",
)
```

If your application prefers environment variables, load them in your own config
layer and pass them to `TTS`. The plugin does not read environment variables by
itself.

Suggested variable names:

```bash
export VOLCENGINE_TTS_V3_APP_KEY=...
export VOLCENGINE_TTS_V3_ACCESS_KEY=...
export VOLCENGINE_TTS_V3_RESOURCE_ID=seed-tts-2.0
```

## Usage

Use the default TTS V3 model and speaker:

```python
from livekit.plugins import bytedance

tts = bytedance.TTS(
    app_key="your-app-key",
    access_key="your-access-key",
)
```

Use a specific Seed TTS resource and speaker:

```python
tts = bytedance.TTS(
    app_key="your-app-key",
    access_key="your-access-key",
    resource_id="seed-tts-2.0",
    speaker="zh_female_vv_uranus_bigtts",
)
```

Use TTS 2.0 style controls:

```python
tts = bytedance.TTS(
    app_key="your-app-key",
    access_key="your-access-key",
    resource_id="seed-tts-2.0",
    speaker="zh_female_vv_uranus_bigtts",
    context_texts=["自然、专业、和善，像面试官一样说话"],
    speech_rate=0,
    loudness_rate=0,
)
```

Use the descriptive class name if you prefer:

```python
from livekit.plugins.bytedance import VolcengineV3TTS

tts = VolcengineV3TTS(
    app_key="your-app-key",
    access_key="your-access-key",
)
```

## Testing

Run the default suite:

```bash
uv run pytest livekit-plugins/livekit-plugins-bytedance
```

The default tests are hermetic and do not require Volcengine credentials. They
cover the V3 binary protocol, websocket handshake headers, retry behavior,
zombie websocket handling, server error classification, and partial audio drain
behavior.

Real end-to-end tests should use a separate marker and require:

```bash
export VOLCENGINE_TTS_V3_APP_KEY=...
export VOLCENGINE_TTS_V3_ACCESS_KEY=...
export VOLCENGINE_TTS_V3_RESOURCE_ID=seed-tts-2.0
```

## License

Apache-2.0.
