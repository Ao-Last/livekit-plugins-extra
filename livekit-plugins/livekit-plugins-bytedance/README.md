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
| Volcengine BigModel streaming ASR, optimized bidirectional mode | `wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async` | `livekit.plugins.bytedance.STT` | Supported |

The implemented clients follow these Volcengine WebSocket APIs:

- Volcengine TTS V3 bidirectional API: <https://www.volcengine.com/docs/6561/1329505>
- Volcengine BigModel ASR WebSocket API, optimized bidirectional streaming
  endpoint: <https://www.volcengine.com/docs/6561/1354869>

The supported WebSocket request paths are:

```text
wss://openspeech.bytedance.com/api/v3/tts/bidirection
wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async
```

The TTS binary protocol constants are also checked against ByteDance's
reference helper package named `TTS Websocket Bidirection protocols`, including
the downstream event codes for `UsageResponse` (`154`), `AudioMuted` (`250`),
`TTSResponse` (`352`), `TTSEnded` (`359`), and `TTSSubtitle` (`364`).

## Explicitly Not Supported Yet

This package does not currently implement:

- Volcengine legacy TTS v1 (`/api/v1/tts/ws_binary`)
- Volcengine ASR batch/offline APIs
- Volcengine ASR `bigmodel_nostream` streaming-input mode
- Volcengine ASR legacy non-optimized bidirectional path
  (`/api/v3/sauc/bigmodel`)
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
- `X-Api-Key` authentication for the current Volcengine console
- Legacy console authentication through `X-Api-App-Key` and `X-Api-Access-Key`
- `resource_id` values documented for this API:
  - `seed-tts-2.0`
  - `seed-icl-2.0`
- Optional cloned-voice model selection:
  - `seed-tts-2.0-standard`
  - `seed-tts-2.0-expressive`
- `speaker`
- `ssml`
- `audio_format`: `pcm`, `mp3`, `ogg_opus`, or `wav`
- `sample_rate`
- `bit_rate`
- `speech_rate`
- `loudness_rate`
- `enable_subtitle` request flag
- `disable_markdown_filter`
- `disable_emoji_filter`
- `enable_latex_tn`
- `latex_parser`
- `explicit_language`
- `explicit_dialect`
- `aigc_watermark`
- `aigc_metadata`
- `cache_config`
- `post_process`
- TTS 2.0 `context_texts`
- `use_tag_parser`
- `X-Control-Require-Usage-Tokens-Return`
- Server-side sentence splitting
- LiveKit retry behavior for transient websocket failures before audio is
  emitted

Subtitle and usage payloads can be requested from Volcengine, but this LiveKit
TTS plugin currently exposes only synthesized audio frames through the LiveKit
TTS stream. Non-audio protocol events such as usage responses, muted-audio
signals, sentence boundaries, subtitles, and TTS-ended markers are parsed and
ignored for now rather than surfaced as LiveKit TTS events.

## Supported STT Features

- Streaming recognition through `STT.stream()`
- Volcengine BigModel ASR WebSocket binary protocol v3
- Optimized bidirectional streaming endpoint:
  `wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async`
- `X-Api-Key` authentication for the current Volcengine console
- Legacy console authentication through `X-Api-App-Key` and `X-Api-Access-Key`
- `X-Api-Resource-Id`, `X-Api-Request-Id`, `X-Api-Sequence`, and
  `X-Api-Connect-Id` headers
- Default ASR 2.0 resource ID: `volc.seedasr.sauc.duration`
- PCM input at 16 kHz, 16-bit, mono
- Server-side VAD/final segmentation through `enable_nonstream=True`
- Interim and final LiveKit transcript events
- Word timestamps when Volcengine returns `utterances[*].words`
- Selected BigModel request options, including `enable_itn`, `enable_punc`,
  `enable_ddc`, `show_utterances`, `enable_speaker_info`, `ssd_version`,
  `result_type`, VAD timing options, sensitive-word filtering, and `corpus`
- Escape hatches through `audio_options` and `request_options` for provider
  fields that are not first-class constructor arguments yet

The plugin does not send the ASR `audio.language` field by default because the
provider document scopes that field to the `bigmodel_nostream` endpoint, which
this package does not currently support.

The plugin sends credentials with Volcengine's V3 websocket headers:

- `X-Api-Key`
- `X-Api-Resource-Id`
- `X-Api-Connect-Id`

For legacy console credentials, it sends:

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
    api_key="your-api-key",
    resource_id="seed-tts-2.0",
)
```

If your application prefers environment variables, load them in your own config
layer and pass them to `TTS`. The plugin does not read environment variables by
itself.

Suggested variable names:

```bash
export VOLCENGINE_TTS_V3_API_KEY=...
export VOLCENGINE_TTS_V3_RESOURCE_ID=seed-tts-2.0
export VOLCENGINE_ASR_API_KEY=...
export VOLCENGINE_ASR_RESOURCE_ID=volc.seedasr.sauc.duration
```

The API also supports old-console authentication. If you still use those
credentials, pass both `app_key` and `access_key` instead of `api_key`.

## Usage

Use the default TTS V3 model and speaker:

```python
from livekit.plugins import bytedance

tts = bytedance.TTS(
    api_key="your-api-key",
)
```

Use a specific Seed TTS resource and speaker:

```python
tts = bytedance.TTS(
    api_key="your-api-key",
    resource_id="seed-tts-2.0",
    speaker="zh_female_vv_uranus_bigtts",
)
```

Use TTS 2.0 style controls:

```python
tts = bytedance.TTS(
    api_key="your-api-key",
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
    api_key="your-api-key",
)
```

Use streaming ASR:

```python
from livekit.plugins import bytedance

stt = bytedance.STT(
    api_key="your-api-key",
    resource_id="volc.seedasr.sauc.duration",
)

stream = stt.stream()
stream.push_frame(audio_frame)
stream.end_input()

async for event in stream:
    if event.type == "final_transcript":
        print(event.alternatives[0].text)
```

## Testing

Run the default suite:

```bash
uv run pytest livekit-plugins/livekit-plugins-bytedance
```

The default tests are hermetic and do not require Volcengine credentials. They
cover the TTS V3 binary protocol, ASR WebSocket v3 binary protocol, websocket
handshake headers, retry behavior, zombie websocket handling, server error
classification, partial audio drain behavior, and ASR transcript event mapping.

Real end-to-end tests should use a separate marker and require:

```bash
export VOLCENGINE_TTS_V3_API_KEY=...
export VOLCENGINE_TTS_V3_RESOURCE_ID=seed-tts-2.0
export VOLCENGINE_ASR_API_KEY=...
export VOLCENGINE_ASR_RESOURCE_ID=volc.seedasr.sauc.duration
```

## License

Apache-2.0.
