"""ByteDance and Volcengine plugin for LiveKit Agents.

Currently supports Volcengine TTS V3 bidirectional streaming.
"""

from livekit.agents import Plugin

from .log import logger
from .tts import TTS, SynthesizeStream, VolcengineV3TTS
from .version import __version__

__all__ = ["TTS", "VolcengineV3TTS", "SynthesizeStream", "__version__"]


class BytedancePlugin(Plugin):
    def __init__(self) -> None:
        super().__init__(__name__, __version__, __package__, logger)


Plugin.register_plugin(BytedancePlugin())

_module = dir()
NOT_IN_ALL = [m for m in _module if m not in __all__]

__pdoc__ = {}

for n in NOT_IN_ALL:
    __pdoc__[n] = False
