"""Boson AI plugin for LiveKit Agents.

Support for Boson AI Higgs Audio TTS.
"""

from livekit.agents import Plugin

from .log import logger
from .tts import TTS, ChunkedStream, SynthesizeStream
from .version import __version__

__all__ = ["TTS", "ChunkedStream", "SynthesizeStream", "__version__"]


class BosonPlugin(Plugin):
    def __init__(self) -> None:
        super().__init__(__name__, __version__, __package__, logger)


Plugin.register_plugin(BosonPlugin())

_module = dir()
NOT_IN_ALL = [m for m in _module if m not in __all__]

__pdoc__ = {}

for n in NOT_IN_ALL:
    __pdoc__[n] = False
