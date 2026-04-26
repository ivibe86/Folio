"""Production dispatcher/specialist Copilot implementation.

This package owns intent routing and bounded specialist behavior. The temporary
legacy escape hatch lives in ``copilot_agent.py`` for rollout safety only.
"""

from .classifier import dispatcher_enabled, route_question
from .dispatcher import run_agent, run_agent_stream

__all__ = ["dispatcher_enabled", "route_question", "run_agent", "run_agent_stream"]
