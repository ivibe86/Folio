"""Production Copilot dispatcher package."""

from .dispatcher import run_agent, run_agent_stream


def dispatcher_enabled() -> bool:
    return True


def route_question(*args, **kwargs):
    from .dispatcher import route_question

    return route_question(*args, **kwargs)


__all__ = ["dispatcher_enabled", "route_question", "run_agent", "run_agent_stream"]
