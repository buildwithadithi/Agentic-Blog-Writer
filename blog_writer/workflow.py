from datetime import date
from typing import Any, Dict, Optional
from uuid import uuid4

from langgraph.types import Command

from blog_writer.graph import build_graph


async def initialize_checkpointer():
    """Initialize the existing asynchronous PostgreSQL checkpointer."""
    from checkpointer import initialize_checkpointer as initialize_existing_checkpointer

    return await initialize_existing_checkpointer()


def build_workflow_graph(checkpointer=None):
    """Build a compiled graph with optional persistence."""
    return build_graph(checkpointer=checkpointer)


def close_checkpointer(connection_pool):
    """Close an initialized connection pool when the application lifecycle ends."""
    if connection_pool is not None:
        connection_pool.close()


def make_thread_config(thread_id: Optional[str] = None) -> Dict[str, Any]:
    resolved_thread_id = thread_id or str(uuid4())
    return {
        "thread_id": resolved_thread_id,
        "config": {
            "configurable": {
                "thread_id": resolved_thread_id
            }
        },
    }


def _serialize(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "value"):
        return _serialize(value.value)
    return str(value)


def _interrupt_details(result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        return {"is_paused": False, "interrupt": None}

    interrupts = result.get("__interrupt__")
    if not interrupts:
        return {"is_paused": False, "interrupt": None}

    first = interrupts[0] if isinstance(interrupts, (list, tuple)) else interrupts
    if isinstance(first, dict) and "value" in first:
        interrupt_value = first["value"]
    else:
        interrupt_value = getattr(first, "value", first)

    return {
        "is_paused": True,
        "interrupt": _serialize(interrupt_value),
    }


def _workflow_response(
    thread_id: str,
    result: Any,
) -> Dict[str, Any]:
    interrupt_details = _interrupt_details(result)
    return {
        "thread_id": thread_id,
        "status": "paused" if interrupt_details["is_paused"] else "completed",
        "result": _serialize(result),
        "interrupt": interrupt_details["interrupt"],
    }


async def start_workflow(
    topic: str,
    as_of: Optional[str] = None,
    thread_id: Optional[str] = None,
    *,
    graph=None,
    checkpointer=None,
):
    """Start a new workflow and return its thread and interrupt/completion state."""
    if graph is None:
        if checkpointer is None:
            _, checkpointer = await initialize_checkpointer()
        graph = build_workflow_graph(checkpointer)

    thread = make_thread_config(thread_id)
    resolved_as_of = as_of or date.today().isoformat()

    initial_state = {
        "topic": topic,
        "mode": "",
        "needs_research": False,
        "queries": [],
        "evidence": [],
        "plan": None,
        "as_of": resolved_as_of,
        "recency_days": 7,
        "sections": [],
        "merged_md": "",
        "md_with_placeholders": "",
        "image_specs": [],
        "review_status": "",
        "review_feedback": "",
        "revision_count": 0,
        "revision_history": [],
        "final": "",
        "wordpress_post_id": None,
        "wordpress_post_url": None,
        "wordpress_status": None,
    }

    result = await graph.ainvoke(initial_state, config=thread["config"])
    return _workflow_response(thread["thread_id"], result)


async def resume_workflow(
    thread_id: str,
    payload: Dict[str, Any],
    *,
    graph=None,
    checkpointer=None,
):
    """Resume a paused workflow using the same LangGraph thread ID."""
    if graph is None:
        if checkpointer is None:
            _, checkpointer = await initialize_checkpointer()
        graph = build_workflow_graph(checkpointer)

    thread = make_thread_config(thread_id)
    result = await graph.ainvoke(
        Command(resume=payload),
        config=thread["config"],
    )
    return _workflow_response(thread["thread_id"], result)
