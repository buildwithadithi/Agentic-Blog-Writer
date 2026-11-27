import asyncio
import inspect
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


async def close_checkpointer(connection_pool):
    """Close an initialized connection pool, awaiting asynchronous pool closes."""
    if connection_pool is not None:
        result = connection_pool.close()
        if inspect.isawaitable(result):
            await result


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


def _initial_state(topic: str, as_of: Optional[str] = None) -> Dict[str, Any]:
    resolved_as_of = as_of or date.today().isoformat()
    return {
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


async def _start_on_graph(
    graph,
    topic: str,
    as_of: Optional[str],
    thread_id: Optional[str],
):
    thread = make_thread_config(thread_id)
    result = await graph.ainvoke(
        _initial_state(topic, as_of),
        config=thread["config"],
    )
    return _workflow_response(thread["thread_id"], result)


async def _resume_on_graph(graph, thread_id: str, payload: Dict[str, Any]):
    thread = make_thread_config(thread_id)
    result = await graph.ainvoke(
        Command(resume=payload),
        config=thread["config"],
    )
    return _workflow_response(thread["thread_id"], result)


class WorkflowRuntime:
    """Owns the checkpointer resources and graph for an application's lifetime."""

    def __init__(self, connection_pool, checkpointer, graph):
        self.connection_pool = connection_pool
        self.checkpointer = checkpointer
        self.graph = graph
        self._active_workflows = 0
        self._closing = False
        self._closed = False
        self._condition = asyncio.Condition()
        self._shutdown_lock = asyncio.Lock()

    @classmethod
    async def initialize(cls):
        """Initialize one pool/checkpointer pair and compile one reusable graph."""
        connection_pool, checkpointer = await initialize_checkpointer()
        try:
            graph = build_workflow_graph(checkpointer)
        except Exception:
            await close_checkpointer(connection_pool)
            raise
        return cls(connection_pool, checkpointer, graph)

    @classmethod
    def from_checkpointer(cls, checkpointer, graph=None):
        """Create a pool-less runtime, for example with LangGraph MemorySaver."""
        compiled_graph = graph if graph is not None else build_workflow_graph(checkpointer)
        return cls(None, checkpointer, compiled_graph)

    async def _begin_workflow(self):
        async with self._condition:
            if self._closing or self._closed:
                raise RuntimeError("Workflow runtime is shutting down or already closed.")
            self._active_workflows += 1

    async def _end_workflow(self):
        async with self._condition:
            self._active_workflows -= 1
            if self._active_workflows == 0:
                self._condition.notify_all()

    async def start_workflow(
        self,
        topic: str,
        as_of: Optional[str] = None,
        thread_id: Optional[str] = None,
    ):
        await self._begin_workflow()
        try:
            return await _start_on_graph(self.graph, topic, as_of, thread_id)
        finally:
            await self._end_workflow()

    async def resume_workflow(self, thread_id: str, payload: Dict[str, Any]):
        await self._begin_workflow()
        try:
            return await _resume_on_graph(self.graph, thread_id, payload)
        finally:
            await self._end_workflow()

    async def shutdown(self):
        """Reject new work, wait for in-flight calls, then close the pool once."""
        async with self._shutdown_lock:
            if self._closed:
                return

            async with self._condition:
                self._closing = True
                await self._condition.wait_for(
                    lambda: self._active_workflows == 0
                )

            try:
                await close_checkpointer(self.connection_pool)
            except Exception:
                async with self._condition:
                    self._closing = False
                    self._condition.notify_all()
                raise

            async with self._condition:
                self._closed = True
                self._condition.notify_all()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.shutdown()


_default_runtime: Optional[WorkflowRuntime] = None
_default_runtime_lock = asyncio.Lock()


async def initialize_workflow_runtime() -> WorkflowRuntime:
    """Initialize and retain the process-default runtime, or return it if ready."""
    global _default_runtime
    async with _default_runtime_lock:
        if _default_runtime is None or _default_runtime._closed:
            _default_runtime = await WorkflowRuntime.initialize()
        return _default_runtime


async def shutdown_workflow_runtime():
    """Shut down the process-default runtime during application shutdown."""
    global _default_runtime
    async with _default_runtime_lock:
        if _default_runtime is not None:
            await _default_runtime.shutdown()
            _default_runtime = None


async def start_workflow(
    topic: str,
    as_of: Optional[str] = None,
    thread_id: Optional[str] = None,
    *,
    graph=None,
    checkpointer=None,
    runtime: Optional[WorkflowRuntime] = None,
):
    """Start a workflow using an explicit runtime or graph, or the shared default."""
    if runtime is not None:
        if graph is not None or checkpointer is not None:
            raise ValueError("Pass runtime, graph, or checkpointer, not multiple.")
        return await runtime.start_workflow(topic, as_of, thread_id)

    if graph is None:
        if checkpointer is not None:
            graph = build_workflow_graph(checkpointer)
        else:
            runtime = await initialize_workflow_runtime()
            return await runtime.start_workflow(topic, as_of, thread_id)
    return await _start_on_graph(graph, topic, as_of, thread_id)


async def resume_workflow(
    thread_id: str,
    payload: Dict[str, Any],
    *,
    graph=None,
    checkpointer=None,
    runtime: Optional[WorkflowRuntime] = None,
):
    """Resume using an explicit runtime or graph, or the shared default."""
    if runtime is not None:
        if graph is not None or checkpointer is not None:
            raise ValueError("Pass runtime, graph, or checkpointer, not multiple.")
        return await runtime.resume_workflow(thread_id, payload)

    if graph is None:
        if checkpointer is not None:
            graph = build_workflow_graph(checkpointer)
        else:
            runtime = await initialize_workflow_runtime()
            return await runtime.resume_workflow(thread_id, payload)
    return await _resume_on_graph(graph, thread_id, payload)
