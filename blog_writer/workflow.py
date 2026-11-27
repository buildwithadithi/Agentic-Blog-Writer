import asyncio
import inspect
import re
from collections import defaultdict, deque
from datetime import date
from typing import Any, Callable, Dict, Optional
from uuid import uuid4

from langgraph.types import Command

from blog_writer.graph import build_graph


ProgressCallback = Callable[[Dict[str, Any]], None]


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


def _workflow_state_response(thread_id: str, snapshot: Any) -> Dict[str, Any]:
    values = snapshot.values
    if not isinstance(values, dict):
        raise TypeError("Checkpoint state must be a mapping.")

    tasks = snapshot.tasks
    interrupts = snapshot.interrupts
    has_checkpoint = bool(
        snapshot.metadata is not None
        or snapshot.created_at is not None
        or values
        or snapshot.next
        or tasks
        or interrupts
    )
    interrupted_tasks = [
        task.name
        for task in tasks
        if getattr(task, "interrupts", ())
    ]

    if not has_checkpoint:
        status = "not_found"
    elif any(getattr(task, "error", None) for task in tasks):
        status = "failed"
    elif interrupts and "human_review" in interrupted_tasks:
        status = "paused_for_review"
    elif interrupts:
        status = "interrupted"
    elif snapshot.next:
        status = "in_progress"
    else:
        status = "completed"

    final = values.get("final", "")
    image_references = (
        re.findall(r'<img\b[^>]*\bsrc=["\']([^"\']+)["\']', final, re.IGNORECASE)
        if isinstance(final, str)
        else []
    )

    return {
        "thread_id": thread_id,
        "status": status,
        "topic": values.get("topic"),
        "plan": _serialize(values.get("plan")),
        "evidence": _serialize(values.get("evidence", [])),
        "sections": _serialize(values.get("sections", [])),
        "final": final,
        "image_specs": _serialize(values.get("image_specs", [])),
        "image_references": image_references,
        "review_status": values.get("review_status", ""),
        "revision_count": values.get("revision_count", 0),
        "wordpress_post_id": values.get("wordpress_post_id"),
        "wordpress_post_url": values.get("wordpress_post_url"),
        "wordpress_status": values.get("wordpress_status"),
        "next": list(snapshot.next),
        "interrupted_at": interrupted_tasks,
        "interrupts": [
            _serialize(interrupt.value)
            for interrupt in interrupts
        ],
        "created_at": snapshot.created_at,
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


async def _stream_graph(graph, input_value, thread, progress_callback: ProgressCallback):
    pending_by_node = defaultdict(deque)
    pending_by_section = defaultdict(deque)
    worker_info = {}

    async for part in graph.astream(
        input_value,
        config=thread["config"],
        stream_mode=["updates", "debug"],
        subgraphs=True,
        version="v2",
    ):
        event_type = part.get("type")
        data = part.get("data", {})

        if event_type == "debug":
            debug_type = data.get("type")
            payload = data.get("payload", {})
            node = payload.get("name")
            execution_id = payload.get("id")
            if not node or not execution_id:
                continue

            if debug_type == "task":
                pending_by_node[node].append(execution_id)
                event = {
                    "event": "node_started",
                    "node": node,
                    "execution_id": execution_id,
                }
                if node == "worker":
                    worker_input = payload.get("input", {})
                    task = worker_input.get("task", {})
                    section_id = task.get("id")
                    section_title = task.get("title")
                    worker_info[execution_id] = {
                        "section_id": section_id,
                        "section_title": section_title,
                    }
                    if section_id is not None:
                        pending_by_section[section_id].append(execution_id)
                        event["section_id"] = section_id
                    if section_title:
                        event["section_title"] = section_title
                progress_callback(event)
            elif debug_type == "task_result":
                if payload.get("interrupts"):
                    event_name = "node_interrupted"
                elif payload.get("error") is not None:
                    event_name = "node_failed"
                else:
                    continue

                event = {
                    "event": event_name,
                    "node": node,
                    "execution_id": execution_id,
                }
                event.update(worker_info.pop(execution_id, {}))
                progress_callback(event)

        elif event_type == "updates":
            for node, update in data.items():
                if node == "__interrupt__" or not isinstance(update, dict):
                    continue

                if node == "orchestrator":
                    plan = _serialize(update.get("plan"))
                    tasks = plan.get("tasks", []) if isinstance(plan, dict) else []
                    progress_callback(
                        {
                            "event": "plan_ready",
                            "node": node,
                            "section_total": len(tasks),
                            "section_titles": [
                                task.get("title", "")
                                for task in tasks
                                if isinstance(task, dict)
                            ],
                        }
                    )

                if node == "worker":
                    sections = update.get("sections", [])
                    for section in sections:
                        if not isinstance(section, dict):
                            continue
                        section_id = section.get("task_id")
                        executions = pending_by_section.get(section_id)
                        execution_id = executions.popleft() if executions else None
                        event = {
                            "event": "node_completed",
                            "node": node,
                            "execution_id": execution_id,
                            "section_id": section_id,
                        }
                        if execution_id is not None:
                            event.update(worker_info.pop(execution_id, {}))
                        progress_callback(event)
                    continue

                executions = pending_by_node.get(node)
                execution_id = executions.popleft() if executions else None
                worker_info.pop(execution_id, None)
                progress_callback(
                    {
                        "event": "node_completed",
                        "node": node,
                        "execution_id": execution_id,
                    }
                )

    snapshot = await graph.aget_state(thread["config"])
    result = dict(snapshot.values)
    if snapshot.interrupts:
        result["__interrupt__"] = list(snapshot.interrupts)
    return _workflow_response(thread["thread_id"], result)


async def _start_on_graph(
    graph,
    topic: str,
    as_of: Optional[str],
    thread_id: Optional[str],
    progress_callback: Optional[ProgressCallback] = None,
):
    thread = make_thread_config(thread_id)
    initial_state = _initial_state(topic, as_of)
    if progress_callback is not None:
        return await _stream_graph(
            graph,
            initial_state,
            thread,
            progress_callback,
        )

    result = await graph.ainvoke(
        initial_state,
        config=thread["config"],
    )
    return _workflow_response(thread["thread_id"], result)


async def _resume_on_graph(
    graph,
    thread_id: str,
    payload: Dict[str, Any],
    progress_callback: Optional[ProgressCallback] = None,
):
    thread = make_thread_config(thread_id)
    resume_command = Command(resume=payload)
    if progress_callback is not None:
        return await _stream_graph(
            graph,
            resume_command,
            thread,
            progress_callback,
        )

    result = await graph.ainvoke(
        resume_command,
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

    async def start_workflow_stream(
        self,
        topic: str,
        as_of: Optional[str],
        thread_id: str,
        progress_callback: ProgressCallback,
    ):
        await self._begin_workflow()
        try:
            return await _start_on_graph(
                self.graph,
                topic,
                as_of,
                thread_id,
                progress_callback,
            )
        finally:
            await self._end_workflow()

    async def resume_workflow(self, thread_id: str, payload: Dict[str, Any]):
        await self._begin_workflow()
        try:
            return await _resume_on_graph(self.graph, thread_id, payload)
        finally:
            await self._end_workflow()

    async def resume_workflow_stream(
        self,
        thread_id: str,
        payload: Dict[str, Any],
        progress_callback: ProgressCallback,
    ):
        await self._begin_workflow()
        try:
            return await _resume_on_graph(
                self.graph,
                thread_id,
                payload,
                progress_callback,
            )
        finally:
            await self._end_workflow()

    async def get_workflow_state(self, thread_id: str) -> Dict[str, Any]:
        if not isinstance(thread_id, str) or not thread_id.strip():
            raise ValueError("thread_id must be a non-empty string.")

        thread = make_thread_config(thread_id)
        await self._begin_workflow()
        try:
            snapshot = await self.graph.aget_state(thread["config"])
            return _workflow_state_response(thread["thread_id"], snapshot)
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
