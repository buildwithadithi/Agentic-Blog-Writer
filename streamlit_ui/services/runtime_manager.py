import asyncio
import selectors
import sys
import threading
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from queue import Queue
from typing import Any


RuntimeFactory = Callable[[], Awaitable[Any]]
_STREAM_COMPLETE = object()


async def _initialize_workflow_runtime():
    from blog_writer.workflow import WorkflowRuntime

    return await WorkflowRuntime.initialize()


class RuntimeManager:
    """Own a workflow runtime on a dedicated, process-lifetime event loop."""

    def __init__(
        self,
        runtime_factory: RuntimeFactory | None = None,
        *,
        oauth_registry_factory: Callable[[], Any] | None = None,
    ):
        self._runtime_factory = runtime_factory or _initialize_workflow_runtime
        self._oauth_registry_factory = oauth_registry_factory
        self._oauth_registry = None
        self._condition = threading.Condition()
        self._state = "stopped"
        self._runtime = None
        self._runtime_initializing = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_thread_id: int | None = None
        self._loop_ready = threading.Event()
        self._startup_complete = threading.Event()
        self._startup_error: Exception | None = None
        self._shutdown_error: Exception | None = None
        self._shutdown_in_progress = False
        self._active_operations = 0
        self._active_thread_ids: set[str] = set()
        self._loop_error: Exception | None = None

    def initialize(self) -> None:
        """Start the loop and initialize one runtime, or reuse the running one."""
        self._ensure_event_loop()
        with self._condition:
            while self._runtime_initializing:
                self._condition.wait()
            if self._runtime is not None:
                return
            if self._state != "running":
                raise RuntimeError("Workflow runtime manager is not accepting work.")
            loop = self._loop
            if loop is None or loop.is_closed():
                raise RuntimeError("Workflow event loop is not available.")
            if threading.get_ident() == self._loop_thread_id:
                raise RuntimeError(
                    "Workflow runtime cannot be initialized from its event loop."
                )
            self._runtime_initializing = True
            self._active_operations += 1

        try:
            initialization = asyncio.run_coroutine_threadsafe(
                self._create_runtime(),
                loop,
            )
            runtime = initialization.result()
        except Exception as exc:
            with self._condition:
                self._runtime_initializing = False
                self._active_operations -= 1
                self._condition.notify_all()
            raise
        with self._condition:
            self._runtime = runtime
            self._runtime_initializing = False
            self._active_operations -= 1
            self._condition.notify_all()

    def _ensure_event_loop(self) -> None:
        with self._condition:
            if self._state == "running":
                return
            if self._state == "stopping":
                raise RuntimeError("Workflow runtime manager is shutting down.")
            if self._state == "starting":
                startup_complete = self._startup_complete
                owner = False
            else:
                self._state = "starting"
                self._startup_error = None
                self._shutdown_error = None
                self._loop_error = None
                self._loop_ready = threading.Event()
                self._startup_complete = threading.Event()
                startup_complete = self._startup_complete
                owner = True
                self._thread = threading.Thread(
                    target=self._run_event_loop,
                    name="blog-writer-asyncio",
                    daemon=True,
                )
                try:
                    self._thread.start()
                except Exception as exc:
                    self._thread = None
                    self._state = "stopped"
                    self._startup_error = exc
                    self._startup_complete.set()
                    self._condition.notify_all()
                    raise

        if not owner:
            startup_complete.wait()
            with self._condition:
                if self._state == "running":
                    return
                error = self._startup_error
            if error is not None:
                raise RuntimeError("Workflow event loop initialization failed.") from error
            raise RuntimeError("Workflow event loop stopped during initialization.")

        try:
            self._loop_ready.wait()
            with self._condition:
                loop = self._loop
                thread_error = self._loop_error
            if thread_error is not None:
                raise RuntimeError("Could not start the workflow event loop.") from thread_error
            if loop is None:
                raise RuntimeError("Workflow event loop did not start.")

            with self._condition:
                self._state = "running"
                self._startup_complete.set()
                self._condition.notify_all()
        except Exception as exc:
            self._stop_event_loop_after_failed_start()
            with self._condition:
                self._loop = None
                self._thread = None
                self._loop_thread_id = None
                self._state = "stopped"
                self._startup_error = exc
                self._startup_complete.set()
                self._condition.notify_all()
            raise

    async def _create_runtime(self):
        return await self._runtime_factory()

    def start_workflow(
        self,
        topic: str,
        as_of: str | None = None,
        thread_id: str | None = None,
        *,
        timeout: float | None = None,
    ):
        return self._run_operation(
            "start_workflow",
            topic,
            as_of,
            thread_id,
            execution_thread_id=thread_id or None,
            timeout=timeout,
        )

    def resume_workflow(
        self,
        thread_id: str,
        payload: dict[str, Any],
        *,
        timeout: float | None = None,
    ):
        return self._run_operation(
            "resume_workflow",
            thread_id,
            payload,
            execution_thread_id=thread_id or None,
            timeout=timeout,
        )

    def stream_workflow(
        self,
        topic: str,
        thread_id: str,
        as_of: str | None = None,
    ):
        return self._stream_operation(
            "start_workflow_stream",
            (topic, as_of, thread_id),
            thread_id,
        )

    def stream_resume_workflow(
        self,
        thread_id: str,
        payload: dict[str, Any],
    ):
        return self._stream_operation(
            "resume_workflow_stream",
            (thread_id, payload),
            thread_id,
        )

    def start_wordpress_oauth(self):
        return self._run_oauth_operation("start_authorization")

    def get_wordpress_authorization_url(self, flow_id: str):
        return self._run_oauth_operation("get_authorization_url", flow_id)

    def complete_wordpress_oauth(self, flow_id: str, callback_url: str):
        return self._run_oauth_operation(
            "complete_authorization",
            flow_id,
            callback_url,
        )

    def cancel_wordpress_oauth(self, flow_id: str):
        return self._run_oauth_operation("cancel_authorization", flow_id)

    def check_wordpress_connection(self):
        return self._run_oauth_operation("check_connection")

    def _run_oauth_operation(self, method_name: str, *args):
        self._ensure_event_loop()
        with self._condition:
            if self._state != "running":
                raise RuntimeError("Workflow runtime manager is not accepting work.")
            if threading.get_ident() == self._loop_thread_id:
                raise RuntimeError(
                    "OAuth operations cannot synchronously call their own event loop."
                )

            loop = self._loop
            if loop is None or loop.is_closed():
                raise RuntimeError("Workflow event loop is not available.")

            self._active_operations += 1
            coroutine = self._invoke_oauth_registry(method_name, args)
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except Exception:
                coroutine.close()
                self._active_operations -= 1
                self._condition.notify_all()
                raise
            future.add_done_callback(
                lambda completed: self._operation_finished(completed)
            )

        return future.result()

    async def _invoke_oauth_registry(
        self,
        method_name: str,
        args: tuple[Any, ...],
    ):
        if self._oauth_registry is None:
            if self._oauth_registry_factory is None:
                from streamlit_ui.services.oauth_flow_registry import (
                    OAuthFlowRegistry,
                )

                self._oauth_registry = OAuthFlowRegistry()
            else:
                self._oauth_registry = self._oauth_registry_factory()
        operation = getattr(self._oauth_registry, method_name)
        return await operation(*args)

    def _stream_operation(
        self,
        method_name: str,
        args: tuple[Any, ...],
        thread_id: str,
    ):
        if not isinstance(thread_id, str) or not thread_id.strip():
            raise ValueError("thread_id must be a non-empty string.")

        self.initialize()
        events: Queue = Queue()
        with self._condition:
            if self._state != "running":
                raise RuntimeError("Workflow runtime manager is not accepting work.")
            if thread_id in self._active_thread_ids:
                raise RuntimeError("A workflow is already running for this thread_id.")
            if threading.get_ident() == self._loop_thread_id:
                raise RuntimeError(
                    "Workflow operations cannot synchronously call their own event loop."
                )

            loop = self._loop
            if loop is None or loop.is_closed():
                raise RuntimeError("Workflow event loop is not available.")

            self._active_operations += 1
            self._active_thread_ids.add(thread_id)
            coroutine = self._invoke_runtime(
                method_name,
                (*args, events.put),
            )
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except Exception:
                coroutine.close()
                self._active_operations -= 1
                self._active_thread_ids.discard(thread_id)
                self._condition.notify_all()
                raise
            future.add_done_callback(
                lambda completed: self._operation_finished(
                    completed,
                    execution_thread_id=thread_id,
                    completion_queue=events,
                )
            )

        def consume_events():
            while True:
                event = events.get()
                if event is _STREAM_COMPLETE:
                    break
                yield {"type": "progress", "event": event}

            yield {
                "type": "result",
                "response": future.result(),
            }

        return consume_events()

    def get_workflow_state(
        self,
        thread_id: str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        return self._run_operation(
            "get_workflow_state",
            thread_id,
            timeout=timeout,
        )

    def _run_operation(
        self,
        method_name: str,
        *args,
        execution_thread_id: str | None = None,
        timeout: float | None,
    ):
        self.initialize()
        with self._condition:
            if self._state != "running":
                raise RuntimeError("Workflow runtime manager is not accepting work.")
            if (
                execution_thread_id is not None
                and execution_thread_id in self._active_thread_ids
            ):
                raise RuntimeError("A workflow is already running for this thread_id.")
            if threading.get_ident() == self._loop_thread_id:
                raise RuntimeError(
                    "Workflow operations cannot synchronously call their own event loop."
                )

            loop = self._loop
            if loop is None or loop.is_closed():
                raise RuntimeError("Workflow event loop is not available.")

            self._active_operations += 1
            if execution_thread_id is not None:
                self._active_thread_ids.add(execution_thread_id)
            coroutine = self._invoke_runtime(method_name, args)
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, loop)
            except Exception:
                coroutine.close()
                self._active_operations -= 1
                if execution_thread_id is not None:
                    self._active_thread_ids.discard(execution_thread_id)
                self._condition.notify_all()
                raise
            future.add_done_callback(
                lambda completed: self._operation_finished(
                    completed,
                    execution_thread_id=execution_thread_id,
                )
            )

        return future.result(timeout=timeout)

    async def _invoke_runtime(self, method_name: str, args: tuple[Any, ...]):
        runtime = self._runtime
        if runtime is None:
            raise RuntimeError("Workflow runtime has not been initialized.")
        operation = getattr(runtime, method_name)
        return await operation(*args)

    def _operation_finished(
        self,
        _: Future,
        *,
        execution_thread_id: str | None = None,
        completion_queue: Queue | None = None,
    ) -> None:
        with self._condition:
            self._active_operations -= 1
            if execution_thread_id is not None:
                self._active_thread_ids.discard(execution_thread_id)
            self._condition.notify_all()
        if completion_queue is not None:
            completion_queue.put(_STREAM_COMPLETE)

    def shutdown(self) -> None:
        """Stop accepting work, close the runtime, then stop and join its loop."""
        with self._condition:
            if threading.get_ident() == self._loop_thread_id:
                raise RuntimeError(
                    "Workflow runtime manager cannot shut down from its event loop."
                )
            while self._state == "starting":
                self._condition.wait()

            if self._state == "stopped":
                return

            if self._shutdown_in_progress:
                while self._shutdown_in_progress:
                    self._condition.wait()
                if self._state == "stopped":
                    return
                if self._shutdown_error is not None:
                    raise RuntimeError("Workflow runtime shutdown failed.") from self._shutdown_error

            self._state = "stopping"
            self._shutdown_in_progress = True
            self._shutdown_error = None
            while self._active_operations:
                self._condition.wait()

            loop = self._loop
            thread = self._thread

        shutdown_error = None
        if loop is not None:
            if (
                thread is None
                or not thread.is_alive()
                or not loop.is_running()
            ):
                shutdown_error = RuntimeError(
                    "Workflow event loop exited before runtime shutdown."
                )
            else:
                if self._oauth_registry is not None:
                    try:
                        cleanup_future = asyncio.run_coroutine_threadsafe(
                            self._shutdown_oauth_registry(),
                            loop,
                        )
                        cleanup_future.result()
                    except Exception as exc:
                        shutdown_error = exc

                if self._runtime is not None:
                    try:
                        close_future = asyncio.run_coroutine_threadsafe(
                            self._close_runtime(),
                            loop,
                        )
                        close_future.result()
                    except Exception as exc:
                        shutdown_error = shutdown_error or exc

                loop.call_soon_threadsafe(loop.stop)
                thread.join()
                shutdown_error = shutdown_error or self._loop_error
        elif self._runtime is not None or self._oauth_registry is not None:
            shutdown_error = RuntimeError(
                "Workflow event loop exited before runtime shutdown."
            )

        with self._condition:
            self._runtime = None
            self._oauth_registry = None
            self._loop = None
            self._thread = None
            self._loop_thread_id = None
            self._runtime_initializing = False
            self._state = "stopped"
            self._shutdown_in_progress = False
            self._shutdown_error = shutdown_error
            self._condition.notify_all()

        if shutdown_error is not None:
            raise RuntimeError("Workflow runtime shutdown failed.") from shutdown_error

    async def _shutdown_oauth_registry(self) -> None:
        registry = self._oauth_registry
        if registry is not None:
            await registry.shutdown()

    async def _close_runtime(self) -> None:
        runtime = self._runtime
        if runtime is not None:
            await runtime.shutdown()

    def _run_event_loop(self) -> None:
        try:
            if sys.platform == "win32":
                loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
            else:
                loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            with self._condition:
                self._loop = loop
                self._loop_thread_id = threading.get_ident()
                self._loop_ready.set()
                self._condition.notify_all()

            loop.run_forever()
        except Exception as exc:
            with self._condition:
                self._loop_error = exc
                self._loop_ready.set()
                self._condition.notify_all()
        finally:
            if self._loop is not None and not self._loop.is_closed():
                try:
                    pending = asyncio.all_tasks(self._loop)
                    for task in pending:
                        task.cancel()
                    if pending:
                        self._loop.run_until_complete(
                            asyncio.gather(*pending, return_exceptions=True)
                        )
                    self._loop.run_until_complete(self._loop.shutdown_asyncgens())
                    self._loop.run_until_complete(self._loop.shutdown_default_executor())
                except Exception as exc:
                    with self._condition:
                        self._loop_error = self._loop_error or exc
                finally:
                    self._loop.close()
                    asyncio.set_event_loop(None)

            with self._condition:
                self._loop_ready.set()
                self._condition.notify_all()

    def _stop_event_loop_after_failed_start(self) -> None:
        with self._condition:
            loop = self._loop
            thread = self._thread

        if thread is not None and thread.is_alive():
            if loop is not None and loop.is_running():
                loop.call_soon_threadsafe(loop.stop)
            thread.join()

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()
