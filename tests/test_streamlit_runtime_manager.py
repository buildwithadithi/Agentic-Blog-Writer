import asyncio
import importlib
import os
import selectors
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from streamlit_ui.services.runtime_manager import RuntimeManager


class FakeWorkflowRuntime:
    def __init__(
        self,
        *,
        block_start=False,
        block_state=False,
        block_stream=False,
        block_resume_stream=False,
    ):
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = None
        self.loop_thread_id = threading.get_ident()
        self.started = threading.Event()
        self.release_start = asyncio.Event()
        self.block_start = block_start
        self.state_started = threading.Event()
        self.release_state = asyncio.Event()
        self.block_state = block_state
        self.stream_started = threading.Event()
        self.release_stream = asyncio.Event()
        self.block_stream = block_stream
        self.resume_stream_started = threading.Event()
        self.release_resume_stream = asyncio.Event()
        self.block_resume_stream = block_resume_stream
        self.shutdown_calls = 0
        self.pool_close_calls = 0
        self.calls = []
        self.call_thread_ids = []

    async def start_workflow(self, topic, as_of=None, thread_id=None):
        self.calls.append(("start", topic, as_of, thread_id))
        self.call_thread_ids.append(threading.get_ident())
        self.started.set()
        if self.block_start:
            await self.release_start.wait()
        return {"topic": topic, "thread_id": thread_id}

    async def resume_workflow(self, thread_id, payload):
        self.calls.append(("resume", thread_id, payload))
        self.call_thread_ids.append(threading.get_ident())
        return {"thread_id": thread_id, "payload": payload}

    async def get_workflow_state(self, thread_id):
        self.calls.append(("get_state", thread_id))
        self.call_thread_ids.append(threading.get_ident())
        self.state_started.set()
        if self.block_state:
            await self.release_state.wait()
        return {"thread_id": thread_id, "status": "paused_for_review"}

    async def start_workflow_stream(
        self,
        topic,
        as_of,
        thread_id,
        progress_callback,
    ):
        self.calls.append(("stream", topic, as_of, thread_id))
        self.call_thread_ids.append(threading.get_ident())
        progress_callback(
            {
                "event": "node_started",
                "node": "router",
                "execution_id": "router-run",
            }
        )
        self.stream_started.set()
        if self.block_stream:
            await self.release_stream.wait()
        progress_callback(
            {
                "event": "node_completed",
                "node": "router",
                "execution_id": "router-run",
            }
        )
        return {
            "thread_id": thread_id,
            "status": "paused",
            "result": {"final": "# Generated"},
            "interrupt": {"message": "Review the generated blog"},
        }

    async def resume_workflow_stream(
        self,
        thread_id,
        payload,
        progress_callback,
    ):
        self.calls.append(("resume_stream", thread_id, payload))
        self.call_thread_ids.append(threading.get_ident())
        progress_callback(
            {
                "event": "node_started",
                "node": "revision",
                "execution_id": "revision-run",
            }
        )
        self.resume_stream_started.set()
        if self.block_resume_stream:
            await self.release_resume_stream.wait()
        progress_callback(
            {
                "event": "node_completed",
                "node": "revision",
                "execution_id": "revision-run",
            }
        )
        return {
            "thread_id": thread_id,
            "status": "paused",
            "result": {"final": "# Revised"},
            "interrupt": {"message": "Review the generated blog"},
        }

    async def shutdown(self):
        self.shutdown_calls += 1
        self.pool_close_calls += 1


class RuntimeManagerTests(unittest.TestCase):
    def setUp(self):
        self.runtimes = []
        self.factory_calls = 0
        self.blocking_runtime_ready = threading.Event()

        async def factory():
            self.factory_calls += 1
            runtime = FakeWorkflowRuntime()
            self.runtimes.append(runtime)
            return runtime

        self.manager = RuntimeManager(factory)
        self.addCleanup(self.manager.shutdown)

    def test_initialization_creates_one_dedicated_event_loop_thread(self):
        self.manager.initialize()
        self.manager.initialize()

        runtime = self.runtimes[0]
        self.assertEqual(self.factory_calls, 1)
        self.assertNotEqual(runtime.loop_thread_id, threading.get_ident())
        self.assertTrue(runtime.loop.is_running())

    def test_windows_uses_selector_loop_compatible_with_async_psycopg(self):
        if sys.platform != "win32":
            self.skipTest("Windows psycopg event-loop compatibility check.")

        self.manager.initialize()
        loop = self.runtimes[0].loop

        self.assertIsInstance(loop, asyncio.SelectorEventLoop)
        self.assertIsInstance(loop._selector, selectors.SelectSelector)

    def test_start_and_resume_run_on_the_dedicated_loop(self):
        started = self.manager.start_workflow(
            "Topic",
            as_of="2026-11-27",
            thread_id="thread-a",
        )
        resumed = self.manager.resume_workflow(
            "thread-a",
            {"action": "approved"},
        )

        self.assertEqual(
            started,
            {"topic": "Topic", "thread_id": "thread-a"},
        )
        self.assertEqual(
            resumed,
            {
                "thread_id": "thread-a",
                "payload": {"action": "approved"},
            },
        )
        runtime = self.runtimes[0]
        self.assertEqual(self.factory_calls, 1)
        self.assertTrue(all(call[0] == "start" for call in runtime.calls[:1]))
        self.assertEqual(runtime.calls[0], ("start", "Topic", "2026-11-27", "thread-a"))
        self.assertEqual(runtime.calls[1], ("resume", "thread-a", {"action": "approved"}))
        self.assertEqual(set(runtime.call_thread_ids), {runtime.loop_thread_id})

    def test_state_read_is_delegated_to_the_dedicated_loop(self):
        state = self.manager.get_workflow_state("state-thread")

        self.assertEqual(
            state,
            {"thread_id": "state-thread", "status": "paused_for_review"},
        )
        runtime = self.runtimes[0]
        self.assertEqual(runtime.calls, [("get_state", "state-thread")])
        self.assertEqual(runtime.call_thread_ids, [runtime.loop_thread_id])

    def test_stream_progress_reaches_caller_thread_and_returns_result(self):
        events = list(self.manager.stream_workflow("Topic", "stream-thread"))

        self.assertEqual(events[0]["type"], "progress")
        self.assertEqual(events[0]["event"]["node"], "router")
        self.assertEqual(events[-1]["type"], "result")
        self.assertEqual(
            events[-1]["response"]["thread_id"],
            "stream-thread",
        )
        runtime = self.runtimes[0]
        self.assertEqual(runtime.calls, [("stream", "Topic", None, "stream-thread")])
        self.assertEqual(runtime.call_thread_ids, [runtime.loop_thread_id])
        self.assertNotEqual(runtime.loop_thread_id, threading.get_ident())

    def test_resume_stream_uses_same_thread_and_transfers_progress(self):
        events = list(
            self.manager.stream_resume_workflow(
                "review-thread",
                {
                    "action": "revision_requested",
                    "feedback": "Clarify this.",
                },
            )
        )

        self.assertEqual(events[0]["type"], "progress")
        self.assertEqual(events[0]["event"]["node"], "revision")
        self.assertEqual(events[-1]["type"], "result")
        self.assertEqual(
            events[-1]["response"]["thread_id"],
            "review-thread",
        )
        runtime = self.runtimes[0]
        self.assertEqual(
            runtime.calls,
            [
                (
                    "resume_stream",
                    "review-thread",
                    {
                        "action": "revision_requested",
                        "feedback": "Clarify this.",
                    },
                )
            ],
        )
        self.assertEqual(runtime.call_thread_ids, [runtime.loop_thread_id])

    def test_duplicate_resume_stream_is_rejected_and_shutdown_waits(self):
        runtime_ready = threading.Event()
        runtime_box = []

        async def factory():
            runtime = FakeWorkflowRuntime(block_resume_stream=True)
            runtime_box.append(runtime)
            runtime_ready.set()
            return runtime

        manager = RuntimeManager(factory)
        self.addCleanup(manager.shutdown)
        stream_result = []
        stream_thread = threading.Thread(
            target=lambda: stream_result.extend(
                manager.stream_resume_workflow(
                    "shared-review-thread",
                    {"action": "approved"},
                )
            )
        )
        stream_thread.start()
        self.assertTrue(runtime_ready.wait(timeout=5))
        runtime = runtime_box[0]
        self.assertTrue(runtime.resume_stream_started.wait(timeout=5))

        with self.assertRaisesRegex(RuntimeError, "already running"):
            manager.stream_resume_workflow(
                "shared-review-thread",
                {"action": "approved"},
            )

        shutdown_done = threading.Event()
        shutdown = threading.Thread(
            target=lambda: self._shutdown_and_signal(manager, shutdown_done)
        )
        shutdown.start()
        try:
            self._wait_for_state(manager, "stopping")
            self.assertFalse(shutdown_done.is_set())
        finally:
            runtime.loop.call_soon_threadsafe(runtime.release_resume_stream.set)

        stream_thread.join(timeout=5)
        shutdown.join(timeout=5)
        self.assertFalse(stream_thread.is_alive())
        self.assertFalse(shutdown.is_alive())
        self.assertTrue(shutdown_done.is_set())
        self.assertEqual(stream_result[-1]["type"], "result")
        self.assertEqual(runtime.shutdown_calls, 1)

    def test_duplicate_stream_thread_is_rejected_and_shutdown_waits(self):
        runtime_ready = threading.Event()
        runtime_box = []

        async def factory():
            runtime = FakeWorkflowRuntime(block_stream=True)
            runtime_box.append(runtime)
            runtime_ready.set()
            return runtime

        manager = RuntimeManager(factory)
        self.addCleanup(manager.shutdown)
        stream_result = []
        stream_thread = threading.Thread(
            target=lambda: stream_result.extend(
                manager.stream_workflow("Topic", "shared-thread")
            )
        )
        stream_thread.start()
        self.assertTrue(runtime_ready.wait(timeout=5))
        runtime = runtime_box[0]
        self.assertTrue(runtime.stream_started.wait(timeout=5))

        with self.assertRaisesRegex(RuntimeError, "already running"):
            manager.stream_workflow("Another topic", "shared-thread")

        shutdown_done = threading.Event()
        shutdown = threading.Thread(
            target=lambda: self._shutdown_and_signal(manager, shutdown_done)
        )
        shutdown.start()
        try:
            self._wait_for_state(manager, "stopping")
            self.assertFalse(shutdown_done.is_set())
        finally:
            runtime.loop.call_soon_threadsafe(runtime.release_stream.set)

        stream_thread.join(timeout=5)
        shutdown.join(timeout=5)
        self.assertFalse(stream_thread.is_alive())
        self.assertFalse(shutdown.is_alive())
        self.assertTrue(shutdown_done.is_set())
        self.assertEqual(stream_result[-1]["type"], "result")
        self.assertEqual(runtime.shutdown_calls, 1)

    def test_concurrent_initialization_and_calls_create_only_one_runtime(self):
        entered_factory = threading.Event()
        release_factory = threading.Event()
        runtimes = []

        async def gated_factory():
            entered_factory.set()
            await asyncio.to_thread(release_factory.wait)
            runtime = FakeWorkflowRuntime()
            runtimes.append(runtime)
            return runtime

        manager = RuntimeManager(gated_factory)
        self.addCleanup(manager.shutdown)
        barrier = threading.Barrier(6)

        def initialize_and_start(index):
            barrier.wait()
            manager.initialize()
            return manager.start_workflow(f"Topic {index}", thread_id=f"thread-{index}")

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [
                executor.submit(initialize_and_start, index)
                for index in range(6)
            ]
            self.assertTrue(entered_factory.wait(timeout=5))
            release_factory.set()
            results = [future.result(timeout=5) for future in futures]

        self.assertEqual(len(runtimes), 1)
        self.assertEqual(len(results), 6)
        self.assertEqual(len(runtimes[0].calls), 6)
        self.assertEqual(
            len({call[3] for call in runtimes[0].calls}),
            6,
        )

    def test_initialization_failure_cleans_up_and_allows_retry(self):
        calls = 0
        runtime = FakeWorkflowRuntime()

        async def factory():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("initialization failed")
            return runtime

        manager = RuntimeManager(factory)
        with self.assertRaisesRegex(ValueError, "initialization failed"):
            manager.initialize()

        self.assertEqual(manager._state, "running")
        self.assertIsNone(manager._runtime)
        self.assertTrue(manager._loop.is_running())
        self.assertTrue(manager._thread.is_alive())
        manager.initialize()
        self.assertEqual(manager.start_workflow("Retry")["topic"], "Retry")
        manager.shutdown()
        self.assertEqual(runtime.shutdown_calls, 1)

    def test_operations_are_rejected_after_shutdown_begins(self):
        manager = RuntimeManager(self._blocking_factory)
        self.addCleanup(manager.shutdown)
        operation = threading.Thread(
            target=lambda: self._capture(
                lambda: manager.start_workflow("Blocking")
            )
        )
        operation.start()
        self.assertTrue(self.blocking_runtime_ready.wait(timeout=5))
        self.assertTrue(self.blocking_runtime.started.wait(timeout=5))
        self.addCleanup(self._release_blocking_operation)

        shutdown_done = threading.Event()
        shutdown = threading.Thread(
            target=lambda: self._shutdown_and_signal(manager, shutdown_done)
        )
        shutdown.start()
        try:
            self._wait_for_state(manager, "stopping")
            with self.assertRaisesRegex(RuntimeError, "shutting down"):
                manager.start_workflow("Rejected")
        finally:
            self.blocking_runtime.loop.call_soon_threadsafe(
                self.blocking_runtime.release_start.set
            )
        operation.join(timeout=5)
        shutdown.join(timeout=5)
        self.assertTrue(shutdown_done.is_set())
        self.assertFalse(operation.is_alive())
        self.assertFalse(shutdown.is_alive())

    def test_shutdown_waits_for_active_operation_and_closes_once(self):
        manager = RuntimeManager(self._blocking_factory)
        operation_results = []
        operation = threading.Thread(
            target=lambda: operation_results.append(
                manager.start_workflow("Blocking")
            )
        )
        operation.start()
        self.assertTrue(self.blocking_runtime_ready.wait(timeout=5))
        self.assertTrue(self.blocking_runtime.started.wait(timeout=5))
        self.addCleanup(self._release_blocking_operation)

        shutdown_done = threading.Event()
        shutdown = threading.Thread(
            target=lambda: self._shutdown_and_signal(manager, shutdown_done)
        )
        shutdown.start()
        try:
            self._wait_for_state(manager, "stopping")
            self.assertFalse(shutdown_done.is_set())
            self.assertEqual(self.blocking_runtime.shutdown_calls, 0)
        finally:
            self.blocking_runtime.loop.call_soon_threadsafe(
                self.blocking_runtime.release_start.set
            )
        operation.join(timeout=5)
        shutdown.join(timeout=5)

        self.assertEqual(operation_results, [{"topic": "Blocking", "thread_id": None}])
        self.assertTrue(shutdown_done.is_set())
        self.assertEqual(self.blocking_runtime.shutdown_calls, 1)
        self.assertEqual(self.blocking_runtime.pool_close_calls, 1)
        manager.shutdown()
        self.assertEqual(self.blocking_runtime.shutdown_calls, 1)
        self.assertEqual(self.blocking_runtime.pool_close_calls, 1)

    def test_state_read_is_tracked_and_rejected_during_shutdown(self):
        runtime_ready = threading.Event()
        runtime_box = []

        async def factory():
            runtime = FakeWorkflowRuntime(block_state=True)
            runtime_box.append(runtime)
            runtime_ready.set()
            return runtime

        manager = RuntimeManager(factory)
        state_results = []
        operation = threading.Thread(
            target=lambda: state_results.append(
                manager.get_workflow_state("active-state-thread")
            )
        )
        operation.start()
        self.assertTrue(runtime_ready.wait(timeout=5))
        runtime = runtime_box[0]
        self.assertTrue(runtime.state_started.wait(timeout=5))

        shutdown_done = threading.Event()
        shutdown = threading.Thread(
            target=lambda: self._shutdown_and_signal(manager, shutdown_done)
        )
        shutdown.start()
        try:
            self._wait_for_state(manager, "stopping")
            self.assertFalse(shutdown_done.is_set())
            with self.assertRaisesRegex(RuntimeError, "shutting down"):
                manager.get_workflow_state("rejected-state-thread")
        finally:
            runtime.loop.call_soon_threadsafe(runtime.release_state.set)

        operation.join(timeout=5)
        shutdown.join(timeout=5)
        self.assertEqual(
            state_results,
            [{"thread_id": "active-state-thread", "status": "paused_for_review"}],
        )
        self.assertTrue(shutdown_done.is_set())
        self.assertEqual(runtime.calls, [("get_state", "active-state-thread")])
        self.assertEqual(runtime.shutdown_calls, 1)
        manager.shutdown()

    def test_manager_can_be_initialized_again_after_shutdown(self):
        first = FakeWorkflowRuntime()
        second = FakeWorkflowRuntime()
        runtimes = iter((first, second))
        factory_calls = 0

        async def factory():
            nonlocal factory_calls
            factory_calls += 1
            return next(runtimes)

        manager = RuntimeManager(factory)
        manager.initialize()
        manager.start_workflow("First")
        manager.shutdown()
        manager.initialize()
        manager.start_workflow("Second")
        manager.shutdown()

        self.assertEqual(factory_calls, 2)
        self.assertEqual(first.shutdown_calls, 1)
        self.assertEqual(second.shutdown_calls, 1)
        self.assertEqual(first.pool_close_calls, 1)
        self.assertEqual(second.pool_close_calls, 1)

    def test_checkpointer_setup_failure_closes_the_open_pool(self):
        with patch.dict(
            os.environ,
            {"SUPABASE_DB_URI": "postgresql://test:test@localhost/test"},
        ):
            checkpointer_module = importlib.import_module("checkpointer")

        pool = SimpleNamespace(
            open=AsyncMock(),
            close=AsyncMock(),
        )
        saver = SimpleNamespace(setup=AsyncMock(side_effect=ValueError("setup failed")))

        with (
            patch.object(checkpointer_module, "AsyncConnectionPool", return_value=pool),
            patch.object(checkpointer_module, "AsyncPostgresSaver", return_value=saver),
        ):
            with self.assertRaisesRegex(ValueError, "setup failed"):
                asyncio.run(checkpointer_module.initialize_checkpointer())

        pool.open.assert_awaited_once()
        pool.close.assert_awaited_once()
        saver.setup.assert_awaited_once()

    async def _blocking_factory(self):
        self.blocking_runtime = FakeWorkflowRuntime(block_start=True)
        self.blocking_runtime_ready.set()
        return self.blocking_runtime

    def _release_blocking_operation(self):
        runtime = getattr(self, "blocking_runtime", None)
        if runtime is not None and runtime.loop is not None and runtime.loop.is_running():
            runtime.loop.call_soon_threadsafe(runtime.release_start.set)

    @staticmethod
    def _capture(operation):
        operation()

    @staticmethod
    def _shutdown_and_signal(manager, done):
        try:
            manager.shutdown()
        finally:
            done.set()

    @staticmethod
    def _wait_for_state(manager, expected_state):
        with manager._condition:
            reached = manager._condition.wait_for(
                lambda: manager._state == expected_state,
                timeout=5,
            )
        if not reached:
            raise AssertionError(f"Manager did not reach {expected_state!r}.")


if __name__ == "__main__":
    unittest.main()
