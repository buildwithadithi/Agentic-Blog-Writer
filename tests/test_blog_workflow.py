import importlib
import asyncio
import os
import re
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import TypedDict
from unittest.mock import AsyncMock, patch

os.environ.setdefault("GOOGLE_API_KEY", "unit-test-placeholder")

from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from blog_writer.models import (
    EvidenceItem,
    EvidencePack,
    GlobalImagePlan,
    ImageSpec,
    Plan,
    RouterDecision,
    Task,
)
from blog_writer import workflow
from blog_writer.nodes import planning, research, review, router, wordpress, writing
from blog_writer.serialization import create_checkpoint_serializer
from blog_writer.subgraphs import reducer


def make_plan():
    return Plan(
        blog_title="Test Blog",
        audience="Developers",
        tone="Clear",
        tasks=[
            Task(
                id=2,
                title="Second Section",
                goal="Explain the second section.",
                bullets=["Point A", "Point B", "Point C"],
                target_words=120,
            ),
            Task(
                id=1,
                title="First Section",
                goal="Explain the first section.",
                bullets=["Point A", "Point B", "Point C"],
                target_words=120,
            ),
        ],
    )


class FakeLLM:
    def __init__(self, *, needs_research=False, evidence=None, image_plan=None):
        self.needs_research = needs_research
        self.evidence = evidence or []
        self.image_plan = image_plan or GlobalImagePlan(md_with_placeholders="")
        self.plan = make_plan()
        self.structured_calls = []
        self.worker_calls = []
        self.revision_calls = []

    def with_structured_output(self, schema):
        owner = self

        class StructuredRunner:
            def invoke(self, messages):
                owner.structured_calls.append((schema, messages))
                if schema is RouterDecision:
                    return RouterDecision(
                        needs_research=owner.needs_research,
                        mode="hybrid" if owner.needs_research else "closed_book",
                        queries=["test query"] if owner.needs_research else [],
                    )
                if schema is EvidencePack:
                    return EvidencePack(evidence=owner.evidence)
                if schema is Plan:
                    return owner.plan
                if schema is GlobalImagePlan:
                    return owner.image_plan
                raise AssertionError(f"Unexpected structured output schema: {schema}")

        return StructuredRunner()

    def invoke(self, prompt):
        if isinstance(prompt, str):
            self.revision_calls.append(prompt)
            return SimpleNamespace(content="# Revised Blog\n\nUpdated after feedback.")

        human_content = "\n".join(
            str(message.content)
            for message in prompt
            if hasattr(message, "content")
        )
        self.worker_calls.append(human_content)
        match = re.search(r"Section title: ([^\n]+)", human_content)
        title = match.group(1) if match else "Untitled Section"
        return SimpleNamespace(content=f"## {title}\n\nGenerated content for {title}.")


class FakeWordPressClient:
    calls = []

    async def connect(self):
        self.calls.append(("connect",))

    async def upload_images(self, content, image_specs=None):
        return content

    async def create_post(self, *, title, content, confirmed=False):
        self.calls.append(("create_post", title, content, confirmed))
        return {
            "id": 871,
            "link": "https://example.test/draft/871",
            "status": "draft",
        }

    async def close(self):
        self.calls.append(("close",))


class FakeWorkflowGraph:
    def __init__(self, results=None):
        self.results = iter(results or [])
        self.calls = []

    async def ainvoke(self, input_value, config=None):
        self.calls.append((input_value, config))
        return next(self.results)


class AsyncClosePool:
    def __init__(self):
        self.close_calls = 0

    async def close(self):
        self.close_calls += 1


class WorkflowIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.previous_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.addAsyncCleanup(self._cleanup_temp_dir)

        FakeWordPressClient.calls = []
        self.evidence = [
            EvidenceItem(
                title="Test Source",
                url="https://example.test/source",
                snippet="Evidence for the test.",
            )
        ]
        self.fake_llm = FakeLLM(evidence=self.evidence)

        self.patches = [
            patch.object(router, "llm", self.fake_llm),
            patch.object(research, "llm", self.fake_llm),
            patch.object(planning, "llm", self.fake_llm),
            patch.object(writing, "llm", self.fake_llm),
            patch.object(review, "llm", self.fake_llm),
            patch.object(reducer, "llm", self.fake_llm),
            patch.object(research, "_tavily_search", self.mock_tavily),
            patch.object(
                reducer,
                "_cloudflare_generate_image_bytes",
                lambda prompt: b"mock-image-bytes",
            ),
            patch.object(wordpress, "WordPressMCPClient", FakeWordPressClient),
        ]
        for mocked in self.patches:
            mocked.start()
            self.addCleanup(mocked.stop)

    async def _cleanup_temp_dir(self):
        os.chdir(self.previous_cwd)
        self.temp_dir.cleanup()

    def mock_tavily(self, query, max_results=5):
        self.assertEqual(query, "test query")
        self.assertEqual(max_results, 6)
        return [
            {
                "title": "Test Source",
                "url": "https://example.test/source",
                "snippet": "Evidence for the test.",
                "published_at": None,
                "source": "test",
            }
        ]

    def build_test_graph(self):
        return workflow.build_workflow_graph(
            MemorySaver(serde=create_checkpoint_serializer())
        )

    async def start_until_review(self, *, needs_research=False, thread_id="workflow-test"):
        self.fake_llm.needs_research = needs_research
        graph = self.build_test_graph()
        response = await workflow.start_workflow(
            "A test blog topic",
            as_of="2026-11-26",
            thread_id=thread_id,
            graph=graph,
        )
        self.assertEqual(response["status"], "paused")
        self.assertEqual(response["thread_id"], thread_id)
        return graph, response

    async def test_scenario_a_direct_writing_fanout_reducer_and_review(self):
        graph, response = await self.start_until_review()

        self.assertFalse(self.fake_llm.needs_research)
        self.assertEqual(response["interrupt"]["message"], "Review the generated blog")
        self.assertEqual(response["interrupt"]["revision_count"], 0)

        state = response["result"]
        self.assertEqual(state["mode"], "closed_book")
        self.assertEqual(state["evidence"], [])
        self.assertEqual([task["id"] for task in state["plan"]["tasks"]], [2, 1])
        self.assertEqual(len(state["sections"]), 2)
        self.assertEqual(len(self.fake_llm.worker_calls), 2)
        self.assertLess(
            state["final"].index("## First Section"),
            state["final"].index("## Second Section"),
        )
        self.assertEqual(FakeWordPressClient.calls, [])

    async def test_scenario_b_research_evidence_flows_to_workers(self):
        _, response = await self.start_until_review(
            needs_research=True,
            thread_id="research-thread",
        )

        state = response["result"]
        self.assertEqual(state["mode"], "hybrid")
        self.assertEqual(len(state["evidence"]), 1)
        self.assertEqual(state["evidence"][0]["url"], "https://example.test/source")
        self.assertTrue(
            any(
                "https://example.test/source" in str(message.content)
                for schema, messages in self.fake_llm.structured_calls
                if schema is Plan
                for message in messages
                if hasattr(message, "content")
            )
        )
        self.assertTrue(
            all(
                "https://example.test/source" in worker_prompt
                for worker_prompt in self.fake_llm.worker_calls
            )
        )
        self.assertEqual(len(state["sections"]), 2)

    async def test_scenario_c_approval_creates_wordpress_draft(self):
        graph, started = await self.start_until_review(thread_id="approval-thread")
        approved = await workflow.resume_workflow(
            "approval-thread",
            {"action": "approved"},
            graph=graph,
        )

        self.assertEqual(approved["status"], "completed")
        self.assertEqual(approved["thread_id"], "approval-thread")
        self.assertEqual(approved["result"]["review_status"], "approved")
        self.assertEqual(approved["result"]["wordpress_post_id"], 871)
        self.assertEqual(approved["result"]["wordpress_status"], "draft")

        create_calls = [call for call in FakeWordPressClient.calls if call[0] == "create_post"]
        self.assertEqual(len(create_calls), 1)
        _, title, content, confirmed = create_calls[0]
        self.assertEqual(title, "A test blog topic")
        self.assertEqual(content, started["result"]["final"])
        self.assertTrue(confirmed)
        self.assertIn(("close",), FakeWordPressClient.calls)
        self.assertNotEqual(approved["result"]["wordpress_status"], "publish")

    async def test_scenario_d_revision_returns_to_review(self):
        graph, started = await self.start_until_review(thread_id="revision-thread")
        resumed = await workflow.resume_workflow(
            "revision-thread",
            {
                "action": "revision_requested",
                "feedback": "Add a clearer conclusion.",
            },
            graph=graph,
        )

        self.assertEqual(resumed["status"], "paused")
        self.assertEqual(resumed["interrupt"]["message"], "Review the generated blog")
        self.assertEqual(resumed["interrupt"]["revision_count"], 1)
        self.assertEqual(resumed["result"]["revision_count"], 1)
        self.assertEqual(resumed["result"]["final"], "# Revised Blog\n\nUpdated after feedback.")
        self.assertIn("Add a clearer conclusion.", self.fake_llm.revision_calls[0])
        self.assertEqual(len(self.fake_llm.worker_calls), 2)
        self.assertEqual(started["thread_id"], resumed["thread_id"])

    async def test_scenario_e_manual_edit_is_preserved_and_can_be_approved(self):
        graph, _ = await self.start_until_review(thread_id="manual-edit-thread")
        edited = await workflow.resume_workflow(
            "manual-edit-thread",
            {
                "action": "manually_edited",
                "edited_blog": "# Human Edited Blog\n\nThe human wording.",
            },
            graph=graph,
        )

        self.assertEqual(edited["status"], "paused")
        self.assertEqual(edited["interrupt"]["blog"], "# Human Edited Blog\n\nThe human wording.")
        self.assertEqual(edited["result"]["final"], "# Human Edited Blog\n\nThe human wording.")
        self.assertEqual(edited["result"]["review_status"], "manually_edited")

        approved = await workflow.resume_workflow(
            "manual-edit-thread",
            {"action": "approved"},
            graph=graph,
        )
        self.assertEqual(approved["status"], "completed")
        self.assertEqual(approved["result"]["final"], "# Human Edited Blog\n\nThe human wording.")
        create_call = next(call for call in FakeWordPressClient.calls if call[0] == "create_post")
        self.assertEqual(create_call[2], "# Human Edited Blog\n\nThe human wording.")

    async def test_scenario_f_revision_limit_routes_to_manual_intervention(self):
        graph, _ = await self.start_until_review(thread_id="revision-limit-thread")
        for revision_number in range(1, review.MAX_REVISIONS + 1):
            response = await workflow.resume_workflow(
                "revision-limit-thread",
                {
                    "action": "revision_requested",
                    "feedback": f"Revision {revision_number}",
                },
                graph=graph,
            )
            self.assertEqual(response["status"], "paused")
            self.assertEqual(response["interrupt"]["revision_count"], revision_number)

        final = await workflow.resume_workflow(
            "revision-limit-thread",
            {"action": "revision_requested", "feedback": "One more revision"},
            graph=graph,
        )
        self.assertEqual(final["status"], "completed")
        self.assertEqual(final["result"]["review_status"], "manual_intervention_required")
        self.assertEqual(final["result"]["revision_count"], review.MAX_REVISIONS)
        self.assertEqual(len(self.fake_llm.revision_calls), review.MAX_REVISIONS)
        self.assertEqual(FakeWordPressClient.calls, [])

    async def test_scenario_g_image_placeholder_and_file_are_isolated(self):
        image_spec = ImageSpec(
            placeholder="[[IMAGE_1]]",
            filename="test-diagram.png",
            alt="Test diagram",
            caption="Generated test diagram",
            prompt="A test diagram",
        )
        merged_markdown = "# Test Blog\n\n[[IMAGE_1]]"
        self.fake_llm.image_plan = GlobalImagePlan(
            md_with_placeholders=merged_markdown,
            images=[image_spec],
        )

        _, response = await self.start_until_review(thread_id="image-thread")
        result = response["result"]

        self.assertIn('<img src="../images/test-diagram.png"', result["final"])
        self.assertIn("*Generated test diagram*", result["final"])
        self.assertTrue(Path("images/test-diagram.png").is_file())
        self.assertEqual(Path("images/test-diagram.png").read_bytes(), b"mock-image-bytes")
        self.assertFalse(
            (Path(self.previous_cwd) / "images" / "test-diagram.png").exists()
        )

    async def test_streaming_reports_actual_nodes_and_parallel_worker_sections(self):
        thread_id = "streamed-workflow-thread"
        checkpointer = MemorySaver(serde=create_checkpoint_serializer())
        graph = workflow.build_workflow_graph(checkpointer)
        runtime = workflow.WorkflowRuntime.from_checkpointer(
            checkpointer,
            graph=graph,
        )
        events = []

        response = await runtime.start_workflow_stream(
            "A test blog topic",
            "2026-11-26",
            thread_id,
            events.append,
        )

        started = [
            event for event in events if event["event"] == "node_started"
        ]
        completed = [
            event for event in events if event["event"] == "node_completed"
        ]
        started_workers = {
            event["section_id"]
            for event in started
            if event["node"] == "worker"
        }
        completed_workers = {
            event["section_id"]
            for event in completed
            if event["node"] == "worker"
        }

        self.assertEqual(response["thread_id"], thread_id)
        self.assertEqual(response["status"], "paused")
        self.assertEqual(response["interrupt"]["message"], "Review the generated blog")
        self.assertIn("router", {event["node"] for event in completed})
        self.assertIn("orchestrator", {event["node"] for event in completed})
        self.assertIn("merge_content", {event["node"] for event in completed})
        self.assertIn("decide_images", {event["node"] for event in completed})
        self.assertIn(
            "generate_and_place_images",
            {event["node"] for event in completed},
        )
        self.assertEqual(started_workers, {1, 2})
        self.assertEqual(completed_workers, {1, 2})
        self.assertIn(
            "human_review",
            {
                event["node"]
                for event in events
                if event["event"] == "node_interrupted"
            },
        )
        await runtime.shutdown()

    async def test_streaming_reports_research_only_for_research_route(self):
        self.fake_llm.needs_research = True
        checkpointer = MemorySaver(serde=create_checkpoint_serializer())
        graph = workflow.build_workflow_graph(checkpointer)
        runtime = workflow.WorkflowRuntime.from_checkpointer(
            checkpointer,
            graph=graph,
        )
        events = []

        response = await runtime.start_workflow_stream(
            "A current-events blog topic",
            "2026-11-26",
            "research-stream-thread",
            events.append,
        )

        completed_nodes = {
            event["node"]
            for event in events
            if event["event"] == "node_completed"
        }
        self.assertEqual(response["status"], "paused")
        self.assertIn("research", completed_nodes)
        await runtime.shutdown()

    async def test_streaming_resumes_review_checkpoint_with_same_thread_id(self):
        thread_id = "review-resume-stream-thread"
        graph, _ = await self.start_until_review(thread_id=thread_id)
        runtime = workflow.WorkflowRuntime.from_checkpointer(
            graph.checkpointer,
            graph=graph,
        )
        revision_events = []

        revised = await runtime.resume_workflow_stream(
            thread_id,
            {
                "action": "revision_requested",
                "feedback": "Clarify the conclusion.",
            },
            revision_events.append,
        )

        self.assertEqual(revised["thread_id"], thread_id)
        self.assertEqual(revised["status"], "paused")
        self.assertEqual(revised["interrupt"]["revision_count"], 1)
        self.assertIn(
            "revision",
            {event["node"] for event in revision_events},
        )
        self.assertIn(
            "human_review",
            {event["node"] for event in revision_events},
        )
        self.assertIn(
            "human_review",
            {
                event["node"]
                for event in revision_events
                if event["event"] == "node_interrupted"
            },
        )

        approval_events = []
        approved = await runtime.resume_workflow_stream(
            thread_id,
            {"action": "approved"},
            approval_events.append,
        )
        self.assertEqual(approved["thread_id"], thread_id)
        self.assertEqual(approved["status"], "completed")
        self.assertEqual(approved["result"]["review_status"], "approved")
        self.assertIn(
            "wordpress_draft",
            {event["node"] for event in approval_events},
        )
        await runtime.shutdown()

    async def test_checkpointer_initializer_contract_is_mockable(self):
        pool = SimpleNamespace(close=lambda: None)
        saver = MemorySaver()
        fake_module = types.ModuleType("checkpointer")

        async def initialize():
            return pool, saver

        fake_module.initialize_checkpointer = initialize
        with patch.dict(sys.modules, {"checkpointer": fake_module}):
            result_pool, result_saver = await workflow.initialize_checkpointer()

        self.assertIs(result_pool, pool)
        self.assertIs(result_saver, saver)
        self.assertIsNotNone(workflow.build_workflow_graph(result_saver))

    async def test_checkpoint_serializer_round_trips_state_models(self):
        serializer = create_checkpoint_serializer()
        self.assertIsInstance(serializer, JsonPlusSerializer)
        self.assertFalse(serializer.pickle_fallback)

        plan = make_plan()
        evidence = EvidenceItem(
            title="Optional fields",
            url="https://example.test/optional",
            published_at=None,
            snippet=None,
            source=None,
        )

        for model in (plan, evidence):
            with self.subTest(model=type(model).__name__):
                with self.assertNoLogs(
                    "langgraph.checkpoint.serde.jsonplus",
                    level="WARNING",
                ):
                    type_tag, payload = serializer.dumps_typed(model)
                    restored = serializer.loads_typed((type_tag, payload))
                self.assertIs(type(restored), type(model))
                self.assertEqual(restored, model)

        restored_plan = serializer.loads_typed(serializer.dumps_typed(plan))
        self.assertIsInstance(restored_plan.tasks, list)
        self.assertTrue(all(isinstance(task, Task) for task in restored_plan.tasks))
        self.assertIsInstance(restored_plan.tasks[0].id, int)
        self.assertIsInstance(restored_plan.tasks[0].bullets, list)
        restored_evidence = serializer.loads_typed(
            serializer.dumps_typed(evidence)
        )
        self.assertIsNone(restored_evidence.published_at)
        self.assertIsNone(restored_evidence.snippet)
        self.assertIsNone(restored_evidence.source)

    async def test_postgres_checkpointer_uses_explicit_serializer_without_database(self):
        with patch.dict(
            os.environ,
            {"SUPABASE_DB_URI": "postgresql://test:test@localhost/test"},
        ):
            checkpointer_module = importlib.import_module("checkpointer")

        pool = SimpleNamespace(open=AsyncMock())
        saver = SimpleNamespace(setup=AsyncMock())
        with (
            patch.object(
                checkpointer_module,
                "AsyncConnectionPool",
                return_value=pool,
            ),
            patch.object(
                checkpointer_module,
                "AsyncPostgresSaver",
                return_value=saver,
            ) as saver_factory,
        ):
            result_pool, result_saver = await checkpointer_module.initialize_checkpointer()

        self.assertIs(result_pool, pool)
        self.assertIs(result_saver, saver)
        pool.open.assert_awaited_once()
        saver.setup.assert_awaited_once()
        args, kwargs = saver_factory.call_args
        self.assertIs(args[0], pool)
        configured_serializer = kwargs["serde"]
        self.assertIsInstance(configured_serializer, JsonPlusSerializer)
        self.assertEqual(
            configured_serializer._allowed_msgpack_modules,
            {
                ("blog_writer.models", "EvidenceItem"),
                ("blog_writer.models", "Plan"),
            },
        )
        self.assertFalse(configured_serializer.pickle_fallback)

    async def test_checkpoint_restore_preserves_models_for_revision_and_approval(self):
        self.fake_llm.needs_research = True
        checkpointer = MemorySaver(serde=create_checkpoint_serializer())
        thread_id = "serializer-resume-thread"
        config = workflow.make_thread_config(thread_id)["config"]
        graph = workflow.build_workflow_graph(checkpointer)

        with self.assertNoLogs(
            "langgraph.checkpoint.serde.jsonplus",
            level="WARNING",
        ):
            started = await workflow.start_workflow(
                "A test blog topic",
                as_of="2026-11-26",
                thread_id=thread_id,
                graph=graph,
            )
            self.assertEqual(started["status"], "paused")
            original_state = graph.get_state(config).values
            self.assertIsInstance(original_state["plan"], Plan)
            self.assertIsInstance(original_state["plan"].tasks[0], Task)
            self.assertIsInstance(original_state["evidence"][0], EvidenceItem)
            self.assertIsInstance(original_state["sections"][0], dict)
            self.assertIsInstance(original_state["sections"][0]["task_id"], int)
            self.assertIsInstance(original_state["sections"][0]["markdown"], str)

            checkpoint = checkpointer.get_tuple(config)
            self.assertIsNotNone(checkpoint)
            self.assertIsInstance(
                checkpoint.checkpoint["channel_values"]["plan"],
                Plan,
            )
            self.assertIsInstance(
                checkpoint.checkpoint["channel_values"]["evidence"][0],
                EvidenceItem,
            )

            restored_graph = workflow.build_workflow_graph(checkpointer)
            restored_state = restored_graph.get_state(config).values
            self.assertEqual(restored_state, original_state)

            revised = await workflow.resume_workflow(
                thread_id,
                {
                    "action": "revision_requested",
                    "feedback": "Add a clearer conclusion.",
                },
                graph=restored_graph,
            )

        self.assertEqual(revised["status"], "paused")
        self.assertEqual(revised["interrupt"]["revision_count"], 1)
        self.assertEqual(revised["result"]["revision_count"], 1)
        self.assertEqual(revised["result"]["plan"], started["result"]["plan"])
        self.assertEqual(revised["result"]["evidence"], started["result"]["evidence"])
        self.assertIn("Add a clearer conclusion.", self.fake_llm.revision_calls[0])

        approval_graph = workflow.build_workflow_graph(checkpointer)
        with self.assertNoLogs(
            "langgraph.checkpoint.serde.jsonplus",
            level="WARNING",
        ):
            approval_state = approval_graph.get_state(config).values
            self.assertIsInstance(approval_state["plan"], Plan)
            self.assertIsInstance(approval_state["evidence"][0], EvidenceItem)
            approved = await workflow.resume_workflow(
                thread_id,
                {"action": "approved"},
                graph=approval_graph,
            )

        self.assertEqual(approved["status"], "completed")
        self.assertEqual(approved["result"]["review_status"], "approved")
        self.assertEqual(approved["result"]["wordpress_post_id"], 871)
        self.assertEqual(approved["result"]["wordpress_status"], "draft")
        self.assertEqual(
            approved["result"]["final"],
            revised["result"]["final"],
        )
        create_call = next(
            call for call in FakeWordPressClient.calls if call[0] == "create_post"
        )
        self.assertEqual(create_call[2], revised["result"]["final"])

    async def test_get_workflow_state_restores_review_checkpoint_without_mutation(self):
        thread_id = "state-read-review-thread"
        image_spec = ImageSpec(
            placeholder="[[IMAGE_1]]",
            filename="state-diagram.png",
            alt="State diagram",
            caption="Workflow state",
            prompt="A state diagram",
        )
        self.fake_llm.image_plan = GlobalImagePlan(
            md_with_placeholders="# Test Blog\n\n[[IMAGE_1]]",
            images=[image_spec],
        )
        graph, started = await self.start_until_review(thread_id=thread_id)
        checkpointer = graph.checkpointer
        config = workflow.make_thread_config(thread_id)["config"]
        checkpoint_before = checkpointer.get_tuple(config)
        llm_call_counts = (
            len(self.fake_llm.structured_calls),
            len(self.fake_llm.worker_calls),
            len(self.fake_llm.revision_calls),
        )

        restored_graph = workflow.build_workflow_graph(checkpointer)
        restored_runtime = workflow.WorkflowRuntime.from_checkpointer(
            checkpointer,
            graph=restored_graph,
        )
        state = await restored_runtime.get_workflow_state(thread_id)

        checkpoint_after = checkpointer.get_tuple(config)
        self.assertEqual(state["thread_id"], thread_id)
        self.assertEqual(state["status"], "paused_for_review")
        self.assertEqual(state["topic"], "A test blog topic")
        self.assertEqual(state["plan"]["blog_title"], "Test Blog")
        self.assertIsInstance(state["plan"]["tasks"][0]["id"], int)
        self.assertEqual(state["evidence"], [])
        self.assertEqual(len(state["sections"]), 2)
        self.assertEqual(state["final"], started["result"]["final"])
        self.assertEqual(state["image_specs"][0]["filename"], "state-diagram.png")
        self.assertEqual(
            state["image_references"],
            ["../images/state-diagram.png"],
        )
        self.assertEqual(state["review_status"], "")
        self.assertEqual(state["revision_count"], 0)
        self.assertEqual(state["interrupted_at"], ["human_review"])
        self.assertEqual(state["interrupts"][0]["blog"], state["final"])
        self.assertEqual(state["next"], ["human_review"])
        self.assertEqual(checkpoint_after.checkpoint, checkpoint_before.checkpoint)
        self.assertEqual(
            llm_call_counts,
            (
                len(self.fake_llm.structured_calls),
                len(self.fake_llm.worker_calls),
                len(self.fake_llm.revision_calls),
            ),
        )

    async def test_get_workflow_state_after_revision_and_completion(self):
        thread_id = "state-read-revision-thread"
        graph, _ = await self.start_until_review(thread_id=thread_id)
        checkpointer = graph.checkpointer

        revised = await workflow.resume_workflow(
            thread_id,
            {
                "action": "revision_requested",
                "feedback": "Add a clearer conclusion.",
            },
            graph=graph,
        )
        runtime = workflow.WorkflowRuntime.from_checkpointer(
            checkpointer,
            graph=workflow.build_workflow_graph(checkpointer),
        )
        revision_state = await runtime.get_workflow_state(thread_id)
        self.assertEqual(revision_state["status"], "paused_for_review")
        self.assertEqual(revision_state["revision_count"], 1)
        self.assertEqual(revision_state["final"], revised["result"]["final"])

        approved = await workflow.resume_workflow(
            thread_id,
            {"action": "approved"},
            graph=graph,
        )
        completed_state = await workflow.WorkflowRuntime.from_checkpointer(
            checkpointer,
            graph=workflow.build_workflow_graph(checkpointer),
        ).get_workflow_state(thread_id)

        self.assertEqual(approved["status"], "completed")
        self.assertEqual(completed_state["status"], "completed")
        self.assertEqual(completed_state["review_status"], "approved")
        self.assertEqual(completed_state["wordpress_post_id"], 871)
        self.assertEqual(completed_state["wordpress_status"], "draft")

    async def test_get_workflow_state_distinguishes_non_review_interrupts(self):
        class InterruptState(TypedDict):
            topic: str

        def wait_for_external_input(state):
            interrupt({"message": "Waiting for external input"})
            return {}

        builder = StateGraph(InterruptState)
        builder.add_node("external_wait", wait_for_external_input)
        builder.add_edge(START, "external_wait")
        builder.add_edge("external_wait", END)
        checkpointer = MemorySaver(serde=create_checkpoint_serializer())
        graph = builder.compile(checkpointer=checkpointer)
        thread_id = "external-interrupt-thread"
        config = workflow.make_thread_config(thread_id)["config"]

        await graph.ainvoke({"topic": "Test"}, config=config)
        runtime = workflow.WorkflowRuntime.from_checkpointer(checkpointer, graph=graph)
        state = await runtime.get_workflow_state(thread_id)

        self.assertEqual(state["status"], "interrupted")
        self.assertEqual(state["interrupted_at"], ["external_wait"])
        self.assertEqual(
            state["interrupts"],
            [{"message": "Waiting for external input"}],
        )

    async def test_get_workflow_state_returns_not_found_and_validates_thread_id(self):
        graph = self.build_test_graph()
        runtime = workflow.WorkflowRuntime.from_checkpointer(
            graph.checkpointer,
            graph=graph,
        )

        missing = await runtime.get_workflow_state("unknown-thread")
        self.assertEqual(missing["status"], "not_found")
        self.assertEqual(missing["thread_id"], "unknown-thread")
        self.assertIsNone(missing["topic"])
        self.assertEqual(missing["sections"], [])

        for invalid_thread_id in ("", "   ", None, 42):
            with self.subTest(thread_id=invalid_thread_id):
                with self.assertRaisesRegex(ValueError, "thread_id"):
                    await runtime.get_workflow_state(invalid_thread_id)

    async def test_runtime_initialization_retains_pool_saver_and_compiles_graph(self):
        pool = AsyncClosePool()
        saver = MemorySaver()
        graph = FakeWorkflowGraph()
        build_calls = []

        async def initialize():
            return pool, saver

        def build(checkpointer):
            build_calls.append(checkpointer)
            return graph

        with (
            patch.object(workflow, "initialize_checkpointer", initialize),
            patch.object(workflow, "build_workflow_graph", build),
        ):
            runtime = await workflow.WorkflowRuntime.initialize()

        self.assertIs(runtime.connection_pool, pool)
        self.assertIs(runtime.checkpointer, saver)
        self.assertIs(runtime.graph, graph)
        self.assertEqual(build_calls, [saver])
        self.assertEqual(pool.close_calls, 0)
        await runtime.shutdown()
        self.assertEqual(pool.close_calls, 1)

    async def test_runtime_reuses_graph_pool_and_closes_once_on_shutdown(self):
        pool = AsyncClosePool()
        saver = MemorySaver()
        graph = FakeWorkflowGraph(
            [
                {"__interrupt__": [{"value": {"message": "Review", "blog": "Draft"}}]},
                {"__interrupt__": [{"value": {"message": "Review again", "blog": "Revised"}}]},
                {"review_status": "approved", "wordpress_status": "draft"},
            ]
        )
        runtime = workflow.WorkflowRuntime(pool, saver, graph)

        started = await workflow.start_workflow("Topic", thread_id="runtime-thread", runtime=runtime)
        self.assertEqual(started["status"], "paused")
        self.assertEqual(pool.close_calls, 0)

        revised = await workflow.resume_workflow(
            "runtime-thread",
            {"action": "revision_requested", "feedback": "Clarify"},
            runtime=runtime,
        )
        self.assertEqual(revised["status"], "paused")
        self.assertEqual(pool.close_calls, 0)

        completed = await workflow.resume_workflow(
            "runtime-thread",
            {"action": "approved"},
            runtime=runtime,
        )
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(pool.close_calls, 0)
        self.assertEqual(len(graph.calls), 3)
        self.assertTrue(
            all(call[1]["configurable"]["thread_id"] == "runtime-thread" for call in graph.calls)
        )

        await runtime.shutdown()
        await runtime.shutdown()
        self.assertEqual(pool.close_calls, 1)
        with self.assertRaisesRegex(RuntimeError, "shutting down or already closed"):
            await runtime.start_workflow("After shutdown")

    async def test_runtime_shutdown_waits_for_active_workflow(self):
        pool = AsyncClosePool()
        saver = MemorySaver()
        started_event = asyncio.Event()
        release_event = asyncio.Event()

        class BlockingGraph:
            async def ainvoke(self, input_value, config=None):
                started_event.set()
                await release_event.wait()
                return {"__interrupt__": [{"value": {"message": "Review"}}]}

        runtime = workflow.WorkflowRuntime(pool, saver, BlockingGraph())
        start_task = asyncio.create_task(runtime.start_workflow("Topic"))
        await started_event.wait()
        shutdown_task = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0)

        self.assertEqual(pool.close_calls, 0)
        self.assertFalse(shutdown_task.done())

        release_event.set()
        result = await start_task
        await shutdown_task
        self.assertEqual(result["status"], "paused")
        self.assertEqual(pool.close_calls, 1)

    async def test_default_runtime_initializes_once_until_shutdown(self):
        pool = AsyncClosePool()
        graph = FakeWorkflowGraph(
            [
                {"__interrupt__": [{"value": {"message": "Review"}}]},
                {"review_status": "approved"},
            ]
        )
        runtime = workflow.WorkflowRuntime(pool, MemorySaver(), graph)
        with (
            patch.object(workflow, "_default_runtime", None),
            patch.object(
                workflow.WorkflowRuntime,
                "initialize",
                new=AsyncMock(return_value=runtime),
            ) as initialize,
        ):
            started = await workflow.start_workflow("Topic", thread_id="default-thread")
            resumed = await workflow.resume_workflow(
                "default-thread",
                {"action": "approved"},
            )
            self.assertEqual(started["status"], "paused")
            self.assertEqual(resumed["status"], "completed")
            initialize.assert_awaited_once()
            self.assertEqual(len(graph.calls), 2)
            self.assertEqual(pool.close_calls, 0)

            await workflow.shutdown_workflow_runtime()
            self.assertEqual(pool.close_calls, 1)

    async def test_runtime_initialization_compiles_graph_with_retained_checkpointer(self):
        pool = AsyncClosePool()
        saver = MemorySaver()

        async def initialize():
            return pool, saver

        with patch.object(workflow, "initialize_checkpointer", initialize):
            runtime = await workflow.WorkflowRuntime.initialize()

        self.assertIs(runtime.connection_pool, pool)
        self.assertIs(runtime.checkpointer, saver)
        self.assertIs(runtime.graph.checkpointer.storage, saver.storage)
        self.assertEqual(pool.close_calls, 0)
        await runtime.shutdown()
        self.assertEqual(pool.close_calls, 1)


if __name__ == "__main__":
    unittest.main()
