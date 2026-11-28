import os
import tempfile
import unittest
from contextlib import contextmanager
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock, call, patch

from streamlit.testing.v1 import AppTest

from streamlit_ui.components import article_renderer, progress_panel
from streamlit_ui.services import runtime_service, workflow_service
from streamlit_ui.services import wordpress_service
from streamlit_ui.services.runtime_manager import RuntimeManager


APP_FILE = Path(__file__).resolve().parents[1] / "app.py"


def click_button(app, label):
    button = next(item for item in app.button if item.label == label)
    button.click()


def workflow_response(
    thread_id,
    status="paused",
    final="# Generated Blog",
    revision_count=0,
):
    return {
        "thread_id": thread_id,
        "status": status,
        "result": {
            "final": final,
            "wordpress_status": "draft" if status == "completed" else None,
            "revision_count": revision_count,
        },
        "interrupt": (
            {
                "message": "Review the generated blog",
                "blog": final,
                "revision_count": revision_count,
            }
            if status == "paused"
            else None
        ),
    }


class FakeWorkflowRuntime:
    def __init__(
        self,
        *,
        status="paused",
        failure=None,
        resume_failure=None,
        final_content=None,
    ):
        self.status = status
        self.failure = failure
        self.resume_failure = resume_failure
        self.final_content = final_content or (
            "# A Generated Blog\n\nA paragraph with **formatting**."
        )
        self.started_workflows = []
        self.resumed_workflows = []
        self.shutdown_calls = 0

    async def start_workflow(self, topic, as_of=None, thread_id=None):
        if self.failure is not None:
            raise self.failure
        self.started_workflows.append((topic, as_of, thread_id))
        return workflow_response(
            thread_id,
            status=self.status,
            final=self.final_content,
        )

    async def start_workflow_stream(
        self,
        topic,
        as_of,
        thread_id,
        progress_callback,
    ):
        self.started_workflows.append((topic, as_of, thread_id))
        events = [
            {
                "event": "node_started",
                "node": "router",
                "execution_id": "router-run",
            },
            {
                "event": "node_completed",
                "node": "router",
                "execution_id": "router-run",
            },
            {
                "event": "plan_ready",
                "node": "orchestrator",
                "section_total": 2,
                "section_titles": ["First section", "Second section"],
            },
            {
                "event": "node_started",
                "node": "worker",
                "execution_id": "worker-1",
                "section_id": 1,
                "section_title": "First section",
            },
            {
                "event": "node_started",
                "node": "worker",
                "execution_id": "worker-2",
                "section_id": 2,
                "section_title": "Second section",
            },
            {
                "event": "node_completed",
                "node": "worker",
                "execution_id": "worker-2",
                "section_id": 2,
                "section_title": "Second section",
            },
            {
                "event": "node_completed",
                "node": "worker",
                "execution_id": "worker-1",
                "section_id": 1,
                "section_title": "First section",
            },
        ]
        for event in events:
            progress_callback(event)
        if self.failure is not None:
            raise self.failure
        return workflow_response(
            thread_id,
            status=self.status,
            final=self.final_content,
        )

    async def resume_workflow_stream(
        self,
        thread_id,
        payload,
        progress_callback,
    ):
        self.resumed_workflows.append((thread_id, payload))
        action = payload["action"]
        node = {
            "approved": "wordpress_draft",
            "revision_requested": "revision",
            "manually_edited": "human_review",
        }[action]
        progress_callback(
            {
                "event": "node_started",
                "node": node,
                "execution_id": "resume-run",
            }
        )
        if self.resume_failure is not None:
            raise self.resume_failure
        progress_callback(
            {
                "event": "node_completed",
                "node": node,
                "execution_id": "resume-run",
            }
        )
        if action == "approved":
            return workflow_response(
                thread_id,
                status="completed",
                final="# A Generated Blog\n\nA paragraph with **formatting**.",
            )
        if action == "revision_requested":
            return workflow_response(
                thread_id,
                final="# Revised Blog\n\nUpdated using the feedback.",
                revision_count=1,
            )
        return workflow_response(
            thread_id,
            final=payload["edited_blog"],
        )

    async def shutdown(self):
        self.shutdown_calls += 1


class WorkflowServiceTests(unittest.TestCase):
    def test_valid_topic_uses_shared_manager_and_returns_workflow_response(self):
        manager = Mock()
        manager.start_workflow.side_effect = lambda topic, thread_id: workflow_response(
            thread_id
        )
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=(),
            ),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            result = workflow_service.start_blog_workflow("  Streamlit testing  ")

        self.assertEqual(result["status"], "paused")
        self.assertEqual(result["result"]["final"], "# Generated Blog")
        topic, = manager.start_workflow.call_args.args
        self.assertEqual(topic, "Streamlit testing")
        thread_id = manager.start_workflow.call_args.kwargs["thread_id"]
        self.assertEqual(result["thread_id"], thread_id)
        self.assertTrue(thread_id)

    def test_empty_topic_is_rejected_before_runtime_access(self):
        with patch.object(
            runtime_service,
            "get_runtime_manager",
            side_effect=AssertionError("runtime should not be requested"),
        ):
            with self.assertRaisesRegex(
                workflow_service.WorkflowServiceError,
                "Enter a blog topic",
            ):
                workflow_service.start_blog_workflow("  ")

    def test_missing_configuration_prevents_runtime_access(self):
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=("SUPABASE_DB_URI",),
            ),
            patch.object(
                runtime_service,
                "get_runtime_manager",
                side_effect=AssertionError("runtime should not be requested"),
            ),
        ):
            with self.assertRaisesRegex(
                workflow_service.WorkflowServiceError,
                "SUPABASE_DB_URI",
            ):
                workflow_service.start_blog_workflow("A topic")

    def test_runtime_error_is_wrapped_without_exposing_sensitive_details(self):
        manager = Mock()
        manager.start_workflow.side_effect = RuntimeError(
            "failed to connect with postgresql://private-secret@host/database"
        )
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=(),
            ),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            self.assertLogs(level="ERROR") as captured,
        ):
            with self.assertRaises(workflow_service.WorkflowServiceError) as error:
                workflow_service.start_blog_workflow("A topic")

        self.assertNotIn("private-secret", str(error.exception))
        self.assertNotIn("private-secret", "\n".join(captured.output))

    def test_streaming_service_returns_thread_progress_and_final_response(self):
        manager = Mock()
        manager.stream_workflow.return_value = iter(
            [
                {
                    "type": "progress",
                    "event": {
                        "event": "node_started",
                        "node": "router",
                        "execution_id": "router-run",
                    },
                },
                {
                    "type": "result",
                    "response": workflow_response("stream-thread"),
                },
            ]
        )
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=(),
            ),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            events = list(
                workflow_service.stream_blog_workflow(
                    "A streaming topic",
                    thread_id="stream-thread",
                )
            )

        self.assertEqual(events[0], {"type": "started", "thread_id": "stream-thread"})
        self.assertEqual(events[1]["type"], "progress")
        self.assertEqual(events[1]["event"], "node_started")
        self.assertEqual(events[2]["type"], "result")
        self.assertEqual(events[2]["response"]["thread_id"], "stream-thread")
        manager.stream_workflow.assert_called_once_with(
            "A streaming topic",
            thread_id="stream-thread",
        )

    def test_streaming_service_reports_errors_without_details(self):
        manager = Mock()
        manager.stream_workflow.side_effect = RuntimeError(
            "database password: private-secret"
        )
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=(),
            ),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            self.assertLogs(level="ERROR") as captured,
        ):
            events = list(workflow_service.stream_blog_workflow("A topic"))

        self.assertEqual(events[-1]["type"], "error")
        self.assertNotIn("private-secret", events[-1]["message"])
        self.assertNotIn("private-secret", "\n".join(captured.output))

    def test_approval_stream_uses_exact_payload_and_same_thread_id(self):
        manager = Mock()
        manager.stream_resume_workflow.return_value = iter(
            [
                {"type": "progress", "event": {"event": "node_started", "node": "wordpress_draft"}},
                {"type": "result", "response": workflow_response("review-thread", "completed")},
            ]
        )
        with (
            patch.object(runtime_service, "get_missing_required_configuration", return_value=()),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            events = list(workflow_service.approve_article("review-thread"))

        self.assertEqual(events[0], {"type": "resuming", "thread_id": "review-thread"})
        self.assertEqual(events[-1]["response"]["thread_id"], "review-thread")
        manager.stream_resume_workflow.assert_called_once_with(
            "review-thread",
            {"action": "approved"},
        )

    def test_revision_and_manual_edit_use_backend_payload_shapes(self):
        manager = Mock()
        manager.stream_resume_workflow.side_effect = lambda thread_id, payload: iter(
            [
                {
                    "type": "result",
                    "response": workflow_response(
                        thread_id,
                        final=payload.get("edited_blog", "# Revised"),
                    ),
                }
            ]
        )
        with (
            patch.object(runtime_service, "get_missing_required_configuration", return_value=()),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            list(workflow_service.request_article_revision("same-thread", "  Clarify this.  "))
            list(workflow_service.submit_manual_edit("same-thread", "# Human edit"))

        self.assertEqual(
            manager.stream_resume_workflow.call_args_list,
            [
                call(
                    "same-thread",
                    {
                        "action": "revision_requested",
                        "feedback": "Clarify this.",
                    },
                ),
                call(
                    "same-thread",
                    {
                        "action": "manually_edited",
                        "edited_blog": "# Human edit",
                    },
                ),
            ],
        )

    def test_empty_revision_feedback_and_manual_article_are_rejected(self):
        manager = Mock()
        with (
            patch.object(runtime_service, "get_missing_required_configuration", return_value=()),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            feedback_events = list(
                workflow_service.request_article_revision("review-thread", "  ")
            )
            edit_events = list(
                workflow_service.submit_manual_edit("review-thread", " \n")
            )

        self.assertIn("feedback", feedback_events[-1]["message"])
        self.assertIn("cannot be empty", edit_events[-1]["message"])
        manager.stream_resume_workflow.assert_not_called()

    def test_resume_errors_are_safe(self):
        manager = Mock()
        manager.stream_resume_workflow.side_effect = RuntimeError(
            "postgresql://private-secret@host/database"
        )
        with (
            patch.object(runtime_service, "get_missing_required_configuration", return_value=()),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            self.assertLogs(level="ERROR") as captured,
        ):
            events = list(workflow_service.approve_article("review-thread"))

        self.assertEqual(events[-1]["type"], "error")
        self.assertNotIn("private-secret", events[-1]["message"])
        self.assertNotIn("private-secret", "\n".join(captured.output))


class ArticleRendererTests(unittest.TestCase):
    def test_renders_list_of_text_content_blocks_as_markdown(self):
        content = [
            {"type": "text", "text": "# Article title"},
            {"type": "image", "image_url": "not-rendered-by-markdown"},
            {"type": "text", "text": "A paragraph with **formatting**."},
        ]
        with patch.object(article_renderer.st, "markdown") as markdown:
            article_renderer.render_article(content)

        markdown.assert_called_once_with(
            "# Article title\nA paragraph with **formatting**."
        )

    def test_normalizes_plain_string_and_mixed_text_blocks(self):
        self.assertEqual(
            article_renderer.normalize_markdown(
                ["First paragraph", {"text": "Second paragraph"}]
            ),
            "First paragraph\nSecond paragraph",
        )
        self.assertEqual(
            article_renderer.normalize_markdown("# Already Markdown"),
            "# Already Markdown",
        )

    def test_unsupported_content_type_shows_safe_error(self):
        with (
            patch.object(article_renderer.st, "markdown") as markdown,
            patch.object(article_renderer.st, "error") as error,
        ):
            article_renderer.render_article({"unexpected": "content"})

        markdown.assert_not_called()
        error.assert_called_once_with(
            "The generated article content has an unsupported format."
        )

    def test_displays_local_image_at_its_markdown_position(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            previous_cwd = os.getcwd()
            try:
                os.chdir(temp_dir)
                images = Path("images")
                images.mkdir()
                image_file = images / "diagram.png"
                image_file.write_bytes(b"test image")

                with (
                    patch.object(article_renderer.st, "markdown") as markdown,
                    patch.object(article_renderer.st, "image") as image,
                ):
                    article_renderer.render_article(
                        '# Title\n\nBefore image.\n\n'
                        '<img src="../images/diagram.png" alt="Architecture diagram" '
                        'width="650"/>\n\n*Diagram caption*'
                    )

                markdown.assert_any_call("# Title\n\nBefore image.\n\n")
                markdown.assert_any_call("\n\n*Diagram caption*")
                image.assert_called_once_with(str(image_file.resolve()))
            finally:
                os.chdir(previous_cwd)

    def test_remote_image_reference_is_not_fetched(self):
        source = '<img src="https://example.test/image.png" alt="Remote image"/>'
        with (
            patch.object(article_renderer.st, "markdown") as markdown,
            patch.object(article_renderer.st, "image") as image,
            patch.object(article_renderer.st, "caption") as caption,
        ):
            article_renderer.render_article(source)

        image.assert_not_called()
        caption.assert_not_called()
        markdown.assert_called_once_with(source)

    def test_missing_local_image_shows_safe_fallback(self):
        source = '<img src="../images/missing.png" alt="Missing diagram"/>'
        with (
            patch.object(article_renderer.st, "markdown") as markdown,
            patch.object(article_renderer.st, "image") as image,
            patch.object(article_renderer.st, "caption") as caption,
        ):
            article_renderer.render_article(source)

        image.assert_not_called()
        caption.assert_called_once_with(
            "Generated image is unavailable: Missing diagram"
        )
        markdown.assert_not_called()

    def test_html_image_example_in_code_fence_is_preserved_as_markdown(self):
        source = (
            '```html\n<img src="../images/example.png" alt="Example"/>\n```'
        )
        with (
            patch.object(article_renderer.st, "markdown") as markdown,
            patch.object(article_renderer.st, "image") as image,
        ):
            article_renderer.render_article(source)

        markdown.assert_called_once_with(source)
        image.assert_not_called()


class ProgressPanelTests(unittest.TestCase):
    def test_parallel_sections_are_shown_as_concurrent_work(self):
        events = [
            {"event": "plan_ready", "section_total": 2},
            {
                "event": "node_started",
                "node": "worker",
                "execution_id": "worker-a",
                "section_id": 1,
                "section_title": "First section",
            },
            {
                "event": "node_started",
                "node": "worker",
                "execution_id": "worker-b",
                "section_id": 2,
                "section_title": "Second section",
            },
        ]
        with (
            patch.object(progress_panel.st, "subheader"),
            patch.object(progress_panel.st, "markdown"),
            patch.object(progress_panel.st, "write") as write,
            patch.object(
                progress_panel.st,
                "expander",
                return_value=nullcontext(),
            ),
        ):
            progress_panel.render_progress_panel(events, "running")

        self.assertIn(
            call("Writing 2 section(s) in parallel"),
            write.call_args_list,
        )
        self.assertIn(
            call("Writing: First section"),
            write.call_args_list,
        )
        self.assertIn(
            call("Writing: Second section"),
            write.call_args_list,
        )

    def test_revision_limit_completion_reports_manual_intervention(self):
        with (
            patch.object(progress_panel.st, "subheader"),
            patch.object(progress_panel.st, "warning") as warning,
            patch.object(progress_panel.st, "success") as success,
        ):
            progress_panel.render_progress_panel(
                [],
                "completed",
                review_status="manual_intervention_required",
            )

        warning.assert_called_once_with(
            "The automatic revision limit was reached; manual intervention is required."
        )
        success.assert_not_called()


class StreamlitWorkflowAppTests(unittest.TestCase):
    @contextmanager
    def _run_app_with_runtime(
        self,
        *,
        status="paused",
        failure=None,
        resume_failure=None,
        final_content=None,
    ):
        runtime = FakeWorkflowRuntime(
            status=status,
            failure=failure,
            resume_failure=resume_failure,
            final_content=final_content,
        )
        factory_calls = []

        async def factory():
            factory_calls.append(True)
            return runtime

        manager = RuntimeManager(factory)
        self.addCleanup(manager.shutdown)
        manager_provider = Mock(return_value=manager)
        missing_configuration = Mock(return_value=())
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                missing_configuration,
            ),
            patch.object(
                runtime_service,
                "get_runtime_manager",
                manager_provider,
            ),
            patch.object(
                wordpress_service,
                "check_wordpress_connection",
                return_value="disconnected",
            ),
        ):
            app = AppTest.from_file(str(APP_FILE)).run()
            self.assertFalse(app.exception)
            yield app, runtime, factory_calls, manager_provider

    def test_paused_workflow_displays_article_and_survives_reruns(self):
        with self._run_app_with_runtime() as (
            app,
            runtime,
            factory_calls,
            manager_provider,
        ):
            app.text_input[0].set_value("A topic for the blog")
            click_button(app, "Generate Blog")
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(len(runtime.started_workflows), 1)
            self.assertEqual(runtime.started_workflows[0][0], "A topic for the blog")
            thread_id = app.session_state["current_thread_id"]
            self.assertEqual(runtime.started_workflows[0][2], thread_id)
            self.assertEqual(
                app.session_state["current_workflow_status"],
                "paused_for_review",
            )
            self.assertEqual(
                app.session_state["current_final_markdown"],
                "# A Generated Blog\n\nA paragraph with **formatting**.",
            )
            self.assertTrue(
                any("ready for human review" in item.value for item in app.info)
            )
            self.assertTrue(
                any("A Generated Blog" in item.value for item in app.markdown)
            )
            self.assertEqual(
                len(app.session_state["current_progress_events"]),
                7,
            )
            self.assertTrue(
                any("Sections completed: 2 of 2" in item.value for item in app.markdown)
            )

            app.run()
            self.assertEqual(len(runtime.started_workflows), 1)
            self.assertEqual(app.session_state["current_thread_id"], thread_id)
            self.assertEqual(len(factory_calls), 1)
            self.assertTrue(manager_provider.call_count >= 2)

    def test_list_content_blocks_are_normalized_before_review_and_session_storage(self):
        content = [
            {"type": "text", "text": "# Generated from blocks"},
            {"type": "text", "text": "Article body."},
        ]
        with self._run_app_with_runtime(final_content=content) as (
            app,
            _,
            _,
            _,
        ):
            app.text_input[0].set_value("Block content topic")
            click_button(app, "Generate Blog")
            app.run()

            expected = "# Generated from blocks\nArticle body."
            self.assertFalse(app.exception)
            self.assertEqual(
                app.session_state["current_final_markdown"],
                expected,
            )
            self.assertTrue(any("Generated from blocks" in item.value for item in app.markdown))

    def test_approval_resumes_the_paused_thread_and_completes(self):
        with self._run_app_with_runtime() as (app, runtime, _, _):
            app.text_input[0].set_value("Review this topic")
            click_button(app, "Generate Blog")
            app.run()
            thread_id = app.session_state["current_thread_id"]

            next(
                button
                for button in app.button
                if button.label == "Approve article"
            ).click()
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(
                runtime.resumed_workflows,
                [(thread_id, {"action": "approved"})],
            )
            self.assertEqual(app.session_state["current_thread_id"], thread_id)
            self.assertEqual(app.session_state["current_workflow_status"], "completed")
            self.assertTrue(
                any("workflow is complete" in item.value for item in app.success)
            )
            self.assertTrue(
                any(
                    "WordPress draft status: draft" in item.value
                    for item in app.markdown
                )
            )

    def test_revision_returns_to_review_with_updated_article_and_progress(self):
        with self._run_app_with_runtime() as (app, runtime, _, _):
            app.text_input[0].set_value("Revise this topic")
            click_button(app, "Generate Blog")
            app.run()
            thread_id = app.session_state["current_thread_id"]
            next(
                item for item in app.text_area if item.label == "Revision feedback"
            ).set_value("Clarify the conclusion.")
            next(
                button
                for button in app.button
                if button.label == "Request revision"
            ).click()
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(
                runtime.resumed_workflows,
                [
                    (
                        thread_id,
                        {
                            "action": "revision_requested",
                            "feedback": "Clarify the conclusion.",
                        },
                    )
                ],
            )
            self.assertEqual(app.session_state["current_thread_id"], thread_id)
            self.assertEqual(
                app.session_state["current_workflow_status"],
                "paused_for_review",
            )
            self.assertEqual(app.session_state["current_revision_count"], 1)
            self.assertEqual(
                app.session_state["current_final_markdown"],
                "# Revised Blog\n\nUpdated using the feedback.",
            )
            self.assertTrue(
                any("Revised Blog" in item.value for item in app.markdown)
            )
            self.assertTrue(
                any(
                    event.get("node") == "revision"
                    and event.get("event") == "node_completed"
                    for event in app.session_state["current_progress_events"]
                )
            )

    def test_empty_revision_feedback_does_not_resume_workflow(self):
        with self._run_app_with_runtime() as (app, runtime, _, _):
            app.text_input[0].set_value("A topic")
            click_button(app, "Generate Blog")
            app.run()
            next(
                button
                for button in app.button
                if button.label == "Request revision"
            ).click()
            app.run()

            self.assertEqual(runtime.resumed_workflows, [])
            self.assertEqual(
                app.session_state["current_workflow_status"],
                "paused_for_review",
            )
            self.assertTrue(
                any("Enter feedback" in item.value for item in app.error)
            )

    def test_manual_edit_is_submitted_without_overwriting_before_submit(self):
        with self._run_app_with_runtime() as (app, runtime, _, _):
            app.text_input[0].set_value("Manually edit this topic")
            click_button(app, "Generate Blog")
            app.run()
            thread_id = app.session_state["current_thread_id"]
            original = app.session_state["current_final_markdown"]
            next(
                item
                for item in app.text_area
                if item.label == "Edit article Markdown"
            ).set_value("# My edited Markdown\n\nKeep these words.")
            self.assertEqual(
                app.session_state["current_final_markdown"],
                original,
            )
            next(
                button
                for button in app.button
                if button.label == "Submit edited article"
            ).click()
            app.run()

            self.assertEqual(
                runtime.resumed_workflows,
                [
                    (
                        thread_id,
                        {
                            "action": "manually_edited",
                            "edited_blog": "# My edited Markdown\n\nKeep these words.",
                        },
                    )
                ],
            )
            self.assertEqual(
                app.session_state["current_final_markdown"],
                "# My edited Markdown\n\nKeep these words.",
            )
            self.assertEqual(
                app.session_state["current_workflow_status"],
                "paused_for_review",
            )

    def test_resume_failure_is_safe_and_does_not_expose_exception_details(self):
        with self._run_app_with_runtime(
            resume_failure=RuntimeError("postgresql://private-secret@host/database")
        ) as (app, _, _, _):
            app.text_input[0].set_value("A topic")
            click_button(app, "Generate Blog")
            app.run()
            next(
                button
                for button in app.button
                if button.label == "Approve article"
            ).click()
            with self.assertLogs(level="ERROR"):
                app.run()

            error_text = "\n".join(item.value for item in app.error)
            self.assertNotIn("private-secret", error_text)
            self.assertIn("review action could not be completed", error_text)
            self.assertEqual(app.session_state["current_workflow_status"], "error")

    def test_empty_form_submission_does_not_start_a_workflow(self):
        with self._run_app_with_runtime() as (app, runtime, _, _):
            click_button(app, "Generate Blog")
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(runtime.started_workflows, [])
            self.assertTrue(
                any("Enter a blog topic" in item.value for item in app.error)
            )

    def test_completed_workflow_displays_content_and_wordpress_draft_status(self):
        with self._run_app_with_runtime(status="completed") as (app, _, _, _):
            app.text_input[0].set_value("A completed topic")
            click_button(app, "Generate Blog")
            app.run()

            self.assertEqual(app.session_state["current_workflow_status"], "completed")
            self.assertTrue(
                any("workflow is complete" in item.value for item in app.success)
            )
            self.assertTrue(
                any(
                    "WordPress draft status: draft" in item.value
                    for item in app.markdown
                )
            )

    def test_workflow_failure_is_displayed_without_sensitive_details(self):
        with self._run_app_with_runtime(
            failure=RuntimeError("postgresql://private-secret@host/database")
        ) as (app, _, _, _):
            app.text_input[0].set_value("A failing topic")
            click_button(app, "Generate Blog")
            with self.assertLogs(level="ERROR"):
                app.run()

            self.assertFalse(app.exception)
            self.assertEqual(app.session_state["current_workflow_status"], "error")
            error_text = "\n".join(item.value for item in app.error)
            self.assertIn("Blog generation failed", error_text)
            self.assertNotIn("private-secret", error_text)


if __name__ == "__main__":
    unittest.main()
