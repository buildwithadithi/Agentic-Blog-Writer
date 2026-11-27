import os
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from streamlit_ui.services import runtime_service
from streamlit_ui.services import wordpress_service
from streamlit_ui.services.runtime_manager import RuntimeManager


REQUIRED_ENV = {
    "GOOGLE_API_KEY": "test-google-key",
    "TAVILY_API_KEY": "test-tavily-key",
    "CLOUDFLARE_ACCOUNT_ID": "test-cloudflare-account",
    "CLOUDFLARE_API_TOKEN": "test-cloudflare-token",
    "SUPABASE_DB_URI": "postgresql://test:test@localhost/test",
}
APP_FILE = Path(__file__).resolve().parents[1] / "app.py"


class FakeWorkflowRuntime:
    def __init__(self):
        self.started_workflows = []
        self.shutdown_calls = 0

    async def start_workflow(self, topic, as_of=None, thread_id=None):
        self.started_workflows.append((topic, as_of, thread_id))
        return {"topic": topic}

    async def resume_workflow(self, thread_id, payload):
        return {"thread_id": thread_id, "payload": payload}

    async def shutdown(self):
        self.shutdown_calls += 1


class RuntimeServiceTests(unittest.TestCase):
    def setUp(self):
        runtime_service.get_runtime_manager.clear()
        self.addCleanup(runtime_service.get_runtime_manager.clear)

    def test_repeated_service_calls_return_the_same_lazy_manager(self):
        manager = Mock(spec=RuntimeManager)
        with patch.object(runtime_service, "RuntimeManager", return_value=manager) as factory:
            first = runtime_service.get_runtime_manager()
            second = runtime_service.get_runtime_manager()

        self.assertIs(first, second)
        factory.assert_called_once_with()
        manager.initialize.assert_not_called()

    def test_missing_configuration_lists_names_without_values(self):
        with patch.dict(
            os.environ,
            {name: "" for name in REQUIRED_ENV},
        ):
            missing = runtime_service.get_missing_required_configuration()

        self.assertEqual(missing, tuple(REQUIRED_ENV))
        self.assertNotIn("test-google-key", str(missing))
        self.assertNotIn("test-tavily-key", str(missing))

    def test_app_renders_missing_configuration_without_creating_manager(self):
        manager = Mock(spec=RuntimeManager)
        manager.check_wordpress_connection.return_value = {"status": "disconnected"}
        with (
            patch.dict(os.environ, {name: "" for name in REQUIRED_ENV}),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
        ):
            app = AppTest.from_file(str(APP_FILE)).run()

        self.assertFalse(app.exception)
        self.assertEqual(app.title[0].value, "Blog Writing Agent")
        self.assertTrue(app.error)
        error_text = app.error[0].value
        self.assertIn("GOOGLE_API_KEY", error_text)
        self.assertIn("SUPABASE_DB_URI", error_text)
        manager.initialize.assert_not_called()
        manager.check_wordpress_connection.assert_called_once()

    def test_runtime_initializes_once_across_app_reruns_without_starting_work(self):
        runtime = FakeWorkflowRuntime()
        factory_calls = 0

        async def factory():
            nonlocal factory_calls
            factory_calls += 1
            return runtime

        manager = RuntimeManager(factory)
        self.addCleanup(manager.shutdown)
        with (
            patch.dict(os.environ, REQUIRED_ENV),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            patch.object(
                wordpress_service,
                "check_wordpress_connection",
                return_value="disconnected",
            ),
        ):
            app = AppTest.from_file(str(APP_FILE)).run()
            self.assertFalse(app.exception)
            self.assertTrue(app.success)

            app.run()
            self.assertFalse(app.exception)
            self.assertEqual(factory_calls, 1)

        self.assertEqual(runtime.started_workflows, [])

    def test_initialization_errors_are_shown_without_exception_details(self):
        secret_like_error = "failure using postgresql://private-secret@host/database"
        manager = Mock(spec=RuntimeManager)
        manager.initialize.side_effect = RuntimeError(secret_like_error)

        with (
            patch.dict(os.environ, REQUIRED_ENV),
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            patch.object(
                wordpress_service,
                "check_wordpress_connection",
                return_value="disconnected",
            ),
        ):
            with self.assertLogs(level="ERROR") as captured:
                app = AppTest.from_file(str(APP_FILE)).run()

        self.assertFalse(app.exception)
        self.assertTrue(app.error)
        error_text = app.error[0].value
        self.assertIn("could not be initialized", error_text)
        self.assertNotIn("private-secret", error_text)
        self.assertNotIn("private-secret", "\n".join(captured.output))
        manager.initialize.assert_called_once()


if __name__ == "__main__":
    unittest.main()
