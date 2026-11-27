import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

from streamlit.testing.v1 import AppTest

from blog_writer.integrations.wordpress_mcp import REDIRECT_URI
from streamlit_ui.services import runtime_service, wordpress_service
from streamlit_ui.services.runtime_manager import RuntimeManager


APP_FILE = Path(__file__).resolve().parents[1] / "app.py"
CALLBACK_URL = f"{REDIRECT_URI}?code=authorization-code&state=authorization-state"
AUTHORIZATION_URL = (
    "https://wordpress.example.test/authorize?client_id=client&state=secret-state"
)


def click_button(app, label):
    next(button for button in app.button if button.label == label).click()


class WordPressServiceTests(unittest.TestCase):
    def test_start_and_get_authorization_url_use_manager_flow_id(self):
        manager = Mock(spec=RuntimeManager)
        manager.start_wordpress_oauth.return_value = {
            "flow_id": "flow-123",
            "authorization_url": AUTHORIZATION_URL,
            "connected": False,
        }
        manager.get_wordpress_authorization_url.return_value = AUTHORIZATION_URL

        with patch.object(runtime_service, "get_runtime_manager", return_value=manager):
            started = wordpress_service.start_wordpress_authorization()
            authorization_url = wordpress_service.get_authorization_url(
                started["flow_id"]
            )

        self.assertEqual(started["flow_id"], "flow-123")
        self.assertEqual(authorization_url, AUTHORIZATION_URL)
        manager.get_wordpress_authorization_url.assert_called_once_with("flow-123")

    def test_callback_validation_rejects_malformed_url_without_sending_it(self):
        manager = Mock(spec=RuntimeManager)
        with (
            patch.object(runtime_service, "get_runtime_manager", return_value=manager),
            self.assertRaises(wordpress_service.WordPressServiceError) as error,
        ):
            wordpress_service.complete_wordpress_authorization(
                "flow-123",
                "not-a-callback-url?code=private-code&state=private-state",
            )

        self.assertNotIn("private-code", str(error.exception))
        manager.complete_wordpress_oauth.assert_not_called()

    def test_callback_uses_pending_flow_and_exact_redirected_url(self):
        manager = Mock(spec=RuntimeManager)
        manager.complete_wordpress_oauth.return_value = {"connected": True}
        with patch.object(runtime_service, "get_runtime_manager", return_value=manager):
            result = wordpress_service.complete_wordpress_authorization(
                "flow-123",
                CALLBACK_URL,
            )

        self.assertEqual(result, {"connected": True})
        manager.complete_wordpress_oauth.assert_called_once_with(
            "flow-123",
            CALLBACK_URL,
        )

    def test_cancellation_and_status_use_runtime_manager(self):
        manager = Mock(spec=RuntimeManager)
        manager.check_wordpress_connection.return_value = {
            "status": "connected"
        }
        with patch.object(runtime_service, "get_runtime_manager", return_value=manager):
            wordpress_service.cancel_wordpress_authorization("flow-123")
            status = wordpress_service.check_wordpress_connection()

        manager.cancel_wordpress_oauth.assert_called_once_with("flow-123")
        self.assertEqual(status, "connected")

    def test_disconnect_reports_existing_storage_limitation(self):
        with self.assertRaisesRegex(
            wordpress_service.WordPressServiceError,
            "not supported",
        ):
            wordpress_service.disconnect_wordpress()


class StreamlitWordPressAppTests(unittest.TestCase):
    @contextmanager
    def _run_app(self, manager):
        with (
            patch.object(
                runtime_service,
                "get_missing_required_configuration",
                return_value=("GOOGLE_API_KEY", "SUPABASE_DB_URI"),
            ),
            patch.object(
                runtime_service,
                "get_runtime_manager",
                return_value=manager,
            ),
        ):
            app = AppTest.from_file(str(APP_FILE)).run()
            self.assertFalse(app.exception)
            yield app

    def _manager(self, status="disconnected"):
        manager = Mock(spec=RuntimeManager)
        manager.check_wordpress_connection.return_value = {"status": status}
        return manager

    def test_disconnected_state_can_start_authorization_without_workflow_config(self):
        manager = self._manager()
        manager.start_wordpress_oauth.return_value = {
            "flow_id": "flow-abc",
            "authorization_url": AUTHORIZATION_URL,
            "connected": False,
        }
        manager.get_wordpress_authorization_url.return_value = AUTHORIZATION_URL

        with self._run_app(manager) as app:
            self.assertTrue(any("not connected" in item.value for item in app.info))
            click_button(app, "Connect WordPress")
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(app.session_state["oauth_flow_id"], "flow-abc")
            self.assertEqual(
                app.session_state["wordpress_connection_status"],
                "pending",
            )
            manager.initialize.assert_not_called()
            manager.start_wordpress_oauth.assert_called_once()
            manager.get_wordpress_authorization_url.assert_called_with("flow-abc")
            self.assertTrue(
                any(
                    item.label == "Open WordPress authorization page"
                    for item in app.link_button
                )
            )
            visible_text = "\n".join(
                [item.value for item in app.info]
                + [item.value for item in app.caption]
                + [item.value for item in app.error]
            )
            self.assertNotIn("secret-state", visible_text)

    def test_pending_callback_completes_and_clears_callback_input(self):
        manager = self._manager()
        manager.start_wordpress_oauth.return_value = {
            "flow_id": "flow-abc",
            "authorization_url": AUTHORIZATION_URL,
            "connected": False,
        }
        manager.get_wordpress_authorization_url.return_value = AUTHORIZATION_URL
        manager.complete_wordpress_oauth.return_value = {"connected": True}

        with self._run_app(manager) as app:
            click_button(app, "Connect WordPress")
            app.run()
            next(item for item in app.text_area).set_value(CALLBACK_URL)
            click_button(app, "Complete connection")
            app.run()

            self.assertFalse(app.exception)
            self.assertEqual(
                manager.complete_wordpress_oauth.call_args.args,
                ("flow-abc", CALLBACK_URL),
            )
            self.assertIsNone(app.session_state["oauth_flow_id"])
            self.assertEqual(
                app.session_state["wordpress_connection_status"],
                "connected",
            )
            self.assertEqual(
                app.session_state["wordpress_oauth_callback_url"],
                "",
            )
            self.assertTrue(
                any("WordPress is connected" in item.value for item in app.success)
            )
            self.assertNotIn("authorization-code", str(dict(app.session_state)))
            self.assertNotIn("secret-state", str(dict(app.session_state)))

    def test_invalid_callback_is_safe_and_keeps_authorization_pending(self):
        manager = self._manager()
        manager.start_wordpress_oauth.return_value = {
            "flow_id": "flow-abc",
            "authorization_url": AUTHORIZATION_URL,
            "connected": False,
        }
        manager.get_wordpress_authorization_url.return_value = AUTHORIZATION_URL

        with self._run_app(manager) as app:
            click_button(app, "Connect WordPress")
            app.run()
            next(item for item in app.text_area).set_value(
                "https://example.test/not-the-redirect?code=private-code&state=private"
            )
            click_button(app, "Complete connection")
            app.run()

            self.assertEqual(manager.complete_wordpress_oauth.call_count, 0)
            self.assertEqual(app.session_state["oauth_flow_id"], "flow-abc")
            error_text = "\n".join(item.value for item in app.error)
            self.assertIn("does not match", error_text)
            self.assertNotIn("private-code", error_text)
            self.assertNotIn("private", error_text)

    def test_cancel_clears_pending_flow(self):
        manager = self._manager()
        manager.start_wordpress_oauth.return_value = {
            "flow_id": "flow-abc",
            "authorization_url": AUTHORIZATION_URL,
            "connected": False,
        }
        manager.get_wordpress_authorization_url.return_value = AUTHORIZATION_URL

        with self._run_app(manager) as app:
            click_button(app, "Connect WordPress")
            app.run()
            click_button(app, "Cancel authorization")
            app.run()

            manager.cancel_wordpress_oauth.assert_called_once_with("flow-abc")
            self.assertIsNone(app.session_state["oauth_flow_id"])
            self.assertEqual(
                app.session_state["wordpress_connection_status"],
                "disconnected",
            )

    def test_connected_state_displays_no_token_data_or_disconnect_button(self):
        manager = self._manager("connected")
        with self._run_app(manager) as app:
            self.assertTrue(
                any("WordPress is connected" in item.value for item in app.success)
            )
            self.assertTrue(
                any("Disconnect is not supported" in item.value for item in app.caption)
            )
            self.assertFalse(
                any("Disconnect" in item.label for item in app.button)
            )
            output = "\n".join(
                [item.value for item in app.success]
                + [item.value for item in app.caption]
                + [item.value for item in app.error]
            )
            self.assertNotIn("access_token", output)
            self.assertNotIn("refresh_token", output)
            self.assertNotIn("client_secret", output)

    def test_start_error_is_user_safe(self):
        manager = self._manager()
        manager.start_wordpress_oauth.side_effect = RuntimeError(
            "client_secret=private-secret"
        )
        with self._run_app(manager) as app:
            click_button(app, "Connect WordPress")
            with self.assertLogs(level="ERROR"):
                app.run()

            error_text = "\n".join(item.value for item in app.error)
            self.assertIn("could not start", error_text)
            self.assertNotIn("private-secret", error_text)


if __name__ == "__main__":
    unittest.main()
