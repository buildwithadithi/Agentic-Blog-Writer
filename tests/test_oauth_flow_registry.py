import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from mcp.shared.auth import OAuthClientInformationFull, OAuthToken

from blog_writer.integrations.wordpress_oauth import FileTokenStorage
from streamlit_ui.services.oauth_flow_registry import OAuthFlowRegistry
from streamlit_ui.services.runtime_manager import RuntimeManager


class FakeOAuthClient:
    def __init__(self, storage):
        self.storage = storage
        self.authorization_flow = SimpleNamespace(is_pending=False)
        self.loop_ids = []
        self.callback_urls = []
        self.cancel_calls = 0
        self.close_calls = 0
        self.connect_calls = 0
        self.connect_error = None
        self.pending_task = None
        self._release_task = asyncio.Event()

    async def get_authorization_url(self):
        self.loop_ids.append(id(asyncio.get_running_loop()))
        self.authorization_flow.is_pending = True
        self.pending_task = asyncio.create_task(self._wait_for_callback())
        return "https://wordpress.example.test/authorize?state=private-state"

    async def _wait_for_callback(self):
        await self._release_task.wait()

    async def handle_oauth_callback(self, callback_url):
        self.loop_ids.append(id(asyncio.get_running_loop()))
        self.callback_urls.append(callback_url)
        if "bad-state" in callback_url:
            raise ValueError("OAuth state validation failed.")
        self._release_task.set()
        await self.pending_task
        self.authorization_flow.is_pending = False

    async def cancel_oauth(self):
        self.loop_ids.append(id(asyncio.get_running_loop()))
        self.cancel_calls += 1
        if self.pending_task is not None and not self.pending_task.done():
            self.pending_task.cancel()
            try:
                await self.pending_task
            except asyncio.CancelledError:
                pass
        self.authorization_flow.is_pending = False

    async def connect(self):
        self.loop_ids.append(id(asyncio.get_running_loop()))
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def close(self):
        self.loop_ids.append(id(asyncio.get_running_loop()))
        self.close_calls += 1
        await self.cancel_oauth()


class OAuthFlowRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.storage = FileTokenStorage(
            Path(self.temp_dir.name) / "wordpress_oauth.json"
        )
        self.clients = []

        def client_factory(storage):
            client = FakeOAuthClient(storage)
            self.clients.append(client)
            return client

        self.registry = OAuthFlowRegistry(
            client_factory=client_factory,
            storage_factory=lambda: self.storage,
        )

    async def test_authorization_url_and_callback_reuse_same_client_and_loop(self):
        started = await self.registry.start_authorization()
        client = self.clients[0]

        self.assertFalse(started["connected"])
        self.assertEqual(
            await self.registry.get_authorization_url(started["flow_id"]),
            started["authorization_url"],
        )
        completed = await self.registry.complete_authorization(
            started["flow_id"],
            "http://localhost:8080/callback?code=auth-code&state=state",
        )

        self.assertEqual(completed, {"connected": True})
        self.assertEqual(len(self.clients), 1)
        self.assertEqual(len(set(client.loop_ids)), 1)
        self.assertEqual(client.callback_urls, [
            "http://localhost:8080/callback?code=auth-code&state=state"
        ])
        self.assertEqual(client.close_calls, 1)
        with self.assertRaises(LookupError):
            await self.registry.get_authorization_url(started["flow_id"])
        with self.assertRaises(LookupError):
            await self.registry.complete_authorization(
                started["flow_id"],
                "http://localhost:8080/callback?code=again&state=state",
            )

    async def test_duplicate_authorization_start_is_rejected(self):
        await self.registry.start_authorization()

        with self.assertRaisesRegex(RuntimeError, "already pending"):
            await self.registry.start_authorization()

        self.assertEqual(len(self.clients), 1)

    async def test_state_validation_failure_keeps_pending_flow_for_retry(self):
        started = await self.registry.start_authorization()
        client = self.clients[0]

        with self.assertRaisesRegex(ValueError, "state validation failed"):
            await self.registry.complete_authorization(
                started["flow_id"],
                "http://localhost:8080/callback?code=auth-code&state=bad-state",
            )

        self.assertTrue(client.authorization_flow.is_pending)
        self.assertEqual(
            await self.registry.get_authorization_url(started["flow_id"]),
            started["authorization_url"],
        )

    async def test_cancellation_cleans_up_pending_task_and_removes_flow(self):
        started = await self.registry.start_authorization()
        client = self.clients[0]

        self.assertEqual(
            await self.registry.cancel_authorization(started["flow_id"]),
            {"cancelled": True},
        )
        self.assertTrue(client.pending_task.cancelled())
        self.assertEqual(client.cancel_calls, 2)
        self.assertEqual(client.close_calls, 1)
        with self.assertRaises(LookupError):
            await self.registry.get_authorization_url(started["flow_id"])

    async def test_connection_status_checks_stored_credentials_and_client(self):
        disconnected = await self.registry.check_connection()
        self.assertEqual(disconnected, {"status": "disconnected"})
        self.assertEqual(self.clients, [])

        await self.storage.set_tokens(
            OAuthToken(
                access_token="test-token",
                token_type="Bearer",
                expires_in=3600,
            )
        )
        await self.storage.set_client_info(
            OAuthClientInformationFull(
                client_id="test-client",
                redirect_uris=["http://localhost:8080/callback"],
                token_endpoint_auth_method="none",
            )
        )

        connected = await self.registry.check_connection()

        self.assertEqual(connected, {"status": "connected"})
        self.assertEqual(self.clients[-1].connect_calls, 1)
        self.assertEqual(self.clients[-1].close_calls, 1)

    async def test_connection_status_is_unverified_when_client_cannot_connect(self):
        await self.storage.set_tokens(
            OAuthToken(
                access_token="test-token",
                token_type="Bearer",
                expires_in=3600,
            )
        )
        await self.storage.set_client_info(
            OAuthClientInformationFull(
                client_id="test-client",
                redirect_uris=["http://localhost:8080/callback"],
                token_endpoint_auth_method="none",
            )
        )

        def failing_client_factory(storage):
            client = FakeOAuthClient(storage)
            client.connect_error = OSError("private network detail")
            self.clients.append(client)
            return client

        registry = OAuthFlowRegistry(
            client_factory=failing_client_factory,
            storage_factory=lambda: self.storage,
        )
        self.assertEqual(
            await registry.check_connection(),
            {"status": "unverified"},
        )

    def test_runtime_manager_shutdown_cleans_pending_flow_on_its_loop(self):
        client_box = []

        def client_factory(storage):
            client = FakeOAuthClient(storage)
            client_box.append(client)
            return client

        registry = OAuthFlowRegistry(
            client_factory=client_factory,
            storage_factory=lambda: self.storage,
        )
        manager = RuntimeManager(
            runtime_factory=lambda: self._unexpected_runtime_factory(),
            oauth_registry_factory=lambda: registry,
        )
        self.addCleanup(manager.shutdown)
        manager.start_wordpress_oauth()
        client = client_box[0]
        manager.shutdown()

        self.assertTrue(client.pending_task.cancelled())
        self.assertEqual(client.cancel_calls, 2)
        self.assertEqual(client.close_calls, 1)
        self.assertEqual(len(set(client.loop_ids)), 1)
        self.assertIsNone(manager._loop)
        self.assertIsNone(manager._oauth_registry)

    def test_workflow_runtime_uses_existing_oauth_event_loop(self):
        client_box = []
        runtime_loop_ids = []

        def client_factory(storage):
            client = FakeOAuthClient(storage)
            client_box.append(client)
            return client

        async def runtime_factory():
            runtime_loop_ids.append(id(asyncio.get_running_loop()))
            return SimpleNamespace(shutdown=self._noop_shutdown)

        registry = OAuthFlowRegistry(
            client_factory=client_factory,
            storage_factory=lambda: self.storage,
        )
        manager = RuntimeManager(
            runtime_factory=runtime_factory,
            oauth_registry_factory=lambda: registry,
        )
        self.addCleanup(manager.shutdown)
        started = manager.start_wordpress_oauth()
        client = client_box[0]
        manager.complete_wordpress_oauth(
            started["flow_id"],
            "http://localhost:8080/callback?code=auth-code&state=state",
        )
        manager.initialize()

        self.assertEqual(len(runtime_loop_ids), 1)
        self.assertEqual(client.loop_ids[0], runtime_loop_ids[0])
        self.assertEqual(len(set(client.loop_ids)), 1)
        manager.shutdown()

    async def _unexpected_runtime_factory(self):
        raise AssertionError("OAuth operations must not initialize the workflow runtime.")

    @staticmethod
    async def _noop_shutdown():
        return None


if __name__ == "__main__":
    unittest.main()
