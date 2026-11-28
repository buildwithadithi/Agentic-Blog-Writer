import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlencode, urlparse
from unittest.mock import AsyncMock, patch

import httpx2
from mcp.shared.auth import (
    OAuthClientInformationFull,
    OAuthMetadata,
    OAuthToken,
)

from blog_writer.integrations import wordpress_mcp
from blog_writer.integrations.wordpress_mcp import WordPressMCPClient
from blog_writer.integrations.wordpress_oauth import FileTokenStorage
from blog_writer.nodes import wordpress


ROOT = Path(__file__).resolve().parents[1]
TEST_REDIRECT_URI = "https://example.test/oauth/callback"
TEST_ISSUER = "https://auth.example.test"


def make_oauth_metadata():
    return OAuthMetadata(
        issuer=TEST_ISSUER,
        authorization_endpoint=f"{TEST_ISSUER}/authorize",
        token_endpoint=f"{TEST_ISSUER}/token",
        authorization_response_iss_parameter_supported=True,
    )


def make_client_info(redirect_uri=TEST_REDIRECT_URI):
    return OAuthClientInformationFull(
        client_id="test-client-id",
        redirect_uris=[redirect_uri],
        token_endpoint_auth_method="none",
    )


class AsyncContext:
    def __init__(self, value):
        self.value = value
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self.value

    async def __aexit__(self, exc_type, exc_value, traceback):
        self.exited = True


class TaskBoundAsyncContext(AsyncContext):
    def __init__(self, value):
        super().__init__(value)
        self.owner_task = None

    async def __aenter__(self):
        self.owner_task = asyncio.current_task()
        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        if asyncio.current_task() is not self.owner_task:
            raise RuntimeError("Async context exited from a different task.")
        await super().__aexit__(exc_type, exc_value, traceback)


class WordPressOAuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addAsyncCleanup(self.temp_dir.cleanup)
        self.token_path = Path(self.temp_dir.name) / "wordpress_oauth.json"
        self.storage = FileTokenStorage(self.token_path)

    async def test_token_storage_preserves_existing_json_shape(self):
        tokens = OAuthToken(
            access_token="test-access-token",
            token_type="Bearer",
            expires_in=3600,
            refresh_token="test-refresh-token",
            scope="auth",
        )
        client_info = make_client_info()

        await self.storage.set_client_info(client_info)
        await self.storage.set_tokens(tokens)

        reloaded = FileTokenStorage(self.token_path)
        self.assertEqual(await reloaded.get_tokens(), tokens)
        self.assertEqual(await reloaded.get_client_info(), client_info)
        self.assertEqual(
            set(json.loads(self.token_path.read_text(encoding="utf-8"))),
            {"tokens", "client_info", "token_issued_at"},
        )
        self.assertIsInstance(await reloaded.get_token_issued_at(), float)
        if os.name == "posix":
            self.assertEqual(self.token_path.stat().st_mode & 0o777, 0o600)

    async def test_legacy_token_storage_without_issue_time_is_treated_as_expired(self):
        self.token_path.write_text(
            json.dumps(
                {
                    "tokens": {
                        "access_token": "legacy-access-token",
                        "token_type": "Bearer",
                        "expires_in": 3600,
                        "refresh_token": "legacy-refresh-token",
                    },
                    "client_info": make_client_info().model_dump(mode="json"),
                }
            ),
            encoding="utf-8",
        )
        client = WordPressMCPClient(token_storage=self.storage)

        await client.oauth_provider._initialize()

        self.assertFalse(client.oauth_provider.context.is_token_valid())
        self.assertTrue(client.oauth_provider.context.can_refresh_token())

    async def test_expired_persisted_token_uses_discovered_refresh_endpoint(self):
        await self.storage.set_client_info(make_client_info())
        await self.storage.set_tokens(
            OAuthToken(
                access_token="expired-access-token",
                token_type="Bearer",
                expires_in=0,
                refresh_token="existing-refresh-token",
                scope="auth",
            )
        )
        client = WordPressMCPClient(token_storage=self.storage)
        metadata = OAuthMetadata(
            issuer="https://public-api.wordpress.com",
            authorization_endpoint="https://auth.example.test/authorize",
            token_endpoint="https://auth.example.test/custom/oauth/token",
        )
        discovery_client = SimpleNamespace()

        async def send(request):
            if request.url.path.endswith("/.well-known/oauth-authorization-server"):
                return httpx2.Response(
                    200,
                    json=metadata.model_dump(mode="json"),
                    request=request,
                )
            return httpx2.Response(404, request=request)

        discovery_client.send = AsyncMock(side_effect=send)
        discovery_context = AsyncContext(discovery_client)

        with patch.object(
            wordpress_mcp.httpx2,
            "AsyncClient",
            return_value=discovery_context,
        ):
            await client._prepare_oauth_metadata()

        self.assertFalse(client.oauth_provider.context.is_token_valid())
        refresh_request = await client.oauth_provider._refresh_token()
        self.assertEqual(
            str(refresh_request.url),
            "https://auth.example.test/custom/oauth/token",
        )
        self.assertEqual(
            str(discovery_client.send.await_args_list[-1].args[0].url),
            "https://public-api.wordpress.com/.well-known/oauth-authorization-server",
        )

    async def test_authorization_url_and_callback_exchange_use_mcp_provider(self):
        client = WordPressMCPClient(
            redirect_uri=TEST_REDIRECT_URI,
            token_storage=self.storage,
        )
        provider = client.oauth_provider
        provider.context.client_info = make_client_info()
        provider.context.oauth_metadata = make_oauth_metadata()
        provider.context.auth_server_url = TEST_ISSUER
        await self.storage.set_client_info(provider.context.client_info)
        captured = {}

        async def mock_connect():
            code, verifier = await provider._perform_authorization_code_grant()
            request = await provider._exchange_token_authorization_code(
                code,
                verifier,
            )
            captured["request"] = request
            response = httpx2.Response(
                200,
                json={
                    "access_token": "exchanged-access-token",
                    "token_type": "Bearer",
                    "expires_in": 3600,
                    "refresh_token": "exchanged-refresh-token",
                },
                request=request,
            )
            await provider._handle_token_response(response)

        client.connect = mock_connect
        authorization_url = await client.get_authorization_url()
        self.assertIsInstance(authorization_url, str)

        query = parse_qs(urlparse(authorization_url).query)
        self.assertEqual(query["response_type"], ["code"])
        self.assertEqual(query["client_id"], ["test-client-id"])
        self.assertEqual(query["redirect_uri"], [TEST_REDIRECT_URI])
        self.assertEqual(query["code_challenge_method"], ["S256"])
        self.assertTrue(query["code_challenge"][0])

        callback_url = f"{TEST_REDIRECT_URI}?{urlencode({
            'code': 'test-authorization-code',
            'state': query['state'][0],
            'iss': TEST_ISSUER,
        })}"
        await client.handle_oauth_callback(callback_url)

        request = captured["request"]
        form = parse_qs(request.content.decode("utf-8"))
        self.assertEqual(request.method, "POST")
        self.assertEqual(str(request.url), f"{TEST_ISSUER}/token")
        self.assertEqual(form["grant_type"], ["authorization_code"])
        self.assertEqual(form["code"], ["test-authorization-code"])
        self.assertEqual(form["redirect_uri"], [TEST_REDIRECT_URI])
        self.assertTrue(form["code_verifier"][0])
        self.assertEqual(
            (await self.storage.get_tokens()).access_token,
            "exchanged-access-token",
        )

    async def test_callback_rejects_state_mismatch(self):
        client = WordPressMCPClient(
            redirect_uri=TEST_REDIRECT_URI,
            token_storage=self.storage,
        )
        provider = client.oauth_provider
        provider.context.client_info = make_client_info()
        provider.context.oauth_metadata = make_oauth_metadata()

        async def mock_connect():
            await provider._perform_authorization_code_grant()

        client.connect = mock_connect
        authorization_url = await client.get_authorization_url()
        query = parse_qs(urlparse(authorization_url).query)
        callback_url = f"{TEST_REDIRECT_URI}?{urlencode({
            'code': 'test-authorization-code',
            'state': 'incorrect-state',
            'iss': TEST_ISSUER,
        })}"

        with self.assertRaisesRegex(ValueError, "OAuth state validation failed"):
            await client.handle_oauth_callback(callback_url)
        valid_callback_url = f"{TEST_REDIRECT_URI}?{urlencode({
            'code': 'test-authorization-code',
            'state': query['state'][0],
            'iss': TEST_ISSUER,
        })}"
        await client.handle_oauth_callback(valid_callback_url)
        self.assertIsNone(await self.storage.get_tokens())

    async def test_refresh_uses_stored_refresh_token_and_persists_rotation(self):
        await self.storage.set_client_info(make_client_info())
        await self.storage.set_tokens(
            OAuthToken(
                access_token="expired-access-token",
                token_type="Bearer",
                expires_in=0,
                refresh_token="existing-refresh-token",
                scope="auth",
            )
        )
        client = WordPressMCPClient(token_storage=self.storage)
        provider = client.oauth_provider
        provider.context.oauth_metadata = make_oauth_metadata()
        await provider._initialize()

        request = await provider._refresh_token()
        form = parse_qs(request.content.decode("utf-8"))
        self.assertEqual(form["grant_type"], ["refresh_token"])
        self.assertEqual(form["refresh_token"], ["existing-refresh-token"])
        self.assertEqual(form["client_id"], ["test-client-id"])

        response = httpx2.Response(
            200,
            json={
                "access_token": "rotated-access-token",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": "rotated-refresh-token",
                "scope": "auth",
            },
            request=request,
        )
        self.assertTrue(await provider._handle_refresh_response(response))
        persisted = await self.storage.get_tokens()
        self.assertEqual(persisted.access_token, "rotated-access-token")
        self.assertEqual(persisted.refresh_token, "rotated-refresh-token")

    async def test_connect_initializes_using_stored_credentials(self):
        stored_tokens = OAuthToken(
            access_token="mock-access-token",
            token_type="Bearer",
            expires_in=3600,
            refresh_token="mock-refresh-token",
            scope="auth",
        )
        stored_client = make_client_info()
        await self.storage.set_tokens(stored_tokens)
        await self.storage.set_client_info(stored_client)

        client = WordPressMCPClient(token_storage=self.storage)
        http_client = SimpleNamespace(auth=None)
        session = SimpleNamespace(initialize=AsyncMock())
        http_context = AsyncContext(http_client)
        stream_context = AsyncContext((object(), object()))
        session_context = AsyncContext(session)

        with (
            patch.object(client, "_prepare_oauth_metadata", new=AsyncMock()),
            patch.object(
                wordpress_mcp.httpx2,
                "AsyncClient",
                return_value=http_context,
            ) as http_client_factory,
            patch.object(
                wordpress_mcp,
                "streamable_http_client",
                return_value=stream_context,
            ),
            patch.object(
                wordpress_mcp,
                "ClientSession",
                return_value=session_context,
            ),
        ):
            await client.oauth_provider._initialize()
            await client.connect()

        self.assertEqual(client.oauth_provider.context.current_tokens, stored_tokens)
        self.assertEqual(client.oauth_provider.context.client_info, stored_client)
        self.assertIs(client.session, session)
        self.assertIs(
            http_client_factory.call_args.kwargs["auth"],
            client.oauth_provider,
        )
        session.initialize.assert_awaited_once()
        await client.close()
        self.assertTrue(http_context.exited)
        self.assertTrue(stream_context.exited)
        self.assertTrue(session_context.exited)

    async def test_oauth_client_closes_resources_in_the_task_that_opened_them(self):
        client = WordPressMCPClient(
            token_storage=self.storage,
            close_after_connect=True,
        )
        http_client = SimpleNamespace(auth=None)
        session = SimpleNamespace(initialize=AsyncMock())
        http_context = TaskBoundAsyncContext(http_client)
        stream_context = TaskBoundAsyncContext((object(), object()))
        session_context = TaskBoundAsyncContext(session)

        with (
            patch.object(client, "_prepare_oauth_metadata", new=AsyncMock()),
            patch.object(
                wordpress_mcp.httpx2,
                "AsyncClient",
                return_value=http_context,
            ),
            patch.object(
                wordpress_mcp,
                "streamable_http_client",
                return_value=stream_context,
            ),
            patch.object(
                wordpress_mcp,
                "ClientSession",
                return_value=session_context,
            ),
        ):
            authorization_url = await client.get_authorization_url()
            await client.close()

        self.assertIsNone(authorization_url)
        self.assertTrue(http_context.exited)
        self.assertTrue(stream_context.exited)
        self.assertTrue(session_context.exited)
        self.assertIsNone(client.exit_stack)
        self.assertIsNone(client.session)

    async def test_create_post_stays_a_draft_with_expected_payload(self):
        result = SimpleNamespace(
            is_error=False,
            structuredContent={
                "data": {
                    "id": 42,
                    "title": "Mock title",
                    "content": "Mock content",
                    "status": "draft",
                    "link": "https://example.test/draft/42",
                }
            },
            content=[],
        )
        session = SimpleNamespace(call_tool=AsyncMock(return_value=result))
        client = WordPressMCPClient()
        client.session = session

        with patch.object(client, "_print_post"):
            post = await client.create_post(
                "Mock title",
                "Mock content",
                confirmed=True,
            )

        self.assertEqual(post["status"], "draft")
        call_args = session.call_tool.await_args
        self.assertEqual(call_args.args[0], "execute")
        self.assertEqual(
            call_args.kwargs["arguments"]["input"],
            {
                "title": "Mock title",
                "content": "Mock content",
                "status": "draft",
            },
        )
        self.assertTrue(call_args.kwargs["arguments"]["user_confirmed"])

    async def test_langgraph_wordpress_node_still_uses_client_contract(self):
        class FakeClient:
            def __init__(self):
                self.connect_calls = 0
                self.create_calls = []
                self.close_calls = 0

            async def connect(self):
                self.connect_calls += 1

            async def upload_images(self, content, image_specs=None):
                return content

            async def create_post(self, *, title, content, confirmed=False):
                self.create_calls.append((title, content, confirmed))
                return {
                    "id": 42,
                    "link": "https://example.test/draft/42",
                    "status": "draft",
                }

            async def close(self):
                self.close_calls += 1

        fake_client = FakeClient()
        with patch.object(wordpress, "WordPressMCPClient", return_value=fake_client):
            result = await wordpress.wordpress_draft_node(
                {
                    "review_status": "approved",
                    "topic": "Mock title",
                    "final": "Mock content",
                    "wordpress_post_id": None,
                }
            )

        self.assertEqual(fake_client.connect_calls, 1)
        self.assertEqual(fake_client.create_calls, [("Mock title", "Mock content", True)])
        self.assertEqual(fake_client.close_calls, 1)
        self.assertEqual(result["wordpress_status"], "draft")

    def test_core_import_does_not_load_notebook_ui_dependencies(self):
        code = (
            "import sys; "
            "import blog_writer.integrations.wordpress_mcp; "
            "assert 'ipywidgets' not in sys.modules; "
            "assert 'IPython' not in sys.modules"
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_package_import_exports_notebook_widgets_lazily(self):
        code = (
            "import blog_writer.integrations.wordpress_mcp as wordpress_mcp; "
            "assert wordpress_mcp.WordPressMCPClient; "
            "assert wordpress_mcp.auth_callback_widget is not None; "
            "assert wordpress_mcp.callback_status is not None"
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
