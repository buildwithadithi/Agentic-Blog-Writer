
import json
from contextlib import AsyncExitStack
from collections.abc import Awaitable, Callable
from typing import Protocol

import httpx2

from pydantic import AnyUrl

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.client.auth import OAuthClientProvider
from mcp.shared.auth import (
    AuthorizationCodeResult,
    OAuthClientInformationFull,
    OAuthClientMetadata,
    OAuthToken,
)

from blog_writer.integrations.wordpress_oauth import (
    FileTokenStorage,
    OAuthAuthorizationFlow,
    parse_callback_url,
)

WORDPRESS_MCP_URL = (
    "https://public-api.wordpress.com/wpcom/v2/mcp/v1"
)

WORDPRESS_SITE = "https://buildwithadithi.wordpress.com"

REDIRECT_URI = "http://localhost:8080/callback"


_notebook_redirect_handler: Callable[[str], Awaitable[None]] | None = None
_notebook_callback_handler: Callable[[], Awaitable[AuthorizationCodeResult]] | None = None


class OAuthTokenStorage(Protocol):
    async def get_tokens(self) -> OAuthToken | None: ...

    async def set_tokens(self, tokens: OAuthToken) -> None: ...

    async def get_client_info(self) -> OAuthClientInformationFull | None: ...

    async def set_client_info(
        self,
        client_info: OAuthClientInformationFull,
    ) -> None: ...


def register_notebook_oauth_handlers(
    redirect_handler: Callable[[str], Awaitable[None]],
    callback_handler: Callable[[], Awaitable[AuthorizationCodeResult]],
) -> None:
    global _notebook_redirect_handler, _notebook_callback_handler
    _notebook_redirect_handler = redirect_handler
    _notebook_callback_handler = callback_handler


async def _missing_redirect_handler(_: str) -> None:
    raise RuntimeError(
        "Start authorization with WordPressMCPClient.get_authorization_url()."
    )


async def _missing_callback_handler() -> AuthorizationCodeResult:
    raise RuntimeError(
        "Complete authorization with WordPressMCPClient.handle_oauth_callback()."
    )


class WordPressMCPClient:
    def __init__(
        self,
        *,
        redirect_uri: str = REDIRECT_URI,
        token_storage: OAuthTokenStorage | None = None,
        close_after_connect: bool = False,
        redirect_handler: Callable[[str], Awaitable[None]] | None = None,
        callback_handler: Callable[[], Awaitable[AuthorizationCodeResult]] | None = None,
    ):
        self._close_after_connect = close_after_connect
        self.token_storage = (
            token_storage if token_storage is not None else FileTokenStorage()
        )

        self.client_metadata = OAuthClientMetadata(
            client_name="Blog Writing Agent",
            redirect_uris=[AnyUrl(redirect_uri)],
            grant_types=[
                "authorization_code",
                "refresh_token"
            ],
            response_types=["code"],
            scope="auth"
        )

        self.oauth_provider = OAuthClientProvider(
            server_url=WORDPRESS_MCP_URL,
            client_metadata=self.client_metadata,
            storage=self.token_storage,
            redirect_handler=(
                redirect_handler
                or _notebook_redirect_handler
                or _missing_redirect_handler
            ),
            callback_handler=(
                callback_handler
                or _notebook_callback_handler
                or _missing_callback_handler
            ),
        )

        self.session = None
        self.exit_stack = None
        self.authorization_flow = OAuthAuthorizationFlow(
            self.oauth_provider,
            lambda: self.connect(),
        )

    async def get_authorization_url(self) -> str | None:
        """Start the MCP OAuth flow and return its authorization URL, if needed."""
        return await self.authorization_flow.get_authorization_url()

    async def handle_oauth_callback(self, callback_url: str) -> None:
        """Validate and complete the pending OAuth callback and token exchange."""
        await self.authorization_flow.handle_callback(callback_url)

    async def cancel_oauth(self) -> None:
        """Cancel an authorization flow that is waiting for its callback."""
        await self.authorization_flow.cancel()

    async def _prepare_oauth_metadata(self):
        """Discover OAuth metadata before the first token refresh."""

        provider = self.oauth_provider
        context = provider.context

        # Load previously saved credentials.
        if not provider._initialized:
            await provider._initialize()

        # Do not repeat discovery if already completed.
        if context.oauth_metadata is not None:
            return

        # Use the SDK's own metadata-discovery helpers.
        helpers = type(provider)._auth_flow.__globals__

        build_urls = helpers[
            "build_oauth_authorization_server_metadata_discovery_urls"
        ]
        create_request = helpers["create_oauth_metadata_request"]
        handle_response = helpers["handle_auth_metadata_response"]

        # First discover the protected resource's authorization server.
        build_resource_urls = helpers[
            "build_protected_resource_metadata_discovery_urls"
        ]
        handle_resource = helpers["handle_protected_resource_response"]

        async with httpx2.AsyncClient(
            follow_redirects=False,
            timeout=30.0,
        ) as client:

            for url in build_resource_urls(None, WORDPRESS_MCP_URL):
                response = await client.send(create_request(url))
                resource_metadata = await handle_resource(response)

                if resource_metadata is not None:
                    context.protected_resource_metadata = resource_metadata

                    context.auth_server_url = (
                        provider._select_authorization_server(
                            [str(url) for url in resource_metadata.authorization_servers]
                        )
                    )
                    break

            # Discover the authorization server's token endpoint.
            urls = build_urls(
                context.auth_server_url,
                WORDPRESS_MCP_URL,
            )

            for url in urls:
                response = await client.send(create_request(url))
                should_continue, metadata = await handle_response(response)

                if metadata is not None:
                    # Validate the discovered issuer before trusting endpoints.
                    expected_issuer = provider._expected_issuer()

                    if context.auth_server_url is None:
                        if helpers["issuers_match"](
                            str(metadata.issuer), expected_issuer
                        ):
                            expected_issuer = str(metadata.issuer)

                    helpers["validate_metadata_issuer"](
                        metadata, expected_issuer
                    )

                    context.oauth_metadata = metadata
                    break

                if not should_continue:
                    raise RuntimeError(
                        f"OAuth metadata discovery failed: HTTP {response.status_code}"
                    )

        if context.oauth_metadata is None:
            raise RuntimeError(
                "Could not discover WordPress OAuth metadata."
            )

    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    async def connect(self):

        if self.session is not None:
            return
        
         # NEW: Discover OAuth metadata before automatic refresh.
        await self._prepare_oauth_metadata()

        self.exit_stack = AsyncExitStack()

        try:

            http_client = httpx2.AsyncClient(
                auth=self.oauth_provider,
                follow_redirects=False,
                timeout=30.0
            )

            await self.exit_stack.enter_async_context(
                http_client
            )

            read_stream, write_stream = (
                await self.exit_stack.enter_async_context(
                    streamable_http_client(
                        WORDPRESS_MCP_URL,
                        http_client=http_client
                    )
                )
            )

            self.session = await self.exit_stack.enter_async_context(
                ClientSession(read_stream, write_stream)
            )

            await self.session.initialize()

            if self._close_after_connect:
                await self._close_connection_resources()
            else:
                print("Connected to WordPress!")

        except BaseException:

            await self.exit_stack.aclose()

            self.exit_stack = None
            self.session = None

            raise

    # --------------------------------------------------------
    # CLOSE
    # --------------------------------------------------------

    async def close(self):

        await self.authorization_flow.cancel()

        if self.exit_stack is not None:
            await self._close_connection_resources()
            print("WordPress connection closed.")

    async def _close_connection_resources(self):
        exit_stack = self.exit_stack
        if exit_stack is None:
            return
        try:
            await exit_stack.aclose()
        finally:
            self.exit_stack = None
            self.session = None

    # --------------------------------------------------------
    # INTERNAL TOOL EXECUTION
    # --------------------------------------------------------

    async def _execute(self, name, input_data,
                       summary=None, confirmed=False):

        if self.session is None:
            raise RuntimeError(
                "Not connected. Call connect() first."
            )

        arguments = {
            "name": name,
            "wpcom_site": WORDPRESS_SITE,
            "input": input_data
        }

        if summary is not None:
            arguments["summary"] = summary
            arguments["user_confirmed"] = confirmed

        result = await self.session.call_tool(
            "execute",
            arguments=arguments
        )

        if result.is_error:

            error_text = "\n".join(
                item.text
                for item in result.content
                if hasattr(item, "text")
            )

            raise RuntimeError(
                f"WordPress operation failed: {error_text}"
            )

        data = (
            getattr(result, "structuredContent", None)
            or getattr(result, "structured_content", None)
        )

        if data is None:

            for item in result.content:
                if hasattr(item, "text"):
                    try:
                        return json.loads(item.text)
                    except json.JSONDecodeError:
                        pass

            raise RuntimeError(
                "No structured response received from WordPress."
            )

        return data

    # --------------------------------------------------------
    # SEARCH ABILITIES
    # --------------------------------------------------------

    async def search_abilities(
        self,
        query,
        readonly=None,
        limit=10
    ):

        if self.session is None:
            raise RuntimeError("Call connect() first.")

        arguments = {
            "query": query,
            "wpcom_site": WORDPRESS_SITE,
            "limit": limit,
            "detailed": False
        }

        if readonly is not None:
            arguments["readonly"] = readonly

        result = await self.session.call_tool(
            "search",
            arguments=arguments
        )

        if result.is_error:
            raise RuntimeError(str(result.content))

        return (
            getattr(result, "structuredContent", None)
            or getattr(result, "structured_content", None)
        )

    # --------------------------------------------------------
    # DESCRIBE ABILITY
    # --------------------------------------------------------

    async def describe_ability(self, name):

        if self.session is None:
            raise RuntimeError("Call connect() first.")

        result = await self.session.call_tool(
            "describe",
            arguments={
                "names": [name],
                "wpcom_site": WORDPRESS_SITE
            }
        )

        if result.is_error:
            raise RuntimeError(str(result.content))

        return (
            getattr(result, "structuredContent", None)
            or getattr(result, "structured_content", None)
        )

    # --------------------------------------------------------
    # CREATE POST
    # --------------------------------------------------------

    async def create_post(
        self,
        title,
        content,
        confirmed=False
    ):

        if not confirmed:
            raise ValueError(
                "Explicit confirmation is required to create a post."
            )

        data = await self._execute(
            name="wpcom-mcp/posts-create",

            summary=f"Create a draft WordPress post titled {title}.",

            confirmed=True,

            input_data={
                "title": title,
                "content": content,
                "status": "draft"
            }
        )

        post = data["data"]

        self._print_post(post)

        return post

    # --------------------------------------------------------
    # GET POST
    # --------------------------------------------------------

    async def get_post(self, post_id):

        data = await self._execute(
            name="wpcom-mcp/posts-get",

            input_data={
                "id": post_id,
                "context": "view",
                "include_fields": [
                    "id",
                    "title",
                    "content",
                    "status",
                    "date",
                    "link"
                ]
            }
        )

        post = data["data"]

        self._print_post(post)

        return post

    # --------------------------------------------------------
    # PUBLISH POST
    # --------------------------------------------------------

    async def publish_post(
        self,
        post_id,
        confirmed=False,
        suppress_subscriber_email=True
    ):

        if not confirmed:
            raise ValueError(
                "Explicit confirmation is required to publish."
            )

        input_data = {
            "id": post_id,
            "status": "publish"
        }

        if suppress_subscriber_email:

            input_data["meta"] = {
                "_jetpack_dont_email_post_to_subs": True
            }

        data = await self._execute(
            name="wpcom-mcp/posts-update",

            summary=(
                f"Publish WordPress post ID {post_id} "
                + (
                    "without emailing subscribers."
                    if suppress_subscriber_email
                    else "using the site's default email settings."
                )
            ),

            confirmed=True,

            input_data=input_data
        )

        post = data["data"]

        self._print_post(post)

        # Display WordPress content warnings if present.
        if data.get("_content_warnings"):
            print("CONTENT WARNINGS:")
            print(data["_content_warnings"])

        if data.get("_newsletter_warning"):
            print("NEWSLETTER WARNING:")
            print(data["_newsletter_warning"])

        return post

    # --------------------------------------------------------
    # READABLE OUTPUT
    # --------------------------------------------------------

    @staticmethod
    def _print_post(post):

        print("\n========== WORDPRESS POST ==========")

        print("Post ID:", post.get("id"))
        print("Title:", post.get("title"))
        print("Status:", post.get("status"))
        print("Date:", post.get("date"))

        if post.get("content") is not None:
            print("\nContent:")
            print(post["content"])

        if post.get("link"):
            print("\nURL:", post["link"])

        print("=" * 38)


_parse_callback_url = parse_callback_url


def __getattr__(name: str):
    notebook_exports = {
        "auth_callback_widget",
        "callback_status",
        "callback_url_input",
        "callback_submit_button",
        "handle_redirect",
        "handle_callback",
    }
    if name not in notebook_exports:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from blog_writer.integrations import wordpress_notebook

    return getattr(wordpress_notebook, name)
