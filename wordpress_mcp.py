
import json
import webbrowser

from pathlib import Path
from urllib.parse import urlparse, parse_qs
from contextlib import AsyncExitStack

import httpx2

from pydantic import AnyUrl

from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.client.auth import OAuthClientProvider
import asyncio
import ipywidgets as widgets

from IPython.display import display

from mcp.shared.auth import (
    OAuthClientMetadata,
    AuthorizationCodeResult,
    OAuthClientInformationFull,
    OAuthToken
)

# ============================================================
# 1. CONFIGURATION
# ============================================================

WORDPRESS_MCP_URL = (
    "https://public-api.wordpress.com/wpcom/v2/mcp/v1"
)

WORDPRESS_SITE = "https://buildwithadithi.wordpress.com"

REDIRECT_URI = "http://localhost:8080/callback"


# ============================================================
# 2. TOKEN STORAGE
# ============================================================

class FileTokenStorage:

    def __init__(self, file_path="wordpress_oauth.json"):

        self.file_path = (
            Path(__file__).resolve().parent / file_path
        )

    def _load(self):

        if not self.file_path.exists():
            return {}

        return json.loads(
            self.file_path.read_text(encoding="utf-8")
        )

    def _save(self, data):

        self.file_path.write_text(
            json.dumps(data, indent=2),
            encoding="utf-8"
        )

    async def get_tokens(self) -> OAuthToken | None:

        data = self._load()
        tokens = data.get("tokens")

        if tokens is None:
            return None

        return OAuthToken.model_validate(tokens)

    async def set_tokens(self, tokens: OAuthToken):

        data = self._load()

        data["tokens"] = tokens.model_dump(
            mode="json",
            exclude_none=True
        )

        self._save(data)

    async def get_client_info(
        self
    ) -> OAuthClientInformationFull | None:

        data = self._load()
        client_info = data.get("client_info")

        if client_info is None:
            return None

        return OAuthClientInformationFull.model_validate(
            client_info
        )

    async def set_client_info(
        self,
        client_info: OAuthClientInformationFull
    ):

        data = self._load()

        data["client_info"] = client_info.model_dump(
            mode="json",
            exclude_none=True
        )

        self._save(data)


# ============================================================
# 3. OAUTH HANDLERS
# ============================================================

async def handle_redirect(auth_url: str):

    print("Opening WordPress authorization page...")

    webbrowser.open(auth_url)




def _parse_callback_url(callback_url):
    params = parse_qs(
        urlparse(callback_url).query
    )

    if "error" in params:
        raise ValueError(
            f"Authorization failed: {params['error'][0]}"
        )

    if "code" not in params:
        raise ValueError(
            "Authorization code not found."
        )

    return AuthorizationCodeResult(
        code=params["code"][0],
        state=params.get("state", [None])[0],
        iss=params.get("iss", [None])[0]
    )


_callback_result: asyncio.Future | None = None
callback_url_input = widgets.Text(
    description="Redirect URL:",
    placeholder="Paste the complete redirected URL here",
    layout=widgets.Layout(width="100%")
)
callback_status = widgets.HTML(
    "Click <b>Approve</b>, authorize WordPress, then paste the redirected URL here."
)


def _submit_callback_url(_):
    if _callback_result is None or _callback_result.done():
        callback_status.value = (
            "Waiting for the workflow to start WordPress authorization. "
            "Click <b>Approve</b> first."
        )
        return

    try:
        result = _parse_callback_url(callback_url_input.value.strip())
    except ValueError as exc:
        callback_status.value = f"<span style='color: red'>{exc}</span>"
        return

    callback_url_input.value = ""
    callback_status.value = "Authorization URL submitted."
    _callback_result.set_result(result)


callback_submit_button = widgets.Button(
    description="Submit authorization URL",
    button_style="primary"
)
callback_submit_button.on_click(_submit_callback_url)
callback_url_input.on_submit(_submit_callback_url)
auth_callback_widget = widgets.VBox([
    callback_status,
    callback_url_input,
    callback_submit_button
])


async def handle_callback():
    global _callback_result

    loop = asyncio.get_running_loop()

    _callback_result = loop.create_future()

    callback_status.value = (
        "<b>Waiting for WordPress authorization...</b> "
        "Paste the redirected URL and click Submit."
    )

    try:
        result = await _callback_result

        callback_status.value = (
            "<span style='color:green'>"
            "Authorization code received. Connecting to WordPress..."
            "</span>"
        )

        return result

    finally:
        _callback_result = None



# ============================================================
# 4. WORDPRESS MCP CLIENT
# ============================================================

class WordPressMCPClient:

    def __init__(self):

        self.token_storage = FileTokenStorage()

        self.client_metadata = OAuthClientMetadata(
            client_name="Blog Writing Agent",
            redirect_uris=[AnyUrl(REDIRECT_URI)],
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
            redirect_handler=handle_redirect,
            callback_handler=handle_callback
        )

        self.session = None
        self.exit_stack = None

    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    async def connect(self):

        if self.session is not None:
            return

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

            print("Connected to WordPress!")

        except Exception:

            await self.exit_stack.aclose()

            self.exit_stack = None
            self.session = None

            raise

    # --------------------------------------------------------
    # CLOSE
    # --------------------------------------------------------

    async def close(self):

        if self.exit_stack is not None:

            await self.exit_stack.aclose()

            self.exit_stack = None
            self.session = None

            print("WordPress connection closed.")

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
