
import base64
import hashlib
import html
import json
import re
from contextlib import AsyncExitStack
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol
from urllib.parse import unquote

import httpx2

from markdown_it import MarkdownIt

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

_IMAGE_TAG = re.compile(r"<img\b(?P<attributes>[^>]*)>", re.IGNORECASE)
_IMAGE_SOURCE = re.compile(
    r"""(?P<prefix>\bsrc\s*=\s*)(?P<quote>["'])(?P<source>.*?)(?P=quote)""",
    re.IGNORECASE,
)
_CODE_FENCE = re.compile(r"(```.*?```|~~~.*?~~~)", re.DOTALL)
_IMAGE_ALT = re.compile(
    r"""\balt\s*=\s*(?P<quote>["'])(?P<alt>.*?)(?P=quote)""",
    re.IGNORECASE,
)
_IMAGE_WIDTH = re.compile(
    r"""\bwidth\s*=\s*(?P<quote>["']?)(?P<width>\d+)(?P=quote)""",
    re.IGNORECASE,
)
_LOCAL_IMAGE_PREFIX = "../images/"
_WORDPRESS_MARKDOWN = MarkdownIt("commonmark", {"html": False})
_IMAGE_FORMATS = (
    (".png", "image/png", lambda data: data.startswith(b"\x89PNG\r\n\x1a\n")),
    (".jpg", "image/jpeg", lambda data: data.startswith(b"\xff\xd8\xff")),
    (".gif", "image/gif", lambda data: data.startswith((b"GIF87a", b"GIF89a"))),
    (
        ".webp",
        "image/webp",
        lambda data: (
            len(data) >= 12
            and data[:4] == b"RIFF"
            and data[8:12] == b"WEBP"
        ),
    ),
)
_SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp"}

_notebook_redirect_handler: Callable[[str], Awaitable[None]] | None = None
_notebook_callback_handler: Callable[[], Awaitable[AuthorizationCodeResult]] | None = None


def markdown_to_wordpress_html(markdown: str) -> str:
    if not isinstance(markdown, str):
        raise TypeError("WordPress draft content must be Markdown text.")

    protected_images = {}
    image_index = 0

    def protect_image(match):
        nonlocal image_index
        attributes = match.group("attributes")
        source_match = _IMAGE_SOURCE.search(match.group(0))
        if source_match is None:
            return match.group(0)

        source = html.unescape(source_match.group("source"))
        if not source.startswith(("https://", "http://")):
            raise ValueError("WordPress image URLs must use HTTP or HTTPS.")

        alt_match = _IMAGE_ALT.search(attributes)
        alt_text = html.unescape(alt_match.group("alt")) if alt_match else ""
        width_match = _IMAGE_WIDTH.search(attributes)
        width = (
            min(int(width_match.group("width")), 650)
            if width_match
            else 650
        )
        image_html = (
            f'<img src="{html.escape(source, quote=True)}" '
            f'alt="{html.escape(alt_text, quote=True)}" '
            f'width="{width}" style="max-width:100%;height:auto;">'
        )
        token = f"WORDPRESSIMAGEPLACEHOLDER{image_index}TOKEN"
        image_index += 1
        protected_images[token] = image_html
        return token

    safe_markdown = "".join(
        segment
        if segment.startswith(("```", "~~~"))
        else _IMAGE_TAG.sub(protect_image, segment)
        for segment in _CODE_FENCE.split(markdown)
    )
    rendered = _WORDPRESS_MARKDOWN.render(safe_markdown)
    for token, image_html in protected_images.items():
        rendered = rendered.replace(token, image_html)
    rendered = re.sub(
        r"<img\b(?P<attributes>[^>]*?)(?P<close>\s*/?)>",
        lambda match: _add_image_size_limit(match),
        rendered,
        flags=re.IGNORECASE,
    )
    return rendered


def _add_image_size_limit(match: re.Match[str]) -> str:
    attributes = match.group("attributes")
    width_match = _IMAGE_WIDTH.search(attributes)
    if width_match:
        width = min(int(width_match.group("width")), 650)
        attributes = (
            attributes[:width_match.start()]
            + f'width="{width}"'
            + attributes[width_match.end():]
        )
    else:
        attributes += ' width="650"'
    if "style=" not in attributes.lower():
        attributes += ' style="max-width:100%;height:auto;"'
    return f"<img{attributes}{match.group('close')}>"


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

        self.oauth_provider = WordPressOAuthClientProvider(
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

    async def upload_images(
        self,
        content: str,
        image_specs: list[dict] | None = None,
    ) -> str:
        if not isinstance(content, str):
            raise TypeError("WordPress draft content must be Markdown text.")

        references = self._local_image_references(content)
        if not references:
            return content

        specs_by_filename = {
            spec.get("filename"): spec
            for spec in (image_specs or [])
            if isinstance(spec, dict) and isinstance(spec.get("filename"), str)
        }
        prepared_images = []
        for reference, filename in references.items():
            path = self._resolve_local_image(filename)
            image_bytes = path.read_bytes()
            image_format = self._detect_image_format(image_bytes)
            if (
                path.suffix.lower() not in _SUPPORTED_IMAGE_EXTENSIONS
                or image_format is None
            ):
                raise ValueError(
                    f"Generated image {filename!r} has an unsupported or invalid image format."
                )
            upload_extension, mime_type = image_format
            image_hash = hashlib.sha256(image_bytes).hexdigest()
            spec = specs_by_filename.get(filename, {})
            prepared_images.append(
                {
                    "reference": reference,
                    "filename": filename,
                    "upload_filename": f"{path.stem}{upload_extension}",
                    "bytes": image_bytes,
                    "mime_type": mime_type,
                    "sha256": image_hash,
                    "title": f"Blog Writer {image_hash} {filename}",
                    "alt_text": spec.get("alt", ""),
                    "caption": spec.get("caption", ""),
                }
            )

        replacement_urls = {}
        for image in prepared_images:
            existing = await self._find_uploaded_image(
                image["title"],
                image["mime_type"],
            )
            if existing is None:
                existing = await self._create_uploaded_image(image)
            replacement_urls[image["reference"]] = existing["source_url"]

        def replace_image_tag(tag_match):
            tag = tag_match.group(0)
            source_match = _IMAGE_SOURCE.search(tag)
            if source_match is None:
                return tag
            reference = html.unescape(source_match.group("source"))
            url = replacement_urls.get(reference)
            if url is None:
                return tag
            replacement = (
                source_match.group("prefix")
                + source_match.group("quote")
                + html.escape(url, quote=True)
                + source_match.group("quote")
            )
            return (
                tag[:source_match.start()]
                + replacement
                + tag[source_match.end():]
            )

        return "".join(
            segment
            if segment.startswith(("```", "~~~"))
            else _IMAGE_TAG.sub(replace_image_tag, segment)
            for segment in _CODE_FENCE.split(content)
        )

    @staticmethod
    def _local_image_references(content: str) -> dict[str, str]:
        references = {}
        for segment in _CODE_FENCE.split(content):
            if segment.startswith(("```", "~~~")):
                continue
            for tag_match in _IMAGE_TAG.finditer(segment):
                source_match = _IMAGE_SOURCE.search(tag_match.group(0))
                if source_match is None:
                    continue
                reference = html.unescape(source_match.group("source"))
                if not reference.startswith(_LOCAL_IMAGE_PREFIX):
                    continue
                filename = unquote(reference[len(_LOCAL_IMAGE_PREFIX):])
                if (
                    not filename
                    or Path(filename).name != filename
                    or filename in {".", ".."}
                    or "?" in filename
                    or "#" in filename
                ):
                    raise ValueError(
                        "Generated image reference is not a valid local path."
                    )
                references[reference] = filename
        return references

    @staticmethod
    def _resolve_local_image(filename: str) -> Path:
        image_root = (Path.cwd() / "images").resolve()
        try:
            image_path = (image_root / filename).resolve(strict=True)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"Generated image {filename!r} was not found in the local images directory."
            ) from exc
        if image_path.parent != image_root or not image_path.is_file():
            raise ValueError("Generated image reference is outside the images directory.")
        return image_path

    @staticmethod
    def _detect_image_format(image_bytes: bytes) -> tuple[str, str] | None:
        for extension, mime_type, signature_matches in _IMAGE_FORMATS:
            if signature_matches(image_bytes):
                return extension, mime_type
        return None

    async def _find_uploaded_image(
        self,
        title: str,
        mime_type: str,
    ) -> dict | None:
        data = await self._execute(
            name="wpcom-mcp/media-list",
            input_data={
                "wpcom_site": WORDPRESS_SITE,
                "media_type": ["image"],
                "search": title,
                "per_page": 100,
                "include_fields": ["id", "title", "source_url", "mime_type"],
            },
        )
        if isinstance(data, dict):
            items = data.get("data", data.get("items", data.get("media", [])))
        else:
            items = data
        if not isinstance(items, list):
            raise RuntimeError("WordPress media lookup returned an unexpected response.")

        for item in items:
            if (
                isinstance(item, dict)
                and item.get("title") == title
                and item.get("mime_type") == mime_type
                and isinstance(item.get("id"), int)
                and isinstance(item.get("source_url"), str)
                and item["source_url"].startswith("https://")
            ):
                return {
                    "id": item["id"],
                    "source_url": item["source_url"],
                }
        return None

    async def _create_uploaded_image(self, image: dict) -> dict:
        await self._execute(
            name="wpcom/media-create",
            summary=(
                "Upload the approved blog article image "
                f"{image['filename']} to the WordPress media library."
            ),
            confirmed=True,
            input_data={
                "wpcom_site": WORDPRESS_SITE,
                "file_content_base64": base64.b64encode(
                    image["bytes"]
                ).decode("ascii"),
                "filename": image["upload_filename"],
                "mime_type": image["mime_type"],
                "title": image["title"],
                "alt_text": image["alt_text"],
                "caption": image["caption"],
            },
        )
        uploaded = await self._find_uploaded_image(
            image["title"],
            image["mime_type"],
        )
        if uploaded is None:
            raise RuntimeError(
                "WordPress did not return the uploaded image in its media library."
            )
        return uploaded

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


class WordPressOAuthClientProvider(OAuthClientProvider):
    async def _initialize(self) -> None:
        await super()._initialize()
        tokens = self.context.current_tokens
        if tokens is None or tokens.expires_in is None:
            return

        get_token_issued_at = getattr(
            self.context.storage,
            "get_token_issued_at",
            None,
        )
        issued_at = (
            await get_token_issued_at()
            if get_token_issued_at is not None
            else None
        )
        self.context.token_expiry_time = (
            issued_at + tokens.expires_in
            if issued_at is not None
            else 1.0
        )


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
