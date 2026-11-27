import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

from blog_writer.integrations.wordpress_mcp import REDIRECT_URI
from streamlit_ui.services import runtime_service


_logger = logging.getLogger(__name__)


class WordPressServiceError(RuntimeError):
    """A safe, user-facing WordPress connection error."""


def _validate_callback_url(callback_url: str) -> None:
    if not isinstance(callback_url, str) or not callback_url.strip():
        raise WordPressServiceError("Paste the complete WordPress callback URL.")

    callback = urlparse(callback_url.strip())
    expected = urlparse(REDIRECT_URI)
    if (
        callback.scheme not in {"http", "https"}
        or not callback.netloc
        or (
            callback.scheme,
            callback.netloc.lower(),
            callback.path.rstrip("/"),
        )
        != (
            expected.scheme,
            expected.netloc.lower(),
            expected.path.rstrip("/"),
        )
    ):
        raise WordPressServiceError(
            "The URL does not match the configured WordPress callback address."
        )

    parameters = parse_qs(callback.query)
    if not parameters.get("code") or not parameters.get("state"):
        raise WordPressServiceError(
            "The callback URL must include the authorization response."
        )


def start_wordpress_authorization() -> dict[str, Any]:
    try:
        result = runtime_service.get_runtime_manager().start_wordpress_oauth()
    except Exception as exc:
        _logger.error(
            "WordPress authorization could not start (%s).",
            type(exc).__name__,
        )
        raise WordPressServiceError(
            "WordPress authorization could not start. Please try again."
        ) from exc

    if (
        not isinstance(result, dict)
        or not isinstance(result.get("connected"), bool)
        or (
            not result["connected"]
            and (
                not isinstance(result.get("flow_id"), str)
                or not result.get("flow_id")
                or not isinstance(result.get("authorization_url"), str)
                or not result.get("authorization_url")
            )
        )
    ):
        raise WordPressServiceError(
            "WordPress authorization returned an unexpected result."
        )
    return result


def get_authorization_url(flow_id: str) -> str:
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise WordPressServiceError("The pending authorization could not be found.")
    try:
        authorization_url = (
            runtime_service.get_runtime_manager()
            .get_wordpress_authorization_url(flow_id)
        )
    except Exception as exc:
        _logger.error(
            "WordPress authorization URL could not be retrieved (%s).",
            type(exc).__name__,
        )
        raise WordPressServiceError(
            "The pending authorization is no longer available. Start again."
        ) from exc
    if not isinstance(authorization_url, str) or not authorization_url:
        raise WordPressServiceError(
            "The pending authorization is no longer available. Start again."
        )
    return authorization_url


def complete_wordpress_authorization(
    flow_id: str,
    callback_url: str,
) -> dict[str, bool]:
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise WordPressServiceError("The pending authorization could not be found.")
    _validate_callback_url(callback_url)
    try:
        result = (
            runtime_service.get_runtime_manager()
            .complete_wordpress_oauth(flow_id, callback_url.strip())
        )
    except Exception as exc:
        _logger.error(
            "WordPress authorization callback failed (%s).",
            type(exc).__name__,
        )
        raise WordPressServiceError(
            "Authorization could not be completed. Check the callback URL and try again."
        ) from exc
    if result != {"connected": True}:
        raise WordPressServiceError(
            "WordPress authorization returned an unexpected result."
        )
    return result


def cancel_wordpress_authorization(flow_id: str) -> None:
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise WordPressServiceError("The pending authorization could not be found.")
    try:
        runtime_service.get_runtime_manager().cancel_wordpress_oauth(flow_id)
    except Exception as exc:
        _logger.error(
            "WordPress authorization cancellation failed (%s).",
            type(exc).__name__,
        )
        raise WordPressServiceError(
            "The pending authorization could not be cancelled. Please try again."
        ) from exc


def check_wordpress_connection() -> str:
    try:
        result = runtime_service.get_runtime_manager().check_wordpress_connection()
    except Exception as exc:
        _logger.error(
            "WordPress connection status could not be checked (%s).",
            type(exc).__name__,
        )
        return "unverified"
    if not isinstance(result, dict) or result.get("status") not in {
        "connected",
        "disconnected",
        "unverified",
    }:
        return "unverified"
    return result["status"]


def disconnect_wordpress() -> None:
    raise WordPressServiceError(
        "Disconnect is not supported by the current WordPress token storage."
    )
