"""Compatibility exports for the original notebook import path."""

from blog_writer.integrations.wordpress_mcp import (
    WordPressMCPClient,
    auth_callback_widget,
    callback_status,
)

__all__ = [
    "WordPressMCPClient",
    "auth_callback_widget",
    "callback_status",
]
