import logging
import os

import streamlit as st

from streamlit_ui.services.runtime_manager import RuntimeManager


_REQUIRED_CONFIGURATION = (
    "GOOGLE_API_KEY",
    "TAVILY_API_KEY",
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_API_TOKEN",
    "SUPABASE_DB_URI",
)

_logger = logging.getLogger(__name__)


def get_missing_required_configuration() -> tuple[str, ...]:
    from blog_writer.config import get_required_env

    missing = []
    for name in _REQUIRED_CONFIGURATION:
        try:
            get_required_env(name)
        except RuntimeError:
            missing.append(name)
    return tuple(missing)


def _release_runtime_manager(manager: RuntimeManager) -> None:
    try:
        manager.shutdown()
    except Exception as exc:
        _logger.error(
            "Runtime manager shutdown failed during cache release (%s).",
            type(exc).__name__,
        )


@st.cache_resource(
    show_spinner=False,
    on_release=_release_runtime_manager,
)
def get_runtime_manager() -> RuntimeManager:
    """Return one cached manager; runtime initialization remains lazy.

    Streamlit's release callback handles cache removal but is not guaranteed
    to run when the application process shuts down.
    """
    return RuntimeManager()
