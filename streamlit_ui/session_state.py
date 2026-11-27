from collections.abc import MutableMapping
from typing import Any


_SESSION_DEFAULTS = {
    "current_thread_id": None,
    "current_workflow_status": "idle",
    "current_topic": "",
    "current_final_markdown": "",
    "current_revision_count": 0,
    "current_review_status": "",
    "current_wordpress_status": None,
    "current_progress_events": [],
    "current_workflow_error": "",
    "current_review_content": "",
    "current_page": "Workspace",
    "oauth_flow_id": None,
    "wordpress_connection_status": "unknown",
    "wordpress_oauth_error": "",
}


def initialize_session_state(session_state: MutableMapping[str, Any]) -> None:
    """Set defaults for UI state without replacing values on Streamlit reruns."""
    for key, value in _SESSION_DEFAULTS.items():
        if key not in session_state:
            session_state[key] = list(value) if isinstance(value, list) else value
