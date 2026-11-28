import logging

import streamlit as st

from streamlit_ui.components.article_review import render_article_review
from streamlit_ui.components.article_renderer import normalize_markdown, render_article
from streamlit_ui.components.progress_panel import render_progress_panel
from streamlit_ui.components.topic_form import render_topic_form
from streamlit_ui.components.wordpress_connect import render_wordpress_connection
from streamlit_ui.session_state import initialize_session_state
from streamlit_ui.services import runtime_service
from streamlit_ui.services import workflow_service


_logger = logging.getLogger(__name__)


st.set_page_config(
    page_title="Blog Writing Agent",
    layout="wide",
)

initialize_session_state(st.session_state)

st.title("Blog Writing Agent")
st.caption(
    "Generate, review, and prepare technical blog posts with an AI-assisted workflow."
)

st.sidebar.title("Navigation")
st.sidebar.info("Navigation options will be added here.")

st.header("Workspace")

progress_placeholder = st.empty()


def refresh_progress_panel() -> None:
    with progress_placeholder.container():
        render_progress_panel(
            st.session_state["current_progress_events"],
            st.session_state["current_workflow_status"],
            st.session_state["current_workflow_error"],
            st.session_state["current_review_status"],
        )


def apply_workflow_response(response: dict) -> None:
    result = response["result"]
    interrupt = response.get("interrupt")
    status = response["status"]
    if (
        status == "paused"
        and isinstance(interrupt, dict)
        and interrupt.get("message") == "Review the generated blog"
    ):
        status = "paused_for_review"

    st.session_state["current_thread_id"] = response["thread_id"]
    st.session_state["current_workflow_status"] = status
    st.session_state["current_final_markdown"] = normalize_markdown(
        result.get("final", "")
    )
    st.session_state["current_revision_count"] = result.get(
        "revision_count",
        interrupt.get("revision_count", 0) if isinstance(interrupt, dict) else 0,
    )
    st.session_state["current_review_status"] = result.get("review_status", "")
    st.session_state["current_wordpress_status"] = result.get("wordpress_status")
    st.session_state["current_workflow_error"] = ""


def consume_workflow_events(events) -> None:
    for event in events:
        if event["type"] in {"started", "resuming"}:
            st.session_state["current_thread_id"] = event["thread_id"]
        elif event["type"] == "progress":
            progress = {
                key: value
                for key, value in event.items()
                if key not in {"type", "thread_id"}
            }
            st.session_state["current_progress_events"].append(progress)
        elif event["type"] == "result":
            apply_workflow_response(event["response"])
        elif event["type"] == "error":
            st.session_state["current_workflow_status"] = "error"
            st.session_state["current_workflow_error"] = event["message"]
        refresh_progress_panel()


missing_configuration = runtime_service.get_missing_required_configuration()
if missing_configuration:
    st.error(
        "Workflow runtime is not initialized. Missing configuration: "
        + ", ".join(missing_configuration)
    )
else:
    runtime_manager = runtime_service.get_runtime_manager()
    try:
        runtime_manager.initialize()
    except Exception as exc:
        _logger.error(
            "Workflow runtime initialization failed (%s).",
            type(exc).__name__,
        )
        st.error(
            "The workflow runtime could not be initialized. Check the server "
            "configuration and database availability."
        )
    else:
        st.success("Workflow runtime is connected and ready.")

st.divider()
render_wordpress_connection()
st.divider()

submitted_topic = render_topic_form()
if submitted_topic is not None:
    if st.session_state["current_workflow_status"] == "running":
        st.warning("A workflow is already running for this session.")
    else:
        st.session_state["current_topic"] = submitted_topic
        st.session_state["current_thread_id"] = None
        st.session_state["current_workflow_status"] = "running"
        st.session_state["current_final_markdown"] = ""
        st.session_state["current_revision_count"] = 0
        st.session_state["current_review_status"] = ""
        st.session_state["current_wordpress_status"] = None
        st.session_state["current_progress_events"] = []
        st.session_state["current_workflow_error"] = ""

        refresh_progress_panel()
        consume_workflow_events(
            workflow_service.stream_blog_workflow(submitted_topic)
        )

refresh_progress_panel()

if st.session_state["current_thread_id"]:
    st.caption(f"Workflow ID: {st.session_state['current_thread_id']}")

if st.session_state["current_topic"]:
    st.subheader(st.session_state["current_topic"])

review_placeholder = st.empty()
if (
    st.session_state["current_workflow_status"] == "paused_for_review"
    and st.session_state["current_thread_id"]
):
    with review_placeholder.container():
        review_action = render_article_review(
            thread_id=st.session_state["current_thread_id"],
            topic=st.session_state["current_topic"],
            markdown=st.session_state["current_final_markdown"],
            revision_count=st.session_state["current_revision_count"],
        )

    if review_action is not None:
        st.session_state["current_workflow_status"] = "running"
        st.session_state["current_workflow_error"] = ""
        refresh_progress_panel()

        thread_id = st.session_state["current_thread_id"]
        action = review_action["action"]
        if action == "approved":
            events = workflow_service.approve_article(thread_id)
        elif action == "revision_requested":
            events = workflow_service.request_article_revision(
                thread_id,
                review_action["feedback"],
            )
        else:
            events = workflow_service.submit_manual_edit(
                thread_id,
                review_action["edited_blog"],
            )
        consume_workflow_events(events)
        st.rerun()

if (
    st.session_state["current_final_markdown"]
    and st.session_state["current_workflow_status"] != "paused_for_review"
):
    render_article(st.session_state["current_final_markdown"])

if st.session_state["current_wordpress_status"]:
    st.write(
        "WordPress draft status: "
        f"{st.session_state['current_wordpress_status']}"
    )
