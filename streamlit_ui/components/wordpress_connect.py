import streamlit as st

from streamlit_ui.services import wordpress_service
from streamlit_ui.services.wordpress_service import WordPressServiceError


_CALLBACK_WIDGET_KEY = "wordpress_oauth_callback_url"


def _show_error() -> None:
    error = st.session_state.get("wordpress_oauth_error", "")
    if error:
        st.error(error)


def _check_connection() -> None:
    st.session_state["wordpress_connection_status"] = (
        wordpress_service.check_wordpress_connection()
    )
    st.session_state["wordpress_oauth_error"] = ""


def _start_authorization() -> None:
    try:
        result = wordpress_service.start_wordpress_authorization()
        st.session_state["wordpress_oauth_error"] = ""
        if result["connected"]:
            st.session_state["oauth_flow_id"] = None
            st.session_state["wordpress_connection_status"] = "connected"
        else:
            st.session_state["oauth_flow_id"] = result["flow_id"]
            st.session_state["wordpress_connection_status"] = "pending"
    except WordPressServiceError as exc:
        st.session_state["wordpress_oauth_error"] = str(exc)


def _complete_authorization(flow_id: str) -> None:
    callback_url = st.session_state.get(_CALLBACK_WIDGET_KEY, "")
    try:
        wordpress_service.complete_wordpress_authorization(flow_id, callback_url)
        st.session_state["oauth_flow_id"] = None
        st.session_state["wordpress_connection_status"] = "connected"
        st.session_state["wordpress_oauth_error"] = ""
    except WordPressServiceError as exc:
        st.session_state["wordpress_oauth_error"] = str(exc)
    finally:
        st.session_state[_CALLBACK_WIDGET_KEY] = ""


def _cancel_authorization(flow_id: str) -> None:
    try:
        wordpress_service.cancel_wordpress_authorization(flow_id)
        st.session_state["oauth_flow_id"] = None
        st.session_state["wordpress_connection_status"] = "disconnected"
        st.session_state["wordpress_oauth_error"] = ""
    except WordPressServiceError as exc:
        st.session_state["wordpress_oauth_error"] = str(exc)
    finally:
        st.session_state[_CALLBACK_WIDGET_KEY] = ""


def render_wordpress_connection() -> None:
    st.header("WordPress connection")
    status = st.session_state["wordpress_connection_status"]
    flow_id = st.session_state["oauth_flow_id"]

    if flow_id:
        status = "pending"
        st.session_state["wordpress_connection_status"] = status
    elif status == "unknown":
        _check_connection()
        status = st.session_state["wordpress_connection_status"]

    if status == "pending" and flow_id:
        try:
            authorization_url = wordpress_service.get_authorization_url(flow_id)
        except WordPressServiceError as exc:
            st.session_state["oauth_flow_id"] = None
            st.session_state["wordpress_connection_status"] = "disconnected"
            st.session_state["wordpress_oauth_error"] = str(exc)
            status = "disconnected"
        else:
            st.info(
                "Authorize Blog Writing Agent in WordPress. The configured "
                "redirect does not provide an automatic callback page; after "
                "authorization, copy the full redirected browser URL below."
            )
            st.link_button(
                "Open WordPress authorization page",
                authorization_url,
                type="primary",
            )
            with st.form(f"wordpress_callback_form_{flow_id}"):
                st.text_area(
                    "Paste the complete redirected callback URL",
                    key=_CALLBACK_WIDGET_KEY,
                )
                st.form_submit_button(
                    "Complete connection",
                    on_click=_complete_authorization,
                    args=(flow_id,),
                )
            st.button(
                "Cancel authorization",
                on_click=_cancel_authorization,
                args=(flow_id,),
                key=f"cancel_wordpress_oauth_{flow_id}",
            )

    if status == "connected":
        st.success("WordPress is connected.")
        st.caption(
            "Disconnect is not supported by the current token-storage implementation."
        )
        st.button(
            "Check WordPress connection",
            on_click=_check_connection,
            key="check_wordpress_connection",
        )
    elif status == "disconnected":
        st.info("WordPress is not connected.")
        st.button(
            "Connect WordPress",
            on_click=_start_authorization,
            type="primary",
            key="connect_wordpress",
        )
    elif status == "unverified":
        st.warning(
            "Stored WordPress credentials could not be verified. Check your "
            "network connection or authorize again."
        )
        st.button(
            "Retry connection check",
            on_click=_check_connection,
            key="retry_wordpress_connection",
        )
        st.button(
            "Connect WordPress",
            on_click=_start_authorization,
            key="reconnect_wordpress",
        )

    _show_error()
