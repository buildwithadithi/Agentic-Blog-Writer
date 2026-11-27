import streamlit as st

from streamlit_ui.components.article_renderer import render_article


def render_article_review(
    *,
    thread_id: str,
    topic: str,
    markdown: str,
    revision_count: int,
) -> dict[str, str] | None:
    st.subheader("Article review")
    if topic:
        st.caption(f"Topic: {topic}")
    st.caption(f"Revision count: {revision_count}")
    render_article(markdown)

    with st.form(f"approve_article_{thread_id}"):
        approve = st.form_submit_button("Approve article")
    if approve:
        return {"action": "approved"}

    with st.form(f"request_revision_{thread_id}_{revision_count}"):
        feedback = st.text_area(
            "Revision feedback",
            key=f"review_feedback_{thread_id}_{revision_count}",
        )
        request_revision = st.form_submit_button("Request revision")
    if request_revision:
        if not feedback.strip():
            st.error("Enter feedback before requesting a revision.")
        else:
            return {
                "action": "revision_requested",
                "feedback": feedback.strip(),
            }

    with st.form(f"submit_manual_edit_{thread_id}_{revision_count}"):
        edited_blog = st.text_area(
            "Edit article Markdown",
            value=markdown,
            key=f"review_manual_edit_{thread_id}_{revision_count}",
            height=400,
        )
        submit_edit = st.form_submit_button("Submit edited article")
    if submit_edit:
        if not edited_blog.strip():
            st.error("The edited article cannot be empty.")
        else:
            return {
                "action": "manually_edited",
                "edited_blog": edited_blog,
            }

    return None
