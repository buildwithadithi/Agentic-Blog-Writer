import streamlit as st


def render_topic_form() -> str | None:
    with st.form("blog_topic_form"):
        topic = st.text_input(
            "Blog topic",
            placeholder="For example: How retrieval-augmented generation works",
        )
        submitted = st.form_submit_button("Generate Blog")

    if not submitted:
        return None

    normalized_topic = topic.strip()
    if not normalized_topic:
        st.error("Enter a blog topic before generating.")
        return None
    return normalized_topic
