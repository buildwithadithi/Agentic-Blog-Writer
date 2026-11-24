from langgraph.types import interrupt

from blog_writer.llm import llm
from blog_writer.state import State

MAX_REVISIONS = 5


def human_review(state: State) -> dict:
    review = interrupt(
        {
            "message": "Review the generated blog",
            "blog": state["final"],
            "revision_count": state["revision_count"],
        }
    )

    return {
        "review_status": review["action"],
        "review_feedback": review.get("feedback", ""),
        "final": review.get("edited_blog", state["final"]),
    }


def route_after_review(state: State) -> str:
    status = state["review_status"]

    if status == "approved":
        return "wordpress_draft_node"

    elif status == "revision_requested":
        if state["revision_count"] >= MAX_REVISIONS:
            return "manual_intervention"

        return "revision"

    elif status == "manually_edited":
        return "human_review"

    else:
        raise ValueError(f"Unknown review status: {status}")


def revision_node(state: State) -> dict:
    existing_blog = state["final"]

    # Normalize blog content into a string
    if isinstance(existing_blog, list):
        existing_blog = "\n".join(
            item.get("text", "")
            for item in existing_blog
            if isinstance(item, dict)
        )

    prompt = f"""
    You are an expert blog editor.

    Revise the existing blog based on the human feedback.

    EXISTING BLOG:
    {existing_blog}

    HUMAN FEEDBACK:
    {state["review_feedback"]}

    Instructions:
    - Preserve the original topic and core meaning.
    - Address all the feedback.
    - Improve clarity, structure, and readability.
    - Return only the revised Markdown blog.
    """

    response = llm.invoke(prompt)

    return {
        "final": response.content,
        "revision_count": state["revision_count"] + 1,
    }


def manual_intervention_node(state: State) -> dict:
    return {
        "review_status": "manual_intervention_required"
    }
