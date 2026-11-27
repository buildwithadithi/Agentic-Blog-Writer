import logging
from typing import Any, Iterator
from uuid import uuid4

from streamlit_ui.services import runtime_service


_logger = logging.getLogger(__name__)


class WorkflowServiceError(RuntimeError):
    """A user-safe error raised when a workflow cannot be started."""


def _validate_response(response: Any, thread_id: str) -> dict[str, Any]:
    if (
        not isinstance(response, dict)
        or response.get("thread_id") != thread_id
        or response.get("status") not in {"paused", "completed"}
        or not isinstance(response.get("result"), dict)
    ):
        raise WorkflowServiceError(
            "Blog generation returned an unexpected result. Please try again."
        )
    return response


def start_blog_workflow(topic: str) -> dict[str, Any]:
    normalized_topic = topic.strip() if isinstance(topic, str) else ""
    if not normalized_topic:
        raise WorkflowServiceError("Enter a blog topic before generating.")

    missing_configuration = runtime_service.get_missing_required_configuration()
    if missing_configuration:
        raise WorkflowServiceError(
            "Workflow runtime is not configured. Missing: "
            + ", ".join(missing_configuration)
        )

    thread_id = str(uuid4())
    try:
        manager = runtime_service.get_runtime_manager()
        response = manager.start_workflow(
            normalized_topic,
            thread_id=thread_id,
        )
    except Exception as exc:
        _logger.error(
            "Blog workflow start failed (%s).",
            type(exc).__name__,
        )
        raise WorkflowServiceError(
            "Blog generation failed. Check the runtime status and try again."
        ) from exc

    try:
        return _validate_response(response, thread_id)
    except WorkflowServiceError:
        _logger.error("Workflow start returned an unexpected response shape.")
        raise


def stream_blog_workflow(
    topic: str,
    thread_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    normalized_topic = topic.strip() if isinstance(topic, str) else ""
    if not normalized_topic:
        yield {
            "type": "error",
            "message": "Enter a blog topic before generating.",
        }
        return

    missing_configuration = runtime_service.get_missing_required_configuration()
    if missing_configuration:
        yield {
            "type": "error",
            "message": (
                "Workflow runtime is not configured. Missing: "
                + ", ".join(missing_configuration)
            ),
        }
        return

    resolved_thread_id = thread_id or str(uuid4())
    yield {"type": "started", "thread_id": resolved_thread_id}

    try:
        manager = runtime_service.get_runtime_manager()
        for event in manager.stream_workflow(
            normalized_topic,
            thread_id=resolved_thread_id,
        ):
            if event.get("type") == "progress":
                progress = event.get("event")
                if isinstance(progress, dict):
                    yield {
                        "type": "progress",
                        "thread_id": resolved_thread_id,
                        **progress,
                    }
            elif event.get("type") == "result":
                response = _validate_response(
                    event.get("response"),
                    resolved_thread_id,
                )
                yield {
                    "type": "result",
                    "thread_id": resolved_thread_id,
                    "response": response,
                }
    except Exception as exc:
        _logger.error(
            "Blog workflow stream failed (%s).",
            type(exc).__name__,
        )
        yield {
            "type": "error",
            "thread_id": resolved_thread_id,
            "message": (
                "Blog generation failed. Check the runtime status and try again."
            ),
        }


def _stream_review_action(
    thread_id: str,
    payload: dict[str, str],
) -> Iterator[dict[str, Any]]:
    if not isinstance(thread_id, str) or not thread_id.strip():
        yield {
            "type": "error",
            "message": "The workflow ID is missing. Start the workflow again.",
        }
        return

    missing_configuration = runtime_service.get_missing_required_configuration()
    if missing_configuration:
        yield {
            "type": "error",
            "thread_id": thread_id,
            "message": (
                "Workflow runtime is not configured. Missing: "
                + ", ".join(missing_configuration)
            ),
        }
        return

    yield {"type": "resuming", "thread_id": thread_id}
    try:
        manager = runtime_service.get_runtime_manager()
        for event in manager.stream_resume_workflow(thread_id, payload):
            if event.get("type") == "progress":
                progress = event.get("event")
                if isinstance(progress, dict):
                    yield {
                        "type": "progress",
                        "thread_id": thread_id,
                        **progress,
                    }
            elif event.get("type") == "result":
                try:
                    response = _validate_response(
                        event.get("response"),
                        thread_id,
                    )
                except WorkflowServiceError:
                    _logger.error(
                        "Workflow resume returned an unexpected response shape."
                    )
                    yield {
                        "type": "error",
                        "thread_id": thread_id,
                        "message": (
                            "The review action returned an unexpected result. "
                            "Check the workflow status before trying again."
                        ),
                    }
                    return
                yield {
                    "type": "result",
                    "thread_id": thread_id,
                    "response": response,
                }
    except Exception as exc:
        _logger.error(
            "Workflow resume failed (%s).",
            type(exc).__name__,
        )
        yield {
            "type": "error",
            "thread_id": thread_id,
            "message": (
                "The review action could not be completed. Check the workflow "
                "status before trying again."
            ),
        }


def approve_article(thread_id: str) -> Iterator[dict[str, Any]]:
    return _stream_review_action(thread_id, {"action": "approved"})


def request_article_revision(
    thread_id: str,
    feedback: str,
) -> Iterator[dict[str, Any]]:
    normalized_feedback = feedback.strip() if isinstance(feedback, str) else ""
    if not normalized_feedback:
        return iter(
            [
                {
                    "type": "error",
                    "message": "Enter feedback before requesting a revision.",
                }
            ]
        )
    return _stream_review_action(
        thread_id,
        {
            "action": "revision_requested",
            "feedback": normalized_feedback,
        },
    )


def submit_manual_edit(
    thread_id: str,
    edited_blog: str,
) -> Iterator[dict[str, Any]]:
    if not isinstance(edited_blog, str) or not edited_blog.strip():
        return iter(
            [
                {
                    "type": "error",
                    "message": "The edited article cannot be empty.",
                }
            ]
        )
    return _stream_review_action(
        thread_id,
        {
            "action": "manually_edited",
            "edited_blog": edited_blog,
        },
    )
