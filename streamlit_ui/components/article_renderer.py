import html
import re
from pathlib import Path
from typing import Any

import streamlit as st


_IMAGE_TAG = re.compile(
    r"""<img\b(?=[^>]*\bsrc=["']\.\./images/)(?P<attributes>[^>]*)>""",
    re.IGNORECASE,
)
_IMAGE_SOURCE = re.compile(r"""\bsrc=["']([^"']+)["']""", re.IGNORECASE)
_IMAGE_ALT = re.compile(r"""\balt=["']([^"']*)["']""", re.IGNORECASE)
_CODE_FENCE = re.compile(r"(```.*?```|~~~.*?~~~)", re.DOTALL)
_IMAGE_PREFIX = "../images/"


def normalize_markdown(markdown: Any) -> str:
    if isinstance(markdown, str):
        return markdown
    if isinstance(markdown, list):
        text_parts = []
        for item in markdown:
            if isinstance(item, str):
                text_parts.append(item)
            elif (
                isinstance(item, dict)
                and item.get("type", "text") == "text"
                and isinstance(item.get("text"), str)
            ):
                text_parts.append(item["text"])
        return "\n".join(part for part in text_parts if part)
    raise TypeError("Article content must be Markdown text or a list of text blocks.")


def _local_image_path(reference: str) -> Path | None:
    if not reference.startswith(_IMAGE_PREFIX):
        return None

    filename = reference[len(_IMAGE_PREFIX):]
    if not filename or Path(filename).name != filename or filename in {".", ".."}:
        return None

    image_root = (Path.cwd() / "images").resolve()
    image_path = (image_root / filename).resolve()
    if image_path.parent != image_root or not image_path.is_file():
        return None
    return image_path


def _render_markdown_with_images(markdown: str) -> None:
    cursor = 0
    for image_tag in _IMAGE_TAG.finditer(markdown):
        preceding_markdown = markdown[cursor:image_tag.start()]
        if preceding_markdown.strip():
            st.markdown(preceding_markdown)

        attributes = image_tag.group("attributes")
        source_match = _IMAGE_SOURCE.search(attributes)
        alt_match = _IMAGE_ALT.search(attributes)
        source = html.unescape(source_match.group(1)) if source_match else ""
        alt_text = html.unescape(alt_match.group(1)) if alt_match else ""
        image_path = _local_image_path(source)

        if image_path is not None:
            st.image(str(image_path))
        else:
            st.caption(
                f"Generated image is unavailable: {alt_text or 'image artifact missing'}"
            )
        cursor = image_tag.end()

    remaining_markdown = markdown[cursor:]
    if remaining_markdown.strip():
        st.markdown(remaining_markdown)


def render_article(markdown: Any) -> None:
    """Render Markdown safely and display existing local reducer image artifacts."""
    try:
        markdown = normalize_markdown(markdown)
    except TypeError:
        st.error("The generated article content has an unsupported format.")
        return

    if not markdown:
        st.info("No generated blog content is available yet.")
        return

    for segment in _CODE_FENCE.split(markdown):
        if segment.startswith(("```", "~~~")):
            st.markdown(segment)
        else:
            _render_markdown_with_images(segment)
