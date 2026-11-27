import html
import re
from pathlib import Path

import streamlit as st


_IMAGE_TAG = re.compile(
    r"""<img\b(?=[^>]*\bsrc=["']\.\./images/)(?P<attributes>[^>]*)>""",
    re.IGNORECASE,
)
_IMAGE_SOURCE = re.compile(r"""\bsrc=["']([^"']+)["']""", re.IGNORECASE)
_IMAGE_ALT = re.compile(r"""\balt=["']([^"']*)["']""", re.IGNORECASE)
_CODE_FENCE = re.compile(r"(```.*?```|~~~.*?~~~)", re.DOTALL)
_IMAGE_PREFIX = "../images/"


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


def render_article(markdown: str) -> None:
    """Render Markdown safely and display existing local reducer image artifacts."""
    if not markdown:
        st.info("No generated blog content is available yet.")
        return

    for segment in _CODE_FENCE.split(markdown):
        if segment.startswith(("```", "~~~")):
            st.markdown(segment)
        else:
            _render_markdown_with_images(segment)
