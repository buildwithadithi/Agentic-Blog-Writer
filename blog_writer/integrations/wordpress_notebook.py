import asyncio
import html
import webbrowser

import ipywidgets as widgets
from IPython.display import display

from blog_writer.integrations.wordpress_mcp import (
    register_notebook_oauth_handlers,
)
from blog_writer.integrations.wordpress_oauth import parse_callback_url


_callback_result: asyncio.Future | None = None
callback_input = widgets.Text(
    description="Redirect URL:",
    placeholder="Paste the complete redirected URL here",
    layout=widgets.Layout(width="100%"),
)
callback_status = widgets.HTML(
    "Click <b>Approve</b>, authorize WordPress, then paste the redirected URL here."
)


def _submit_callback_url(_=None):
    if _callback_result is None or _callback_result.done():
        callback_status.value = (
            "Waiting for the workflow to start WordPress authorization. "
            "Click <b>Approve</b> first."
        )
        return

    try:
        result = parse_callback_url(callback_input.value.strip())
    except ValueError as exc:
        callback_status.value = (
            f'<span style="color: red">{html.escape(str(exc))}</span>'
        )
        return

    callback_input.value = ""
    callback_status.value = "Authorization URL submitted."
    _callback_result.set_result(result)


callback_submit_button = widgets.Button(
    description="Submit authorization URL",
    button_style="primary",
)
callback_submit_button.on_click(_submit_callback_url)
callback_input.on_submit(_submit_callback_url)
auth_callback_widget = widgets.VBox(
    [callback_status, callback_input, callback_submit_button]
)


async def handle_redirect(authorization_url: str) -> None:
    webbrowser.open(authorization_url)


async def handle_callback():
    global _callback_result

    _callback_result = asyncio.get_running_loop().create_future()
    callback_status.value = (
        "<b>Waiting for WordPress authorization...</b><br>"
        "1. Approve access in your browser.<br>"
        "2. Copy the complete redirected URL.<br>"
        "3. Paste it below and click Submit."
    )
    display(auth_callback_widget)

    try:
        return await _callback_result
    finally:
        _callback_result = None


register_notebook_oauth_handlers(handle_redirect, handle_callback)
