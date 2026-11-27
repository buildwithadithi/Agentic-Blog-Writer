import asyncio
import json
import os
import secrets
from collections.abc import Awaitable, Callable
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import parse_qs, urlparse

from mcp.client.auth import OAuthClientProvider
from mcp.shared.auth import (
    AuthorizationCodeResult,
    OAuthClientInformationFull,
    OAuthToken,
)


class FileTokenStorage:
    def __init__(self, file_path: str | Path = "wordpress_oauth.json"):
        path = Path(file_path)
        if not path.is_absolute():
            path = Path(__file__).resolve().parents[2] / path
        self.file_path = path

    def _load(self) -> dict:
        if not self.file_path.exists():
            return {}
        return json.loads(self.file_path.read_text(encoding="utf-8"))

    def _save(self, data: dict) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if os.name != "posix":
            self.file_path.write_text(
                json.dumps(data, indent=2),
                encoding="utf-8",
            )
            return

        temporary_path = None
        try:
            with NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.file_path.parent,
                prefix=f".{self.file_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temporary_file:
                temporary_path = Path(temporary_file.name)
                json.dump(data, temporary_file, indent=2)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            temporary_path.chmod(0o600)
            os.replace(temporary_path, self.file_path)
            self.file_path.chmod(0o600)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()

    async def get_tokens(self) -> OAuthToken | None:
        tokens = self._load().get("tokens")
        if tokens is None:
            return None
        return OAuthToken.model_validate(tokens)

    async def set_tokens(self, tokens: OAuthToken) -> None:
        data = self._load()
        data["tokens"] = tokens.model_dump(mode="json", exclude_none=True)
        self._save(data)

    async def get_client_info(self) -> OAuthClientInformationFull | None:
        client_info = self._load().get("client_info")
        if client_info is None:
            return None
        return OAuthClientInformationFull.model_validate(client_info)

    async def set_client_info(
        self,
        client_info: OAuthClientInformationFull,
    ) -> None:
        data = self._load()
        data["client_info"] = client_info.model_dump(
            mode="json",
            exclude_none=True,
        )
        self._save(data)


def parse_callback_url(callback_url: str) -> AuthorizationCodeResult:
    params = parse_qs(urlparse(callback_url).query)

    if "error" in params:
        raise ValueError(f"Authorization failed: {params['error'][0]}")

    if "code" not in params:
        raise ValueError("Authorization code not found.")

    return AuthorizationCodeResult(
        code=params["code"][0],
        state=params.get("state", [None])[0],
        iss=params.get("iss", [None])[0],
    )


class OAuthAuthorizationFlow:
    def __init__(
        self,
        provider: OAuthClientProvider,
        connect: Callable[[], Awaitable[None]],
    ):
        self._provider = provider
        self._connect = connect
        self._authorization_url: asyncio.Future[str] | None = None
        self._callback_result: asyncio.Future[AuthorizationCodeResult] | None = None
        self._connection_task: asyncio.Task[None] | None = None
        self._previous_redirect_handler = None
        self._previous_callback_handler = None
        self._expected_state: str | None = None
        self._redirect_handler = self._capture_authorization_url
        self._callback_handler = self._wait_for_callback

    @property
    def is_pending(self) -> bool:
        return (
            self._connection_task is not None
            and not self._connection_task.done()
        )

    async def get_authorization_url(self) -> str | None:
        if self._connection_task is not None and not self._connection_task.done():
            raise RuntimeError("WordPress authorization is already in progress.")

        loop = asyncio.get_running_loop()
        self._authorization_url = loop.create_future()
        self._callback_result = loop.create_future()
        self._previous_redirect_handler = self._provider.context.redirect_handler
        self._previous_callback_handler = self._provider.context.callback_handler
        self._provider.context.redirect_handler = self._redirect_handler
        self._provider.context.callback_handler = self._callback_handler
        self._connection_task = asyncio.create_task(self._connect())

        try:
            done, _ = await asyncio.wait(
                (self._authorization_url, self._connection_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if self._connection_task in done:
                await self._connection_task
                self._finish()
                return None
            return self._authorization_url.result()
        except asyncio.CancelledError:
            await self.cancel()
            raise
        except Exception:
            if self._connection_task.done():
                self._finish()
            raise

    async def handle_callback(self, callback_url: str) -> None:
        if (
            self._connection_task is None
            or self._connection_task.done()
            or self._callback_result is None
        ):
            raise RuntimeError("No WordPress authorization is awaiting a callback.")

        result = parse_callback_url(callback_url)
        if (
            self._expected_state is None
            or result.state is None
            or not secrets.compare_digest(result.state, self._expected_state)
        ):
            raise ValueError("OAuth state validation failed.")

        if self._callback_result.done():
            raise RuntimeError("A WordPress authorization callback was already received.")

        self._callback_result.set_result(result)
        try:
            await self._connection_task
        finally:
            self._finish()

    async def cancel(self) -> None:
        task = self._connection_task
        if task is None:
            return

        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._finish()

    async def _capture_authorization_url(self, authorization_url: str) -> None:
        if self._authorization_url is None or self._authorization_url.done():
            raise RuntimeError("No WordPress authorization request is active.")
        states = parse_qs(urlparse(authorization_url).query).get("state")
        if not states or not states[0]:
            raise RuntimeError("WordPress authorization URL did not include state.")
        self._expected_state = states[0]
        self._authorization_url.set_result(authorization_url)

    async def _wait_for_callback(self) -> AuthorizationCodeResult:
        if self._callback_result is None:
            raise RuntimeError("No WordPress authorization callback is expected.")
        return await self._callback_result

    def _finish(self) -> None:
        self._provider.context.redirect_handler = self._previous_redirect_handler
        self._provider.context.callback_handler = self._previous_callback_handler
        self._authorization_url = None
        self._callback_result = None
        self._connection_task = None
        self._previous_redirect_handler = None
        self._previous_callback_handler = None
        self._expected_state = None
