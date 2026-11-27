import asyncio
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

from blog_writer.integrations.wordpress_mcp import WordPressMCPClient
from blog_writer.integrations.wordpress_oauth import FileTokenStorage


ClientFactory = Callable[[FileTokenStorage], WordPressMCPClient]
StorageFactory = Callable[[], FileTokenStorage]


@dataclass
class _PendingFlow:
    client: WordPressMCPClient
    authorization_url: str
    completing: bool = False


class OAuthFlowRegistry:
    """Keep WordPress OAuth clients alive on the RuntimeManager event loop."""

    def __init__(
        self,
        *,
        client_factory: ClientFactory | None = None,
        storage_factory: StorageFactory | None = None,
    ):
        self._client_factory = client_factory or (
            lambda storage: WordPressMCPClient(
                token_storage=storage,
                close_after_connect=True,
            )
        )
        self._storage_factory = storage_factory or FileTokenStorage
        self._pending: dict[str, _PendingFlow] = {}
        self._lock = asyncio.Lock()

    async def start_authorization(self) -> dict[str, Any]:
        async with self._lock:
            if self._pending:
                raise RuntimeError("A WordPress authorization is already pending.")

            client = self._client_factory(self._storage_factory())
            try:
                authorization_url = await client.get_authorization_url()
            except Exception:
                await client.close()
                raise

            if authorization_url is None:
                await client.close()
                return {
                    "flow_id": None,
                    "authorization_url": None,
                    "connected": True,
                }

            flow_id = str(uuid4())
            self._pending[flow_id] = _PendingFlow(
                client=client,
                authorization_url=authorization_url,
            )
            return {
                "flow_id": flow_id,
                "authorization_url": authorization_url,
                "connected": False,
            }

    async def get_authorization_url(self, flow_id: str) -> str:
        flow = self._pending.get(flow_id)
        if flow is None:
            raise LookupError("No matching WordPress authorization is pending.")
        return flow.authorization_url

    async def complete_authorization(
        self,
        flow_id: str,
        callback_url: str,
    ) -> dict[str, bool]:
        async with self._lock:
            flow = self._pending.get(flow_id)
            if flow is None:
                raise LookupError("No matching WordPress authorization is pending.")
            if flow.completing:
                raise RuntimeError("This WordPress authorization is already completing.")
            flow.completing = True

        try:
            await flow.client.handle_oauth_callback(callback_url)
        except Exception:
            if flow.client.authorization_flow.is_pending:
                async with self._lock:
                    flow.completing = False
            else:
                async with self._lock:
                    self._pending.pop(flow_id, None)
                await flow.client.close()
            raise

        async with self._lock:
            self._pending.pop(flow_id, None)
        await flow.client.close()
        return {"connected": True}

    async def cancel_authorization(self, flow_id: str) -> dict[str, bool]:
        async with self._lock:
            flow = self._pending.pop(flow_id, None)
        if flow is None:
            raise LookupError("No matching WordPress authorization is pending.")

        try:
            await flow.client.cancel_oauth()
        finally:
            await flow.client.close()
        return {"cancelled": True}

    async def check_connection(self) -> dict[str, str]:
        storage = self._storage_factory()
        tokens = await storage.get_tokens()
        client_info = await storage.get_client_info()
        if (
            tokens is None
            or not tokens.access_token
            or client_info is None
            or not client_info.client_id
        ):
            return {"status": "disconnected"}

        client = self._client_factory(storage)
        try:
            await client.connect()
        except Exception:
            return {"status": "unverified"}
        else:
            return {"status": "connected"}
        finally:
            await client.close()

    async def shutdown(self) -> None:
        async with self._lock:
            flows = list(self._pending.values())
            self._pending.clear()

        first_error = None
        for flow in flows:
            try:
                await flow.client.cancel_oauth()
            except Exception as exc:
                first_error = first_error or exc
            try:
                await flow.client.close()
            except Exception as exc:
                first_error = first_error or exc

        if first_error is not None:
            raise RuntimeError("Pending WordPress authorization cleanup failed.") from first_error
