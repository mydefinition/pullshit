"""OneBot 11 forward WebSocket adapter, with one reader and correlated calls."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from pullshit.config import ConfigError, decimal_id


class TransportError(RuntimeError):
    pass


class ApiError(RuntimeError):
    pass


class NapCat:
    READ_ACTIONS = {"get_login_info", "get_version_info", "get_msg", "get_forward_msg"}

    def __init__(self, endpoint: str, token: str, account: str | None = None, timeout: int = 15,
                 *, allow_send: bool = False):
        self.endpoint = endpoint
        self._token = token
        self.account = account
        self.timeout = timeout
        self.allow_send = allow_send
        self.pending: dict[str, asyncio.Future] = {}
        self.events: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.socket = None
        self.reader = None
        self.logger = logging.getLogger("pullshit.ws.silent")
        self.logger.disabled = True

    async def __aenter__(self) -> NapCat:
        try:
            self.socket = await connect(
                self.endpoint, additional_headers={"Authorization": f"Bearer {self._token}"},
                open_timeout=self.timeout, close_timeout=2, max_size=2 * 1024 * 1024,
                proxy=None, logger=self.logger,
            )
            self.reader = asyncio.create_task(self._read())
            login = await self.call("get_login_info")
            actual_account = decimal_id(login.get("user_id"))
            if self.account is not None and actual_account != self.account:
                raise ConfigError("登录账号与配置不匹配")
            self.account = actual_account
        except (asyncio.CancelledError, ConfigError, TransportError):
            await self.__aexit__(None, None, None)
            raise
        except InvalidStatus as exc:
            await self.__aexit__(None, None, None)
            if exc.response.status_code in {401, 403}:
                raise ConfigError("平台拒绝连接认证，请核对 token") from None
            raise TransportError("平台连接失败") from None
        except Exception:
            await self.__aexit__(None, None, None)
            raise TransportError("平台连接失败") from None
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self.socket:
            await self.socket.close()
        if self.reader:
            self.reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.reader
        self.socket = None

    async def _read(self) -> None:
        try:
            async for frame in self.socket:
                try:
                    item = json.loads(frame)
                    if not isinstance(item, dict):
                        raise ValueError
                    echo = item.get("echo")
                    if isinstance(echo, str) and echo in self.pending:
                        future = self.pending[echo]
                        if not future.done():
                            future.set_result(item)
                    elif "post_type" in item:
                        self.events.put_nowait(item)
                except (ValueError, asyncio.QueueFull):
                    raise TransportError("平台载荷无效或事件缓冲区已满") from None
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never include endpoint, headers, payload or the library exception.
            pass
        finally:
            for future in tuple(self.pending.values()):
                if not future.done():
                    future.set_exception(TransportError("连接中断，调用结果未知"))
            # Consumer observes reader.done() even when the queue is full.

    async def call(self, action: str, params: dict | None = None) -> dict:
        if action not in self.READ_ACTIONS and not (action == "send_group_msg" and self.allow_send):
            raise ApiError("适配器未授权此 API")
        if action == "send_group_msg":
            login = await self.call("get_login_info")
            if decimal_id(login.get("user_id")) != self.account:
                raise ConfigError("平台登录账号发生变化，停止发送")
        if self.socket is None or self.reader.done():
            raise TransportError("连接不可用")
        echo = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[echo] = future
        try:
            async with asyncio.timeout(self.timeout):
                await self.socket.send(json.dumps({"action": action, "params": params or {}, "echo": echo}))
                response = await future
        except TimeoutError:
            raise TransportError("等待回执超时，调用结果未知") from None
        except asyncio.CancelledError:
            raise
        except TransportError:
            raise
        except Exception:
            raise TransportError("请求中断，调用结果未知") from None
        finally:
            self.pending.pop(echo, None)
            if not future.done():
                future.cancel()
        if response.get("status") != "ok" or type(response.get("retcode")) is not int or response["retcode"] != 0:
            if response.get("status") == "failed":
                raise ApiError("平台明确拒绝请求")
            raise TransportError("平台回执不确定")
        data = response.get("data")
        if not isinstance(data, dict):
            raise TransportError("平台结果格式未知")
        return data

    async def event(self) -> dict:
        while True:
            if not self.events.empty():
                return self.events.get_nowait()
            if self.reader.done():
                raise TransportError("事件连接已关闭")
            try:
                return await asyncio.wait_for(self.events.get(), timeout=0.2)
            except TimeoutError:
                continue
