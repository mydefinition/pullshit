"""Receive-only service: reconnect gaps are explicit, credentials stay local."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from pullshit.config import load_settings
from pullshit.adapters.qq.events import normalize
from pullshit.adapters.qq.napcat import NapCat, TransportError
from pullshit.media.cache import prune_media
from pullshit.storage.store import Store


def cleanup(settings, store: Store) -> None:
    store.prune(settings.data["storage"]["raw_event_retention_days"],
                settings.data["context"]["failure_cache_seconds"])
    prune_media(settings.path("storage", "media_directory"),
                settings.data["storage"]["media_retention_days"])


async def receive(root: Path, profile: str, *, max_events: int | None = None) -> dict:
    settings = load_settings(root, profile)
    settings.validate_live("receive")
    store = Store(settings.path("storage", "database_path"))
    gap = store.gap(settings.account, "receiver_startup")
    backoff = settings.data["napcat"]["reconnect_initial_seconds"]
    received = 0
    logger = logging.getLogger("pullshit")
    loop = asyncio.get_running_loop()
    try:
        while True:
            settings = load_settings(root, profile)
            settings.validate_live("receive")
            nc = settings.data["napcat"]
            try:
                async with NapCat(nc["endpoint"], settings.token(), timeout=nc["request_timeout_seconds"]) as client:
                    settings = settings.with_account(client.account)
                    store.bind_gap(gap, settings.account)
                    store.close_gap(gap)
                    cleanup(settings, store)
                    last_cleanup = loop.time()
                    backoff = nc["reconnect_initial_seconds"]
                    logger.info("平台连接已建立，已获取运行账号")
                    while True:
                        maintenance = load_settings(root, profile).with_account(settings.account)
                        remaining = last_cleanup + maintenance.data["storage"]["cleanup_interval_seconds"] - loop.time()
                        if remaining <= 0:
                            cleanup(maintenance, store)
                            last_cleanup = loop.time()
                            continue
                        try:
                            async with asyncio.timeout(remaining):
                                raw = await client.event()
                        except TimeoutError:
                            continue
                        # Re-read routing permissions before each event is persisted.
                        current = load_settings(root, profile).with_account(settings.account)
                        if current.data["napcat"] != nc:
                            raise TransportError("连接配置改变，需要重新连接")
                        try:
                            event = normalize(raw, current.account, current.sources, current.targets,
                                              current.data["messages"]["forward_max_depth"],
                                              current.data["messages"]["forward_max_nodes"])
                        except (ValueError, TypeError):
                            logger.warning("收到无效事件，已忽略")
                            continue
                        if event is not None:
                            store.ingest(event)
                            received += 1
                            if max_events is not None and received >= max_events:
                                return store.summary()
            except TransportError:
                gap = store.gap(settings.account, "connection_interrupted")
                logger.warning("平台连接中断，等待重新连接；覆盖缺口已记录")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, nc["reconnect_max_seconds"])
    finally:
        store.close()
