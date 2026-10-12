"""Command line, with separate online and offline entry points."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import sqlite3
import sys
from pathlib import Path

from pullshit.config import ConfigError, decimal_id, load_settings, read_json
from pullshit.media.cache import MediaError, cache_image, prune_media
from pullshit.application.replay import replay
from pullshit.storage.store import Store, StoreError


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(prog="pullshit")
    result.add_argument("--root", type=Path, default=Path.cwd(), help="项目根目录")
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("check-config", "receive", "probe", "status", "pause", "resume", "confirm", "cleanup", "cache-image", "context"):
        command = commands.add_parser(name)
        command.add_argument("--profile", choices=("development", "production"), required=True)
        if name == "check-config":
            command.add_argument("--ready", choices=("receive", "probe"))
        elif name == "receive":
            command.add_argument("--max-events", type=int)
        elif name == "probe":
            command.add_argument("request", type=Path)
        elif name == "status":
            command.add_argument("--probe-id")
        elif name == "confirm":
            command.add_argument("--probe-id", required=True)
            command.add_argument("--part", type=int, required=True)
            command.add_argument("--message-id", required=True)
        elif name == "cache-image":
            command.add_argument("--url-file", type=Path, required=True)
        elif name == "context":
            command.add_argument("--group-id", required=True)
            command.add_argument("--message-id", required=True)
            command.add_argument("--online", action="store_true", help="允许只读平台回取")
            command.add_argument("--cache-media", action="store_true", help="下载图片，需要 --online")
        elif name == "cleanup":
            command.add_argument("--compact", action="store_true", help="清理后压缩数据库并尝试截断 WAL")
    command = commands.add_parser("replay")
    command.add_argument("fixture", type=Path)
    command.add_argument("--database", type=Path, required=True)
    command.add_argument("--account", required=True)
    command.add_argument("--source-group", action="append", default=[])
    command.add_argument("--target-group", action="append", default=[])
    return result


def configure_logging(settings) -> None:
    logger = logging.getLogger("pullshit")
    logger.setLevel(settings.data["logging"]["level"].upper())
    logger.propagate = False
    directory = settings.path("logging", "directory")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    handler = logging.handlers.TimedRotatingFileHandler(
        directory / "pullshit.log", when="midnight", utc=True,
        backupCount=settings.data["logging"]["retention_days"], encoding="utf-8",
    )
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    websocket_logger = logging.getLogger("pullshit.ws.silent")
    websocket_logger.disabled = True


async def _probe(settings, request: dict, store: Store) -> dict:
    from pullshit.adapters.qq.napcat import NapCat
    from pullshit.application.probe import send_probe, validate_request
    settings.validate_live("probe")
    validate_request(request, settings)
    nc = settings.data["napcat"]
    async with NapCat(nc["endpoint"], settings.token(), timeout=nc["request_timeout_seconds"], allow_send=True) as client:
        return await send_probe(settings.with_account(client.account), store, client, request)


async def _context(settings, args, store):
    from pullshit.application.context import ContextResolver
    group = decimal_id(args.group_id)
    message = decimal_id(args.message_id, signed=True)
    if group not in settings.sources | settings.targets:
        raise ConfigError("上下文查询超出当前群授权")
    if args.cache_media and not args.online:
        raise ConfigError("--cache-media 需要 --online")
    if not args.online:
        account = store.context_account(group, message)
        return await ContextResolver(settings.with_account(account), store).resolve(group, message)
    from pullshit.adapters.qq.napcat import NapCat
    settings.validate_live("receive")
    nc = settings.data["napcat"]
    deadline = asyncio.get_running_loop().time() + settings.data["context"]["total_timeout_seconds"]
    client = NapCat(nc["endpoint"], settings.token(), timeout=nc["request_timeout_seconds"], allow_send=False)
    try:
        async with asyncio.timeout_at(deadline):
            await client.__aenter__()
        resolver = ContextResolver(settings.with_account(client.account), store, client,
                                   cache_media=args.cache_media, deadline=deadline)
        return await resolver.resolve(group, message)
    except TimeoutError:
        return {"group_id": group, "message_id": message, "online": True, "status": "partial",
                "message": None, "nearby": {"relation": "time_neighbor", "before": [], "after": []},
                "issues": ["timeout"], "stats": {"fetch_calls": 0, "identity_checks": 0, "nodes": 0,
                                                   "media_downloads": 0, "media_cache_hits": 0}}
    finally:
        await client.__aexit__(None, None, None)


def dispatch(args) -> dict:
    if args.command == "replay":
        return replay(args.root, args.fixture, args.database, args.account,
                      set(args.source_group), set(args.target_group))
    settings = load_settings(args.root, args.profile)
    if args.command == "check-config":
        if args.ready:
            settings.validate_live(args.ready)
        return {"profile": args.profile, "configuration": "valid", "online_ready": bool(args.ready),
                "sending_enabled": settings.data["sending"]["enabled"], "paused": settings.data["sending"]["paused"]}
    if args.command == "receive":
        from pullshit.application.service import receive
        if args.max_events is not None and args.max_events <= 0:
            raise ConfigError("max-events 必须是正整数")
        settings.validate_live("receive")
        configure_logging(settings)
        return asyncio.run(receive(args.root, args.profile, max_events=args.max_events))
    if args.command == "cache-image":
        return cache_image(args.url_file.read_text().strip(), settings.path("storage", "media_directory"), settings.data["media"])
    store = Store(settings.path("storage", "database_path"))
    try:
        if args.command == "context":
            return asyncio.run(_context(settings, args, store))
        if args.command == "probe":
            return asyncio.run(_probe(settings, read_json(args.request), store))
        if args.command in {"pause", "resume"}:
            store.set_paused("*", args.command == "pause")
            return {"paused": store.paused("*")}
        if args.command == "confirm":
            from pullshit.application.probe import sender_lock
            account = store.probe_account(args.probe_id)
            with sender_lock(settings.path("storage", "database_path")):
                store.recover_probe(account, args.probe_id)
                status = store.probe(account, args.probe_id)
                if not status or args.part < 0 or args.part >= len(status["parts"]) or status["parts"][args.part]["status"] != "unknown":
                    raise StoreError("只允许确认不确定的消息段")
                store.finish_part(account, args.probe_id, args.part, "sent",
                                  message_id=decimal_id(args.message_id, signed=True), error_code="manually_confirmed")
            return store.probe(account, args.probe_id)
        if args.command == "cleanup":
            result = {"events_removed": store.prune(settings.data["storage"]["raw_event_retention_days"],
                                                   settings.data["context"]["failure_cache_seconds"]),
                      "media_removed": prune_media(settings.path("storage", "media_directory"), settings.data["storage"]["media_retention_days"])}
            if args.compact:
                result.update(store.compact())
            return result
        result = {"stored": store.summary(), "paused": store.paused("*")}
        if args.probe_id:
            result["probe"] = store.probe(store.probe_account(args.probe_id), args.probe_id)
        return result
    finally:
        store.close()


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        result = dispatch(args)
    except (ConfigError, StoreError, MediaError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("已停止", file=sys.stderr)
        return 130
    except (OSError, ValueError, sqlite3.Error):
        print("操作失败：请检查本地文件、输入格式与数据库权限", file=sys.stderr)
        return 2
    except Exception:
        print("平台操作失败；发送结果请查询探测账本", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0
