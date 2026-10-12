"""A disposable download process; the caller can terminate DNS or socket waits."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from pullshit.media.cache import MediaError, cache_image


def main() -> int:
    os.umask(0o077)
    try:
        request = json.loads(sys.stdin.buffer.read(32769))
        result = cache_image(request["url"], Path(request["directory"]), request["limits"])
    except MediaError as exc:
        print(json.dumps({"error": exc.code}))
        return 2
    except Exception:
        print(json.dumps({"error": "媒体缓存操作失败"}, ensure_ascii=False))
        return 2
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
