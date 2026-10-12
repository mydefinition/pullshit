"""Disposable subprocess entry for bounded image downloads.

Reads one URL plus limits, performs the synchronous download and prints a
single JSON result line. The caller enforces the wall-clock kill of this
process group, so the entry must stay free of loops beyond the internal
deadline checks in pullshit.content.media.
"""
from __future__ import annotations

import argparse
import base64
import json
import sys

from pullshit.content.media import MediaError, _download


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog='pullshit-content-media-worker')
    parser.add_argument('url')
    parser.add_argument('--max-bytes', type=int, required=True)
    parser.add_argument('--max-redirects', type=int, required=True)
    parser.add_argument('--timeout', type=float, required=True)
    parser.add_argument('--allow-private', action='store_true')
    args = parser.parse_args(argv)
    result = {'ok': False, 'error': 'media_download_error', 'actual_bytes': 0}
    try:
        body, actual = _download(args.url, args.max_bytes, args.max_redirects,
                                 args.timeout, args.allow_private)
        result = {'ok': True, 'data': base64.b64encode(body).decode('ascii'), 'actual_bytes': actual}
    except MediaError as exc:
        result = {'ok': False, 'error': exc.code, 'actual_bytes': exc.actual_bytes}
    print(json.dumps(result, separators=(',', ':')))
    return 0


if __name__ == '__main__':
    sys.exit(main())
