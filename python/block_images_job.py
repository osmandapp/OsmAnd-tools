#!/usr/bin/env python3

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'lib')))

from python.lib.utils import parse_command_line_into_dict
from python.lib.block_images_utils import BLOCK_BANNED, BLOCK_INVALID, BLOCK_PROHIBITED, block_images
from python.lib.block_images_utils import cleanup_files, cleanup_tables, list_blocked, unblock_reason, unblock_title


def main():
    args = parse_command_line_into_dict()

    if args.get("--unblock-all"): unblock_reason()
    if args.get("--unblock-banned"): unblock_reason(BLOCK_BANNED)
    if args.get("--unblock-invalid"): unblock_reason(BLOCK_INVALID)
    if args.get("--unblock-prohibited"): unblock_reason(BLOCK_PROHIBITED)

    if title := args.get("--unblock-file="): unblock_title(title)

    if title := args.get("--add-banned-file="): block_images({title}, BLOCK_BANNED)
    if title := args.get("--add-invalid-file="): block_images({title}, BLOCK_INVALID)
    if title := args.get("--add-prohibited-file="): block_images({title}, BLOCK_PROHIBITED)

    if args.get("--cleanup-files"): cleanup_files()
    if args.get("--cleanup-tables"): cleanup_tables()

    if args.get("--list-all"): list_blocked()
    if args.get("--list-banned"): list_blocked(BLOCK_BANNED)
    if args.get("--list-invalid"): list_blocked(BLOCK_INVALID)
    if args.get("--list-prohibited"): list_blocked(BLOCK_PROHIBITED)


if __name__ == '__main__':
    main()
