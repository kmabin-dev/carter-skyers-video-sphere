#!/usr/bin/env python3
"""
Simple test script to verify presence and read access for shard files.
Usage:
    python shard_access_test.py --count 128

It returns exit code 0 on success, non-zero on failure.
"""
import os
import sys
import argparse
import config


def main():
    parser = argparse.ArgumentParser(description='Verify shard files exist and are readable')
    parser.add_argument('--count', type=int, default=int(os.environ.get('EXPECTED_SHARD_COUNT', '128')),
                        help='Number of sequential shard files to check (default 128)')
    args = parser.parse_args()

    missing = []
    unreadable = []
    for i in range(args.count):
        padded = str(i).zfill(4)
        p = config.SHARDS_DIR / f'shard_{padded}.mp4'
        p_str = str(p)
        if not os.path.exists(p_str):
            missing.append(p_str)
        else:
            if not os.access(p_str, os.R_OK):
                unreadable.append(p_str)

    if missing or unreadable:
        if missing:
            print(f'Missing {len(missing)} shard files; first few: {missing[:10]}', file=sys.stderr)
        if unreadable:
            print(f'Unreadable {len(unreadable)} shard files; first few: {unreadable[:10]}', file=sys.stderr)
        return 2

    print(f'All {args.count} shard files are present and readable in {config.SHARDS_DIR}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
