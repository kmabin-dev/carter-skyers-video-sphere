import multiprocessing
import logging
import os
import argparse
import random
import sys

import config
from shared_buffer import SharedBuffer
from fan import Fan
from video_jockey import VideoJockey


def producer_worker(fan_id, shared_buf, shard_path):
    """
    Producer worker that reads a single shard from disk (shard_path) and
    writes it to the shared buffer.
    """
    f = Fan(fan_id, shard_path=shard_path)
    # single send per fan (we spawn exactly as many fans as selected shards)
    f.send_shard(shared_buf)


def dj_worker(shared_buf, total_shards):
    vj = VideoJockey()
    vj.start(shared_buf, total_shards)


def run_simulation(num_fans=16, total_shards=128, dj_timeout=None):
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s:%(levelname)-8s] %(message)s')
    # ensure at least 8 fans log at INFO -- root logger is INFO so OK

    manager = multiprocessing.Manager()
    shared_buf = SharedBuffer(manager)

    # We will read a fixed number of shards in parallel (num_fans). Pick
    # up to `num_fans` random shard files from the shards directory.
    # Only select from existing shard files on disk. If there are fewer than
    # the requested `num_fans` shard files, abort with an error so the caller
    # can populate the shard directory correctly.
    shards_dir = config.SHARDS_DIR
    candidates = []
    if os.path.isdir(str(shards_dir)):
        for fn in sorted(os.listdir(str(shards_dir))):
            if fn.startswith('shard_') and fn.endswith('.mp4'):
                candidates.append(os.path.join(str(shards_dir), fn))

    if len(candidates) < num_fans:
        logging.getLogger(__name__).error(
            'Not enough shard files present in %s: found %d, need %d.\n'
            'Run the shard generator or place the shard files under that directory and try again.',
            shards_dir, len(candidates), num_fans
        )
        sys.exit(1)

    # choose exactly num_fans unique random shards from the existing files
    shard_paths = random.sample(candidates, k=num_fans)

    # start DJ - expect as many shards as we selected (len(shard_paths))
    expected_shards = len(shard_paths)
    dj = multiprocessing.Process(target=dj_worker, args=(shared_buf, expected_shards))
    dj.start()

    # start producers (one per selected shard)
    producers = []
    for i in range(len(shard_paths)):
        p = multiprocessing.Process(target=producer_worker, args=(i, shared_buf, shard_paths[i]))
        p.start()
        producers.append(p)

    # wait for producers
    for p in producers:
        p.join()

    # wait for dj to finish (timeout can be specified via env var DJ_TIMEOUT or CLI)
    if dj_timeout is None:
        try:
            dj_timeout = int(os.environ.get('DJ_TIMEOUT', '600'))
        except ValueError:
            dj_timeout = 600

    dj.join(timeout=dj_timeout)
    if dj.is_alive():
        print(f'DJ did not finish in time ({dj_timeout}s), terminating')
        dj.terminate()
    else:
        print('DJ finished')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run shard simulation')
    parser.add_argument('--fans', type=int, default=16, help='Number of fan producers')
    parser.add_argument('--shards', type=int, default=128, help='Total number of shards to send')
    parser.add_argument('--dj-timeout', type=int, default=None, help='DJ timeout in seconds (overrides DJ_TIMEOUT env)')
    args = parser.parse_args()

    # allow environment DJ_TIMEOUT to override default if CLI arg not provided
    run_simulation(num_fans=args.fans, total_shards=args.shards, dj_timeout=args.dj_timeout)
