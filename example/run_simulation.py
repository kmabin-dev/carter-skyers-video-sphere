import multiprocessing
import logging
import os
import argparse
import random

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
    try:
        shards_dir = config.SHARDS_DIR
        # list candidate shard files (shard_0000.mp4 ...)
        candidates = []
        if os.path.isdir(str(shards_dir)):
            for fn in os.listdir(str(shards_dir)):
                if fn.startswith('shard_') and fn.endswith('.mp4'):
                    candidates.append(os.path.join(str(shards_dir), fn))
        # If we found fewer candidates on disk than the number of fans we need,
        # generate the expected shard file names from the total_shards range
        if len(candidates) < num_fans:
            candidates = []
            for i in range(total_shards):
                padded = str(i).zfill(4)
                candidates.append(str(shards_dir / f'shard_{padded}.mp4'))

        # choose exactly num_fans unique random shards (or fewer if not enough exist)
        shard_paths = random.sample(candidates, k=min(num_fans, len(candidates)))
    except Exception as e:
        logging.getLogger(__name__).warning('Failed to enumerate shard files: %s; falling back to random-per-fan', e)
        # fallback: let each fan pick a random shard internally
        shard_paths = [None] * num_fans

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
