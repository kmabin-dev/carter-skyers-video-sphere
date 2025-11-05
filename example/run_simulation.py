import multiprocessing
import time
import logging
import os
import argparse

from shared_buffer import SharedBuffer
from fan import Fan
from video_jockey import VideoJockey


def producer_worker(fan_id, shared_buf, sends_per_fan):
    f = Fan(fan_id)
    for _ in range(sends_per_fan):
        f.send_shard(shared_buf)
        # small jitter
        time.sleep(0.001)


def dj_worker(shared_buf, total_shards):
    vj = VideoJockey()
    vj.start(shared_buf, total_shards)


def run_simulation(num_fans=16, total_shards=128, dj_timeout=None):
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s:%(levelname)-8s] %(message)s')
    # ensure at least 8 fans log at INFO -- root logger is INFO so OK

    manager = multiprocessing.Manager()
    shared_buf = SharedBuffer(manager)

    sends_per_fan = total_shards // num_fans

    # start DJ
    dj = multiprocessing.Process(target=dj_worker, args=(shared_buf, total_shards))
    dj.start()

    # start producers
    producers = []
    for i in range(num_fans):
        p = multiprocessing.Process(target=producer_worker, args=(i, shared_buf, sends_per_fan))
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
