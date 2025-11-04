import multiprocessing
import time
import logging

import config
from shared_buffer import SharedBuffer
from fan import Fan
from video_jockey import VideoJockey


def producer_worker(fan_id, shared_buf, sends_per_fan):
    f = Fan(fan_id)
    for i in range(sends_per_fan):
        f.send_shard(shared_buf)
        # small jitter
        time.sleep(0.001)


def dj_worker(shared_buf, total_shards):
    vj = VideoJockey()
    vj.start(shared_buf, total_shards)


def run_simulation(num_fans=16, total_shards=128, buffer_size=4):
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

    # wait for dj to finish (with timeout)
    dj.join(timeout=30)
    if dj.is_alive():
        print('DJ did not finish in time, terminating')
        dj.terminate()
    else:
        print('DJ finished')


if __name__ == '__main__':
    run_simulation()
