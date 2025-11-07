"""Legacy example orchestration script (kept for reference).

Spawns multiple fans and a single VideoJockey using the older direct API.
Prefer using `run_simulation.py` for the current workflow.
"""

import multiprocessing

import config
import fan
import shared_buffer as sb
import video_jockey
import video


def main():

    # clean any temp file
    video.clean_temp_directory()

    # logger.debug(f'shards={shards}')

    # create a manager to shared data between processes
    manager = multiprocessing.Manager()

    # create the shared buffer which is a buffer of (lock, shard_id, byte_data)
    shared_buffer = sb.SharedBuffer(manager)
    # logger.debug(f'shared_buffer={shared_buffer}')

    # create the vj
    vj = video_jockey.VideoJockey()

    # create the fans
    fans = []
    for i in range(config.NUM_FANS):
        f = fan.Fan(i)
        # logger.debug(f'fan={f}')
        # create a sub-process that calls the target with args
        fan_process = multiprocessing.Process(
            target=f.start, args=([shared_buffer])
        )
        # make the chef to start to prepare the food
        fan_process.start()
        # add the chef to a list, so that later we can join the chefs
        fans.append(fan_process)

    vj_process = multiprocessing.Process(
        target=vj.start, args=([shared_buffer])
    )
    vj_process.start()

    # join all the fans and the vj
    procs = fans + [vj_process]
    for proc in procs:
        proc.join()

    # at this point, all the processes are finish so we return to main


# main should only execute for the main process
if __name__ == "__main__":
    main()
