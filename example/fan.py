"""Fan producer: reads or generates a shard, writes to a temp file and enqueues its path.

Key behaviors:
    - Optionally uses a provided shard path (for tests) or picks a random shard id.
    - Writes shard bytes to a temp file under `config.TEMP_DIR`.
    - Retries enqueueing with backpressure logging.
    - Registers failed temp files for cleanup worker if enqueueing ultimately fails.
"""

import os
import random

from faker import Faker

import config
from config import logger

FAKE = Faker()


class Fan(object):
    '''
    fan class
    '''

    def __init__(self, id, shard_path=None, verbose=True):
        self.__id = id
        self.__name = FAKE.name()
        # optional shard_path; if provided, read_random_shard will use it
        self.__shard_path = shard_path
        # keep a small buffer attribute for tests that expect it
        self.__buffer = b''
        # control whether this fan emits INFO logs (else, downgrade to DEBUG)
        self.__verbose = verbose

    def id(self):
        return self.__id

    def name(self):
        return self.__name

    def buffer(self):
        return self.__buffer

    def read_random_shard(self):
        '''
        read a random shard from disk, returns the shard id and the byte data
        '''
        # If a specific shard path was provided, use it.
        if getattr(self, '_Fan__shard_path', None):
            file_path = self.__shard_path
        else:
            shard_id = random.randint(0, config.NUM_SHARDS - 1)
            padded = str(shard_id).zfill(4)
            # SHARDS_DIR may be a Path; construct path safely
            file_path = config.SHARDS_DIR / f'shard_{padded}.mp4'

        try:
            file = open(str(file_path), 'rb')
        except OSError as e:
            # If shards are not present, generate a small dummy payload
            logger.debug(
                'Unable to open %s exception=%s; using dummy shard',
                file_path, type(e).__name__
            )
            return bytes(
                f'dummy-shard-{self.__id}-{random.randint(0,9999)}',
                'utf-8'
            )
        # read the data
        byte_data = file.read()

        # close file
        file.close()

        return byte_data

    def send_shard(self, shared_buffer):
        '''
        example code to send a shard to shared buffer element 0
        '''
        # Write our shard to a temp file and publish its path to the bounded queue.
        try:
            # ensure temp dir exists
            tmp_dir = config.TEMP_DIR
            os.makedirs(tmp_dir, exist_ok=True)

            # write a temporary file then publish its path
            import tempfile, time
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(tmp_dir))
            payload = self.read_random_shard()
            tmp.write(payload)
            tmp.flush()
            tmp_name = tmp.name
            tmp.close()

            # If the VideoJockey has already claimed all shards, stop and
            # remove our temp file to avoid unnecessary work.
            try:
                if getattr(shared_buffer, 'vj_has_all_shards', None) is not None and shared_buffer.vj_has_all_shards.value:
                    if self.__verbose:
                        logger.info(
                            'fan %s detected DJ has capacity/full; removing temp and '
                            'exiting -> %s',
                            self.name(), tmp_name
                        )
                    else:
                        logger.debug(
                            'fan %s detected DJ has capacity/full; removing temp and '
                            'exiting -> %s',
                            self.name(), tmp_name
                        )
                    try:
                        os.remove(tmp_name)
                    except Exception:
                        pass
                    return
            except Exception:
                # If anything goes wrong reading the flag, continue attempting to enqueue
                pass

            # Try to put into the bounded shared buffer with retries/backoff.
            max_attempts = 5
            attempt = 0
            put_ok = False
            while attempt < max_attempts and not put_ok:
                # block up to 2 seconds to allow DJ to consume
                put_ok = shared_buffer.put_shard(self.__name, tmp_name, timeout=2.0)
                if not put_ok:
                    logger.debug(
                        'fan %s backpressure: buffer full, retrying (%d/%d)',
                        self.name(), attempt + 1, max_attempts
                    )
                    time.sleep(0.2 * (attempt + 1))
                attempt += 1

            if put_ok:
                log_fn = logger.info if self.__verbose else logger.debug
                log_fn('The fan %s sent shard -> shared buffer', self.name())
            else:
                logger.error(
                    'fan %s failed to enqueue shard after %d attempts; '
                    'registering shard for later cleanup %s',
                    self.name(), max_attempts, tmp_name
                )
                # Register the temp file with the shared buffer failed-temp list
                try:
                    shared_buffer.register_failed_temp(tmp_name)
                except Exception:
                    # Fall back to best-effort removal if registration fails
                    try:
                        os.remove(tmp_name)
                    except Exception:
                        pass

        except (OSError, IOError) as e:
            logger.error('fan %s failed to write shard: %s', self.name(), e)

    def start(self, shared_buffer):
        self.send_shard(shared_buffer)
