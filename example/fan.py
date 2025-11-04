import random
import os

import config
from config import logger
from faker import Faker

FAKE = Faker()


class Fan(object):
    '''
    fan class
    '''

    def __init__(self, id):

        self.__id = id
        self.__name = FAKE.name()
        self.__buffer = self.read_random_shard()

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
        shard_id = random.randint(0, config.NUM_SHARDS - 1)
        padded = str(shard_id).zfill(4)
        # SHARDS_DIR may be a Path; construct path safely
        file_path = config.SHARDS_DIR / f'shard_{padded}.mp4'

        try:
            file = open(str(file_path), 'rb')
        except Exception as e:
            # If shards are not present, generate a small dummy payload
            logger.debug('Unable to open %s exception=%s; using dummy shard', file_path, type(e).__name__)
            return bytes(f'dummy-shard-{self.__id}-{random.randint(0,9999)}', 'utf-8')
        # read the data
        byte_data = file.read()

        # close file
        file.close()

        return byte_data

    def send_shard(self, shared_buffer):
        '''
        example code to send a shard to shared buffer element 0
        '''
        # Try to acquire a free slot (non-blocking). If none free, back off and retry.
        logger.debug('fan %s trying to acquire a free slot', self.name())
        idx = shared_buffer.try_acquire_free_slot()
        while idx is None:
            # small backoff to avoid busy spin
            import time
            time.sleep(0.01)
            idx = shared_buffer.try_acquire_free_slot()

        # We now hold the lock for slot idx. Write our shard to a temp file and publish path.
        try:
            # ensure temp dir exists
            tmp_dir = config.TEMP_DIR
            try:
                os.makedirs(tmp_dir, exist_ok=True)
            except Exception:
                # if tmp_dir is a Path-like, os.makedirs accepts it on Python 3.8+
                pass

            # write a temporary file then publish its path
            import tempfile
            tmp = tempfile.NamedTemporaryFile(delete=False, dir=str(tmp_dir))
            # get shard payload for this send
            payload = self.read_random_shard()
            tmp.write(payload)
            tmp.flush()
            tmp_name = tmp.name
            tmp.close()

            # publish to shared buffer and release lock
            shared_buffer.write_slot(idx, self.__name, tmp_name)
            logger.info('The fan %s sent shard to slot %d -> %s', self.name(), idx, tmp_name)
        except Exception as e:
            logger.error('fan %s failed to write shard: %s', self.name(), e)

    def start(self, shared_buffer):
        self.send_shard(shared_buffer)
