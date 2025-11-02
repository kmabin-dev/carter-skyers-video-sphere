import random

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
            logger.error(
                f'Unable to open {file_path} exception={type(e).__name__}')
            quit(-1)

        # read the data
        byte_data = file.read()

        # close file
        file.close()

        return byte_data

    def send_shard(self, shared_buffer):
        '''
        example code to send a shard to shared buffer element 0
        '''
        lock = shared_buffer.lock(0)
        # acquire the lock, if it is free, block if it is not
        logger.debug(f'fan {self.name()} trying to acquire the lock')
        if lock.acquire():
            logger.debug(f'fan {self.name()} acquired the lock')
            # write a shard to the shared buffer
            shared_buffer.buffer()[0] = (lock, self.__name, self.__buffer)
            logger.info(
                f'The fan {self.name()} a sent shard to the shared buffer')

    def start(self, shared_buffer):
        self.send_shard(shared_buffer)
