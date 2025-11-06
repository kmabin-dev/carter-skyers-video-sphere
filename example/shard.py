"""Video shard representation and validation.

Computes and verifies the file hash, and provides simple accessors and
JSON-style stringification for debugging.
"""

import json

from config import logger
import video


class Shard(object):
    '''
    video shard class
    '''

    def __init__(self, id, start, end, file_path, hash):

        self.__id = id
        self.__start = start
        self.__end = end
        self.__file_path = file_path
        self.__hash = video.file_hash(file_path)

        if self.__hash != hash:
            logger.error('File hash does not match. Data is corrupted.')
            raise Exception

    def id(self):
        return self.__id

    def start(self):
        return self.__start

    def end(self):
        return self.__end

    def file_path(self):
        return self.__file_path

    def hash(self):
        return self.__hash

    def __str__(self):
        d = {
            'id': self.__id,
            'start': self.__start,
            'end': self.__end,
            'file_path': self.__file_path,
            'hash': self.__hash
        }
        s = json.dumps(d, indent=4)
        return s
