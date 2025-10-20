import config
from faker import Faker

FAKE = Faker()


class SharedBuffer(object):
    '''
    shared buffer class
    '''

    def __init__(self, manager):

        # a shared variable to indicate when the vj has all the shards
        self.vj_has_all_shards = manager.Value('has_all_shards', False)

        # a shared buffer which is a list of (lock, sender_name, byte_data)
        self.__buffer = manager.list()
        for i in range(config.SHARED_BUFFER_SIZE):
            lock = manager.Lock()
            elem = (lock, None, None)
            self.__buffer.append(elem)

    def lock(self, i):
        return self.__buffer[i][0]

    def sender_name(self, i):
        return self.__buffer[i][1]

    def byte_data(self, i):
        return self.__buffer[i][2]

    def buffer(self):
        return self.__buffer
