import config
from config import logger


class VideoJockey(object):
    '''
    fan class
    '''

    def __init__(self):

        self.__name = 'Marshmello'
        self.__shards = [None]

    def name(self):
        return self.__name

    def shards(self):
        return self.__shards

    def has_all_shards(self):
        '''
        for the example code, we only check if the first shard exists
        '''
        return self.__shards[0] is not None

    def __read_all_shards(self, shared_buffer, total_shards):
        '''
        Read shards by polling the shared buffer until total_shards have been
        collected. Uses non-blocking reads to avoid deadlocks; the function
        returns a list of file paths that were consumed.
        '''
        collected = []
        while len(collected) < total_shards:
            item = shared_buffer.read_any_slot_nonblocking()
            if item is None:
                # no slot currently available, small sleep to avoid busy spin
                import time
                time.sleep(0.01)
                continue
            idx, sender_name, file_path = item
            logger.info('%s received shard from %s in slot %d -> %s', self.name(), sender_name, idx, file_path)
            # append the file_path as the shard representation
            collected.append((sender_name, file_path))

        # indicate to all fans that the vj has all the shards
        try:
            shared_buffer.vj_has_all_shards.value = True
        except Exception:
            pass

        # store as flat list of file paths
        self.__shards = [p for (_, p) in collected]
        return True

    def __write_video(self):
        '''
        For the integration test we compose a simple "collage" file listing
        the shard file paths consumed. In a full implementation this would
        call ffmpeg to tile/concat the videos.
        '''
        import os
        out_dir = config.TEMP_DIR
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            pass
        out_path = os.path.join(str(out_dir), 'final_collage.txt')
        with open(out_path, 'w', encoding='utf-8') as fh:
            for p in self.__shards:
                fh.write(f'{p}\n')
        return out_path

    def start(self, shared_buffer, total_shards=128):
        '''
        1. read all shards from shared buffer
        2. if we have all shards, write the video to disk, add audio, play video
        '''
        has_all_shards = self.__read_all_shards(shared_buffer, total_shards)
        if has_all_shards:
            logger.info('*** SUCCESS! %s has shards! ***', self.__name)
            logger.info('%s writing the video', self.__name)
            video_file_path = self.__write_video()
            logger.info('%s writing done -> %s', self.__name, video_file_path)
