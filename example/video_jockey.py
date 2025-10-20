import config
import video
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

    def __read_all_shards(self, shared_buffer):
        '''
        read all the shards by polling the shared buffer
        1. read a shard from shared buffer, and release it's lock
        2. store it in the vj buffer
        3. once we have a  shard, return True
        '''
        while not self.has_all_shards():
            lock, sender_name, shard_data,  = shared_buffer.buffer()[0]

            # if shared buffer element is not empty (it has shard data)
            if shard_data is not None:
                # add shard to vj buffer
                self.__shards[0] = shard_data
                logger.info(f'{self.name()} received shard from the fan {sender_name}. {
                    len(self.__shards)}/{config.NUM_SHARDS} shards received')

                # release the lock
                lock.release()

                # indicate to all fans that the vj has all the shards
                shared_buffer.vj_has_all_shards.value = True

        return True

    def __write_video(self):
        '''
        write the video from buffer to disk
        '''
        # write all the shards to disk
        all_temp_file_path = []
        for i in range(len(self.__shards)):
            shard_data = self.__shards[i]
            video_file_path = video.write(f'shard_{i}', shard_data)
            all_temp_file_path.append(video_file_path)

        # concat all the shards into one video
        output_file_path = video.concat('concat_all', *all_temp_file_path)
        return output_file_path

    def start(self, shared_buffer):
        '''
        1. read all shards from shared buffer
        2. if we have all shards, write the video to disk, add audio, play video
        '''
        has_all_shards = self.__read_all_shards(shared_buffer)
        if has_all_shards:
            logger.info(
                f'*** SUCCESS! {self.__name} has a shard! ***')
            logger.info(f'{self.__name} writing the video')
            video_file_path = self.__write_video()
            logger.info(f'{self.__name} adding audio to the video {
                        video_file_path}')
            video_with_audio = video.audio('video_with_audio',
                                           video_file_path, config.SOURCE_AUDIO_FILE_PATH)
            logger.info(f'{self.__name} playing the video with audio {
                        video_with_audio}')
            video.play(video_with_audio)
