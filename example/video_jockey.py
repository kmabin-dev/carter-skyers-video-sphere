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
            logger.info(
                '%s received shard from %s in slot %d -> %s',
                self.name(), sender_name, idx, file_path
            )
            # append the file_path as the shard representation
            collected.append((sender_name, file_path))

        # indicate to all fans that the vj has all the shards
        try:
            shared_buffer.vj_has_all_shards.value = True
        except OSError as e:
            logger.warning('Failed to set completion flag: %s', e)

        # store as flat list of file paths
        self.__shards = [p for (_, p) in collected]
        return True

    def __cleanup_temp_files(self):
        '''
        Clean up temp files after they've been consumed for the video composition
        '''
        import os
        cleaned = []
        for temp_path in self.__shards:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    cleaned.append(temp_path)
            except OSError as e:
                logger.warning('Failed to cleanup temp file %s: %s', temp_path, e)
        logger.debug('Cleaned up %d temp files', len(cleaned))

    def __write_video(self):
        '''
        Compose received video shards into a final video using ffmpeg.
        Uses a 4x4 grid layout since we have multiple shards, treating each
        as a tile in the mosaic.
        '''
        import os
        import ffmpeg
        
        # Ensure temp dir exists for output
        out_dir = config.TEMP_DIR
        os.makedirs(str(out_dir), exist_ok=True)

        # First, create a list of input files
        inputs = []
        for shard_path in self.__shards:
            try:
                # Verify the file exists and is readable
                if not os.path.exists(shard_path):
                    logger.warning('Shard file missing: %s', shard_path)
                    continue
                # Add to input list
                inputs.append(ffmpeg.input(shard_path))
            except (ffmpeg.Error, OSError) as e:
                logger.error('Error adding input %s: %s', shard_path, e)
                continue

        if not inputs:
            logger.error('No valid input shards found')
            return None

        # Create output path
        out_path = os.path.join(str(out_dir), 'final_collage.mp4')
        
        try:
            # Use simple concat for now since we know they're sequential parts
            # In a full implementation, we'd use a grid layout with xstack filter
            logger.info('Starting ffmpeg composition with %d inputs...', len(inputs))
            process = (
                ffmpeg
                .concat(*inputs)
                .output(out_path, loglevel='info')
                .overwrite_output()
                .run_async(pipe_stdout=True, pipe_stderr=True)
            )
            
            # Stream ffmpeg output in real-time
            while True:
                line = process.stderr.readline()
                if not line:
                    break
                line = line.decode('utf-8', errors='replace').strip()
                if line:
                    logger.info('ffmpeg: %s', line)
                    
            # Get final output/error streams
            _, stderr = process.communicate()
            
            if process.returncode == 0:
                logger.info('ffmpeg composition completed -> %s', out_path)
                # Clean up temp files only on success
                self.__cleanup_temp_files()
                return out_path
            else:
                stderr_text = stderr.decode('utf-8', errors='replace')
                logger.error('ffmpeg failed with return code %d:\n%s', 
                           process.returncode, stderr_text)
                return None
            
        except ffmpeg.Error as e:
            logger.error('FFmpeg error composing video: %s', e)
            return None

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
