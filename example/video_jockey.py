import os
import config
import subprocess
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
        import time
        while len(collected) < total_shards:
            item = shared_buffer.get_shard(timeout=1.0)
            if item is None:
                # no slot currently available, small sleep to avoid busy spin
                time.sleep(0.05)
                continue
            sender_name, file_path = item
            logger.info('%s received shard from %s -> %s', self.name(), sender_name, file_path)
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
        Uses concatenation to combine the video shards and adds audio from source.
        '''
        # Ensure temp dir exists for output
        out_dir = config.TEMP_DIR
        os.makedirs(str(out_dir), exist_ok=True)

        # First, verify all shards exist and are readable
        valid_shards = []
        for shard_path in self.__shards:
            if os.path.exists(shard_path):
                valid_shards.append(shard_path)
            else:
                logger.warning('Shard file missing: %s', shard_path)

        if not valid_shards:
            logger.error('No valid input shards found')
            return None

        # Create output path
        out_path = os.path.join(str(out_dir), 'final_collage.mp4')
        
        try:
            # Create a temporary file listing all the input files
            list_path = os.path.join(out_dir, 'concat_list.txt')
            with open(list_path, 'w', encoding='utf-8') as fh:
                for p in valid_shards:
                    # FFmpeg concat demuxer requires 'file' prefix and single quotes
                    escaped = p.replace(chr(39), chr(39) + '\\' + chr(39))
                    fh.write(f"file '{escaped}'\n")

            # Build ffmpeg command with audio input (configurable via config.py)
            # Build fade filter string separately to keep lines short
            fade_in = getattr(config, 'AUDIO_FADE_IN_SECONDS', 0.2)
            fade_out = getattr(config, 'AUDIO_FADE_OUT_SECONDS', 1.2)
            fade_filter = f"afade=t=in:d={fade_in},areverse,afade=t=in:d={fade_out},areverse"

            ffmpeg_cmd = [
                'ffmpeg',
                '-f', 'concat',        # Use concat demuxer
                '-safe', '0',          # Allow absolute paths
                '-i', list_path,       # Input list file (video shards)
                # Start audio at configured offset
                '-ss', str(getattr(config, 'AUDIO_OFFSET_SECONDS', 78)),
                '-i', str(config.SOURCE_AUDIO_FILE_PATH),  # Input audio file
                '-c:v', 'copy',        # Stream copy video (no re-encode)
                '-c:a', 'aac',         # Encode audio as AAC
                '-b:a', str(getattr(config, 'AUDIO_BITRATE', '192k')),  # Audio bitrate
                # Apply fade-in and fade-out using areverse trick to avoid needing total duration
                '-af', fade_filter,
                '-shortest',           # Match shortest stream length
                '-movflags', '+faststart',  # Web playback optimization
                '-y',                  # Overwrite output
                out_path              # Output path
            ]

            logger.info('Starting ffmpeg composition with command: %s', ' '.join(ffmpeg_cmd))
            
            # Run ffmpeg process
            process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True  # Get string output instead of bytes
            )

            # Stream output in real-time
            for line in process.stderr:
                line = line.strip()
                if line:
                    logger.info('ffmpeg: %s', line)

            # Wait for completion
            returncode = process.wait()

            if returncode == 0:
                logger.info('ffmpeg composition completed -> %s', out_path)
                # Clean up temp files only on success
                self.__cleanup_temp_files()
                # Also clean up the concat list
                try:
                    os.remove(list_path)
                except OSError:
                    pass
                return out_path
            else:
                stderr = process.stderr.read()
                logger.error('ffmpeg failed with return code %d:\n%s', returncode, stderr)
                return None

        except Exception as e:
            logger.error('Error composing video: %s', e)
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
            # Auto-play the final video on macOS (configurable)
            try:
                if getattr(config, 'AUTO_PLAY_FINAL_VIDEO', True) and video_file_path and os.path.exists(video_file_path):
                    subprocess.Popen(['open', video_file_path])
            except Exception as e:
                logger.warning('Auto-play failed: %s', e)
