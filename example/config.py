"""Global configuration for the video shard composition project.

Centralizes directory paths, tuning parameters (shard counts, buffer sizes),
audio composition settings, and logging setup.
"""

import logging
from pathlib import Path

#
# STUDENTS NEED TO CHANGE THE PROJECT DIRECTORY FOR THEIR COMPUTER
#
# project directory (use Path for safe joins)
# Note: some modules expect strings for file paths; use str(...) when needed.
PROJECT_DIR = Path('/Users/kay/Documents/Advanced Python/Carter-skyers-video-sphere')

#
# STUDENTS MAY WISH TO CHANGE (REDUCE) THE FOLLOWING PARAMETERS WHILE TESTING AND DEBUGGING THEIR CODE
#
# number of shards in the video
# Reduced to 1 for local integration test (only one shard required for the example flow)
NUM_SHARDS = 1

# number of fan processes to spawn
# Reduced for faster local test
NUM_FANS = 4

#
# STUDENTS MAY WISH TO CHANGE THE LOGGING LEVEL WHILE TESTING AND DEBUGGING THEIR CODE
#
# get logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

#
# STUDENTS CAN, BUT SHOULD NOT NEED TO CHANGE ANYTHING BELOW THIS LINE
#

# buffer parameters
FAN_BUFFER_SIZE = 16
SHARED_BUFFER_SIZE = 4

# temporary directory for videos
TEMP_DIR = PROJECT_DIR / 'temp'

# url
URL = (
	'https://www.youtube.com/watch?v=WU4UxWaf8U8'
	'&list=PLIx-eqjsmIuCoHbep0iithAFB6U793VVA'
)

# source hires files
SOURCE_VIDEO_BASENAME = 'carter-skyers-easiest-goodbye-official-music-video'
SOURCE_VIDEO_FILE_PATH = PROJECT_DIR / 'video' / f"{SOURCE_VIDEO_BASENAME}.mp4"
SOURCE_AUDIO_FILE_PATH = PROJECT_DIR / 'video' / f"{SOURCE_VIDEO_BASENAME}.mp3"

# video epsilon
TIME_EPSILON = 0.0000001

# shards json file
SHARDS_DIR = PROJECT_DIR / 'video_shards'
SHARDS_JSON_FILE_PATH = SHARDS_DIR / 'shards.json'

# configure log output format
FORMAT = '[%(asctime)s:%(levelname)-8s] %(message)s'
logging.basicConfig(format=FORMAT)


# test short video files
TEST_VIDEO_BASENAME = 'carter-skyers-easiest-goodbye-official-music-video'
TEST_VIDEO_FILE_PATH = PROJECT_DIR / 'video_test' / f"{TEST_VIDEO_BASENAME}.mp4"
TEST_AUDIO_FILE_PATH = PROJECT_DIR / 'video_test' / f"{TEST_VIDEO_BASENAME}.mp3"

# Convenience string versions (some older code may expect str paths)
PROJECT_DIR_STR = str(PROJECT_DIR)
TEMP_DIR_STR = str(TEMP_DIR)
SOURCE_VIDEO_FILE_PATH_STR = str(SOURCE_VIDEO_FILE_PATH)
SOURCE_AUDIO_FILE_PATH_STR = str(SOURCE_AUDIO_FILE_PATH)
SHARDS_JSON_FILE_PATH_STR = str(SHARDS_JSON_FILE_PATH)
TEST_VIDEO_FILE_PATH_STR = str(TEST_VIDEO_FILE_PATH)
TEST_AUDIO_FILE_PATH_STR = str(TEST_AUDIO_FILE_PATH)

# -------------------------
# Audio/Playback settings
# -------------------------
# Start the audio track at this offset (in seconds) when composing the final video
AUDIO_OFFSET_SECONDS = 78
# Audio fade-in duration in seconds
AUDIO_FADE_IN_SECONDS = 1.0
# Audio fade-out duration in seconds (applied using areverse trick)
AUDIO_FADE_OUT_SECONDS = 2.3
# Audio bitrate for AAC encoding
AUDIO_BITRATE = '192k'
# Auto-play the final video on macOS after composition completes
AUTO_PLAY_FINAL_VIDEO = True
