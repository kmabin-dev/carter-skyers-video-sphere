import hashlib
import os
import platform
import sys
import time
from os.path import isfile, join

import config
import ffmpeg
import vlc
from config import logger


def end_reached_cb(event, params):
    logger.info('video end reached')
    params['finish'] = True


def ms_to_timecode(total_ms):
    hh = int(total_ms / (1000*60*60))
    balance_ms = total_ms - hh*1000*60*60
    mm = int(balance_ms / (1000*60))
    balance_ms = total_ms - hh*1000*60*60 - mm*1000*60
    ss = int(balance_ms / 1000)
    balance_ms = total_ms - hh*1000*60*60 - mm*1000*60 - ss * 1000
    if balance_ms < 0:
        miliseconds = 0
    else:
        miliseconds = balance_ms
    # hh:mm:ss:ff
    t = '%02d:%02d:%02d.%04d' % (hh, mm, ss, miliseconds)
    return t


def position_changed_cb(event, player):
    npos = event.u.new_position * 100
    time_ms = player.get_time()
    ms = ms_to_timecode(time_ms)
    sys.stdout.write('\r%s %s (%.2f%%)' % ('Position', ms, npos))
    sys.stdout.flush()


def write_audio(audio_file_path, input_file):
    (
        ffmpeg
        .output(input_file, audio_file_path, loglevel='quiet')
        .run(overwrite_output=True)
    )


def shake256_hash(s):
    m = hashlib.shake_256()
    m.update(str.encode(s))
    hash = m.hexdigest(8)
    return hash


def file_hash(file_path):

    try:
        file = open(file_path, 'rb')
    except Exception as e:
        logger.error(
            f'Unable to open {file_path} exception={type(e).__name__}')
        quit(-1)

    # read the data
    byte_data = file.read()

    # close file
    file.close()

    # convert to string
    s = byte_data.decode('latin-1')

    # calculate hash
    hash = shake256_hash(s)

    return hash


def dimensions(video_file_path):
    probe = ffmpeg.probe(video_file_path)
    probe_video = next(
        (stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
    width = int(probe_video['width'])
    height = int(probe_video['height'])
    return (width, height)


def probe(video_file_path):
    probe = ffmpeg.probe(video_file_path)
    probe_video = next(
        (stream for stream in probe['streams'] if stream['codec_type'] == 'video'), None)
    return probe_video


def temp_file_path(name, ext):
    '''
    creates a valid path to a temp file
    '''
    if not os.path.exists(config.TEMP_DIR):
        os.makedirs(config.TEMP_DIR)
    hash = shake256_hash(name + str(time.time_ns()))
    temp_file_name = 'temp_' + name + '_' + hash + ext
    temp_file_path = config.TEMP_DIR + '/' + temp_file_name
    return temp_file_path


def audio(name, input_video_file_path, input_audio_file_path):
    '''
    adds audio back into file
    '''
    print('Applying audio...')
    print(f'\tinput video file : {os.path.basename(input_video_file_path)}')
    print(f'\tinput audio file : {os.path.basename(input_audio_file_path)}')
    print(f'\tname             : {name}')
    print('\tstatus           : processing...', end='')
    video_file = ffmpeg.input(input_video_file_path)
    audio_file = ffmpeg.input(input_audio_file_path)
    output_file = temp_file_path(name, '.mp4')

    # process
    (
        ffmpeg
        # add audio back in to video
        .concat(video_file, audio_file, v=1, a=1)
        .output(output_file, loglevel='quiet')
        .run()
    )

    print('done')
    print(f'\toutput file: {os.path.basename(output_file)}')
    return output_file


def concat(name, *input_video_file_paths):
    '''
    concatenates videos
    '''
    n = len(input_video_file_paths)
    if n == 0:
        logger.error(
            'Unable to concat videose because no videos were passed to concat')
        return None
    print('Applying concat...')
    print(f'\tinput       : {n} videos')
    video_files = []
    for i in range(n):
        input_video_file_path = input_video_file_paths[i]
        if not os.path.exists(input_video_file_path):
            logger.error(f'Unable to access the file {input_video_file_path}')
            return None
        input = ffmpeg.input(input_video_file_path)
        video_files.append(input)
    print(f'\tname        : {name}')
    print('\tstatus      : processing...', end='')
    output_file = temp_file_path(name, '.mp4')

    # process
    (
        ffmpeg
        # add audio back in to video
        .concat(*video_files)
        .output(output_file, loglevel='quiet')
        .run()
    )

    print('done')
    print(f'\toutput file : {os.path.basename(output_file)}')
    return output_file


def play(file_path):

    # creating a vlc instance
    vlc_instance = vlc.Instance()

    # creating a media player
    player = vlc_instance.media_player_new()

    # creating a media, prepend C: if on windows
    if platform.system() == 'Windows':
        file_path = 'C:' + file_path
    media = vlc_instance.media_new(file_path)

    # setting media to the player
    player.set_media(media)

    params = {
        'finish': False
    }

    # callbacks
    events = player.event_manager()
    events.event_attach(vlc.EventType.MediaPlayerEndReached,
                        end_reached_cb, params)
    events.event_attach(
        vlc.EventType.MediaPlayerPositionChanged, position_changed_cb, player)

    # play until finished, if not testing
    if 'PYTEST_CURRENT_TEST' not in os.environ:
        player.play()
        while not params['finish']:
            time.sleep(0.5)

        # getting the duration of the video
        duration = player.get_length()

        # printing the duration of the video
        logger.info('Duration : ' + ms_to_timecode(duration))


def clean_temp_directory():

    if os.path.exists(config.TEMP_DIR):
        for temp_file in os.listdir(config.TEMP_DIR):
            if isfile(join(config.TEMP_DIR, temp_file)):
                os.remove(config.TEMP_DIR + '/' + temp_file)


def create_shard(input_file_path, output_file_path, start, end):

    # get input video for ffmpeg
    input_file = ffmpeg.input(input_file_path)

    # create output dir if needed
    dir = os.path.dirname(output_file_path)
    if not os.path.exists(dir):
        os.mkdir(dir)

    # trim video
    start_time = ms_to_timecode(start*1000)
    end_time = ms_to_timecode(end*1000)

    logger.info(f'writing {output_file_path}')
    (
        ffmpeg
        # trim
        .trim(input_file, start=start_time, end=end_time)
        # reset start time code of video to 0
        .setpts('PTS-STARTPTS')
        # output the file
        .output(output_file_path, loglevel='quiet')
        .run(overwrite_output=True)
    )


def write(name, shard_data):
    '''
    writes the shard to disk as a mp4 video file
    '''
    output_file_path = temp_file_path(name, '.mp4')

    try:
        file = open(output_file_path, 'wb')
    except Exception as e:
        logger.error(
            f'Unable to open {output_file_path} exception={type(e).__name__}')
        quit(-1)

    # write the data
    file.write(shard_data)

    # close file
    file.close()

    return output_file_path
