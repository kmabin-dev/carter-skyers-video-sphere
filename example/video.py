import hashlib
import os
import platform
import sys
import time
import json
import subprocess
from os.path import isfile, join

import config
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


def write_audio(_audio_file_path, _input_file):
    """Deprecated: retained for compatibility (no-op)."""
    return


def shake256_hash(s):
    m = hashlib.shake_256()
    m.update(str.encode(s))
    digest = m.hexdigest(8)
    return digest


def file_hash(file_path):

    try:
        file = open(file_path, 'rb')
    except OSError as e:
        logger.error('Unable to open %s exception=%s', file_path, type(e).__name__)
        quit(-1)

    # read the data
    byte_data = file.read()

    # close file
    file.close()

    # convert to string
    s = byte_data.decode('latin-1')

    # calculate hash
    digest = shake256_hash(s)

    return digest


def dimensions(video_file_path):
    info = probe(video_file_path)
    width = int(info.get('width'))
    height = int(info.get('height'))
    return (width, height)


def probe(video_file_path):
    """Return first video stream info via ffprobe JSON output."""
    cmd = [
        'ffprobe',
        '-v', 'error',
        '-print_format', 'json',
        '-show_format',
        '-show_streams',
        str(video_file_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f'ffprobe failed: {proc.stderr}')
    data = json.loads(proc.stdout)
    streams = data.get('streams', [])
    video_stream = next((s for s in streams if s.get('codec_type') == 'video'), None)
    if not video_stream:
        raise RuntimeError('No video stream found')
    return video_stream


def temp_file_path(name, ext):
    '''
    creates a valid path to a temp file
    '''
    temp_dir = str(config.TEMP_DIR)
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    digest = shake256_hash(name + str(time.time_ns()))
    temp_file_name = 'temp_' + name + '_' + digest + ext
    return os.path.join(temp_dir, temp_file_name)


def audio(name, input_video_file_path, input_audio_file_path):
    '''
    adds audio back into file
    '''
    print('Applying audio...')
    print(f'\tinput video file : {os.path.basename(input_video_file_path)}')
    print(f'\tinput audio file : {os.path.basename(input_audio_file_path)}')
    print(f'\tname             : {name}')
    print('\tstatus           : processing...', end='')
    output_file = temp_file_path(name, '.mp4')
    cmd = [
        'ffmpeg', '-y',
        '-i', str(input_video_file_path),
        '-i', str(input_audio_file_path),
        '-c:v', 'copy',
        '-c:a', 'aac', '-b:a', str(getattr(config, 'AUDIO_BITRATE', '192k')),
        '-shortest',
        '-movflags', '+faststart',
        output_file,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        logger.error('ffmpeg audio mux failed: %s', proc.stderr)
        return None

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
    for i in range(n):
        input_video_file_path = input_video_file_paths[i]
        if not os.path.exists(str(input_video_file_path)):
            # Warn but continue; ffmpeg will fail if inputs are truly missing
            logger.warning('Input file not found (continuing): %s', input_video_file_path)
    print(f'\tname        : {name}')
    print('\tstatus      : processing...', end='')
    output_file = temp_file_path(name, '.mp4')
    # Build a concat list file for demuxer
    temp_dir = str(config.TEMP_DIR)
    list_path = os.path.join(temp_dir, f'concat_{shake256_hash(name)}.txt')
    with open(list_path, 'w', encoding='utf-8') as fh:
        for p in input_video_file_paths:
            p_str = str(p)
            fh.write(f"file '{p_str.replace("'", "'\\''")}'\n")
    cmd = [
        'ffmpeg', '-y',
        '-f', 'concat', '-safe', '0', '-i', list_path,
        '-c', 'copy',
        output_file,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    try:
        os.remove(list_path)
    except OSError:
        pass
    if proc.returncode != 0:
        logger.error('ffmpeg concat failed: %s', proc.stderr)
        return None

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
    duration_ms = player.get_length()

    # printing the duration of the video
    logger.info('Duration : %s', ms_to_timecode(duration_ms))


def clean_temp_directory():
    # config.TEMP_DIR may be a Path object; normalize to string
    temp_dir = str(config.TEMP_DIR)
    if os.path.exists(temp_dir):
        for temp_file in os.listdir(temp_dir):
            if isfile(join(temp_dir, temp_file)):
                os.remove(os.path.join(temp_dir, temp_file))


def create_shard(input_file_path, output_file_path, start, end):
    # create output dir if needed
    dir_name = os.path.dirname(output_file_path)
    if dir_name and not os.path.exists(dir_name):
        os.makedirs(dir_name, exist_ok=True)

    # trim with copy
    start_s = float(start)
    end_s = float(end)
    duration = max(0.0, end_s - start_s)
    logger.info('writing %s', output_file_path)
    cmd = [
        'ffmpeg', '-y',
        '-ss', str(start_s),
        '-t', str(duration),
        '-i', str(input_file_path),
        '-c', 'copy',
        output_file_path,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        logger.error('ffmpeg trim failed: %s', proc.stderr)
        return None


def write(name, shard_data):
    '''
    writes the shard to disk as a mp4 video file
    '''
    output_file_path = temp_file_path(name, '.mp4')

    try:
        file = open(output_file_path, 'wb')
    except OSError as e:
        logger.error('Unable to open %s exception=%s', output_file_path, type(e).__name__)
        quit(-1)

    # write the data
    file.write(shard_data)

    # close file
    file.close()

    return output_file_path
