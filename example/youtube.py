"""YouTube helper utilities for downloading and normalizing video
filenames.

Wraps pytubefix to fetch a video, pick a highest-resolution progressive
stream, and download it with a sanitized disk-friendly file name.
"""

import pytubefix
from config import logger


def video_file_name(d_video):
    '''
    simplify video file name for writing to disk
    '''
    fname = d_video.default_filename
    n = len(fname)
    if n == 0:
        return ''

    # replace chars
    fname = (
        fname
        .replace(' ', '-')
        .replace('(', '')
        .replace(')', '')
        .lower()
    )
    # remove duplicate '-' chars
    s = fname[0]
    for i in range(1, len(fname)):
        # if not a dup -, append to s
        if fname[i-1] != '-' or fname[i] != '-':
            s += fname[i]

    return s


def download(url):

    # download video
    logger.info('downloading video for %s', url)
    try:
        yt = pytubefix.YouTube(url)
    except Exception as e:
        logger.warning(
            'WARNING: Exception raised while getting video for %s exception=%s',
            url,
            type(e).__name__,
        )
        raise

    return yt


def get_video(yt):
    # get streams w/ video and audio
    mp4_streams = yt.streams.filter(progressive=True)

    # get highest resolution
    d_video = mp4_streams[-1]

    return d_video


def write_video(d_video, video_dir, file_name):

    # downloading the video
    try:
        d_video.download(output_path=video_dir, filename=file_name)
    except Exception as e:
        logger.warning(
            'WARNING: Unable to write video for %s exception=%s',
            video_dir,
            type(e).__name__,
        )
        raise
