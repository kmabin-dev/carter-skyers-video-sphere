import pytubefix
from config import logger


def video_file_name(d_video):
    '''
    simplify video file name for writing to disk
    '''
    video_file_name = d_video.default_filename
    n = len(video_file_name)
    if n == 0:
        return ''

    # replace chars
    video_file_name = video_file_name \
        .replace(' ', '-') \
        .replace('(', '') \
        .replace(')', '') \
        .lower()
    # remove duplicate '-' chars
    s = video_file_name[0]
    for i in range(1, len(video_file_name)):
        # if not a dup -, append to s
        if video_file_name[i-1] != '-' or video_file_name[i] != '-':
            s += video_file_name[i]

    return s


def download(url):

    # download video
    logger.info(f'downloading video for {url}')
    try:
        yt = pytubefix.YouTube(url)
    except Exception as e:
        logger.warning(
            f'WARNING: Exception raised while getting video for {url} exception={type(e).__name__}')
        raise e

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
            f'WARNING: Unable to write video for {video_dir} exception={type(e).__name__}')
        raise e
