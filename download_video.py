import os
import subprocess
from pathlib import Path
from example.config import (SOURCE_VIDEO_FILE_PATH, SOURCE_AUDIO_FILE_PATH, URL)


def _download_with_pytube(url, video_dst: Path, audio_dst: Path):
    try:
        from pytube import YouTube
    except Exception:
        raise

    yt = YouTube(url)
    # try progressive first (contains audio)
    prog = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
    if prog:
        out_dir = str(video_dst.parent)
        out_name = str(video_dst.name)
        prog.download(output_path=out_dir, filename=out_name)
        return True

    # fallback to adaptive video + audio
    vid = yt.streams.filter(adaptive=True, only_video=True, file_extension='mp4').order_by('resolution').desc().first()
    aud = yt.streams.filter(only_audio=True).order_by('abr').desc().first()
    if vid:
        vid_out = video_dst.with_suffix('.video.mp4')
        vid.download(output_path=str(vid_out.parent), filename=str(vid_out.name))
    if aud:
        aud_ext = '.mp4' if 'mp4' in aud.mime_type else '.webm'
        aud_out = audio_dst.with_suffix(f'.audio{aud_ext}')
        aud.download(output_path=str(aud_out.parent), filename=str(aud_out.name))

    return True


def _download_with_pytubefix(url, video_dst: Path, audio_dst: Path):
    try:
        import pytubefix
        from pytubefix import YouTube
    except Exception:
        raise

    yt = YouTube(url)
    # reuse same logic as pytube
    prog = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
    if prog:
        prog.download(output_path=str(video_dst.parent), filename=str(video_dst.name))
        return True
    vid = yt.streams.filter(adaptive=True, only_video=True, file_extension='mp4').order_by('resolution').desc().first()
    aud = yt.streams.filter(only_audio=True).order_by('abr').desc().first()
    if vid:
        vid_out = video_dst.with_suffix('.video.mp4')
        vid.download(output_path=str(vid_out.parent), filename=str(vid_out.name))
    if aud:
        aud_ext = '.mp4' if 'mp4' in aud.mime_type else '.webm'
        aud_out = audio_dst.with_suffix(f'.audio{aud_ext}')
        aud.download(output_path=str(aud_out.parent), filename=str(aud_out.name))
    return True


def _download_with_ytdlp(url, video_dst: Path, audio_dst: Path):
    # requires yt-dlp installed on PATH; best-effort fallback
    cmd = [
        'yt-dlp',
        '-f', "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/b",
        '-o', str(video_dst),
        url,
    ]
    subprocess.check_call(cmd)
    return True


def download_video(url=URL):
    video_dst = Path(SOURCE_VIDEO_FILE_PATH)
    audio_dst = Path(SOURCE_AUDIO_FILE_PATH)
    video_dst.parent.mkdir(parents=True, exist_ok=True)
    audio_dst.parent.mkdir(parents=True, exist_ok=True)

    # try pytube first, then pytubefix, then yt-dlp
    try:
        print('Trying pytube...')
        return _download_with_pytube(url, video_dst, audio_dst)
    except Exception as e:
        print('pytube failed:', e)

    try:
        print('Trying pytubefix...')
        return _download_with_pytubefix(url, video_dst, audio_dst)
    except Exception as e:
        print('pytubefix failed:', e)

    try:
        print('Trying yt-dlp...')
        return _download_with_ytdlp(url, video_dst, audio_dst)
    except Exception as e:
        print('yt-dlp failed:', e)

    print('All download methods failed')
    return False


if __name__ == '__main__':
    ok = download_video()
    if ok:
        print('Download completed successfully!')
    else:
        print('Download failed!')