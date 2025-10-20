import os

import youtube

import pytest

# test constants
URL = 'https://www.youtube.com/watch?v=WU4UxWaf8U8&list=PLIx-eqjsmIuCoHbep0iithAFB6U793VVA'
URL_INVALID = 'https://www.youtube.com/watch?bogus'
URL_INVALID_VIDEOID = 'https://www.youtube.com/watch?v=does-not-exist'


class DVideo:
    def __init__(self, s=''):
        self.default_filename = s


def test_video_file_name():
    d_video = DVideo('AbcdE ABDe  - AKDEP')
    res = youtube.video_file_name(d_video)
    assert res == 'abcde-abde-akdep'


def test_empty_video_file_name():
    d_video = DVideo()
    res = youtube.video_file_name(d_video)
    assert res == ''


def test_download():
    yt = youtube.download(URL)
    assert yt


def test_get_video():
    yt = youtube.download(URL)
    assert yt
    d_video = youtube.get_video(yt)
    assert d_video


def test_invalid_url():
    with pytest.raises(Exception):
        youtube.download(URL_INVALID)


def test_invalid_url_videoid():
    with pytest.raises(Exception):
        yt = youtube.download(URL_INVALID_VIDEOID)
        assert not yt


def test_write_video():
    yt = youtube.download(URL)
    assert yt
    d_video = youtube.get_video(yt)
    tmp_dir = '.'
    tmp_file_name = d_video.default_filename
    youtube.write_video(d_video, tmp_dir, tmp_file_name)
    video_path = tmp_dir + '/' + tmp_file_name
    exists = os.path.exists(video_path)
    if exists:
        os.remove(video_path)
    assert exists


def test_invalid_path_write_video():
    yt = youtube.download(URL)
    assert yt
    d_video = youtube.get_video(yt)
    bogus_dir = 'Q:/bogus/path'
    tmp_file_name = d_video.default_filename
    with pytest.raises(Exception):
        youtube.write_video(d_video, bogus_dir, tmp_file_name)
