from pathlib import Path
import sys

# Ensure example/ is on sys.path to import local module
example_dir = Path(__file__).resolve().parents[1] / "example"
if str(example_dir) not in sys.path:
    sys.path.insert(0, str(example_dir))

import youtube  # noqa: E402


class _Dummy:
    def __init__(self, default_filename: str):
        self.default_filename = default_filename


def test_filename_normalizes_spaces_parens_and_case():
    d = _Dummy("My Video (Official)")
    name = youtube.video_file_name(d)
    assert name == "my-video-official"


def test_filename_dedups_hyphens():
    d = _Dummy("A   ---   B (Clip)")
    # Spaces and parens removed, hyphens deduped
    name = youtube.video_file_name(d)
    assert name == "a-b-clip"
