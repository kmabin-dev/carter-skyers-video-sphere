import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def git_check_ignore(paths):
    # run git check-ignore -v for provided paths, return set of ignored paths
    cmd = ["git", "check-ignore", "-v"] + [str(p) for p in paths]
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    out = proc.stdout.strip()
    if not out:
        return set()
    ignored = set()
    for line in out.splitlines():
        # git check-ignore -v output format is: <pattern>\t<source>\t<path>
        # split on tab to preserve paths that contain spaces; fallback to whitespace
        if '\t' in line:
            parts = line.split('\t')
            path_part = parts[-1]
        else:
            parts = line.split()
            path_part = parts[-1]
        # Normalize to absolute path
        p = Path(path_part)
        if not p.is_absolute():
            p = REPO_ROOT / p
        ignored.add(str(p))
    return ignored


class TestGitIgnore(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(dir=REPO_ROOT)
        self.temp_path = Path(self.tempdir.name)

    def tearDown(self):
        self.tempdir.cleanup()

    def touch(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
        return path

    def test_media_and_dirs_ignored(self):
        # create sample files and directories
        samples = [
            REPO_ROOT / 'video' / 'sample.mp4',
            REPO_ROOT / 'video_test' / 'sample.mp3',
            REPO_ROOT / 'video_shards' / 'shards.json',
            REPO_ROOT / 'temp' / 'foo.tmp',
            REPO_ROOT / 'nested' / 'movie.mkv',
        ]
        for p in samples:
            self.touch(p)

        ignored = git_check_ignore(samples)
        # Assert each created sample is ignored
        for p in samples:
            self.assertIn(str(p), ignored, f"{p} should be ignored by .gitignore")

    def test_tracked_file_not_ignored(self):
        # README.md should not be ignored
        readme = REPO_ROOT / 'README.md'
        ignored = git_check_ignore([readme])
        self.assertNotIn(str(readme), ignored, "README.md should not be ignored")


if __name__ == '__main__':
    unittest.main()
