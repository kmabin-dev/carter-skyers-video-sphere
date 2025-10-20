# carter-skyers-video-sphere

## Development Setup

1. Clone this repository
2. Install Python dependencies:
   ```bash
   python -m pip install python-vlc pytube pytubefix ffmpeg-python Faker
   ```
3. Install ffmpeg (required for video processing):
   - macOS: `brew install ffmpeg`
   - Ubuntu: `sudo apt-get install ffmpeg`
   - Windows: Download from [ffmpeg.org](https://ffmpeg.org/download.html)

## Running Tests

Tests can be run using Python's unittest:

```bash
python -m unittest discover -v
```

## Continuous Integration

GitHub Actions automatically runs tests on:
- Every push to main branch
- Every pull request to main branch

The CI workflow:
1. Sets up Python 3.13
2. Installs dependencies
3. Creates required project directories
4. Runs all tests

Test status can be viewed in the GitHub Actions tab.
