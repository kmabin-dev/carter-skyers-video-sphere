import os
from pytube import YouTube
from example.config import (SOURCE_VIDEO_FILE_PATH, SOURCE_AUDIO_FILE_PATH, URL)

def download_video():
    try:
        # Extract video ID from URL and construct clean URL
        video_id = "WU4UxWaf8U8"  # Extracted from the playlist URL
        clean_url = f"https://www.youtube.com/watch?v={video_id}"
        print(f"Initializing download from {clean_url}...")
        
        yt = YouTube(clean_url)
        
        # Create video directory if it doesn't exist
        os.makedirs(os.path.dirname(SOURCE_VIDEO_FILE_PATH), exist_ok=True)
        
        # Download highest quality MP4
        print("Downloading video stream...")
        video = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
        if not video:
            print("Error: No suitable video stream found")
            return False
        print(f"Found video stream: {video.resolution}")
        video.download(filename=SOURCE_VIDEO_FILE_PATH)
        print(f"Video saved to: {SOURCE_VIDEO_FILE_PATH}")
        
        # Download audio
        print("\nDownloading audio stream...")
        audio = yt.streams.filter(only_audio=True).first()
        if not audio:
            print("Error: No audio stream found")
            return False
        audio.download(filename=SOURCE_AUDIO_FILE_PATH)
        print(f"Audio saved to: {SOURCE_AUDIO_FILE_PATH}")
        
        return True
        
    except Exception as e:
        print(f"Error downloading: {str(e)}")
        return False

if __name__ == '__main__':
    if download_video():
        print("Download completed successfully!")
    else:
        print("Download failed!")