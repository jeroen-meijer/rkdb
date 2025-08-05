#!/usr/bin/env python3
"""
Debug script for playlist cover upload issues.
"""

import yaml
import os
import sys
from typing import Dict, Any, Optional
from image_generator import generate_playlist_cover, upload_playlist_image, apply_template_variables
from commands.crawl import generate_playlist_name
import datetime


def load_config(config_path: str = "crawl_config.yaml") -> Dict[str, Any]:
  """Load configuration from YAML file."""
  try:
    with open(config_path, 'r') as file:
      return yaml.safe_load(file)
  except Exception as e:
    print(f"❌ Error loading config: {e}")
    return {}


def find_job_by_name(job_name: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
  """Find a job by name in the configuration."""
  for job in config.get('jobs', []):
    if job.get('name') == job_name:
      return job
  return None


def test_cover_upload(job_name: str, playlist_id: str):
  """Test cover generation and upload for debugging."""
  print(f"🔍 Testing cover upload for job: {job_name}")
  print(f"🎯 Target playlist ID: {playlist_id}")

  # Load configuration
  config = load_config()
  if not config:
    print("❌ Failed to load configuration")
    return False

  # Find the job
  job = find_job_by_name(job_name, config)
  if not job:
    print(f"❌ Job '{job_name}' not found in configuration")
    return False

  # Check if job has cover configuration
  cover_config = job.get('cover')
  if not cover_config:
    print(f"❌ Job '{job_name}' has no cover configuration")
    return False

  print(f"✅ Found job: {job_name}")
  print(f"📝 Cover config: {cover_config}")

  # Generate a sample playlist name for testing
  now = datetime.datetime.now()
  cutoff_date = now - datetime.timedelta(days=7)

  playlist_name = generate_playlist_name(
    job.get('output_playlist', {}).get('name', 'Test Playlist'),
    job,
    cutoff_date,
    42,  # Sample track count
    []   # Empty track list
  )

  # Get caption from config or use playlist name
  caption = cover_config.get('caption', playlist_name)

  # Apply template variables to caption
  caption = apply_template_variables(caption, job, playlist_name)

  print(f"📝 Playlist name: {playlist_name}")
  print(f"📝 Caption: {caption}")
  print(f"🖼️  Image: {cover_config.get('image')}")

  # Generate cover image
  output_filename = f"debug_{job_name}_cover.png"

  print(f"🎨 Generating cover image...")
  generated_image = generate_playlist_cover(
    image_path=cover_config.get('image'),
    caption=caption,
    output_path=output_filename
  )

  if generated_image is None:
    print("❌ Failed to generate cover image")
    return False

  # The generate_playlist_cover function converts .png to .jpg for Spotify
  jpeg_filename = output_filename.replace('.png', '.jpg')

  print(f"✅ Generated cover image: {jpeg_filename}")
  print(f"📏 Image size: {generated_image.size}")

  # Check if JPEG file exists and get its size
  if os.path.exists(jpeg_filename):
    file_size = os.path.getsize(jpeg_filename)
    print(f"📁 File size: {file_size} bytes")

    # Check file format
    try:
      from PIL import Image
      with Image.open(jpeg_filename) as img:
        print(f"🖼️  Image format: {img.format}")
        print(f"🖼️  Image mode: {img.mode}")
        print(f"🖼️  Image size: {img.size}")
    except Exception as e:
      print(f"⚠️  Warning: Could not read image info: {e}")
  else:
    print("❌ Generated JPEG file does not exist")
    return False

  # Now test the upload
  print(f"\n🚀 Testing upload to Spotify...")

  # Import Spotify client
  try:
    from services import setup_spotify, get_user_or_sign_in
    sp = setup_spotify()
    user = get_user_or_sign_in(sp)
    print(f"👤 Logged in as: {user.get('display_name', 'Unknown')}")
  except Exception as e:
    print(f"❌ Error getting Spotify client: {e}")
    return False

    # Test upload with more detailed error handling
  print("🔍 Testing upload with detailed error handling...")
  try:
    # Read JPEG image file
    with open(jpeg_filename, 'rb') as f:
      image_data = f.read()
      print(f"📊 Image data size: {len(image_data)} bytes")

      # Check if it's a valid JPEG
      if image_data[:2] == b'\xff\xd8':
        print("✅ File appears to be a valid JPEG")
      else:
        print("❌ File does not appear to be a valid JPEG")

      # Try to get more info about the playlist
      try:
        playlist_info = sp.playlist(playlist_id, fields='name,id,owner')
        print(f"📋 Playlist info: {playlist_info}")
      except Exception as e:
        print(f"⚠️  Could not get playlist info: {e}")

      # Try the upload with more detailed error handling
      print("🚀 Attempting upload...")
      try:
        # Try with base64 encoding (Spotify API requirement)
        import base64
        base64_image = base64.b64encode(image_data).decode('utf-8')
        print(f"📊 Base64 encoded size: {len(base64_image)} characters")

        sp.playlist_upload_cover_image(playlist_id, base64_image)
        print("✅ Upload successful!")
        return True
      except Exception as e:
        print(f"❌ Upload failed with error: {e}")
        print(f"❌ Error type: {type(e).__name__}")

        # Try to get more details about the error
        if hasattr(e, 'http_status'):
          print(f"❌ HTTP status: {e.http_status}")
        if hasattr(e, 'code'):
          print(f"❌ Error code: {e.code}")
        if hasattr(e, 'reason'):
          print(f"❌ Reason: {e.reason}")

        return False

  except Exception as e:
    print(f"⚠️  Error reading file: {e}")
    return False

  return success


if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("❌ Usage: python debug_cover_upload.py <job_name> <playlist_id>")
    print("Example: python debug_cover_upload.py liquid_weekly 28jpG9iTPAvTT9mUWFO3OS")
    sys.exit(1)

  job_name = sys.argv[1]
  playlist_id = sys.argv[2]

  success = test_cover_upload(job_name, playlist_id)
  sys.exit(0 if success else 1)
