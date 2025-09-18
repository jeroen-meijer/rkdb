import yaml
import sys
from typing import Dict, Any, Optional
from image_generator import generate_playlist_cover, apply_template_variables


def load_config(config_path: str = "crawl_config.yaml") -> Dict[str, Any]:
  """Load the crawl configuration file."""
  try:
    with open(config_path, 'r', encoding='utf-8') as f:
      return yaml.safe_load(f)
  except Exception as e:
    print(f"❌ Error loading config file '{config_path}': {e}")
    return {}


def find_job_by_name(jobs: list, job_name: str) -> Optional[Dict[str, Any]]:
  """Find a job by name in the jobs list."""
  for job in jobs:
    if job.get('name') == job_name:
      return job
  return None


def test_cover_generation(job_name: str, config_path: str = "crawl_config.yaml"):
  """
  Test cover generation for a specific job.

  Args:
      job_name: Name of the job to test
      config_path: Path to the configuration file
  """
  print(f"🎨 Testing cover generation for job: {job_name}")

  # Load configuration
  config = load_config(config_path)
  if not config:
    return

  jobs = config.get('jobs', [])
  if not jobs:
    print("❌ No jobs found in configuration")
    return

  # Find the specified job
  job = find_job_by_name(jobs, job_name)
  if not job:
    print(f"❌ Job '{job_name}' not found in configuration")
    print(f"Available jobs: {[j.get('name', 'unnamed') for j in jobs]}")
    return

  # Check if job has cover configuration
  cover_config = job.get('cover')
  if not cover_config:
    print(f"⚠️  Job '{job_name}' does not have a 'cover' field")
    return

  image_path = cover_config.get('image')
  if not image_path:
    print(f"⚠️  Job '{job_name}' cover config missing 'image' field")
    return

  # Generate a sample playlist name for testing
  from commands.crawl import generate_playlist_name
  import datetime

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
  caption = apply_template_variables(
    caption, job, playlist_name, cutoff_date, now)

  print(f"📝 Playlist name: {playlist_name}")
  print(f"📝 Caption: {caption}")
  print(f"🖼️  Image: {image_path}")

  # Generate cover image
  output_filename = f"test_{job_name}_cover.png"

  generated_image = generate_playlist_cover(
      image_path=image_path,
      caption=caption,
      output_path=output_filename
  )

  if generated_image:
    print(f"✅ Cover generated successfully: {output_filename}")
    print(f"📏 Image size: {generated_image.size}")
  else:
    print(f"❌ Failed to generate cover image")


def main():
  """Main function for the test_cover command."""
  args = sys.argv[1:]

  if len(args) == 0:
    print("❌ Please provide a job name")
    print("Usage: python -m src.commands.test_cover <job_name> [config_path]")
    return

  job_name = args[0]
  config_path = args[1] if len(args) > 1 else "crawl_config.yaml"

  test_cover_generation(job_name, config_path)


if __name__ == "__main__":
  main()
