import sys
import atexit
import datetime
import argparse

from commands.search import search_rekordbox_tracks
from commands.sync import sync_spotify_playlists_to_rekordbox
from commands.buy import buy_tracks
from commands.crawl import crawl_spotify_playlists
from commands.extract import extract_playlist_data
from commands.test_cover import test_cover_generation
from cache import CrawlCache
from services import setup_rekordbox


def parse_arguments():
  """Parse command line arguments using argparse."""
  parser = argparse.ArgumentParser(
    description='rkdb - Rekordbox and Spotify management tool',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  python3 main.py crawl                              # Run crawl with current date
  python3 main.py crawl --end-date 2025-08-10       # Run crawl as if today is 2025-08-10
  python3 main.py crawl --start-date 2025-01-01 --end-date 2025-01-03  # Custom date range
  python3 main.py sync playlist_id         # Sync specific playlist
  python3 main.py search                   # Search Rekordbox tracks
  python3 main.py buy                      # Buy tracks
  python3 main.py extract playlist_id      # Extract playlist data
  python3 main.py test-cover job_name     # Test cover generation
  python3 main.py cache-stats             # Show cache statistics
  python3 main.py cache-clear             # Clear cache
    """
  )

  # Create subparsers for each command
  subparsers = parser.add_subparsers(dest='command', help='Available commands')

  # Crawl command with date options
  crawl_parser = subparsers.add_parser(
    'crawl', help='Crawl Spotify playlists, artists, and labels')
  crawl_parser.add_argument('--config',
                            type=str,
                            metavar='PATH',
                            help='Path to YAML config to use instead of crawl_config.yaml')
  crawl_parser.add_argument('--end-date',
                            type=str,
                            metavar='YYYY-MM-DD',
                            help='Custom end date for crawl operations (format: YYYY-MM-DD)')
  crawl_parser.add_argument('--start-date',
                            type=str,
                            metavar='YYYY-MM-DD',
                            help='Custom start date for crawl operations, overrides job config days_back (format: YYYY-MM-DD)')

  # Sync command
  sync_parser = subparsers.add_parser(
    'sync', help='Sync Spotify playlists to Rekordbox')
  sync_parser.add_argument('playlist_ids', nargs='*',
                           help='Playlist IDs to sync')

  # Search command
  subparsers.add_parser('search', help='Search Rekordbox tracks')

  # Buy command
  subparsers.add_parser('buy', help='Buy tracks')

  # Extract command
  extract_parser = subparsers.add_parser(
    'extract', help='Extract playlist data')
  extract_parser.add_argument('playlist_id', help='Playlist ID to extract')

  # Test cover command
  test_cover_parser = subparsers.add_parser(
    'test-cover', help='Test cover generation')
  test_cover_parser.add_argument('job_name', help='Job name to test')

  # Cache commands
  subparsers.add_parser('cache-stats', help='Show cache statistics')
  subparsers.add_parser('cache-clear', help='Clear cache')

  return parser.parse_args()


def validate_date(date_str):
  """Validate and parse date string."""
  if not date_str:
    return None

  try:
    return datetime.datetime.strptime(date_str, '%Y-%m-%d')
  except ValueError:
    raise argparse.ArgumentTypeError(
      f"Invalid date format '{date_str}'. Use YYYY-MM-DD format (e.g., 2025-08-10)")


def main():
  print("--- rkdb ---")

  # Parse arguments
  try:
    parsed_args = parse_arguments()
  except SystemExit:
    sys.exit(1)

  # Check if no command was provided
  if not parsed_args.command:
    print("❌ No command provided. Use --help for usage information.")
    sys.exit(1)

  command = parsed_args.command

  # Validate and parse dates if provided
  custom_end_date = None
  custom_start_date = None
  if hasattr(parsed_args, 'end_date') and getattr(parsed_args, 'end_date', None):
    try:
      custom_end_date = validate_date(parsed_args.end_date)
    except argparse.ArgumentTypeError as e:
      print(f"❌ Error: {e}")
      sys.exit(1)

  if hasattr(parsed_args, 'start_date') and getattr(parsed_args, 'start_date', None):
    try:
      custom_start_date = validate_date(parsed_args.start_date)
    except argparse.ArgumentTypeError as e:
      print(f"❌ Error: {e}")
      sys.exit(1)

  # Validate that start_date is before end_date if both are provided
  if custom_start_date and custom_end_date and custom_start_date >= custom_end_date:
    print("❌ Error: start-date must be before end-date")
    sys.exit(1)

  # Initialize cache early to set up signal handlers
  cache = None
  if command in ['crawl', 'cache-clear', 'cache-stats']:
    cache = CrawlCache()

  command_map = {
    'sync': lambda: sync_spotify_playlists_to_rekordbox(parsed_args.playlist_ids if hasattr(parsed_args, 'playlist_ids') else []),
    'search': lambda: search_rekordbox_tracks(),
    'buy': lambda: buy_tracks(),
    'crawl': lambda: crawl_spotify_playlists(cache, custom_end_date, custom_start_date, getattr(parsed_args, 'config', None)),
    'extract': lambda: extract_playlist_data(parsed_args.playlist_id) if hasattr(parsed_args, 'playlist_id') else print("❌ Please provide a playlist ID"),
    'test-cover': lambda: test_cover_generation(parsed_args.job_name) if hasattr(parsed_args, 'job_name') else print("❌ Please provide a job name"),
    'cache-clear': lambda: cache.clear_cache() if cache else CrawlCache().clear_cache(),
    'cache-stats': lambda: cache.print_cache_stats() if cache else CrawlCache().print_cache_stats(),
  }

  try:
    if command in command_map:
      command_map[command]()
    else:
      raise ValueError(f"Command '{command}' not found")
  except KeyboardInterrupt:
    print("\n⚠️  Interrupted by user (Ctrl+C)")
    if cache:
      print("💾 Saving cache before exit...")
      cache.force_save()
    sys.exit(0)
  except Exception as e:
    print(f"❌ Error: {e}")
    if cache:
      print("💾 Saving cache before exit...")
      cache.force_save()
    sys.exit(1)

  print("Exiting")
  sys.exit(0)


if __name__ == '__main__':
  main()
