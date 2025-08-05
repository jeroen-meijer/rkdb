import sys
import atexit

from commands.search import search_rekordbox_tracks
from commands.sync import sync_spotify_playlists_to_rekordbox
from commands.buy import buy_tracks
from commands.crawl import crawl_spotify_playlists
from commands.extract import extract_playlist_data
from commands.test_cover import test_cover_generation
from cache import CrawlCache


def main():
  print("--- rkdb ---")
  args = sys.argv[1:]
  if len(args) == 0:
    print("No arguments provided. Exiting.")
    sys.exit(1)

  command = args[0]
  command_args = args[1:]

  # Initialize cache early to set up signal handlers
  cache = None
  if command in ['crawl', 'cache-clear', 'cache-stats']:
    cache = CrawlCache()

  command_map = {
    'sync': lambda playlist_ids: sync_spotify_playlists_to_rekordbox(playlist_ids),
    'search': lambda _: search_rekordbox_tracks(),
    'buy': lambda _: buy_tracks(),
    'crawl': lambda _: crawl_spotify_playlists(cache),
    'extract': lambda playlist_ids: extract_playlist_data(playlist_ids[0]) if playlist_ids else print("❌ Please provide a playlist ID"),
    'test-cover': lambda args: test_cover_generation(args[0]) if args else print("❌ Please provide a job name"),
    'cache-clear': lambda _: cache.clear_cache() if cache else CrawlCache().clear_cache(),
    'cache-stats': lambda _: cache.print_cache_stats() if cache else CrawlCache().print_cache_stats(),
  }

  try:
    if command in command_map:
      command_map[command](command_args)
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
