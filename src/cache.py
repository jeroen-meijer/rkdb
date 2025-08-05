import yaml
import datetime
import os
import time
import signal
import atexit
from typing import Dict, Any, Optional, List
from constants import CRAWL_CACHE_FILE_NAME


class CrawlCache:
  """
  Cache system for the crawl command to reduce API calls.
  Caches albums and playlists with snapshot ID support.
  """

  def __init__(self, cache_file: str = CRAWL_CACHE_FILE_NAME):
    self.cache_file = cache_file
    self.cache = self._load_cache()
    self.last_api_call = 0
    self.min_call_interval = 0.1  # 100ms between calls to respect rate limits
    self._dirty = False  # Track if cache has been modified

    # Set up signal handlers and exit handlers to preserve cache
    self._setup_signal_handlers()
    atexit.register(self._save_cache_on_exit)

  def _load_cache(self) -> Dict[str, Any]:
    """Load cache from YAML file."""
    if not os.path.exists(self.cache_file):
      return {
        'playlists': {},
        'albums': {},
        'track_album_mappings': {},
        'metadata': {
          'created': datetime.datetime.now().isoformat(),
          'last_updated': datetime.datetime.now().isoformat()
        }
      }

    try:
      with open(self.cache_file, 'r') as file:
        cache = yaml.safe_load(file)
        if cache is None:
          # Return default if file is empty, don't recurse
          return {
            'playlists': {},
            'albums': {},
            'track_album_mappings': {},
            'metadata': {
              'created': datetime.datetime.now().isoformat(),
              'last_updated': datetime.datetime.now().isoformat()
            }
          }
        return cache
    except Exception as e:
      print(f"⚠️  Warning: Could not load cache file {self.cache_file}: {e}")
      return {
        'playlists': {},
        'albums': {},
        'track_album_mappings': {},
        'metadata': {
          'created': datetime.datetime.now().isoformat(),
          'last_updated': datetime.datetime.now().isoformat()
        }
      }

  def _save_cache(self, force: bool = False):
    """Save cache to YAML file. Only saves if dirty or forced."""
    if not self._dirty and not force:
      return  # Skip saving if nothing changed

    try:
      self.cache['metadata']['last_updated'] = datetime.datetime.now().isoformat()
      with open(self.cache_file, 'w') as file:
        yaml.dump(self.cache, file, default_flow_style=False, sort_keys=False)
      self._dirty = False  # Reset dirty flag after successful save
    except Exception as e:
      print(f"⚠️  Warning: Could not save cache file {self.cache_file}: {e}")

  def get_playlist(self, playlist_id: str) -> Optional[Dict[str, Any]]:
    """Get playlist from cache if it exists and snapshot ID matches."""
    playlist_cache = self.cache.get('playlists', {}).get(playlist_id)
    if playlist_cache:
      return playlist_cache
    return None

  def set_playlist(self, playlist_id: str, playlist_data: Dict[str, Any], snapshot_id: str):
    """Cache playlist data with snapshot ID for change detection."""
    if 'playlists' not in self.cache:
      self.cache['playlists'] = {}

    self.cache['playlists'][playlist_id] = {
      'data': playlist_data,
      'snapshot_id': snapshot_id,
      'cached_at': datetime.datetime.now().isoformat(),
      'type': 'playlist'
    }
    self._dirty = True  # Mark cache as modified
    # Don't save immediately - will be saved on exit

  def get_album(self, album_id: str) -> Optional[Dict[str, Any]]:
    """Get album from cache if it exists."""
    album_cache = self.cache.get('albums', {}).get(album_id)
    if album_cache:
      return album_cache
    return None

  def set_album(self, album_id: str, album_data: Dict[str, Any]):
    """Cache album data."""
    if 'albums' not in self.cache:
      self.cache['albums'] = {}

    self.cache['albums'][album_id] = {
      'data': album_data,
      'cached_at': datetime.datetime.now().isoformat(),
      'type': 'album'
    }
    self._dirty = True  # Mark cache as modified

  def set_track_album_mapping(self, track_id: str, album_id: str):
    """Cache track to album ID mapping."""
    if 'track_album_mappings' not in self.cache:
      self.cache['track_album_mappings'] = {}

    self.cache['track_album_mappings'][track_id] = album_id
    self._dirty = True  # Mark cache as modified

  def get_track_album_id(self, track_id: str) -> Optional[str]:
    """Get album ID for a track from cache."""
    return self.cache.get('track_album_mappings', {}).get(track_id)

  def get_missing_album_ids(self, album_ids: List[str]) -> List[str]:
    """Get list of album IDs that are not in cache."""
    cached_albums = self.cache.get('albums', {})
    return [album_id for album_id in album_ids if album_id not in cached_albums]

  def batch_set_albums(self, albums_data: Dict[str, Dict[str, Any]]):
    """Cache multiple albums at once."""
    if 'albums' not in self.cache:
      self.cache['albums'] = {}

    for album_id, album_data in albums_data.items():
      self.cache['albums'][album_id] = {
        'data': album_data,
        'cached_at': datetime.datetime.now().isoformat(),
        'type': 'album'
      }

    self._dirty = True  # Mark cache as modified

  def is_playlist_changed(self, playlist_id: str, current_snapshot_id: str) -> bool:
    """Check if playlist has changed by comparing snapshot IDs."""
    playlist_cache = self.cache.get('playlists', {}).get(playlist_id)
    if not playlist_cache:
      return True  # Not cached, consider it changed

    cached_snapshot_id = playlist_cache.get('snapshot_id')
    return cached_snapshot_id != current_snapshot_id

  def get_cache_stats(self) -> Dict[str, Any]:
    """Get cache statistics."""
    playlists = self.cache.get('playlists', {})
    albums = self.cache.get('albums', {})
    track_album_mappings = self.cache.get('track_album_mappings', {})

    return {
      'playlists_cached': len(playlists),
      'albums_cached': len(albums),
      'track_album_mappings': len(track_album_mappings),
      'total_cached_items': len(playlists) + len(albums) + len(track_album_mappings),
      'metadata': self.cache.get('metadata', {})
    }

  def clear_cache(self):
    """Clear all cached data."""
    self.cache = {
      'playlists': {},
      'albums': {},
      'track_album_mappings': {},
      'metadata': {
        'created': datetime.datetime.now().isoformat(),
        'last_updated': datetime.datetime.now().isoformat()
      }
    }
    self._dirty = True  # Mark cache as modified
    self._save_cache(force=True)  # Force save when clearing
    print(f"🗑️  Cache cleared: {self.cache_file}")

  def print_cache_stats(self):
    """Print cache statistics to console."""
    stats = self.get_cache_stats()
    print(f"📊 Cache Statistics:")
    print(f"   Playlists cached: {stats['playlists_cached']}")
    print(f"   Albums cached: {stats['albums_cached']}")
    print(f"   Track-album mappings: {stats['track_album_mappings']}")
    print(f"   Total cached items: {stats['total_cached_items']}")
    if stats['metadata'].get('last_updated'):
      print(f"   Last updated: {stats['metadata']['last_updated']}")

  def rate_limit_wait(self):
    """Wait if necessary to respect rate limits."""
    current_time = time.time()
    time_since_last_call = current_time - self.last_api_call
    if time_since_last_call < self.min_call_interval:
      sleep_time = self.min_call_interval - time_since_last_call
      time.sleep(sleep_time)
    self.last_api_call = time.time()

  def _setup_signal_handlers(self):
    """Set up signal handlers to save cache on interrupt."""
    def signal_handler(signum, frame):
      print(f"\n⚠️  Received signal {signum}, saving cache before exit...")
      self._save_cache_on_exit()
      exit(0)

    # Register handlers for common interrupt signals
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal

    # Note: SIGKILL cannot be caught, but SIGTERM and SIGINT can be

  def _save_cache_on_exit(self):
    """Save cache to file when the application exits."""
    try:
      if hasattr(self, 'cache') and self.cache and self._dirty:
        self._save_cache(force=True)
        print(f"💾 Cache saved to {self.cache_file}")
      elif hasattr(self, 'cache') and self.cache:
        # Even if not dirty, save on exit to ensure consistency
        self._save_cache(force=True)
        print(f"💾 Cache saved to {self.cache_file}")
    except Exception as e:
      print(f"⚠️  Warning: Could not save cache on exit: {e}")

  def force_save(self):
    """Force save the cache immediately."""
    self._save_cache(force=True)
