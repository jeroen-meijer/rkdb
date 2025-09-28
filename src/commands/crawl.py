import datetime
import yaml
import spotipy
from typing import List, Dict, Any, Set, Optional
from services import get_user_or_sign_in, setup_spotify
from functions import exhaust_fetch
from cache import CrawlCache
from image_generator import process_playlist_cover
from fuzzywuzzy import fuzz


class AlbumFetchManager:
  """
  Centralized manager for collecting and fetching albums across all sources.
  This optimizes API calls by batching all album fetches together.
  """

  def __init__(self, sp: spotipy.Spotify, cache: CrawlCache):
    self.sp = sp
    self.cache = cache
    self.album_ids_to_fetch: Set[str] = set()
    self.track_album_mappings: Dict[str, str] = {}  # track_id -> album_id
    # album_id -> list of sources requesting it
    self.source_info: Dict[str, List[str]] = {}

  def add_album_request(self, album_id: str, track_id: str, source: str):
    """Add an album to the fetch queue with source tracking."""
    if not album_id or not track_id:
      return

    # Only add if not already cached
    if not self.cache.get_album(album_id):
      self.album_ids_to_fetch.add(album_id)
      self.track_album_mappings[track_id] = album_id

      # Track which sources requested this album
      if album_id not in self.source_info:
        self.source_info[album_id] = []
      if source not in self.source_info[album_id]:
        self.source_info[album_id].append(source)

  def add_album_requests_batch(self, album_requests: List[Dict[str, str]]):
    """Add multiple album requests at once."""
    for request in album_requests:
      self.add_album_request(
        request.get('album_id'),
        request.get('track_id'),
        request.get('source')
      )

  def fetch_all_albums(self) -> Dict[str, Dict[str, Any]]:
    """Fetch all collected albums in optimized batches."""
    if not self.album_ids_to_fetch:
      print("       💾 All albums already cached")
      return {}

    print(
      f"       🔄 Fetching {len(self.album_ids_to_fetch)} albums in optimized batches...")

    # Convert to list and fetch
    album_ids_list = list(self.album_ids_to_fetch)
    fetched_albums = batch_fetch_albums(self.sp, album_ids_list, self.cache)

    # Cache track-to-album mappings
    for track_id, album_id in self.track_album_mappings.items():
      self.cache.set_track_album_mapping(track_id, album_id)

    # Print source statistics
    if self.source_info:
      source_counts = {}
      for sources in self.source_info.values():
        for source in sources:
          source_counts[source] = source_counts.get(source, 0) + 1

      print(f"       📊 Album requests by source:")
      for source, count in sorted(source_counts.items()):
        print(f"         {source}: {count} albums")

    return fetched_albums

  def get_track_album_mappings(self) -> Dict[str, str]:
    """Get all track-to-album mappings collected during this session."""
    return self.track_album_mappings.copy()


class ArtistFetchManager:
  """
  Centralized manager for collecting and fetching artist data across all jobs.
  This optimizes API calls by making one call per artist and sharing results.
  """

  def __init__(self, sp: spotipy.Spotify, cache: CrawlCache):
    self.sp = sp
    self.cache = cache
    # artist_id -> list of job requests
    self.artist_requests: Dict[str, List[Dict[str, Any]]] = {}
    # artist_id -> artist data
    self.fetched_artists: Dict[str, Dict[str, Any]] = {}

  def add_artist_request(self, artist_id: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime):
    """Add an artist request with job-specific filtering criteria."""
    if artist_id not in self.artist_requests:
      self.artist_requests[artist_id] = []

    self.artist_requests[artist_id].append({
      'job_name': job_name,
      'cutoff_date': cutoff_date,
      'end_date': end_date
    })

  def fetch_all_artists(self) -> Dict[str, Dict[str, Any]]:
    """Fetch all requested artists in optimized batches."""
    if not self.artist_requests:
      print("       💾 No artists to fetch")
      return {}

    print(
      f"       🔄 Fetching {len(self.artist_requests)} artists in optimized batches...")

    for artist_id in self.artist_requests:
      try:
        # NOTE(jeroen-meijer): Rate limiting
        self.cache.rate_limit_wait()

        # Get artist info
        artist_info = self.sp.artist(artist_id)
        if artist_info:
          artist_name = artist_info.get('name', 'Unknown Artist')
          print(f"         🎤 Fetching artist: {artist_name} ({artist_id})")

          # Get artist's albums and singles
          albums = self.sp.artist_albums(
            artist_id,
            include_groups='album,single',
            limit=50
          )

          if albums and 'items' in albums:
            self.fetched_artists[artist_id] = {
              'info': artist_info,
              'albums': albums['items']
            }
            print(f"           ✅ Found {len(albums['items'])} albums")
          else:
            print(f"           ⚠️  No albums found for artist {artist_name}")
            self.fetched_artists[artist_id] = {
              'info': artist_info,
              'albums': []
            }

      except Exception as e:
        print(f"         ❌ Error fetching artist {artist_id}: {e}")
        continue

    return self.fetched_artists

  def get_artist_tracks_for_job(self, artist_id: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime) -> List[Dict[str, Any]]:
    """Get tracks for a specific artist and job, filtering by the job's cutoff date."""
    if artist_id not in self.fetched_artists:
      return []

    artist_data = self.fetched_artists[artist_id]
    artist_info = artist_data['info']
    albums = artist_data['albums']

    # Pre-filter albums by release date for this specific job
    recent_albums = []
    end_date_exclusive = end_date + datetime.timedelta(days=1)

    for album in albums:
      if album and album.get('release_date'):
        try:
          release_date = parse_release_date(album['release_date'])
          if release_date > cutoff_date and release_date < end_date_exclusive:
            recent_albums.append(album)
        except ValueError as e:
          print(
            f"           ⚠️  Warning: Could not parse release date '{album['release_date']}' for album '{album.get('name', 'Unknown')}': {e}")
          continue

    # Get tracks from recent albums
    tracks = []
    for album in recent_albums:
      try:
        # NOTE(jeroen-meijer): Rate limiting
        self.cache.rate_limit_wait()

        album_tracks = self.sp.album_tracks(album['id'])
        if album_tracks and 'items' in album_tracks:
          for track in album_tracks['items']:
            if track:
              try:
                release_date = parse_release_date(album['release_date'])
                tracks.append({
                  'id': track['id'],
                  'uri': track['uri'],
                  'name': track['name'],
                  'artists': [artist['name'] for artist in track['artists']] if track.get('artists') else [],
                  'album_release_date': album['release_date'],
                  'added_at': release_date,
                  'source': f'artist:{artist_id}'
                })
              except ValueError as e:
                print(
                  f"             ⚠️  Warning: Could not parse release date for track '{track.get('name', 'Unknown')}': {e}")
                continue

      except Exception as e:
        print(f"           ⚠️  Error processing album {album['id']}: {e}")

    return tracks


class LabelFetchManager:
  """
  Centralized manager for collecting and fetching label data across all jobs.
  This optimizes API calls by making one search call per label and sharing results.
  """

  def __init__(self, sp: spotipy.Spotify, cache: CrawlCache):
    self.sp = sp
    self.cache = cache
    # label_name -> list of job requests
    self.label_requests: Dict[str, List[Dict[str, Any]]] = {}
    # label_name -> list of tracks
    self.fetched_labels: Dict[str, List[Dict[str, Any]]] = {}

  def add_label_request(self, label_name: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime):
    """Add a label request with job-specific filtering criteria."""
    if label_name not in self.label_requests:
      self.label_requests[label_name] = []

    self.label_requests[label_name].append({
      'job_name': job_name,
      'cutoff_date': cutoff_date,
      'end_date': end_date
    })

  def fetch_all_labels(self) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch all requested labels in optimized batches."""
    if not self.label_requests:
      print("       💾 No labels to fetch")
      return {}

    print(
      f"       🔄 Fetching {len(self.label_requests)} labels in optimized batches...")

    for label_name in self.label_requests:
      try:
        print(f"         🏷️  Fetching label: {label_name}")

        # Search for tracks by label
        search_query = f'label:"{label_name}"'
        search_results = self.sp.search(
          q=search_query,
          type='track',
          limit=50
        )

        if search_results and 'tracks' in search_results and 'items' in search_results['tracks']:
          self.fetched_labels[label_name] = search_results['tracks']['items']
          print(
            f"           ✅ Found {len(search_results['tracks']['items'])} tracks")
        else:
          print(
            f"           ⚠️  No search results found for label {label_name}")
          self.fetched_labels[label_name] = []

      except Exception as e:
        print(f"         ❌ Error fetching label {label_name}: {e}")
        continue

    return self.fetched_labels

  def get_label_tracks_for_job(self, label_name: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime) -> List[Dict[str, Any]]:
    """Get tracks for a specific label and job, filtering by the job's cutoff date."""
    if label_name not in self.fetched_labels:
      return []

    tracks = self.fetched_labels[label_name]
    recent_tracks = []
    mismatched_labels = set()

    end_date_exclusive = end_date + datetime.timedelta(days=1)

    for track in tracks:
      if track and track.get('album') and track['album'].get('release_date'):
        try:
          release_date = parse_release_date(track['album']['release_date'])
          if release_date > cutoff_date and release_date < end_date_exclusive:
            # NOTE(jeroen-meijer): Validate that the track's actual label matches our search term
            track_label = track['album'].get('label', '')
            label_match_ratio = fuzz.ratio(
              label_name.lower(), track_label.lower())

            # Use a high confidence threshold (90%) to ensure accurate matches
            if label_match_ratio >= 90:
              recent_tracks.append({
                'id': track['id'],
                'uri': track['uri'],
                'name': track['name'],
                'artists': [artist['name'] for artist in track['artists']] if track.get('artists') else [],
                'album_release_date': track['album']['release_date'],
                'added_at': release_date,
                'source': f'label:{label_name}'
              })
            else:
              # Track mismatched labels for reporting
              if track_label and track_label not in mismatched_labels:
                mismatched_labels.add(track_label)

        except ValueError as e:
          print(
            f"           ⚠️  Warning: Could not parse release date '{track['album']['release_date']}' for track '{track.get('name', 'Unknown')}': {e}")
          continue

    if mismatched_labels:
      print(
        f"           ⚠️  Skipped tracks from mismatched labels: {', '.join(sorted(mismatched_labels))}")

    return recent_tracks


class PlaylistFetchManager:
  """
  Centralized manager for collecting and fetching playlist data across all jobs.
  This optimizes API calls by making one call per playlist and sharing results.
  """

  def __init__(self, sp: spotipy.Spotify, cache: CrawlCache):
    self.sp = sp
    self.cache = cache
    # playlist_id -> list of job requests
    self.playlist_requests: Dict[str, List[Dict[str, Any]]] = {}
    # playlist_id -> playlist data
    self.fetched_playlists: Dict[str, Dict[str, Any]] = {}

  def add_playlist_request(self, playlist_id: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime):
    """Add a playlist request with job-specific filtering criteria."""
    if playlist_id not in self.playlist_requests:
      self.playlist_requests[playlist_id] = []

    self.playlist_requests[playlist_id].append({
      'job_name': job_name,
      'cutoff_date': cutoff_date,
      'end_date': end_date
    })

  def fetch_all_playlists(self) -> Dict[str, Dict[str, Any]]:
    """Fetch all requested playlists in optimized batches."""
    if not self.playlist_requests:
      print("       💾 No playlists to fetch")
      return {}

    print(
      f"       🔄 Fetching {len(self.playlist_requests)} playlists in optimized batches...")

    for playlist_id in self.playlist_requests:
      try:
        # Check cache first
        cached_playlist = self.cache.get_playlist(playlist_id)

        # Get playlist info and snapshot ID to check if it has changed
        try:
          playlist_info = self.sp.playlist(
            playlist_id, fields='name,snapshot_id')
          if playlist_info is None:
            playlist_name = 'Unknown Playlist'
            current_snapshot_id = None
          else:
            playlist_name = playlist_info.get('name', 'Unknown Playlist')
            current_snapshot_id = playlist_info.get('snapshot_id')

          print(
            f"         📜 Fetching playlist: {playlist_name} ({playlist_id})")

          # Check if playlist has changed - if not, use cached data completely
          if cached_playlist and current_snapshot_id and not self.cache.is_playlist_changed(playlist_id, current_snapshot_id):
            print(
              f"           💾 Using cached playlist data (snapshot: {current_snapshot_id})")
            self.fetched_playlists[playlist_id] = {
              'info': playlist_info,
              'tracks': cached_playlist.get('data', {}).get('tracks', []),
              'snapshot_id': current_snapshot_id,
              'cached': True
            }
            continue

        except Exception as e:
          print(
            f"         📜 Fetching playlist: {playlist_id} (could not fetch info: {e})")
          playlist_name = f"Playlist {playlist_id}"
          current_snapshot_id = None

        # If we reach here, we need to fetch tracks from Spotify
        print(f"           🔄 Fetching tracks from Spotify...")

        # Get playlist tracks with added_at field and album information
        tracks = exhaust_fetch(
          fetch=lambda offset, limit: self.sp.playlist_items(
            playlist_id,
            offset=offset,
            limit=limit,
            fields='items(added_at,track(id,uri,name,artists(name),album(id,name,release_date,label))),next'
          ),
          map_elements=lambda res: res['items']
        )

        # NOTE(jeroen-meijer): Convert tracks to use album ID references instead of full album data
        processed_tracks = []
        for item in tracks:
          if not item or not item.get('track'):
            continue

          track = item['track']
          if track and track.get('id'):
            # Create a copy of the track with album ID reference instead of full album data
            processed_track = {
              'id': track['id'],
              'uri': track['uri'],
              'name': track['name'],
              'artists': track.get('artists', []),
              'album_id': track['album']['id'] if track.get('album') and track['album'].get('id') else None,
              'source': f'playlist:{playlist_id}'
            }
            processed_tracks.append({
              'track': processed_track,
              'added_at': item['added_at']
            })

        self.fetched_playlists[playlist_id] = {
          'info': playlist_info if 'playlist_info' in locals() else {'name': playlist_name},
          'tracks': processed_tracks,
          'snapshot_id': current_snapshot_id,
          'cached': False
        }

        # Cache the playlist data if we have a snapshot ID
        if current_snapshot_id:
          playlist_data = {
            'name': playlist_name,
            'tracks': processed_tracks
          }
          self.cache.set_playlist(
            playlist_id, playlist_data, current_snapshot_id)
          print(
            f"           💾 Cached playlist data (snapshot: {current_snapshot_id})")

        print(f"           ✅ Found {len(processed_tracks)} tracks")

      except Exception as e:
        print(f"         ❌ Error fetching playlist {playlist_id}: {e}")
        continue

    return self.fetched_playlists

  def get_playlist_tracks_for_job(self, playlist_id: str, job_name: str, cutoff_date: datetime.datetime, end_date: datetime.datetime) -> List[Dict[str, Any]]:
    """Get tracks for a specific playlist and job, filtering by the job's cutoff date."""
    if playlist_id not in self.fetched_playlists:
      return []

    playlist_data = self.fetched_playlists[playlist_id]
    tracks = playlist_data['tracks']

    # Filter tracks within (cutoff_date, end_date] — lower exclusive, upper inclusive
    recent_tracks = []
    end_date_exclusive = end_date + datetime.timedelta(days=1)
    for item in tracks:
      if not item or not item.get('track'):
        continue

      added_at = datetime.datetime.fromisoformat(
        item['added_at'].replace('Z', '+00:00'))
      # Make cutoff and end dates timezone-aware for comparison
      cutoff_date_aware = cutoff_date.replace(tzinfo=datetime.timezone.utc)
      end_date_aware = end_date_exclusive.replace(tzinfo=datetime.timezone.utc)

      if added_at > cutoff_date_aware and added_at < end_date_aware:
        track = item['track']
        if track and track.get('id'):
          # NOTE(jeroen-meijer): Reconstruct full track data from album ID reference
          if track.get('album_id'):
            cached_album = self.cache.get_album(track['album_id'])
            if cached_album:
              track['album'] = cached_album['data']
            else:
              # Fallback if album not found in cache
              track['album'] = {'release_date': None}
          else:
            track['album'] = {'release_date': None}

          track['source'] = f'playlist:{playlist_id}'
          recent_tracks.append(track)

    return recent_tracks


def extract_essential_album_data(album_data: Dict[str, Any]) -> Dict[str, Any]:
  """
  Extract only the essential album data fields needed for caching.
  This significantly reduces cache size by removing unnecessary metadata.
  """
  if not album_data:
    return {}

  return {
    'id': album_data.get('id'),
    'name': album_data.get('name'),
    'release_date': album_data.get('release_date'),
    'label': album_data.get('label'),
    'artists': [
      {
        'id': artist.get('id'),
        'name': artist.get('name')
      }
      for artist in album_data.get('artists', [])
    ] if album_data.get('artists') else []
  }


def chunks(lst, n):
  """Split a list into chunks of size n."""
  for i in range(0, len(lst), n):
    yield lst[i:i + n]


def batch_fetch_albums(sp: spotipy.Spotify, album_ids: List[str], cache: CrawlCache) -> Dict[str, Dict[str, Any]]:
  """
  Fetch multiple albums in batches and cache them.
  Returns a dict mapping album_id to album_data for albums that were successfully fetched.
  """
  if not album_ids:
    return {}

  # Filter out albums that are already cached
  missing_album_ids = cache.get_missing_album_ids(album_ids)

  if not missing_album_ids:
    print(f"       💾 All {len(album_ids)} albums already cached")
    return {}

  print(f"       🔄 Fetching {len(missing_album_ids)} albums in batches...")

  fetched_albums = {}

  # Fetch albums in batches of 20 (Spotify API limit)
  for batch in chunks(missing_album_ids, 20):
    try:
      # NOTE(jeroen-meijer): Rate limiting
      cache.rate_limit_wait()

      batch_albums = sp.albums(batch)

      if batch_albums and 'albums' in batch_albums:
        for album in batch_albums['albums']:
          if album:  # Check if album exists (not None)
            album_id = album['id']
            # Extract only essential data to minimize cache size
            essential_data = extract_essential_album_data(album)
            fetched_albums[album_id] = essential_data
            print(
              f"         ✅ Fetched album: {album.get('name', 'Unknown')} ({album_id})")
          else:
            print(f"         ⚠️  Album not found in batch")

      print(f"         📦 Processed batch of {len(batch)} albums")

    except Exception as e:
      print(f"         ❌ Error fetching album batch: {e}")
      continue

  # Cache all fetched albums
  if fetched_albums:
    cache.batch_set_albums(fetched_albums)
    print(f"       💾 Cached {len(fetched_albums)} new albums")

  return fetched_albums


def get_album_data_for_track(track: Dict[str, Any], cache: CrawlCache) -> Optional[Dict[str, Any]]:
  """
  Get album data for a track, either from cache or by fetching.
  Returns album data if available, None otherwise.
  """
  if not track or not track.get('id'):
    return None

  track_id = track['id']

  # First check if we have a track-to-album mapping
  album_id = cache.get_track_album_id(track_id)

  # If no mapping, try to get album ID from track data
  if not album_id and track.get('album') and track['album'].get('id'):
    album_id = track['album']['id']
    # Cache the mapping for future use
    cache.set_track_album_mapping(track_id, album_id)

  if not album_id:
    return None

  # Check if album is cached
  cached_album = cache.get_album(album_id)
  if cached_album:
    return cached_album['data']

  return None


def parse_release_date(release_date_str: str) -> datetime.datetime:
  """
  Parse release date string from Spotify API.
  Handles different formats: 'YYYY-MM-DD', 'YYYY-MM', 'YYYY'
  Defaults missing parts to 01 for month/day, 00:00 for time.
  """
  if not release_date_str:
    raise ValueError("Release date string is empty")

  # Try different date formats
  date_formats = [
    '%Y-%m-%d',  # Full date: 2008-01-15
    '%Y-%m',     # Year-month: 2008-01
    '%Y'         # Year only: 2008
  ]

  for fmt in date_formats:
    try:
      parsed_date = datetime.datetime.strptime(release_date_str, fmt)

      # If format was year-only or year-month, default missing parts
      if fmt == '%Y':
        # Year only: default to January 1st
        parsed_date = parsed_date.replace(
          month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
      elif fmt == '%Y-%m':
        # Year-month: default to 1st day
        parsed_date = parsed_date.replace(
          day=1, hour=0, minute=0, second=0, microsecond=0)
      else:
        # Full date: default to 00:00 time
        parsed_date = parsed_date.replace(
          hour=0, minute=0, second=0, microsecond=0)

      return parsed_date
    except ValueError:
      continue

  raise ValueError(f"Could not parse release date: {release_date_str}")


def validate_job_config(job: Dict[str, Any]) -> bool:
  """
  Validate that a job has all required fields.
  Returns True if valid, False otherwise.
  """
  job_name = job.get('name', 'Unnamed Job')

  # Check for required filters
  filters = job.get('filters', {})
  if 'added_between_days' not in filters:
    print(
      f"❌ Error: Job '{job_name}' is missing required 'added_between_days' filter")
    return False

  # Check for required inputs (at least one source)
  # NOTE(jeroen-meijer): Allow empty input arrays - they will be handled gracefully
  # by the processing code. This allows for commented out items in YAML configs.
  inputs = job.get('inputs', {})
  has_playlists = bool(inputs.get('playlists') or [])
  has_artists = bool(inputs.get('artists') or [])
  has_labels = bool(inputs.get('labels') or [])

  if not (has_playlists or has_artists or has_labels):
    print(
      f"❌ Error: Job '{job_name}' has no input sources (playlists, artists, or labels)")
    print(f"   This would create an empty playlist with no purpose. Please add at least one input source.")
    return False

  # Check for required output_playlist
  output_playlist = job.get('output_playlist', {})
  if not output_playlist.get('name'):
    print(
      f"❌ Error: Job '{job_name}' is missing required 'output_playlist.name'")
    return False

  return True


def create_crawl_report(job: Dict[str, Any], all_tracks: List[Dict[str, Any]], cutoff_date: datetime.datetime) -> Dict[str, Any]:
  """
  Create a crawl report with details about what tracks were added and their sources.
  """
  # Ensure all_tracks is a list, not None
  if all_tracks is None:
    all_tracks = []

  report = {
    'crawl_info': {
      'job_name': job.get('name', 'Unnamed Job'),
      'cutoff_date': cutoff_date.isoformat(),
      'total_tracks': len(all_tracks),
      'timestamp': datetime.datetime.now().isoformat()
    },
    'sources': {
      'playlists': {},
      'artists': {},
      'labels': {}
    },
    'tracks': []
  }

  # Group tracks by source
  for track in all_tracks:
    source = track.get('source', 'unknown')
    track_info = {
      'id': track.get('id'),
      'uri': track.get('uri'),
      'name': track.get('name'),
      'artists': track.get('artists', []),
      'source': source,
      'album_release_date': track.get('album_release_date'),
      'added_at': (lambda x: x.isoformat() if x and isinstance(x, datetime.datetime) else None)(track.get('added_at'))
    }
    report['tracks'].append(track_info)

    # Update source counts
    if source.startswith('playlist:'):
      playlist_id = source.split(':', 1)[1]
      if playlist_id not in report['sources']['playlists']:
        report['sources']['playlists'][playlist_id] = 0
      report['sources']['playlists'][playlist_id] += 1
    elif source.startswith('artist:'):
      artist_id = source.split(':', 1)[1]
      if artist_id not in report['sources']['artists']:
        report['sources']['artists'][artist_id] = 0
      report['sources']['artists'][artist_id] += 1
    elif source.startswith('label:'):
      label_name = source.split(':', 1)[1]
      if label_name not in report['sources']['labels']:
        report['sources']['labels'][label_name] = 0
      report['sources']['labels'][label_name] += 1

  return report


def save_combined_crawl_report(all_reports: List[Dict[str, Any]]):
  """
  Save combined crawl report to a YAML file with ISO date in filename.
  """
  timestamp = datetime.datetime.now().strftime('%Y-%m-%d')
  filename = f"crawl_report_{timestamp}.yaml"

  combined_report = {
    'crawl_session': {
      'timestamp': datetime.datetime.now().isoformat(),
      'total_jobs': len(all_reports),
      'total_tracks': sum(len(report.get('tracks', [])) for report in all_reports)
    },
    'jobs': all_reports
  }

  try:
    with open(filename, 'w') as file:
      yaml.dump(combined_report, file,
                default_flow_style=False, sort_keys=False)
    print(f"📊 Combined crawl report saved to: {filename}")
  except Exception as e:
    print(f"⚠️  Warning: Could not save combined crawl report: {e}")


def crawl_spotify_playlists(cache: CrawlCache = None, custom_end_date: datetime.datetime = None, custom_start_date: datetime.datetime = None, config_path: Optional[str] = None):
  """
  Crawl Spotify playlists, artists, and labels based on YAML configuration.
  Creates new playlists with tracks added within the specified time window.

  Args:
    cache: Optional CrawlCache instance
    custom_end_date: Optional custom end date to use instead of current date (for testing/debugging)
    custom_start_date: Optional custom start date to override job config days_back setting
  """
  print("🎵 Spotify Playlist Crawler")
  print("=" * 50)

  # Initialize cache if not provided
  if cache is None:
    cache = CrawlCache()
  cache.print_cache_stats()
  print()

  # Load configuration
  try:
    config_file = config_path if config_path else 'crawl_config.yaml'
    with open(config_file, 'r') as file:
      config = yaml.safe_load(file)
  except FileNotFoundError:
    print(f"❌ Error: {config_path or 'crawl_config.yaml'} not found")
    return
  except yaml.YAMLError as e:
    print(f"❌ Error parsing YAML: {e}")
    return

  # Setup Spotify
  sp = setup_spotify()

  # Get current user
  try:
    user = get_user_or_sign_in(sp)
    if user is None:
      print("❌ Error: Could not get user info")
      return
    print(f"👤 Logged in as: {user['display_name']}")
  except Exception as e:
    print(f"❌ Error getting user info: {e}")
    return

  # Process each job
  jobs = config.get('jobs', [])
  if not jobs:
    print("❌ No jobs found in configuration")
    return

  print(f"📋 Found {len(jobs)} job(s) to process")
  print()

  # Create centralized fetch managers for optimization
  album_manager = AlbumFetchManager(sp, cache)
  artist_manager = ArtistFetchManager(sp, cache)
  label_manager = LabelFetchManager(sp, cache)
  playlist_manager = PlaylistFetchManager(sp, cache)

  # First pass: collect all requests from all jobs
  print("🔄 Collecting requests from all jobs...")
  for job in jobs:
    job_name = job.get('name', 'Unnamed Job')

    # Validate job configuration
    if not validate_job_config(job):
      print(f"⚠️  Skipping job '{job_name}' due to configuration errors")
      continue

    # Get job configuration
    inputs = job.get('inputs', {})
    filters = job.get('filters', {})

    # Calculate time window
    days_back = filters.get('added_between_days', 7)
    current_time = custom_end_date if custom_end_date else datetime.datetime.now()

    # Use custom_start_date if provided, otherwise calculate from days_back
    if custom_start_date:
      # Subtract 1 day since we use > comparison
      cutoff_date = custom_start_date - datetime.timedelta(days=1)
    else:
      cutoff_date = current_time - datetime.timedelta(days=days_back)

    # Collect playlist requests
    playlist_ids = resolve_references(inputs.get('playlists') or [], config)
    for playlist_id in playlist_ids:
      playlist_manager.add_playlist_request(
        playlist_id, job_name, cutoff_date, current_time)

    # Collect artist requests
    artist_ids = resolve_references(inputs.get('artists') or [], config)
    for artist_id in artist_ids:
      artist_manager.add_artist_request(
        artist_id, job_name, cutoff_date, current_time)

    # Collect label requests
    label_ids = resolve_references(inputs.get('labels') or [], config)
    for label_id in label_ids:
      label_manager.add_label_request(
        label_id, job_name, cutoff_date, current_time)

  # Second pass: fetch all data in optimized batches
  print("🔄 Fetching all data in optimized batches...")
  print()

  # Fetch playlists
  playlist_manager.fetch_all_playlists()
  print()

  # Fetch artists
  artist_manager.fetch_all_artists()
  print()

  # Fetch labels
  label_manager.fetch_all_labels()
  print()

  # Fetch albums (this will be called by individual job processing)
  print("🔄 Albums will be fetched during job processing...")
  print()

  # Third pass: process each job with the fetched data
  all_reports = []
  for job in jobs:
    try:
      report = process_job_optimized(sp, job, config, cache, custom_end_date, custom_start_date,
                                     album_manager, artist_manager, label_manager, playlist_manager)
      if report:
        all_reports.append(report)
    except Exception as e:
      print(f"⚠️  Warning: Job '{job.get('name', 'Unnamed')}' failed: {e}")
      print("   Continuing with next job...")
      continue

  # Print optimization statistics
  print_optimization_stats(artist_manager, label_manager, playlist_manager)

  # Save combined report if any jobs were processed (non-critical)
  try:
    if all_reports:
      save_combined_crawl_report(all_reports)
    else:
      print("📊 No jobs were processed successfully, no report generated")
  except Exception as e:
    print(f"⚠️  Warning: Could not save combined report: {e}")
    print("   Playlists were still created successfully.")


def process_job_optimized(sp: spotipy.Spotify, job: Dict[str, Any], config: Dict[str, Any], cache: CrawlCache,
                          custom_end_date: datetime.datetime, custom_start_date: datetime.datetime,
                          album_manager: AlbumFetchManager, artist_manager: ArtistFetchManager,
                          label_manager: LabelFetchManager, playlist_manager: PlaylistFetchManager):
  """Process a single job from the configuration using optimized centralized managers."""
  job_name = job.get('name', 'Unnamed Job')
  print(f"🔄 Processing job: {job_name}")

  # Validate job configuration
  if not validate_job_config(job):
    print(f"❌ Skipping job '{job_name}' due to configuration errors")
    print()
    return None

  # Get job configuration
  inputs = job.get('inputs', {})
  filters = job.get('filters', {})
  options = job.get('options', {})
  output_playlist = job.get('output_playlist', {})

  # Calculate time window
  days_back = filters.get('added_between_days', 7)
  current_time = custom_end_date if custom_end_date else datetime.datetime.now()

  # Use custom_start_date if provided, otherwise calculate from days_back
  if custom_start_date:
    # Lower bound exclusive: include custom_start_date itself
    cutoff_date = custom_start_date - datetime.timedelta(days=1)
  else:
    # Lower bound exclusive so that added_between_days=N yields last N days inclusive
    cutoff_date = current_time - datetime.timedelta(days=days_back)

  if custom_end_date or custom_start_date:
    if custom_start_date and custom_end_date:
      print(
        f"📅 Using custom date range: {custom_start_date.strftime('%Y-%m-%d')} to {custom_end_date.strftime('%Y-%m-%d')}")
    elif custom_end_date:
      print(f"📅 Using custom end date: {custom_end_date.strftime('%Y-%m-%d')}")
    elif custom_start_date:
      print(
        f"📅 Using custom start date: {custom_start_date.strftime('%Y-%m-%d')}")
    print(
      f"📅 Date bounds: ({cutoff_date.strftime('%Y-%m-%d')} exclusive, {current_time.strftime('%Y-%m-%d')} inclusive)")
  else:
    print(
      f"📅 Date bounds: ({cutoff_date.strftime('%Y-%m-%d')} exclusive, {current_time.strftime('%Y-%m-%d')} inclusive)")

  # Collect all tracks
  all_tracks = []

  # Process playlists using centralized manager
  playlist_ids = resolve_references(inputs.get('playlists') or [], config)
  if playlist_ids:
    print(f"📜 Processing {len(playlist_ids)} playlist(s)...")
    try:
      playlist_tracks = []
      for playlist_id in playlist_ids:
        tracks = playlist_manager.get_playlist_tracks_for_job(
          playlist_id, job_name, cutoff_date, current_time)
        playlist_tracks.extend(tracks)

      all_tracks.extend(playlist_tracks)
      print(f"   Found {len(playlist_tracks)} tracks from playlists")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing playlists: {e}")
      print("   Continuing with other sources...")

  # Process artists using centralized manager
  artist_ids = resolve_references(inputs.get('artists') or [], config)
  if artist_ids:
    print(f"🎤 Processing {len(artist_ids)} artist(s)...")
    try:
      artist_tracks = []
      for artist_id in artist_ids:
        tracks = artist_manager.get_artist_tracks_for_job(
          artist_id, job_name, cutoff_date, current_time)
        artist_tracks.extend(tracks)

      all_tracks.extend(artist_tracks)
      print(f"   Found {len(artist_tracks)} tracks from artists")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing artists: {e}")
      print("   Continuing with other sources...")

  # Process labels using centralized manager
  label_ids = resolve_references(inputs.get('labels') or [], config)
  if label_ids:
    print(f"🏷️  Processing {len(label_ids)} label(s)...")
    try:
      label_tracks = []
      for label_id in label_ids:
        tracks = label_manager.get_label_tracks_for_job(
          label_id, job_name, cutoff_date, current_time)
        label_tracks.extend(tracks)

      all_tracks.extend(label_tracks)
      print(f"   Found {len(label_tracks)} tracks from labels")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing labels: {e}")
      print("   Continuing with other sources...")

  # Collect album requests from all tracks and fetch albums
  print("🔄 Collecting album requests and fetching albums...")
  for track in all_tracks:
    if track.get('album_id'):
      album_manager.add_album_request(
        track['album_id'], track['id'], track.get('source', 'unknown'))

  # Fetch all albums in optimized batches
  album_manager.fetch_all_albums()

  if not all_tracks:
    print("⚠️  No tracks found for this job - will create empty playlist")
    # Continue to create the playlist even if empty

  # Deduplicate tracks if requested
  deduplicate_option = options.get('deduplicate')
  if deduplicate_option is not None:
    try:
      if deduplicate_option == DEDUPE_ON_ID:
        unique_tracks = deduplicate_tracks(all_tracks, DEDUPE_ON_ID)
        print(
          f"🔄 Deduplicated by ID: {len(all_tracks)} → {len(unique_tracks)} tracks")
      elif deduplicate_option == DEDUPE_ON_MATCH:
        unique_tracks = deduplicate_tracks(all_tracks, DEDUPE_ON_MATCH)
        print(
          f"🔄 Deduplicated by match: {len(all_tracks)} → {len(unique_tracks)} tracks")
      else:
        print(
          f"⚠️  Warning: Unknown deduplicate option '{deduplicate_option}', skipping deduplication")
        unique_tracks = all_tracks
      all_tracks = unique_tracks
    except Exception as e:
      print(f"⚠️  Warning: Error during deduplication: {e}")
      print("   Continuing with original track list...")

  # Create playlist
  try:
    # Ensure all_tracks is a list, not None
    if all_tracks is None:
      all_tracks = []

    playlist_name = generate_playlist_name(
      output_playlist.get('name', 'Generated Playlist'), job, cutoff_date, len(all_tracks), all_tracks, current_time, None)
    playlist_description = generate_playlist_name(
      output_playlist.get('description', 'Generated by Spotify Crawler'), job, cutoff_date, len(all_tracks), all_tracks, current_time, None)
    is_public = output_playlist.get('public', False)

    print(f"📝 Creating playlist: {playlist_name}")
  except Exception as e:
    print(f"⚠️  Warning: Error generating playlist name: {e}")
    print("   Using fallback playlist name...")
    # Use effective current_time (virtual date) for fallback naming
    playlist_name = f"Generated Playlist {current_time.strftime('%Y-%m-%d')}"
    playlist_description = "Generated by Spotify Crawler"
    is_public = output_playlist.get('public', False)

  try:
    # Get current user for playlist creation
    current_user = get_user_or_sign_in(sp)
    if current_user is None:
      print("❌ Error: Could not get current user for playlist creation")
      return

    # Create the playlist
    playlist = sp.user_playlist_create(
        user=current_user['id'],
        name=playlist_name,
        public=is_public,
        description=playlist_description
    )

    if playlist is None:
      print("❌ Error: Failed to create playlist")
      return

    # Add tracks to playlist
    if all_tracks:
      try:
        track_uris = [track['uri'] for track in all_tracks]

        # Add tracks in batches of 100 (Spotify API limit)
        for i in range(0, len(track_uris), 100):
          batch = track_uris[i:i + 100]
          sp.playlist_add_items(playlist['id'], batch)

        print(f"✅ Added {len(all_tracks)} tracks to playlist")
      except Exception as e:
        print(f"⚠️  Warning: Error adding tracks to playlist: {e}")
        print("   Playlist was created but tracks could not be added.")
    else:
      print("⚠️  No tracks to add to playlist")

    # Show playlist URL if available
    try:
      if playlist.get('external_urls') and playlist['external_urls'].get('spotify'):
        print(f"🔗 Playlist URL: {playlist['external_urls']['spotify']}")
    except Exception as e:
      print(f"⚠️  Warning: Could not display playlist URL: {e}")

    # Process playlist cover (non-critical - continue if it fails)
    try:
      if process_playlist_cover(
              sp,
              job,
              playlist['id'],
              playlist_name,
              cutoff_date=cutoff_date,
              effective_now=current_time):
        print(f"✅ Playlist cover processed successfully")
      else:
        print(f"⚠️  Warning: Could not process playlist cover")
    except Exception as e:
      print(f"⚠️  Warning: Error processing playlist cover: {e}")
      print("   Continuing with playlist creation...")

    # Create crawl report (non-critical - continue if it fails)
    try:
      report = create_crawl_report(job, all_tracks, cutoff_date)
      return report
    except Exception as e:
      print(f"⚠️  Warning: Could not create crawl report: {e}")
      print("   Continuing with playlist creation...")
      return None

  except Exception as e:
    print(f"❌ Error creating playlist: {e}")
    return None

  print()
  return None


def process_job(sp: spotipy.Spotify, job: Dict[str, Any], config: Dict[str, Any], cache: CrawlCache, custom_date: datetime.datetime = None):
  """Process a single job from the configuration."""
  job_name = job.get('name', 'Unnamed Job')
  print(f"🔄 Processing job: {job_name}")

  # Validate job configuration
  if not validate_job_config(job):
    print(f"❌ Skipping job '{job_name}' due to configuration errors")
    print()
    return None

  # Get job configuration
  inputs = job.get('inputs', {})
  filters = job.get('filters', {})
  options = job.get('options', {})
  output_playlist = job.get('output_playlist', {})

  # Calculate time window
  days_back = filters.get('added_between_days', 7)
  current_time = custom_date if custom_date else datetime.datetime.now()
  # Lower bound exclusive so that N yields last N days inclusive
  cutoff_date = current_time - datetime.timedelta(days=days_back)

  if custom_date:
    print(f"📅 Using custom date: {custom_date.strftime('%Y-%m-%d')}")
    print(
      f"📅 Looking for tracks added/released after: {cutoff_date.strftime('%Y-%m-%d')}")
  else:
    print(
      f"📅 Looking for tracks added/released after: {cutoff_date.strftime('%Y-%m-%d')}")

  # Create centralized album fetch manager
  album_manager = AlbumFetchManager(sp, cache)

  # Collect all tracks
  all_tracks = []

  # Process playlists
  playlist_ids = resolve_references(inputs.get('playlists') or [], config)
  if playlist_ids:
    print(f"📜 Processing {len(playlist_ids)} playlist(s)...")
    try:
      playlist_tracks = get_playlist_tracks(
        sp, playlist_ids, cutoff_date, cache, album_manager)
      all_tracks.extend(playlist_tracks)
      print(f"   Found {len(playlist_tracks)} tracks from playlists")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing playlists: {e}")
      print("   Continuing with other sources...")

  # Process artists (no caching since artists can release new music)
  artist_ids = resolve_references(inputs.get('artists') or [], config)
  if artist_ids:
    print(f"🎤 Processing {len(artist_ids)} artist(s)...")
    try:
      artist_tracks = get_artist_tracks(
        sp, artist_ids, cutoff_date, cache, album_manager)
      all_tracks.extend(artist_tracks)
      print(f"   Found {len(artist_tracks)} tracks from artists")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing artists: {e}")
      print("   Continuing with other sources...")

  # Process labels
  label_ids = resolve_references(inputs.get('labels') or [], config)
  if label_ids:
    print(f"🏷️  Processing {len(label_ids)} label(s)...")
    try:
      label_tracks = get_label_tracks(
        sp, label_ids, cutoff_date, cache, album_manager)
      all_tracks.extend(label_tracks)
      print(f"   Found {len(label_tracks)} tracks from labels")
    except Exception as e:
      print(f"   ⚠️  Warning: Error processing labels: {e}")
      print("   Continuing with other sources...")

  # Fetch all albums in optimized batches
  print("🔄 Fetching all albums in optimized batches...")
  album_manager.fetch_all_albums()

  if not all_tracks:
    print("⚠️  No tracks found for this job - will create empty playlist")
    # Continue to create the playlist even if empty

  # Deduplicate tracks if requested
  deduplicate_option = options.get('deduplicate')
  if deduplicate_option is not None:
    try:
      if deduplicate_option == DEDUPE_ON_ID:
        unique_tracks = deduplicate_tracks(all_tracks, DEDUPE_ON_ID)
        print(
          f"🔄 Deduplicated by ID: {len(all_tracks)} → {len(unique_tracks)} tracks")
      elif deduplicate_option == DEDUPE_ON_MATCH:
        unique_tracks = deduplicate_tracks(all_tracks, DEDUPE_ON_MATCH)
        print(
          f"🔄 Deduplicated by match: {len(all_tracks)} → {len(unique_tracks)} tracks")
      else:
        print(
          f"⚠️  Warning: Unknown deduplicate option '{deduplicate_option}', skipping deduplication")
        unique_tracks = all_tracks
      all_tracks = unique_tracks
    except Exception as e:
      print(f"⚠️  Warning: Error during deduplication: {e}")
      print("   Continuing with original track list...")

  # Create playlist
  try:
    # Ensure all_tracks is a list, not None
    if all_tracks is None:
      all_tracks = []

    playlist_name = generate_playlist_name(
      output_playlist.get('name', 'Generated Playlist'), job, cutoff_date, len(all_tracks), all_tracks, current_time, None)
    playlist_description = generate_playlist_name(
      output_playlist.get('description', 'Generated by Spotify Crawler'), job, cutoff_date, len(all_tracks), all_tracks, current_time, None)
    is_public = output_playlist.get('public', False)

    print(f"📝 Creating playlist: {playlist_name}")
  except Exception as e:
    print(f"⚠️  Warning: Error generating playlist name: {e}")
    print("   Using fallback playlist name...")
    # Use effective current_time (virtual date) for fallback naming
    playlist_name = f"Generated Playlist {current_time.strftime('%Y-%m-%d')}"
    playlist_description = "Generated by Spotify Crawler"
    is_public = output_playlist.get('public', False)

  try:
    # Get current user for playlist creation
    current_user = get_user_or_sign_in(sp)
    if current_user is None:
      print("❌ Error: Could not get current user for playlist creation")
      return

    # Create the playlist
    playlist = sp.user_playlist_create(
        user=current_user['id'],
        name=playlist_name,
        public=is_public,
        description=playlist_description
    )

    if playlist is None:
      print("❌ Error: Failed to create playlist")
      return

    # Add tracks to playlist
    if all_tracks:
      try:
        track_uris = [track['uri'] for track in all_tracks]

        # Add tracks in batches of 100 (Spotify API limit)
        for i in range(0, len(track_uris), 100):
          batch = track_uris[i:i + 100]
          sp.playlist_add_items(playlist['id'], batch)

        print(f"✅ Added {len(all_tracks)} tracks to playlist")
      except Exception as e:
        print(f"⚠️  Warning: Error adding tracks to playlist: {e}")
        print("   Playlist was created but tracks could not be added.")
    else:
      print("⚠️  No tracks to add to playlist")

    # Show playlist URL if available
    try:
      if playlist.get('external_urls') and playlist['external_urls'].get('spotify'):
        print(f"🔗 Playlist URL: {playlist['external_urls']['spotify']}")
    except Exception as e:
      print(f"⚠️  Warning: Could not display playlist URL: {e}")

    # Process playlist cover (non-critical - continue if it fails)
    try:
      if process_playlist_cover(
              sp,
              job,
              playlist['id'],
              playlist_name,
              cutoff_date=cutoff_date,
              effective_now=current_time):
        print(f"✅ Playlist cover processed successfully")
      else:
        print(f"⚠️  Warning: Could not process playlist cover")
    except Exception as e:
      print(f"⚠️  Warning: Error processing playlist cover: {e}")
      print("   Continuing with playlist creation...")

    # Create crawl report (non-critical - continue if it fails)
    try:
      report = create_crawl_report(job, all_tracks, cutoff_date)
      return report
    except Exception as e:
      print(f"⚠️  Warning: Could not create crawl report: {e}")
      print("   Continuing with playlist creation...")
      return None

  except Exception as e:
    print(f"❌ Error creating playlist: {e}")
    return None

  print()
  return None


def resolve_references(items: List[str], config: Dict[str, Any]) -> List[str]:
  """Resolve YAML anchors and aliases to actual IDs."""
  resolved = []
  notes = config.get('_notes', {})

  for item in items:
    if item.startswith('*'):
      # This is an alias, look it up in notes
      alias_name = item[1:]  # Remove the *
      found = False

      for category in ['playlists', 'artists', 'labels']:
        if category in notes and alias_name in notes[category]:
          resolved.append(notes[category][alias_name])
          found = True
          break

      if not found:
        print(f"⚠️  Warning: Could not resolve alias {alias_name}")
    else:
      # Direct ID
      resolved.append(item)

  return resolved


def get_playlist_tracks(sp: spotipy.Spotify, playlist_ids: List[str], cutoff_date: datetime.datetime, cache: CrawlCache, album_manager: AlbumFetchManager) -> List[Dict[str, Any]]:
  """Get tracks from playlists that were added after the cutoff date."""
  all_tracks = []

  for playlist_id in playlist_ids:
    try:
      # Check cache first
      cached_playlist = cache.get_playlist(playlist_id)

      # Get playlist info and snapshot ID to check if it has changed
      try:
        playlist_info = sp.playlist(playlist_id, fields='name,snapshot_id')
        if playlist_info is None:
          playlist_name = 'Unknown Playlist'
          current_snapshot_id = None
        else:
          playlist_name = playlist_info.get('name', 'Unknown Playlist')
          current_snapshot_id = playlist_info.get('snapshot_id')

        print(f"     📜 Processing playlist: {playlist_name} ({playlist_id})")

        # Check if playlist has changed - if not, use cached data completely
        if cached_playlist and current_snapshot_id and not cache.is_playlist_changed(playlist_id, current_snapshot_id):
          print(
            f"       💾 Using cached playlist data (snapshot: {current_snapshot_id})")
          cached_tracks = cached_playlist.get('data', {}).get('tracks', [])
          # Filter tracks added after cutoff date from cached data
          recent_tracks = []
          for item in cached_tracks:
            if not item or not item.get('track'):
              continue

            added_at = datetime.datetime.fromisoformat(
                item['added_at'].replace('Z', '+00:00'))
            # Make cutoff_date timezone-aware for comparison
            cutoff_date_aware = cutoff_date.replace(
              tzinfo=datetime.timezone.utc)

            if added_at > cutoff_date_aware:
              track = item['track']
              if track and track.get('id'):
                # NOTE(jeroen-meijer): Reconstruct full track data from album ID reference
                if track.get('album_id'):
                  cached_album = cache.get_album(track['album_id'])
                  if cached_album:
                    track['album'] = cached_album['data']
                  else:
                    # Fallback if album not found in cache
                    track['album'] = {'release_date': None}
                else:
                  track['album'] = {'release_date': None}

                track['source'] = f'playlist:{playlist_id}'
                recent_tracks.append(track)

          print(
            f"       ✅ Found {len(recent_tracks)} recent tracks from cache (from {len(cached_tracks)} total tracks)")
          all_tracks.extend(recent_tracks)
          continue

      except Exception as e:
        print(
          f"     📜 Processing playlist: {playlist_id} (could not fetch info: {e})")
        playlist_name = f"Playlist {playlist_id}"
        current_snapshot_id = None

      # If we reach here, we need to fetch tracks from Spotify
      print(f"       🔄 Fetching tracks from Spotify...")

      # Get playlist tracks with added_at field and album information
      tracks = exhaust_fetch(
          fetch=lambda offset, limit: sp.playlist_items(
              playlist_id,
              offset=offset,
              limit=limit,
              fields='items(added_at,track(id,uri,name,artists(name),album(id,name,release_date,label))),next'
          ),
          map_elements=lambda res: res['items']
      )

      # Collect album requests for centralized fetching
      for item in tracks:
        if not item or not item.get('track'):
          continue

        track = item['track']
        if track and track.get('id') and track.get('album') and track['album'].get('id'):
          album_id = track['album']['id']
          track_id = track['id']

          # Add to centralized album fetch manager
          album_manager.add_album_request(
            album_id, track_id, f'playlist:{playlist_id}')

      # Filter tracks added after cutoff date
      recent_tracks = []
      for item in tracks:
        if not item or not item.get('track'):
          continue

        added_at = datetime.datetime.fromisoformat(
            item['added_at'].replace('Z', '+00:00'))
        # Make cutoff_date timezone-aware for comparison
        cutoff_date_aware = cutoff_date.replace(tzinfo=datetime.timezone.utc)

        if added_at > cutoff_date_aware:
          track = item['track']
          if track and track.get('id'):
            track['source'] = f'playlist:{playlist_id}'
            recent_tracks.append(track)

      # NOTE(jeroen-meijer): Convert tracks to use album ID references instead of full album data
      processed_tracks = []
      for item in tracks:
        if not item or not item.get('track'):
          continue

        track = item['track']
        if track and track.get('id'):
          # Create a copy of the track with album ID reference instead of full album data
          processed_track = {
            'id': track['id'],
            'uri': track['uri'],
            'name': track['name'],
            'artists': track.get('artists', []),
            'album_id': track['album']['id'] if track.get('album') and track['album'].get('id') else None,
            'source': f'playlist:{playlist_id}'
          }
          processed_tracks.append({
            'track': processed_track,
            'added_at': item['added_at']
          })

      print(
        f"       ✅ Found {len(recent_tracks)} recent tracks (from {len(tracks)} total tracks)")

      # Cache the playlist data if we have a snapshot ID
      if current_snapshot_id:
        playlist_data = {
          'name': playlist_name,
          'tracks': processed_tracks
        }
        cache.set_playlist(playlist_id, playlist_data, current_snapshot_id)
        print(
          f"       💾 Cached playlist data (snapshot: {current_snapshot_id})")

      all_tracks.extend(recent_tracks)

    except Exception as e:
      print(f"   ⚠️  Error processing playlist {playlist_id}: {e}")

  return all_tracks


def get_artist_tracks(sp: spotipy.Spotify, artist_ids: List[str], cutoff_date: datetime.datetime, cache: CrawlCache, album_manager: AlbumFetchManager) -> List[Dict[str, Any]]:
  """Get tracks from artists that were released after the cutoff date."""
  all_tracks = []

  for artist_id in artist_ids:
    try:
      # Get artist info to show the name (no caching since artists can release new music)
      try:
        artist_info = sp.artist(artist_id)
        if artist_info is None:
          artist_name = 'Unknown Artist'
        else:
          artist_name = artist_info.get('name', 'Unknown Artist')
        print(f"     🎤 Processing artist: {artist_name} ({artist_id})")
      except Exception as e:
        print(
          f"     🎤 Processing artist: {artist_id} (could not fetch name: {e})")
        artist_name = f"Artist {artist_id}"

      # Get artist's albums and singles
      albums = sp.artist_albums(
          artist_id,
          include_groups='album,single',
          limit=50
      )

      if albums is None or 'items' not in albums:
        print(f"       ⚠️  No albums found for artist {artist_name}")
        continue

      # Pre-filter albums by release date before fetching tracks
      recent_albums = []
      for album in albums['items']:
        if album and album.get('release_date'):
          try:
            release_date = parse_release_date(album['release_date'])
            if release_date > cutoff_date:
              recent_albums.append(album)
          except ValueError as e:
            print(
              f"         ⚠️  Warning: Could not parse release date '{album['release_date']}' for album '{album.get('name', 'Unknown')}': {e}")
            continue

      print(f"       📀 Found {len(recent_albums)} recent albums")

      # Collect album requests for centralized fetching
      for album in recent_albums:
        if album.get('id'):
          # Add to centralized album fetch manager
          album_manager.add_album_request(
            album['id'], f'artist_album:{album["id"]}', f'artist:{artist_id}')

      # Get tracks from recent albums
      for album in recent_albums:
        try:
          print(
            f"         🎵 Processing album: {album.get('name', 'Unknown')} ({album.get('release_date', 'Unknown date')})")

          # Get album tracks
          album_tracks = sp.album_tracks(album['id'])

          if album_tracks and 'items' in album_tracks:
            for track in album_tracks['items']:
              if track:
                try:
                  release_date = parse_release_date(album['release_date'])

                  # Add track-to-album mapping to centralized manager
                  if track.get('id') and album.get('id'):
                    album_manager.add_album_request(
                      album['id'], track['id'], f'artist:{artist_id}')

                  all_tracks.append({
                      'id': track['id'],
                      'uri': track['uri'],
                      'name': track['name'],
                      'artists': [artist['name'] for artist in track['artists']] if track.get('artists') else [],
                      'album_release_date': album['release_date'],
                      'added_at': release_date,
                      'source': f'artist:{artist_id}'
                  })
                except ValueError as e:
                  print(
                    f"           ⚠️  Warning: Could not parse release date for track '{track.get('name', 'Unknown')}': {e}")
                  continue

        except Exception as e:
          print(f"         ⚠️  Error processing album {album['id']}: {e}")

      artist_track_count = len(
        [t for t in all_tracks if t['source'] == f'artist:{artist_id}'])
      print(
        f"       ✅ Found {artist_track_count} tracks from artist {artist_name}")

    except Exception as e:
      print(f"   ⚠️  Error processing artist {artist_id}: {e}")

  return all_tracks


def get_label_tracks(sp: spotipy.Spotify, label_ids: List[str], cutoff_date: datetime.datetime, cache: CrawlCache, album_manager: AlbumFetchManager) -> List[Dict[str, Any]]:
  """Get tracks from labels that were released after the cutoff date."""
  all_tracks = []

  for label_id in label_ids:
    try:
      # For labels, the ID is actually the label name since we're using names in the config
      label_name = label_id
      print(f"     🏷️  Processing label: {label_name}")

      # Search for tracks by label
      search_query = f'label:"{label_name}"'
      print(f"       🔍 Searching for: {search_query}")
      search_results = sp.search(
          q=search_query,
          type='track',
          limit=50
      )

      if search_results and 'tracks' in search_results and 'items' in search_results['tracks']:
        # Collect album requests for centralized fetching
        recent_tracks = []
        mismatched_labels = set()  # Track labels that don't match for reporting

        for track in search_results['tracks']['items']:
          if track and track.get('album') and track['album'].get('release_date'):
            try:
              release_date = parse_release_date(track['album']['release_date'])
              if release_date > cutoff_date:
                # NOTE(jeroen-meijer): Validate that the track's actual label matches our search term
                # This prevents false matches like "ARIES" appearing when searching for "Aquario"
                track_label = track['album'].get('label', '')
                label_match_ratio = fuzz.ratio(
                  label_name.lower(), track_label.lower())

                # Use a high confidence threshold (90%) to ensure accurate matches
                if label_match_ratio >= 90:
                  # Add to centralized album fetch manager
                  if track.get('album') and track['album'].get('id'):
                    album_id = track['album']['id']
                    track_id = track['id']
                    album_manager.add_album_request(
                      album_id, track_id, f'label:{label_id}')

                  recent_tracks.append({
                      'id': track['id'],
                      'uri': track['uri'],
                      'name': track['name'],
                      'artists': [artist['name'] for artist in track['artists']] if track.get('artists') else [],
                      'album_release_date': track['album']['release_date'],
                      'added_at': release_date,
                      'source': f'label:{label_id}'
                  })
                else:
                  # Track mismatched labels for reporting
                  if track_label and track_label not in mismatched_labels:
                    mismatched_labels.add(track_label)
                    print(
                      f"         ⚠️  Skipping track '{track.get('name', 'Unknown')}' - label mismatch: '{track_label}' (match: {label_match_ratio}%)")

            except ValueError as e:
              print(
                f"         ⚠️  Warning: Could not parse release date '{track['album']['release_date']}' for track '{track.get('name', 'Unknown')}': {e}")
              continue

        print(
          f"       ✅ Found {len(recent_tracks)} recent tracks from label {label_name}")
        if mismatched_labels:
          print(
            f"       ⚠️  Skipped tracks from mismatched labels: {', '.join(sorted(mismatched_labels))}")
        all_tracks.extend(recent_tracks)
      else:
        print(f"       ⚠️  No search results found for label {label_name}")

    except Exception as e:
      print(f"   ⚠️  Error processing label {label_id}: {e}")

  return all_tracks


DEDUPE_ON_ID = 'on_id'
DEDUPE_ON_MATCH = 'on_match'


def deduplicate_tracks(tracks: List[Dict[str, Any]], method: str = DEDUPE_ON_ID) -> List[Dict[str, Any]]:
  """Remove duplicate tracks based on the specified method.

  Args:
    tracks: List of track dictionaries
    method: Deduplication method - DEDUPE_ON_ID or DEDUPE_ON_MATCH
      - DEDUPE_ON_ID: Remove tracks with duplicate IDs (original behavior)
      - DEDUPE_ON_MATCH: Remove tracks with identical artist names and track titles
  """
  if method == DEDUPE_ON_ID:
    return _deduplicate_by_id(tracks)
  elif method == DEDUPE_ON_MATCH:
    return _deduplicate_by_match(tracks)
  else:
    raise ValueError(f"Unknown deduplication method: {method}")


def _deduplicate_by_id(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """Remove duplicate tracks based on track ID."""
  seen_ids = set()
  unique_tracks = []

  for track in tracks:
    if track and track.get('id') and track['id'] not in seen_ids:
      seen_ids.add(track['id'])
      unique_tracks.append(track)

  return unique_tracks


def _deduplicate_by_match(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """Remove duplicate tracks based on matching artist names and track titles."""
  seen_matches = set()
  unique_tracks = []

  for track in tracks:
    if not track:
      continue

    # Get track name
    track_name = track.get('name', '').strip().lower()
    if not track_name:
      continue

    # Get artist names - handle both single artist and multiple artists
    artists = track.get('artists', [])
    if not artists:
      continue

    # Create a sorted list of artist names for consistent matching
    artist_names = []
    for artist in artists:
      if isinstance(artist, dict) and 'name' in artist:
        artist_names.append(artist['name'].strip().lower())
      elif isinstance(artist, str):
        artist_names.append(artist.strip().lower())

    if not artist_names:
      continue

    # Sort artist names for consistent matching regardless of order
    artist_names.sort()
    artists_key = '|'.join(artist_names)

    # Create match key combining artists and track name
    match_key = f"{artists_key}::{track_name}"

    if match_key not in seen_matches:
      seen_matches.add(match_key)
      unique_tracks.append(track)

  return unique_tracks


def generate_playlist_name(template: str, job: Optional[Dict[str, Any]] = None, cutoff_date: Optional[datetime.datetime] = None, track_count: int = 0, all_tracks: Optional[List[Dict[str, Any]]] = None, custom_end_date: Optional[datetime.datetime] = None, custom_start_date: Optional[datetime.datetime] = None) -> str:
  """Generate playlist name with template variables."""
  # Ensure all_tracks is a list, not None
  if all_tracks is None:
    all_tracks = []

  # Use custom end date if provided, otherwise use current date
  now = custom_end_date if custom_end_date else datetime.datetime.now()
  real_now = datetime.datetime.now()

  # Calculate date range
  # Since filtering uses > cutoff_date, actual start is cutoff_date + 1 day
  if custom_start_date:
    date_range_start = custom_start_date
    date_range_end = now
  else:
    date_range_start = (cutoff_date + datetime.timedelta(days=1)
                        ) if cutoff_date else now - datetime.timedelta(days=7)
    date_range_end = now

  # Inclusive count of days in the date range
  date_range_days = (date_range_end - date_range_start).days + 1

  # Get job information
  job_name = job.get('name', 'unknown') if job else 'unknown'
  job_name_pretty = job_name.replace('_', ' ').title() if job else 'Unknown'

  # Calculate source counts
  playlist_count = 0
  artist_count = 0
  label_count = 0
  input_sources = []
  input_playlists = []
  input_artists = []
  input_labels = []

  if job and 'inputs' in job:
    inputs = job['inputs']
    # Ensure we get lists, not None
    playlists = inputs.get('playlists', []) or []
    artists = inputs.get('artists', []) or []
    labels = inputs.get('labels', []) or []
    playlist_count = len(playlists)
    artist_count = len(artists)
    label_count = len(labels)

    # Build source lists (simplified - would need actual names from processing)
    if playlist_count > 0:
      input_playlists.append(
        f"{playlist_count} playlist{'s' if playlist_count > 1 else ''}")
    if artist_count > 0:
      input_artists.append(
        f"{artist_count} artist{'s' if artist_count > 1 else ''}")
    if label_count > 0:
      input_labels.append(
        f"{label_count} label{'s' if label_count > 1 else ''}")

    input_sources = input_playlists + input_artists + input_labels

  # Replace template variables
  name = template

  # Date and time variables
  name = name.replace('{week_num}', str(get_week_number(now)))
  name = name.replace('{date}', now.strftime('%Y-%m-%d'))
  name = name.replace('{month}', now.strftime('%B'))
  name = name.replace('{year}', str(now.year))
  name = name.replace('{timestamp}', str(int(now.timestamp())))

  # Enhanced date variables
  name = name.replace('{month_num}', now.strftime('%m'))
  name = name.replace('{month_name}', now.strftime('%B'))
  name = name.replace('{month_name_short}', now.strftime('%b'))
  name = name.replace('{quarter}', f"Q{(now.month - 1) // 3 + 1}")
  name = name.replace('{year_short}', str(now.year)[-2:])

  # Week variables
  week_start = now - datetime.timedelta(days=now.weekday())
  week_end = week_start + datetime.timedelta(days=6)
  name = name.replace('{week_start_date}', week_start.strftime('%Y-%m-%d'))
  name = name.replace('{week_end_date}', week_end.strftime('%Y-%m-%d'))

  # Date range variables
  name = name.replace('{date_range_start_date}',
                      date_range_start.strftime('%Y-%m-%d'))
  name = name.replace('{date_range_end_date}',
                      date_range_end.strftime('%Y-%m-%d'))
  name = name.replace('{date_range_days}', str(date_range_days))
  name = name.replace('{date_range_start_short}',
                      date_range_start.strftime('%b %d'))
  name = name.replace('{date_range_end_short}',
                      date_range_end.strftime('%b %d'))

  # Cross-month range
  if date_range_start.month == date_range_end.month:
    date_range_month = date_range_start.strftime('%B %Y')
    name = name.replace('{date_range_month}', date_range_month)
    name = name.replace('{date_range_cross_month}', date_range_month)
  else:
    name = name.replace(
      '{date_range_month}', f"{date_range_start.strftime('%B')} - {date_range_end.strftime('%B')} {date_range_end.year}")
    name = name.replace('{date_range_cross_month}',
                        f"{date_range_start.strftime('%b %d')} - {date_range_end.strftime('%b %d')}")

  # Format variables
  name = name.replace('{date_format_YYYY_MM_DD}', now.strftime('%Y-%m-%d'))
  name = name.replace('{date_format_DD_MM_YYYY}', now.strftime('%d-%m-%Y'))
  name = name.replace('{date_format_MMM_DD}', now.strftime('%b %d'))
  name = name.replace('{time_format_HH_MM}', now.strftime('%H:%M'))

  # Real date variables that always reflect the actual current date/time
  name = name.replace('{real_date}', real_now.strftime('%Y-%m-%d'))
  name = name.replace('{real_year}', str(real_now.year))
  name = name.replace('{real_month}', real_now.strftime('%B'))
  name = name.replace('{real_month_num}', real_now.strftime('%m'))
  name = name.replace('{real_year_short}', str(real_now.year)[-2:])
  name = name.replace('{real_week_num}', str(get_week_number(real_now)))
  name = name.replace('{real_day}', real_now.strftime('%d'))
  name = name.replace('{real_day_name}', real_now.strftime('%A'))
  name = name.replace('{real_day_name_short}', real_now.strftime('%a'))
  name = name.replace('{real_timestamp}', str(int(real_now.timestamp())))
  real_week_start = real_now - datetime.timedelta(days=real_now.weekday())
  real_week_end = real_week_start + datetime.timedelta(days=6)
  name = name.replace('{real_week_start_date}',
                      real_week_start.strftime('%Y-%m-%d'))
  name = name.replace('{real_week_end_date}',
                      real_week_end.strftime('%Y-%m-%d'))

  # Content statistics
  name = name.replace('{track_count}', str(track_count))
  name = name.replace('{source_count}', str(
    playlist_count + artist_count + label_count))
  name = name.replace('{playlist_count}', str(playlist_count))
  name = name.replace('{artist_count}', str(artist_count))
  name = name.replace('{label_count}', str(label_count))

  # Job and configuration variables
  name = name.replace('{job_name}', job_name)
  name = name.replace('{job_name_pretty}', job_name_pretty)
  name = name.replace('{input_sources}', ', '.join(
    input_sources) if input_sources else 'various sources')
  name = name.replace('{input_playlists}', ', '.join(
    input_playlists) if input_playlists else 'no playlists')
  name = name.replace('{input_artists}', ', '.join(
    input_artists) if input_artists else 'no artists')
  name = name.replace('{input_labels}', ', '.join(
    input_labels) if input_labels else 'no labels')

  return name


def print_optimization_stats(artist_manager: ArtistFetchManager, label_manager: LabelFetchManager, playlist_manager: PlaylistFetchManager):
  """Print statistics about the optimization benefits."""
  print("📊 Optimization Statistics:")
  print("=" * 50)

  # Artist optimization stats
  if artist_manager.artist_requests:
    total_artist_requests = sum(
      len(requests) for requests in artist_manager.artist_requests.values())
    unique_artists = len(artist_manager.artist_requests)
    artist_savings = total_artist_requests - unique_artists
    print(
      f"🎤 Artists: {total_artist_requests} requests → {unique_artists} API calls (saved {artist_savings} calls)")
  else:
    print("🎤 Artists: No artist requests")

  # Label optimization stats
  if label_manager.label_requests:
    total_label_requests = sum(len(requests)
                               for requests in label_manager.label_requests.values())
    unique_labels = len(label_manager.label_requests)
    label_savings = total_label_requests - unique_labels
    print(
      f"🏷️  Labels: {total_label_requests} requests → {unique_labels} API calls (saved {label_savings} calls)")
  else:
    print("🏷️  Labels: No label requests")

  # Playlist optimization stats
  if playlist_manager.playlist_requests:
    total_playlist_requests = sum(
      len(requests) for requests in playlist_manager.playlist_requests.values())
    unique_playlists = len(playlist_manager.playlist_requests)
    playlist_savings = total_playlist_requests - unique_playlists
    print(
      f"📜 Playlists: {total_playlist_requests} requests → {unique_playlists} API calls (saved {playlist_savings} calls)")
  else:
    print("📜 Playlists: No playlist requests")

  # Total savings
  total_requests = (
    sum(len(requests) for requests in artist_manager.artist_requests.values()) +
    sum(len(requests) for requests in label_manager.label_requests.values()) +
    sum(len(requests)
        for requests in playlist_manager.playlist_requests.values())
  )
  total_unique = (
    len(artist_manager.artist_requests) +
    len(label_manager.label_requests) +
    len(playlist_manager.playlist_requests)
  )
  total_savings = total_requests - total_unique

  if total_savings > 0:
    print(
      f"💾 Total: {total_requests} requests → {total_unique} API calls (saved {total_savings} calls)")
    print(
      f"📈 Efficiency improvement: {((total_savings / total_requests) * 100):.1f}%")
  else:
    print("💾 Total: No optimization benefits (no duplicate requests)")

  print()


def get_week_number(date: datetime.datetime) -> int:
  """Get the week number of the year."""
  return date.isocalendar()[1]
