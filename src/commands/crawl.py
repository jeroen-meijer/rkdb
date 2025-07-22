import datetime
import yaml
import spotipy
from typing import List, Dict, Any, Set, Optional
from services import setup_spotify
from functions import exhaust_fetch


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
  inputs = job.get('inputs', {})
  has_playlists = bool(inputs.get('playlists', []))
  has_artists = bool(inputs.get('artists', []))
  has_labels = bool(inputs.get('labels', []))

  if not (has_playlists or has_artists or has_labels):
    print(
      f"❌ Error: Job '{job_name}' must have at least one input source (playlists, artists, or labels)")
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


def crawl_spotify_playlists():
  """
  Crawl Spotify playlists, artists, and labels based on YAML configuration.
  Creates new playlists with tracks added within the specified time window.
  """
  print("🎵 Spotify Playlist Crawler")
  print("=" * 50)

  # Load configuration
  try:
    with open('crawl_config.yaml', 'r') as file:
      config = yaml.safe_load(file)
  except FileNotFoundError:
    print("❌ Error: crawl_config.yaml not found")
    return
  except yaml.YAMLError as e:
    print(f"❌ Error parsing YAML: {e}")
    return

  # Setup Spotify
  sp = setup_spotify()

  # Get current user
  try:
    user = sp.current_user()
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

  # Collect all job reports
  all_reports = []

  for job in jobs:
    report = process_job(sp, job, config)
    if report:
      all_reports.append(report)

  # Save combined report if any jobs were processed
  if all_reports:
    save_combined_crawl_report(all_reports)
  else:
    print("📊 No jobs were processed successfully, no report generated")


def process_job(sp: spotipy.Spotify, job: Dict[str, Any], config: Dict[str, Any]):
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
  current_time = datetime.datetime.now()
  cutoff_date = current_time - datetime.timedelta(days=days_back)
  print(
    f"📅 Looking for tracks added/released after: {cutoff_date.strftime('%Y-%m-%d')}")

  # Collect all tracks
  all_tracks = []

  # Process playlists
  playlist_ids = resolve_references(inputs.get('playlists', []), config)
  if playlist_ids:
    print(f"📜 Processing {len(playlist_ids)} playlist(s)...")
    playlist_tracks = get_playlist_tracks(sp, playlist_ids, cutoff_date)
    all_tracks.extend(playlist_tracks)
    print(f"   Found {len(playlist_tracks)} tracks from playlists")

  # Process artists
  artist_ids = resolve_references(inputs.get('artists', []), config)
  if artist_ids:
    print(f"🎤 Processing {len(artist_ids)} artist(s)...")
    artist_tracks = get_artist_tracks(sp, artist_ids, cutoff_date)
    all_tracks.extend(artist_tracks)
    print(f"   Found {len(artist_tracks)} tracks from artists")

  # Process labels
  label_ids = resolve_references(inputs.get('labels', []), config)
  if label_ids:
    print(f"🏷️  Processing {len(label_ids)} label(s)...")
    label_tracks = get_label_tracks(sp, label_ids, cutoff_date)
    all_tracks.extend(label_tracks)
    print(f"   Found {len(label_tracks)} tracks from labels")

  if not all_tracks:
    print("❌ No tracks found for this job")
    print()
    return None

  # Deduplicate tracks if requested
  if options.get('deduplicate', True):
    unique_tracks = deduplicate_tracks(all_tracks)
    print(f"🔄 Deduplicated: {len(all_tracks)} → {len(unique_tracks)} tracks")
    all_tracks = unique_tracks

  # Create playlist
  playlist_name = generate_playlist_name(
    output_playlist.get('name', 'Generated Playlist'), job, cutoff_date, len(all_tracks), all_tracks)
  playlist_description = generate_playlist_name(
    output_playlist.get('description', 'Generated by Spotify Crawler'), job, cutoff_date, len(all_tracks), all_tracks)
  is_public = output_playlist.get('public', False)

  print(f"📝 Creating playlist: {playlist_name}")

  try:
    # Get current user for playlist creation
    current_user = sp.current_user()
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
      track_uris = [track['uri'] for track in all_tracks]

      # Add tracks in batches of 100 (Spotify API limit)
      for i in range(0, len(track_uris), 100):
        batch = track_uris[i:i + 100]
        sp.playlist_add_items(playlist['id'], batch)

      print(f"✅ Added {len(all_tracks)} tracks to playlist")
      if playlist.get('external_urls') and playlist['external_urls'].get('spotify'):
        print(f"🔗 Playlist URL: {playlist['external_urls']['spotify']}")
    else:
      print("⚠️  No tracks to add to playlist")

    # Create crawl report
    report = create_crawl_report(job, all_tracks, cutoff_date)
    return report

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


def get_playlist_tracks(sp: spotipy.Spotify, playlist_ids: List[str], cutoff_date: datetime.datetime) -> List[Dict[str, Any]]:
  """Get tracks from playlists that were added after the cutoff date."""
  all_tracks = []

  for playlist_id in playlist_ids:
    try:
      # Get playlist info to show the name
      try:
        playlist_info = sp.playlist(playlist_id, fields='name')
        if playlist_info is None:
          playlist_name = 'Unknown Playlist'
        else:
          playlist_name = playlist_info.get('name', 'Unknown Playlist')
        print(f"     📜 Processing playlist: {playlist_name} ({playlist_id})")
      except Exception as e:
        print(
          f"     📜 Processing playlist: {playlist_id} (could not fetch name: {e})")
        playlist_name = f"Playlist {playlist_id}"

      # Get playlist tracks with added_at field
      tracks = exhaust_fetch(
          fetch=lambda offset, limit: sp.playlist_items(
              playlist_id,
              offset=offset,
              limit=limit,
              fields='items(added_at,track(id,uri,name,artists(name),album(release_date))),next'
          ),
          map_elements=lambda res: res['items']
      )

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

      print(
        f"       ✅ Found {len(recent_tracks)} recent tracks (from {len(tracks)} total tracks)")
      all_tracks.extend(recent_tracks)

    except Exception as e:
      print(f"   ⚠️  Error processing playlist {playlist_id}: {e}")

  return all_tracks


def get_artist_tracks(sp: spotipy.Spotify, artist_ids: List[str], cutoff_date: datetime.datetime) -> List[Dict[str, Any]]:
  """Get tracks from artists that were released after the cutoff date."""
  all_tracks = []

  for artist_id in artist_ids:
    try:
      # Get artist info to show the name
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

      # Filter albums released after cutoff date
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

      # Get tracks from recent albums
      for album in recent_albums:
        try:
          print(
            f"         🎵 Processing album: {album.get('name', 'Unknown')} ({album.get('release_date', 'Unknown date')})")
          album_tracks = sp.album_tracks(album['id'])

          if album_tracks and 'items' in album_tracks:
            for track in album_tracks['items']:
              if track:
                try:
                  release_date = parse_release_date(album['release_date'])
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


def get_label_tracks(sp: spotipy.Spotify, label_ids: List[str], cutoff_date: datetime.datetime) -> List[Dict[str, Any]]:
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
        # Filter tracks released after cutoff date
        recent_tracks = []
        for track in search_results['tracks']['items']:
          if track and track.get('album') and track['album'].get('release_date'):
            try:
              release_date = parse_release_date(track['album']['release_date'])
              if release_date > cutoff_date:
                recent_tracks.append({
                    'id': track['id'],
                    'uri': track['uri'],
                    'name': track['name'],
                    'artists': [artist['name'] for artist in track['artists']] if track.get('artists') else [],
                    'album_release_date': track['album']['release_date'],
                    'added_at': release_date,
                    'source': f'label:{label_id}'
                })
            except ValueError as e:
              print(
                f"         ⚠️  Warning: Could not parse release date '{track['album']['release_date']}' for track '{track.get('name', 'Unknown')}': {e}")
              continue

        print(
          f"       ✅ Found {len(recent_tracks)} recent tracks from label {label_name}")
        all_tracks.extend(recent_tracks)
      else:
        print(f"       ⚠️  No search results found for label {label_name}")

    except Exception as e:
      print(f"   ⚠️  Error processing label {label_id}: {e}")

  return all_tracks


def deduplicate_tracks(tracks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """Remove duplicate tracks based on track ID."""
  seen_ids = set()
  unique_tracks = []

  for track in tracks:
    if track and track.get('id') and track['id'] not in seen_ids:
      seen_ids.add(track['id'])
      unique_tracks.append(track)

  return unique_tracks


def generate_playlist_name(template: str, job: Optional[Dict[str, Any]] = None, cutoff_date: Optional[datetime.datetime] = None, track_count: int = 0, all_tracks: Optional[List[Dict[str, Any]]] = None) -> str:
  """Generate playlist name with template variables."""
  now = datetime.datetime.now()

  # Calculate date range if cutoff_date is provided
  date_range_start = cutoff_date if cutoff_date else now - \
      datetime.timedelta(days=7)
  date_range_end = now
  date_range_days = (date_range_end - date_range_start).days

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
    playlist_count = len(inputs.get('playlists', []))
    artist_count = len(inputs.get('artists', []))
    label_count = len(inputs.get('labels', []))

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


def get_week_number(date: datetime.datetime) -> int:
  """Get the week number of the year."""
  return date.isocalendar()[1]
