import datetime
import sys
import time
import threading
import queue
import concurrent.futures
import constants
import deepmerge
import humanfriendly
import iGetMusic as iGet
import pyrekordbox as r
from typing import List
from db import get_custom_tracks_db, get_missing_tracks_db, get_track_id_db, get_track_id_overrides_db, save_sync_report, set_missing_tracks_db, set_track_id_db
from functions import attempt_get_key, ensure_custom_track_schema, ensure_track_db_schema, exhaust_fetch, find_best_match, find_track, first_or_none, generate_itunes_store_url
from services import get_user_or_sign_in, setup_rekordbox, setup_spotify
from requests import JSONDecodeError
from collections import namedtuple
import traceback

CustomTrack = namedtuple('CustomTrack', ['rekordbox_id', 'index', 'target'])

# NOTE(jeroen-meijer): Structure to hold fetched playlist data for background processing
PlaylistData = namedtuple('PlaylistData', ['playlist', 'items', 'error'])


def blue_log(message: str):
  """Log message in blue (background fetching activities)"""
  print(f"\033[94m🧵 {message}\033[0m")


def orange_log(message: str):
  """Log message in orange (main thread waiting/blocking activities)"""
  print(f"\033[93m⏳ {message}\033[0m")

# Notes
# Typical spotify playlist link: https://open.spotify.com/playlist/1UObZqUr1MtbveqsSw6sFP?si=5d14331bb8174c1e
# Everything after the last slash is the playlist ID, until and not including the question mark (if any)
# We will want to parse the playlist ID from the URL
# The custom_playlist_ids arg may contain any number of playlist IDs _or_ playlist URLs.
# The URLs need to be parsed to get the playlist ID, and this new list should override the custom_playlist_ids list.


def fetch_playlist_tracks(sp, sp_playlist) -> PlaylistData:
  """
  Fetch tracks for a single playlist.
  Returns PlaylistData with playlist, items (or None if error), and error info.
  """
  try:
    sp_playlist_items = exhaust_fetch(
      fetch=lambda offset, limit: sp.playlist_items(
        sp_playlist['id'],
        offset=offset,
        limit=limit,
        fields='items(added_at,track(id,artists,name)),next'
      ),
      # For each res, get the items (preserving both track and added_at)
      map_elements=lambda res: res['items']
    )
    return PlaylistData(playlist=sp_playlist, items=sp_playlist_items, error=None)
  except Exception as e:
    return PlaylistData(playlist=sp_playlist, items=None, error=e)


def playlist_fetcher_worker(sp, all_playlists, results_queue):
  """
  Background worker thread that fetches all playlist tracks.
  Gets all playlists upfront and manages internal batching (max 3 concurrent).
  """
  blue_log(f"Background fetcher starting with {len(all_playlists)} playlists")

  # Use ThreadPoolExecutor to manage up to 3 concurrent fetches
  with concurrent.futures.ThreadPoolExecutor(max_workers=3, thread_name_prefix="PlaylistFetch") as executor:
    try:
      # Submit all playlists for fetching
      future_to_playlist = {
        executor.submit(fetch_playlist_with_logging, sp, playlist): playlist
        for playlist in all_playlists
      }

      # Process results as they complete (in completion order, not submission order)
      for future in concurrent.futures.as_completed(future_to_playlist):
        playlist = future_to_playlist[future]
        try:
          result = future.result()
          results_queue.put(result)
        except Exception as e:
          blue_log(f"❌ Background worker error for \"{playlist['name']}\": {e}")
          error_result = PlaylistData(playlist=playlist, items=None, error=e)
          results_queue.put(error_result)

    except Exception as e:
      blue_log(f"❌ Critical error in background fetcher: {e}")

  blue_log("Background fetcher completed all playlists")


def fetch_playlist_with_logging(sp, sp_playlist) -> PlaylistData:
  """
  Fetch tracks for a single playlist with logging.
  Used by the background worker.
  """
  blue_log(f"Fetching tracks for playlist: \"{sp_playlist['name']}\"...")

  result = fetch_playlist_tracks(sp, sp_playlist)

  if result.error:
    blue_log(f"❌ Failed to fetch \"{sp_playlist['name']}\": {result.error}")
  else:
    blue_log(
      f"✅ Fetched {len(result.items)} tracks for \"{sp_playlist['name']}\"")

  return result


def sync_spotify_playlists_to_rekordbox(custom_playlist_ids: List[str] = []):
  custom_playlist_ids = list(
    map(lambda id: id.split('/')[-1].split('?')[0], custom_playlist_ids))

  rb = setup_rekordbox()
  sp = setup_spotify()

  track_id_db = ensure_track_db_schema(get_track_id_db())
  id_overrides_db = ensure_track_db_schema(get_track_id_overrides_db())
  track_id_db = deepmerge.always_merger.merge(track_id_db, id_overrides_db)
  missing_tracks_db = get_missing_tracks_db()
  custom_tracks_db = ensure_custom_track_schema(get_custom_tracks_db())

  missing_track_count = len(missing_tracks_db)
  missing_tracks_without_ignore_count = len(
    list(
      filter(
        lambda track: not track.get('ignored', False),
        missing_tracks_db.values()
      )
    )
  )

  print(f"Missing tracks before sync: {missing_track_count} ({
        missing_tracks_without_ignore_count} unignored)")

  print('Fetching Rekordbox playlists...')
  rb_playlists = list(rb.get_playlist() or [])
  print('Fetching Rekordbox tracks...')
  rb_all_tracks: List[r.db6.tables.DjmdContent] = list(filter(
    lambda track: track.Title != None and track.Artist != None, rb.get_content() or []))
  print('Fetching Rekordbox keys...')
  camelot_key_starts = tuple(str(n + 1) for n in range(12))
  rb_camelot_keys: dict[str, r.db6.tables.DjmdKey] = {k.ScaleName.upper(
  ): k for k in list(rb.get_key() or []) if k.ScaleName.startswith(camelot_key_starts)}

  if (len(rb_camelot_keys) != 24):
    raise ValueError(f"Expected 24 keys but found {len(rb_camelot_keys)}: {
                     list(map(lambda k: k.ScaleName, rb_camelot_keys.values()))}")

  print("Logging into Spotify...")
  sp_user = get_user_or_sign_in(sp)
  if sp_user is None:
    raise ValueError("Failed to get Spotify user information")
  print(f"Logged in as {sp_user['display_name']} ('{sp_user['id']}')")

  print("Fetching Spotify playlists...")
  sp_all_playlists = exhaust_fetch(
    fetch=lambda offset, limit: sp.current_user_playlists(
      offset=offset,
      limit=limit
    ),
    map_elements=lambda res: res['items'],
  )
  sp_all_playlists = list(
    filter(lambda playlist: playlist != None, sp_all_playlists))

  print(f"Found {len(sp_all_playlists)} playlist(s)")

  # Filter out playlists so that only playlists are retained that:
  # - start with one of the prefixes defined in constants.SPOTIFY_PLAYLIST_PREFIXES
  # - have a name that is fully equal to one of the playlists defined in constants.SPOTIFY_PLAYLISTS
  sp_target_playlists = []

  if len(custom_playlist_ids) > 0:
    print(f"Filtering playlists by provided IDs...")
    for playlist_id in custom_playlist_ids:
      sp_playlist = first_or_none(
        filter(lambda playlist: playlist['id'] == playlist_id, sp_all_playlists))
      if sp_playlist != None:
        sp_target_playlists.append(sp_playlist)
        print(f"Playlist with ID {playlist_id} found: \"{
              sp_playlist['name']}\"")
      else:
        print(f"Playlist with ID {playlist_id} not found")
  else:
    print("Syncing following playlists:")
    print("  - Playlists starting with one of the following prefixes:")
    for prefix in constants.SPOTIFY_PLAYLIST_PREFIXES:
      print(f"    - \"{prefix}\"")
    print("  - Playlists with the following names:")
    for playlist_name in constants.SPOTIFY_PLAYLISTS:
      print(f"    - \"{playlist_name}\"")
    sp_target_playlists = list(filter(
      lambda playlist: any(
        map(lambda prefix: playlist['name'].startswith(prefix), constants.SPOTIFY_PLAYLIST_PREFIXES))
        or playlist['name'] in constants.SPOTIFY_PLAYLISTS,
      sp_all_playlists
    ))
  print(f"Syncing {len(sp_target_playlists)
                   } Spotify playlist(s) to Rekordbox...")

  itunes_rate_limit_reached = False

  def sync_playlist(sp_playlist, sp_playlist_items) -> dict:
    # A dict that maps the position of each track in the playlist (starting from 1)
    # to a dict containing the Spotify and Rekordbox track information.
    # If no rekordbox track was found, the value on the 'rekordbox' key will be None.
    playlist_sync_report = {
      'all_tracks': {},
      'missing_tracks': {
        'count': 0,
        'tracks': {},
      }
    }

    sp_playlist_name = sp_playlist['name']

    def log(message: str):
      def grey(text: str):
        return f"\033[90m{text}\033[0m"
      print(grey(f"[{sp_playlist_name}]") + f" {message}")

    start_datetime = datetime.datetime.now()

    log(f"Syncing playlist {sp_playlist_name}")

    sp_playlist_camelot_key = attempt_get_key(sp_playlist_name)
    rb_playlist_key: r.db6.tables.DjmdKey | None = rb_camelot_keys.get(
      sp_playlist_camelot_key, None) if sp_playlist_camelot_key is not None else None
    if rb_playlist_key != None:
      log(f"Detected camelot key: {rb_playlist_key.ScaleName}")

    log(f"Processing {len(sp_playlist_items)} tracks...")

    log(f"Creating playlist")
    rb_playlist_with_same_name = first_or_none(
      filter(lambda playlist: playlist.Name == sp_playlist_name, rb_playlists))
    if rb_playlist_with_same_name != None:
      log(f"Deleting existing playlist")
      rb.delete_playlist(rb_playlist_with_same_name)

    # Use playlist folder if playlist starts with a folder name followed by underscore.
    # The name of the playlist itself is not changed.
    # For example:
    #   FLOW_xxx -> FLOWS/FLOW_xxx
    #   SET_xxx -> SETS/SET_xxx
    #   MyPlaylist -> MyPlaylist
    playlist_folder_name: str | None = None
    playlist_name_parts = sp_playlist_name.split('_')
    if len(playlist_name_parts) > 1:
      potential_folder_name = playlist_name_parts[0]
      if potential_folder_name.isupper():
        playlist_folder_name = f"{potential_folder_name}S"

    rb_playlist: r.db6.tables.DjmdPlaylist | None = None

    if playlist_folder_name != None:
      playlist_folder = first_or_none(filter(
        lambda playlist: playlist.Name == playlist_folder_name and playlist.is_folder, list(rb.get_playlist() or [])))
      if playlist_folder == None:
        log(f"Creating playlist folder {playlist_folder_name}")
        playlist_folder = rb.create_playlist_folder(playlist_folder_name)
      rb_playlist = rb.create_playlist(sp_playlist_name, playlist_folder)
    else:
      rb_playlist = rb.create_playlist(sp_playlist_name)

    rb_playlist_song_queue: List[r.db6.tables.DjmdContent | None] = []

    for track_index in range(len(sp_playlist_items)):
      sp_playlist_item = sp_playlist_items[track_index]
      sp_track = sp_playlist_item['track']
      sp_track_added_at = sp_playlist_item['added_at']
      sp_track_id = sp_track['id']
      sp_track_artist_str = ', '.join(
        list(map(lambda artist: artist['name'], sp_track['artists'])))
      sp_track_name_str = sp_track['name']
      sp_track_full_str = f"{sp_track_artist_str} - {sp_track_name_str}"

      def attempt_add_track_to_missing_db():
        nonlocal itunes_rate_limit_reached

        existing_entry = missing_tracks_db.get(sp_track['id'], {})

        itunes_url: str | None = existing_entry.get('itunes_url', None)

        if itunes_url != None:
          log(f"  ├ 🛜 Found pre-existing iTunes URL: {itunes_url}")
        elif itunes_rate_limit_reached:
          log("  ├ ⏩ Skipping fetching iTunes URL due to rate limit")
        else:
          try:
            log(f"  ├ 🎧 Retrieving iTunes URL...")
            itunes_search_res: List[iGet.iGet.song] = list(filter(
              lambda content: content.kind == 'song', iGet.get(term=sp_track_full_str, country='NL')))
            itunes_song = find_best_match(
              sp_track_name_str, lambda song: song.trackName, itunes_search_res)
            itunes_url = generate_itunes_store_url(
              itunes_song) if itunes_song != None else None
            if itunes_url != None:
              log(f"  ├ 🛜 Found iTunes URL: {itunes_url}")
            else:
              log(f"  ├ ❔ No iTunes URL found")
          except Exception as e:
            if isinstance(e, JSONDecodeError) and e.args[0] == 'Expecting value: line 1 column 1 (char 0)':
              log(f"  ├ ❗️ iTunes rate limit reached")
              # we need to access the itunes_rate_limit_reached variable from the outer scope
              # so we need to declare it as nonlocal
              itunes_rate_limit_reached = True
            else:
              log(f"  ├ ❗️ Failed to retrieve iTunes URL. Error: {e}")
            log(f"  ├    Skipping...")
        log(f"  └ ➕ Adding track to missing tracks database...")

        # Use the playlist added_at date, or if track exists in multiple playlists, use the latest date
        existing_date_added = existing_entry.get('date_added', None)
        if existing_date_added:
          # Convert existing date to datetime for comparison
          try:
            existing_dt = datetime.datetime.fromisoformat(
              existing_date_added.replace('Z', '+00:00'))
            playlist_added_dt = datetime.datetime.fromisoformat(
              sp_track_added_at.replace('Z', '+00:00'))
            # Use the latest date between existing and current playlist
            final_date_added = max(existing_dt, playlist_added_dt).isoformat()
          except:
            # Fallback to playlist date if parsing fails
            final_date_added = sp_track_added_at
        else:
          # First time seeing this track, use playlist added_at date
          final_date_added = sp_track_added_at

        missing_tracks_db[sp_track['id']] = {
          'artist': sp_track_artist_str,
          'title': sp_track_name_str,
          'itunes_url': itunes_url,
          'ignored': False,
          'date_added': final_date_added
        }

      log(f"🔎 Searching for track:   [{sp_track_id}] \"{sp_track_full_str}\"")
      rb_track_id = track_id_db['content']['spotify'].get(sp_track_id, None)
      rb_track: r.db6.DjmdContent | None = first_or_none(filter(
        lambda track: track.ID == rb_track_id, rb_all_tracks)) if rb_track_id != None else None
      if rb_track != None:
        log(f"└ ✅ Found ID match:      {rb_track.ID}")
      else:
        search_results = find_track(
          {'artist': sp_track_artist_str, 'title': sp_track_name_str}, rb_all_tracks)
        top_match = search_results[0] if len(search_results) > 0 else None
        rb_track = top_match[0] if top_match != None else None
        if rb_track != None and top_match != None:
          match_percentage = top_match[1]
          log(f"└ ✅ Found closest match: \"{
              rb_track.ArtistName} - {rb_track.Title}\" ({match_percentage}%)")

          track_id_db['content']['spotify'][sp_track_id] = rb_track.ID

      if rb_track != None:
        missing_tracks_db.pop(sp_track_id, None)

        if rb_playlist_key != None:
          log(f"  └ Attaching key {rb_playlist_key.ScaleName}")
          rb_track.Key = rb_playlist_key

        rb_playlist_song_queue.append(rb_track)

      else:
        log(f"└ ❌ Could not find track \"{sp_track_full_str}\" in Rekordbox")
        if missing_tracks_db.get(sp_track_id, {}).get('ignored', False) == True:
          log(f"  └ 🚫 Track is ignored")
        else:
          attempt_add_track_to_missing_db()

      playlist_sync_report['all_tracks'][track_index + 1] = {
        'spotify': {
          'id': sp_track_id,
          'artist': sp_track_artist_str,
          'title': sp_track_name_str,
        },
        'rekordbox': ({
          'id': rb_track.ID,
          'artist': rb_track.ArtistName,
          'title': rb_track.Title,
        } if rb_track != None else None)
      }

      if rb_track == None:
        playlist_sync_report['missing_tracks']['count'] += 1
        playlist_sync_report['missing_tracks']['tracks'][track_index +
                                                         1] = {
          'id': sp_track_id,
          'artist': sp_track_artist_str,
          'title': sp_track_name_str,
        }

    # Get the custom_tracks_db entry for this playlist, otherwise empty list
    custom_tracks = custom_tracks_db.get('custom_tracks', {}).get('spotify', {}).get(
      sp_playlist['id'], [])
    if custom_tracks is None:
      custom_tracks = []

    has_custom_tracks = len(custom_tracks) > 0
    has_missing_tracks = playlist_sync_report['missing_tracks']['count'] > 0

    if not has_custom_tracks:
      log(f"  ⏩ No custom tracks found")
    else:
      playlist_sync_report['custom_tracks_included'] = True
      log(f"  🎨 Applying custom tracks...")
      tracks_to_insert = []
      tracks_to_replace = []
      for custom_track in custom_tracks:
        c_rb_id = str(custom_track['rekordbox_id'])
        if c_rb_id == None:
          log(f"  ❌ Skipping custom track with missing rekordbox ID")
          continue

        # Default to 'insert' if no type specified
        c_type = custom_track.get('type', 'insert')

        c_rb = first_or_none(
          filter(lambda track: track.ID == c_rb_id, rb_all_tracks))
        if c_rb == None:
          log(f"  ❌ Skipping custom track with unknown rekordbox ID {c_rb_id}")
          continue

        # Validate that only one position field is provided
        position_fields = []
        if custom_track.get('index') is not None:
          position_fields.append('index')
        if custom_track.get('offset') is not None:
          position_fields.append('offset')
        if custom_track.get('position') is not None:
          position_fields.append('position')

        if len(position_fields) > 1:
          raise ValueError(
            f"Custom track {c_rb_id} has multiple position fields specified: {position_fields}. Only one of 'index', 'offset', or 'position' can be used.")

        c_index = custom_track.get('index', None)
        if c_index == None:
          # The user may have used 'offset'
          c_index = custom_track.get('offset', None)
        if c_index == None:
          # The user may have used 'position' which is 1-based, so we need to subtract 1.
          c_index = custom_track.get('position', None)
          if c_index != None:
            c_index = c_index - 1

        c_target = custom_track.get('target', None)
        if c_target != None:
          c_target = str(c_target)

        c = CustomTrack(rekordbox_id=c_rb_id, index=c_index, target=c_target)

        if c_type == 'insert':
          # Always allow insert tracks, regardless of missing tracks
          tracks_to_insert.append(c)
        elif c_type == 'replace':
          # Always allow replace tracks, regardless of missing tracks
          tracks_to_replace.append(c)
        else:
          log(f"Skipping custom track with unknown type '{c_type}'")

      if len(tracks_to_insert) == 0 and len(tracks_to_replace) == 0:
        log(f"  ⏩ No custom tracks found")
        playlist_sync_report['custom_tracks_included'] = False
      else:
        if has_missing_tracks:
          log(f"  ⚠️  Applying custom tracks (some original tracks are missing)")
        else:
          log(f"  ✅ All custom tracks will be applied")

      rb_playlist_tracks_by_index: List[tuple[int | None, str, bool]] = [
        # (original_index, track, is_custom)
        (i, track.ID, False) for i, track in enumerate(rb_playlist_song_queue, start=1) if track is not None
      ]

      tracks_to_insert_grouped: dict[int | None, List[str]] = {}
      for rb_id, target_index_or_offset, target_track_id in tracks_to_insert:
        target_index: int | None = None
        if target_track_id != None:
          if target_index_or_offset == None:
            target_index_or_offset = 0
          # Find the position in rb_playlist_tracks_by_index where the track with the target_track_id is.
          # Then add the target_index_or_offset to its index to get the target index.
          found_target = False
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if track == target_track_id:
              target_index = (index_from_playlist_or_custom or 0) + \
                  (target_index_or_offset or 0)
              found_target = True
              break
          if not found_target:
            log(
              f"  ⚠️  WARNING: Custom track {rb_id} references missing target track ID {target_track_id}. It will not be inserted at the intended position.")
            # Optionally, append to end
            target_index = None
        else:
          target_index = target_index_or_offset

        # If index is out of bounds, warn and append to end
        if target_index is not None and (not isinstance(target_index, int) or target_index < 0 or target_index > len(rb_playlist_tracks_by_index)):
          log(
            f"  ⚠️  WARNING: Custom track {rb_id} specifies out-of-bounds index {target_index}. Appending to end.")
          target_index = None

        # Initialize an empty list for the tracks to insert at this index if it doesn't exist yet.
        if target_index not in tracks_to_insert_grouped:
          tracks_to_insert_grouped[target_index] = []

        tracks_to_insert_grouped[target_index].append(rb_id)

      # Remove all the tracks that have index None, and add them to the end of rb_playlist_tracks_by_index
      tracks_to_insert_at_end = tracks_to_insert_grouped.pop(None, [])
      for rb_id in tracks_to_insert_at_end:
        log(f"  ├ Appending custom track {rb_id} to the end of the playlist")
        rb_playlist_tracks_by_index.append((None, rb_id, True))

      for target_index, tracks in tracks_to_insert_grouped.items():
        # Find the spot in rb_playlist_tracks_by_index where we append the track.
        insert_index = 0
        inserted = False
        for index_in_list, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
          if index_from_playlist_or_custom is not None and target_index is not None and index_from_playlist_or_custom > target_index:
            insert_index = index_in_list
            inserted = True
            break
          insert_index = index_in_list + 1
        if not inserted and target_index is not None and target_index > len(rb_playlist_tracks_by_index):
          log(
            f"  ⚠️  WARNING: Custom track(s) intended for index {target_index} but playlist is shorter. Appending to end.")
        log(
          f"  ├ Inserting {len(tracks)} custom track(s) at index {target_index}: {tracks}")
        for rb_id in tracks:
          rb_playlist_tracks_by_index.insert(
            insert_index, (target_index, rb_id, True))
          insert_index += 1

      for rb_id, target_index, target_track_id in tracks_to_replace:
        replaced = False
        if target_index != None:
          # Find the track in the playlist with the index that matches the target_index.
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if index_from_playlist_or_custom == (target_index + 1) and not is_custom:
              log(
                f"  ├ Replacing track with ID {track} at index {target_index} with custom track {rb_id}")
              rb_playlist_tracks_by_index[index] = (target_index, rb_id, True)
              replaced = True
              break
          if not replaced:
            log(
              f"  ⚠️  WARNING: Could not replace track at index {target_index} (not found). Custom track {rb_id} not inserted.")
        else:
          # Find the track in the playlist with an ID that matches the target_track.
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if track == target_track_id:
              log(
                f"  ├ Replacing track with ID {track} with custom track {rb_id}")
              rb_playlist_tracks_by_index[index] = (
                index_from_playlist_or_custom, rb_id, True)
              replaced = True
          if not replaced:
            log(
              f"  ⚠️  WARNING: Could not replace track with target ID {target_track_id} (not found). Custom track {rb_id} not inserted.")

      # Now we have a list of tracks that should be in the playlist, with the custom tracks inserted at the correct index.
      # We can now create the final tracklist by mapping the list to its ID and then looking up the tracks.
      # Then we remove all songs from the playlist and add the new ones in the correct order.

      final_tracklist_ids = list(
        map(lambda entry: entry[1], rb_playlist_tracks_by_index))
      rb_playlist_song_queue_final: List[r.db6.tables.DjmdContent | None] = list(
        map(lambda track_id: first_or_none(
          filter(lambda track: track.ID == track_id, rb_all_tracks)),
          final_tracklist_ids))
      rb_playlist_song_queue = rb_playlist_song_queue_final
      log(f"  └ Done processing custom tracks")

    log(f"Adding tracks to playlist...")
    for track in rb_playlist_song_queue:
      rb.add_to_playlist(rb_playlist, track)

    end_datetime = datetime.datetime.now()
    log(f"Finished syncing playlist in {
        humanfriendly.format_timespan(end_datetime - start_datetime)}")

    return playlist_sync_report

  sync_report = {}

  def save_dbs():
    print(f"Saving ID DB ({len(track_id_db['content']['spotify'])} entries)...")
    set_track_id_db(track_id_db)
    print(f"Saving missing tracks DB ({len(missing_tracks_db)} entries)...")
    set_missing_tracks_db(missing_tracks_db)
    print(f"Saving sync report ({len(sync_report)} playlists)...")
    save_sync_report(sync_report)

  try:
    start_datetime = datetime.datetime.now()

    # NOTE(jeroen-meijer): Set up background fetching for playlist tracks
    results_queue = queue.Queue()

    # Start background fetcher thread with ALL playlists
    fetcher_thread = threading.Thread(
      target=playlist_fetcher_worker,
      args=(sp, sp_target_playlists, results_queue),
      daemon=True
    )
    fetcher_thread.start()

    # Process playlists as their tracks become available
    processed_count = 0
    total_playlists = len(sp_target_playlists)

    while processed_count < total_playlists:
      try:
        # Check if data is immediately available
        if results_queue.empty():
          orange_log(
            f"Waiting for background fetcher... ({processed_count + 1}/{total_playlists})")

        # Get next fetched playlist data (results come in completion order, not original order)
        playlist_data = results_queue.get(timeout=30)  # 30 second timeout

        # Log that we got the data and are starting processing
        if not playlist_data.error:
          print(
            f"🎵 Processing playlist: \"{playlist_data.playlist['name']}\" ({len(playlist_data.items)} tracks)")

        if playlist_data.error:
          print(
            f"❌ Error fetching tracks for playlist '{playlist_data.playlist['name']}': {playlist_data.error}")
          # Try to fetch synchronously as fallback
          print(f"🔄 Retrying synchronously...")
          fallback_data = fetch_playlist_tracks(sp, playlist_data.playlist)
          if fallback_data.error:
            print(f"❌ Fallback failed too: {fallback_data.error}")
            print(f"⏩ Skipping playlist '{playlist_data.playlist['name']}'")
            processed_count += 1
            continue
          else:
            playlist_data = fallback_data

        # Process the playlist with its fetched tracks
        res = sync_playlist(playlist_data.playlist, playlist_data.items)
        sync_report[playlist_data.playlist['name']] = res
        processed_count += 1

      except queue.Empty:
        print("⏱️  Timeout waiting for playlist data - continuing anyway")
        break

    # Wait for background fetcher to complete
    blue_log("Waiting for background fetcher to complete...")
    fetcher_thread.join(timeout=10)  # Wait up to 10 seconds for completion
    if fetcher_thread.is_alive():
      blue_log("⚠️  Background fetcher still running after timeout")
    else:
      blue_log("Background fetcher completed")

    end_datetime = datetime.datetime.now()
    print(f"Synced all playlists in {
          humanfriendly.format_timespan(end_datetime - start_datetime)}")
  except Exception as e:
    print(f"Interrupted or crash detected:\n{e}\n")
    traceback.print_exc()
    save_dbs()
    print("Exiting")
    sys.exit(130)

  save_dbs()
  print("Committing changes to Rekordbox...")
  rb.commit()
  print("Done")
