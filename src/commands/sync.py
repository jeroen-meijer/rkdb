import datetime
import sys
import time
import constants
import deepmerge
import humanfriendly
import iGetMusic as iGet
import pyrekordbox as r
from typing import List
from db import get_custom_tracks_db, get_missing_tracks_db, get_track_id_db, get_track_id_overrides_db, save_sync_report, set_missing_tracks_db, set_track_id_db
from functions import attempt_get_key, ensure_custom_track_schema, ensure_track_db_schema, exhaust_fetch, find_best_match, find_track, first_or_none, generate_itunes_store_url
from services import setup_rekordbox, setup_spotify
from requests import JSONDecodeError
from collections import namedtuple

CustomTrack = namedtuple('CustomTrack', ['rekordbox_id', 'index', 'target'])

# Notes
# Typical spotify playlist link: https://open.spotify.com/playlist/1UObZqUr1MtbveqsSw6sFP?si=5d14331bb8174c1e
# Everything after the last slash is the playlist ID, until and not including the question mark (if any)
# We will want to parse the playlist ID from the URL
# The custom_playlist_ids arg may contain any number of playlist IDs _or_ playlist URLs.
# The URLs need to be parsed to get the playlist ID, and this new list should override the custom_playlist_ids list.


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
  rb_playlists = list(rb.get_playlist())
  print('Fetching Rekordbox tracks...')
  rb_all_tracks: List[r.db6.tables.DjmdContent] = list(filter(
    lambda track: track.Title != None and track.Artist != None, rb.get_content()))
  print('Fetching Rekordbox keys...')
  camelot_key_starts = tuple(str(n + 1) for n in range(12))
  rb_camelot_keys: dict[str, r.db6.tables.DjmdKey] = {k.ScaleName.upper(
  ): k for k in list(rb.get_key()) if k.ScaleName.startswith(camelot_key_starts)}

  if (len(rb_camelot_keys) != 24):
    raise ValueError(f"Expected 24 keys but found {len(rb_camelot_keys)}: {
                     list(map(lambda k: k.ScaleName, rb_camelot_keys))}")

  print("Logging into Spotify...")
  sp_user = sp.current_user()
  print(f"Logged in as {sp_user['display_name']} ('{sp_user['id']}')")

  print("Fetching Spotify playlists...")
  sp_all_playlists = exhaust_fetch(
    fetch=lambda offset, limit: sp.current_user_playlists(
      offset=offset,
      limit=limit
    ),
    map_elements=lambda res: res['items'],
  )

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

  def sync_playlist(sp_playlist) -> dict:
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
      sp_playlist_camelot_key, None)
    if rb_playlist_key != None:
      log(f"Detected camelot key: {rb_playlist_key.ScaleName}")

    log(f"Fetching tracks...")
    sp_playlist_tracks = exhaust_fetch(
      fetch=lambda offset, limit: sp.playlist_items(
        sp_playlist['id'],
        offset=offset,
        limit=limit,
      ),
      # For each res, get the items, and map each of those items to the 'track'
      map_elements=lambda res: list(
        map(lambda item: item['track'], res['items']))
    )

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
    playlist_folder_name: str = None
    playlist_name_parts = sp_playlist_name.split('_')
    if len(playlist_name_parts) > 1:
      potential_folder_name = playlist_name_parts[0]
      if potential_folder_name.isupper():
        playlist_folder_name = f"{potential_folder_name}S"

    rb_playlist: r.db6.tables.DjmdPlaylist = None

    if playlist_folder_name != None:
      playlist_folder = first_or_none(filter(
        lambda playlist: playlist.Name == playlist_folder_name and playlist.is_folder, list(rb.get_playlist())))
      if playlist_folder == None:
        log(f"Creating playlist folder {playlist_folder_name}")
        playlist_folder = rb.create_playlist_folder(playlist_folder_name)
      rb_playlist = rb.create_playlist(sp_playlist_name, playlist_folder)
    else:
      rb_playlist = rb.create_playlist(sp_playlist_name)

    rb_playlist_song_queue: List[r.db6.DjmdContent] = []

    for track_index in range(len(sp_playlist_tracks)):
      sp_track = sp_playlist_tracks[track_index]
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
        missing_tracks_db[sp_track['id']] = {
          'artist': sp_track_artist_str,
          'title': sp_track_name_str,
          'itunes_url': itunes_url,
          'ignored': False,
          'date_added': existing_entry.get('date_added', datetime.datetime.now().isoformat())
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
        if rb_track != None:
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

    has_custom_tracks = len(custom_tracks) > 0
    has_missing_tracks = playlist_sync_report['missing_tracks']['count'] > 0

    if has_missing_tracks:
      log(f"  ❗ Skipping custom tracks because there are missing tracks in the original playlist. This may cause issues when adding or replacing tracks.")
      playlist_sync_report['custom_tracks_included'] = False
    elif not has_custom_tracks:
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

        c_type = custom_track['type']

        c_rb = first_or_none(
          filter(lambda track: track.ID == c_rb_id, rb_all_tracks))
        if c_rb == None:
          log(f"  ❌ Skipping custom track with unknown rekordbox ID {c_rb_id}")
          continue

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
          tracks_to_insert.append(c)
        elif c_type == 'replace':
          tracks_to_replace.append(c)
        else:
          log(f"Skipping custom track with unknown type '{c_type}'")

      rb_playlist_tracks_by_index = [
        # (original_index, track, is_custom)
        (i, track.ID, False) for i, track in enumerate(rb_playlist_song_queue, start=1)
      ]

      tracks_to_insert_grouped: dict[int | None, List[str]] = {}
      for rb_id, target_index_or_offset, target_track_id in tracks_to_insert:
        target_index: int = None
        if target_track_id != None:
          if target_index_or_offset == None:
            target_index_or_offset = 0
          # Find the position in rb_playlist_tracks_by_index where the track with the target_track_id is.
          # Then add the target_index_or_offset to its index to get the target index.
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if track == target_track_id:
              target_index = index_from_playlist_or_custom + target_index_or_offset
              break
        else:
          target_index = target_index_or_offset

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
        # This spot can be found by finding the first track with an index higher than the current index,
        # and inserting it before that track.
        # We also need to keep in mind if the list is empty or if the index is higher than the highest index.

        # The `insert_index` refers to the index in the `rb_playlist_tracks_by_index` list where we will insert the track.
        insert_index = 0

        # The resulting list may look like:
        # - (0, custom_track_1)
        # - (0, custom_track_2)
        # - (1, original_track_1)
        # - (1, custom_track_3)
        # - (2, original_track_2)
        # etc.

        # GOAL: Find the index in the `rb_playlist_tracks_by_index` where its song index (the first element in the tuple)
        # is higher than the target index. Then, we insert the track at that index, pushing the existing track at that
        # index and all following tracks one index higher.
        # For example, inserting (0, test_track) into the list above would result in:
        # - (0, custom_track_1)
        # - (0, custom_track_2)
        # - (0, test_track)
        # - (1, original_track_1)
        # - (1, custom_track_3)
        # ...
        # If the playlist empty, or the target index is higher than the highest index, we append the track to the end.
        # Important: If multiple entries in the list have the same index, the track will be inserted after all of them.
        # So, doing multiple inserts at the same index will result in the tracks being inserted in the order they were added.
        # For example, inserting (0, test_track_1) and (0, test_track_2) would result in:
        # - (0, custom_track_1)
        # - (0, custom_track_2)
        # - (0, test_track_1)
        # - (0, test_track_2)
        # - (1, original_track_1)
        # - (1, custom_track_3)
        # ...

        # Example case to help building: we want to insert a track at index 1,
        # and the list looks like this: [(0, track1, False), (1, track2, False), (2, track3, False)].
        # The track should be inserted before track3, since track3's index is the first in the list that
        # is higher than the target index.

        # If the list is empty, we insert at the start.
        # If

        for index_in_list, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
          if index_from_playlist_or_custom is not None and index_from_playlist_or_custom > target_index:
            insert_index = index_in_list
            break
          insert_index = index_in_list + 1

        log(f"  ├ Inserting {len(tracks)} custom track(s) at index {
            target_index}: {tracks}")
        for rb_id in tracks:
          rb_playlist_tracks_by_index.insert(
            insert_index, (target_index, rb_id, True))
          insert_index += 1

      for rb_id, target_index, target_track_id in tracks_to_replace:
        if target_index != None:
          # Find the track in the playlist with the index that matches the target_index.
          # Then, replace that track with the custom track.
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if index_from_playlist_or_custom == (target_index + 1) and not is_custom:
              log(f"  ├ Replacing track with ID {track} at index {
                  target_index} with custom track {rb_id}")
              rb_playlist_tracks_by_index[index] = (target_index, rb_id, True)
              break
        else:
          # Find the track in the playlist with an ID that matches the target_track.
          # Then, replace that track with the custom track.
          for index, (index_from_playlist_or_custom, track, is_custom) in enumerate(rb_playlist_tracks_by_index):
            if track == target_track_id:
              log(f"  ├ Replacing track with ID {
                  track} with custom track {rb_id}")
              rb_playlist_tracks_by_index[index] = (
                index_from_playlist_or_custom, rb_id, True)

          # Now we have a list of tracks that should be in the playlist, with the custom tracks inserted at the correct index.
          # We can now create the final tracklist by mapping the list to its ID and then looking up the tracks.
          # Then we remove all songs from the playlist and add the new ones in the correct order.

      final_tracklist_ids = list(
        map(lambda entry: entry[1], rb_playlist_tracks_by_index))
      rb_playlist_song_queue = list(
        map(lambda track_id: first_or_none(
          filter(lambda track: track.ID == track_id, rb_all_tracks)),
          final_tracklist_ids))
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

    for sp_playlist in sp_target_playlists:
      res = sync_playlist(sp_playlist)
      sync_report[sp_playlist['name']] = res

    end_datetime = datetime.datetime.now()
    print(f"Synced all playlists in {
          humanfriendly.format_timespan(end_datetime - start_datetime)}")
  except Exception as e:
    print(f"Interrupted or crash detected:\n{e}\n")
    save_dbs()
    print("Exiting")
    sys.exit(130)

  save_dbs()
  print("Committing changes to Rekordbox...")
  rb.commit()
  print("Done")
