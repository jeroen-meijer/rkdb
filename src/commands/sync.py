import datetime
import sys
import time
import constants
import deepmerge
import humanfriendly
import iGetMusic as iGet
import pyrekordbox as r
from typing import List
from db import get_missing_tracks_db, get_track_id_db, get_track_id_overrides_db, save_sync_report, set_missing_tracks_db, set_track_id_db
from functions import attempt_get_key, ensure_track_db_schema, exhaust_fetch, find_best_match, find_track, first_or_none, generate_itunes_store_url
from services import setup_rekordbox, setup_spotify
from requests import JSONDecodeError


def sync_spotify_playlists_to_rekordbox():
  rb = setup_rekordbox()
  sp = setup_spotify()

  track_id_db = ensure_track_db_schema(get_track_id_db())
  id_overrides_db = ensure_track_db_schema(get_track_id_overrides_db())
  track_id_db = deepmerge.always_merger.merge(track_id_db, id_overrides_db)
  missing_tracks_db = get_missing_tracks_db()

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
  sp_target_playlists = list(filter(
    lambda playlist: any(
      map(lambda prefix: playlist['name'].startswith(prefix), constants.SPOTIFY_PLAYLIST_PREFIXES))
      or playlist['name'] in constants.SPOTIFY_PLAYLISTS,
    sp_all_playlists
  ))
  print(f"Syncing {len(sp_target_playlists)} Spotify playlists to Rekordbox...")

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
      print(f"[{sp_playlist_name}] {message}")

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

    for i in range(len(sp_playlist_tracks)):
      sp_track = sp_playlist_tracks[i]
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
        rb.add_to_playlist(rb_playlist, rb_track)
      else:
        log(f"└ ❌ Could not find track \"{sp_track_full_str}\" in Rekordbox")
        if missing_tracks_db.get(sp_track_id, {}).get('ignored', False) == True:
          log(f"  └ 🚫 Track is ignored")
        else:
          attempt_add_track_to_missing_db()

      playlist_sync_report['all_tracks'][i + 1] = {
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
        playlist_sync_report['missing_tracks']['tracks'][i +
                                                         1] = {
          'id': sp_track_id,
          'artist': sp_track_artist_str,
          'title': sp_track_name_str,
        }

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
