import iGetMusic as iGet
import pyrekordbox as r
from typing import Any, Iterable, List
from fuzzywuzzy import fuzz
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

def exhaust_fetch(fetch, map_elements, stop_when = lambda all_elements: False):
  limit = 50
  elements = []
  offset = 0
  res = fetch(offset)
  elements += map_elements(res)
  while res['next'] != None and not stop_when(elements):
    offset += limit
    res = fetch(offset)
    elements += map_elements(res)
  return elements

def ensure_track_db_schema(track_id_db: dict | None):
  copy = {}
  copy = {k:v for k,v in (track_id_db.items() if track_id_db != None else {})} 
  if 'content' not in copy or copy['content'] == None:
    copy['content'] = {}
  if 'spotify' not in copy['content'] or copy['content']['spotify'] == None:
    copy['content']['spotify'] = {}
  return copy

def sanitize(string: str, ignore_chars = [' ', '-', '_', '(', ')']):
  string = string.lower()
  for char in ignore_chars:
    string = string.replace(char, '')
  return string

# Returns the camelot key for the given playlist if it ends with one.
#
# A camelot key is defined as a number of 1 or 2 characters
# from 1-12 (incl), followed by either an A or a B.
#
# The key is returned as an uppercase string if it is valid, otherwise None.
def attempt_get_key(playlist_name) -> str | None:
  if playlist_name == None:
    return None

  last_three_chars = playlist_name[-3:]
  potential_camelot_key = ''.join(filter(str.isalnum, last_three_chars))
  number_part = potential_camelot_key[:-1]
  letter_part = potential_camelot_key[-1]
  
  if not number_part.isdigit() or not letter_part.isalpha():
    return None
  
  if int(number_part) < 1 or int(number_part) > 12:
    return None
  
  if letter_part.lower() not in ['a', 'b']:
    return None
  
  return potential_camelot_key.upper()

def first_or_none(iterable: Iterable) -> Any | None:
  return next(iter(iterable), None)


# Returns the rekordbox tracks that most closely match the given query.
# First attempts to match by artist and then by title for the most accurate match.
# This is not guaranteed to return tracks at all, and may return an empty list, but never None.
#
# Returns a list of entries with the track and the match percentage (in that order).
# The match percentage is the average of the artist and title match percentages,
# and may be a float between 0 and 100.
#
# The query may be a dict with either a single "query" parameter OR
# a dict with an "artist" and "title" parameter.
#
# The 'match_threshold' parameter must be met for the artist and title separately
# for the track to be considered a match.
def find_track(
  query: dict,
  all_tracks: List[r.db6.tables.DjmdContent],
  threshold = 80,
  match_artist_and_title = True,
) -> List[tuple[r.db6.tables.DjmdContent, float]]:
  artist_query: str = None
  title_query: str = None
  if 'query' in query:
    artist_query = sanitize(query['query'])
    title_query = sanitize(query['query'])
  elif 'artist' in query and 'title' in query:
    artist_query = sanitize(query['artist'])
    title_query = sanitize(query['title'])
  else:
    raise ValueError("Query must have either a 'query' parameter or 'artist' and 'title' parameters")
  
  track_and_matches = []
  
  for track in all_tracks:
    artist = sanitize(track.ArtistName)
    title = sanitize(track.Title)
    
    artist_ratio = fuzz.partial_ratio(artist_query, artist)
    title_ratio = fuzz.partial_ratio(title_query, title)
    
    artist_matches = artist_ratio >= threshold
    title_matches = title_ratio >= threshold
    
    if (match_artist_and_title and artist_matches and title_matches) or (not match_artist_and_title and (artist_matches or title_matches)):
      match = float((float(artist_ratio) + float(title_ratio)) / 2)
      track_and_matches.append((track, match))
  
  track_and_matches.sort(key = lambda x: x[1], reverse = True)
  return track_and_matches

# Returns the most likely result based on the given query from the given list of options.
#
# Returns None if no options are provided, otherwise returns the most likely option.
# The query is matched against the result of calling 'get_key' on each option.
def find_best_match(query: dict, get_key: callable, options: List) -> any:
  if (len(options) == 0):
    return None
  
  match_scores = [
    (score, item) for score, item in [
      (fuzz.partial_ratio(sanitize(query), sanitize(get_key(item))), item) for item in options
    ]
  ]
  
  match_scores.sort(key = lambda x: x[0], reverse = True)
  
  return match_scores[0][1] if len(match_scores) > 0 else None

# Generates a direct iTunes Store URL for a particular song.
def generate_itunes_store_url(itunes_song: iGet.iGet.song) -> str:
  apple_music_url = itunes_song.trackViewUrl

  old_url = urlparse(apple_music_url)
  old_query_params = parse_qs(old_url.query)
  old_netloc = old_url.netloc

  new_scheme = 'itmss'

  new_query_params = {}
  new_query_params['app'] = 'itunes'
  new_query_params['i'] = old_query_params['i'][0]

  new_net_loc = old_netloc.replace('music.apple.com', 'itunes.apple.com')

  new_fragment = 'songs'

  new_url = urlunparse((
    new_scheme,
    new_net_loc,
    old_url.path,
    old_url.params,
    urlencode(new_query_params),
    new_fragment
  ))

  return new_url
