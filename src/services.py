import secret_keys
import constants
import spotipy as s
import pyrekordbox as r
from typing import List
from functions import sanitize
from fuzzywuzzy import fuzz

def setup_spotify():
  return s.Spotify(
    auth_manager=s.SpotifyOAuth(
      client_id=secret_keys.SPOTIFY_CLIENT_ID,
      client_secret=secret_keys.SPOTIFY_CLIENT_SECRET,
      redirect_uri=secret_keys.SPOTIFY_REDIRECT_URI,
      scope=constants.SPOTIFY_SCOPES,
    )
  )

def setup_rekordbox():
  return r.Rekordbox6Database(key=secret_keys.REKORDBOX_DB_KEY)

