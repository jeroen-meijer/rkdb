import secret_keys
import constants
import spotipy as s
import pyrekordbox as r
from pyrekordbox import utils as r_utils


def setup_spotify():
  return s.Spotify(
    auth_manager=s.SpotifyOAuth(
      client_id=secret_keys.SPOTIFY_CLIENT_ID,
      client_secret=secret_keys.SPOTIFY_CLIENT_SECRET,
      redirect_uri=secret_keys.SPOTIFY_REDIRECT_URI,
      scope=constants.SPOTIFY_SCOPES,
    )
  )


def setup_rekordbox(allow_while_running: bool = False):
  if (not allow_while_running) and (r_utils.get_rekordbox_pid() != 0):
    raise Exception(
      "Rekordbox is running. Please close Rekordbox before running this script."
    )

  return r.Rekordbox6Database(key=secret_keys.REKORDBOX_DB_KEY)
