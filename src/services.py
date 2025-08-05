import os
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


def get_user_or_sign_in(sp: s.Spotify):
  try:
    user = sp.current_user()
    if user is None:
      raise Exception("Could not get user info")
    return user
  except Exception as e:
    # Remove .cache file from root of project
    script_path = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_path)
    cache_file = os.path.join(project_root, '.cache')
    if os.path.exists(cache_file):
      os.remove(cache_file)

    print("🔄 Signing in...")
    user = sp.current_user()
    return user


def setup_rekordbox(allow_while_running: bool = False):
  if (not allow_while_running) and (r_utils.get_rekordbox_pid() != 0):
    raise Exception(
      "Rekordbox is running. Please close Rekordbox before running this script."
    )

  return r.Rekordbox6Database(key=secret_keys.REKORDBOX_DB_KEY)
