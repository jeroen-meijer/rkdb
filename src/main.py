import sys

from commands.search import search_rekordbox_tracks
from commands.sync import sync_spotify_playlists_to_rekordbox
from commands.buy import buy_tracks


def main():
  print("--- rkdb ---")
  args = sys.argv[1:]
  if len(args) == 0:
    print("No arguments provided. Exiting.")
    sys.exit(1)

  command = args[0]
  command_args = args[1:]

  command_map = {
    'sync': lambda playlist_ids: sync_spotify_playlists_to_rekordbox(playlist_ids),
    'search': lambda _: search_rekordbox_tracks(),
    'buy': lambda _: buy_tracks(),
  }

  if command in command_map:
    command_map[command](command_args)
  else:
    raise ValueError(f"Command '{command}' not found")

  print("Exiting")
  sys.exit(0)


if __name__ == '__main__':
  main()
