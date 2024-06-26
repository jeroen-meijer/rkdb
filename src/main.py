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

  command_map = {
    'sync': sync_spotify_playlists_to_rekordbox,
    'search': search_rekordbox_tracks,
    'buy': buy_tracks,
  }

  if command in command_map:
    command_map[command]()
  else:
    raise ValueError(f"Command '{command}' not found")

  print("Exiting")
  sys.exit(0)


if __name__ == '__main__':
  main()
