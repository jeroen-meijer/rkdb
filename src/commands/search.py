from functions import find_track
from services import setup_rekordbox


def search_rekordbox_tracks(fuzzy_match_threshold=60):
  rb = setup_rekordbox()
  all_tracks = list(
      filter(
          lambda track: track.Title != None and track.ArtistName != None,
          rb.get_content(),
      )
  )
  while True:
    try:
      search_query = input("Enter search query: ")

      results = find_track(
          {"query": search_query},
          all_tracks,
          threshold=fuzzy_match_threshold,
          match_artist_and_title=False,
      )

      if len(results) == 0:
        print("No matches found.")
      else:
        for i in range(min(10, len(results))):
          res = results[i]
          track = res[0]
          match = res[1]

          def bold(string: str):
            return f"\033[1m{string}\033[0m"

          message = f"{i + 1}. [ {str(track.ID).rjust(9)} ] ({int(match)}%) {
              track.ArtistName} - {track.Title} ({len(track.Cues)} cues)"
          if i == 0:
            message = bold(message)
          print(message)

      print()

    except KeyboardInterrupt:
      break
  print("Exiting")
