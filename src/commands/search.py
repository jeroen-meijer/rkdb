from functions import find_track
from services import setup_rekordbox


def search_rekordbox_tracks(
    fuzzy_match_threshold: int = 60,
    select_mode: bool = False,
    result_limit: int = 30,
) -> str | None:
  rb = setup_rekordbox(allow_while_running=True)
  all_tracks = list(
      filter(
          lambda track: track.Title != None and track.ArtistName != None,
          rb.get_content(),
      )
  )
  while True:
    try:
      search_query = input("Enter search query or rekordbox ID: ")
      perform_id_search = search_query.isdigit()

      results = []
      if perform_id_search:
        print("Performing ID search...")
        res = rb.get_content(ID=int(search_query))
        if res != None:
          results.append((res, 100))
      else:
        results = find_track(
            {"query": search_query},
            all_tracks,
            threshold=fuzzy_match_threshold,
            match_artist_and_title=False,
        )

      if len(results) == 0:
        print("No matches found.")
      else:
        for i in range(min(result_limit, len(results))):
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

      if select_mode:
        try:
          selection = int(input("Enter selection: "))
          if selection < 1 or selection > len(results):
            print("Invalid selection.")
            continue
          return results[selection - 1][0].ID
        except ValueError:
          print("Invalid selection.")
          continue

    except KeyboardInterrupt:
      if select_mode:
        return None
      break
  print("Exiting")
