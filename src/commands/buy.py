import os
from enum import Enum
from yaspin import yaspin
from getch import getche
from db import get_missing_tracks_db, set_missing_tracks_db


def buy_tracks():
  """
  Helps buying new tracks.

  This command will read the `missing_tracks` yaml file and open each track
  in the iTunes Store for the user to buy.

  The user can opt to skip the track, mark it as bought, or mark it as not found.
  Tracks that are marked as bought will be removed from the `missing_tracks` yaml file.
  """

  missing_tracks_db: dict = get_missing_tracks_db()
  missing_tracks_without_ignored = {
      k: v for k, v in missing_tracks_db.items() if not v.get("ignored", False)
  }

  missing_tracks_to_process = list(missing_tracks_without_ignored.items())
  total_missing_tracks_to_process = len(missing_tracks_to_process)

  latest_spinner = None

  actions_performed = {}

  try:
    for i in range(total_missing_tracks_to_process):
      spotify_track_id, track_info = missing_tracks_to_process[i]

      full_track_str = f"{track_info['artist']} - {track_info['title']}"
      itunes_url = track_info["itunes_url"]

      if itunes_url == None:
        print(f"🚫 {_bold(full_track_str)} · No iTunes link available.")
        continue

      counter = f"{str(i + 1).rjust(len(str(total_missing_tracks_to_process)))
                   }/{total_missing_tracks_to_process}"
      prompt = f"[{counter}] {_bold(full_track_str)} · Did you buy this track? ({
          _bold('Y')}/N/I/R/Q)"

      with yaspin(text=prompt, color="blue") as sp:
        latest_spinner = sp

        def show_store_page():
          os.system(f"open '{itunes_url}'")

        show_store_page()

        action: _BuyAction | None = None
        possible_actions = [
            _BuyAction.YES,
            _BuyAction.NO,
            _BuyAction.IGNORE,
            _BuyAction.QUIT,
        ]
        while action not in possible_actions:
          print(" ", end=" ")
          res = getche()
          if res == "r":
            show_store_page()
            continue

          action = (
              next((a for a in possible_actions if a.value == res), None)
              if res != "\n"
              else _BuyAction.YES
          )
          if action == None:
            # Clear line
            print("\033[A\033[K", end="")

        if action not in actions_performed:
          actions_performed.update({action: 0})
        actions_performed[action] += 1

        if action == _BuyAction.YES:
          sp.ok("✅")
          missing_tracks_db.pop(spotify_track_id)
        elif action == _BuyAction.NO:
          sp.fail("⏩")
        elif action == _BuyAction.IGNORE:
          sp.fail("🚫")
          missing_tracks_db[spotify_track_id]["ignored"] = True
        elif action == _BuyAction.QUIT:
          raise KeyboardInterrupt()

        latest_spinner = None
  except KeyboardInterrupt:
    if latest_spinner != None:
      latest_spinner.fail("🛑")
    print("🛑 Stopping...")

  with yaspin(text="Saving changes...", color="blue") as sp:
    set_missing_tracks_db(missing_tracks_db)
    sp.ok("✅")

  actions_performed.pop(_BuyAction.QUIT)

  total_tracks_processed = sum(actions_performed.values())
  total_missing_tracks_to_process_afterwards = (
      total_missing_tracks_to_process - total_tracks_processed
  )

  print("🎉 Done!")
  print(
      f"  - ✅ {_bold(total_tracks_processed)
               } out of {total_missing_tracks_to_process} tracks processed."
  )
  print(
      f"  - 🛒 {_bold(actions_performed.get(_BuyAction.YES, 0))} tracks purchased.")
  print(
      f"  - 🚫 {_bold(actions_performed.get(_BuyAction.IGNORE, 0))
               } tracks ignored."
  )
  print(
      f"  - 📦 {_bold(total_missing_tracks_to_process_afterwards)
               } tracks left to process."
  )


def _bold(text: str) -> str:
  return f"\033[1m{text}\033[0m"


class _BuyAction(Enum):
  YES = "y"
  NO = "n"
  IGNORE = "i"
  QUIT = "q"

  def __str__(self):
    return self.value
