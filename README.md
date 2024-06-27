# RKDB

## My tool for syncing Spotify and Rekordbox collections.

### How to use:

1. Clone the repository
2. Install the requirements with `pip install -r requirements.txt` (This project uses `venv` for dependency management).
3. Run the script with `python main.py` with a command: `sync`, `search`, `buy`.

### Disclaimer

- This tool is just for me to sync certain Spotify playlists with my Rekordbox collection.
- As such, only certain playlists are supported. Feel free to create a copy of the codebase and do whatever you want with it. _(Tip: The playlists that are targeted must have a prefix that is hardcoded in [src/constants.py](./src/constants.py).)_
- I am not responsible for any of your use (or misuse) of this tool. If your Spotify playlists get corrupted or your Rekordbox collection gets messed up, that's on you.
