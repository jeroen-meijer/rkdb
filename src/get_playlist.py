import sys
import json

from services import setup_spotify

sp = setup_spotify()


def main():
  args = sys.argv[1:]
  if len(args) == 0:
    print("No arguments provided. Exiting.")
    sys.exit(1)

  playlist = Playlist.get(args[0])
  # <EXPLANATION>
  # Print a JSON object that is a list of dictionaries, each dict has:
  # - index: the index of the track in the playlist
  # - name: the name of the track
  # - artist: artists concatenated by ', '
  # - album: the name of the album
  # - release_date: the release date of the album
  # - label: the label of the album
  # The JSON object should be sorted by the index of the track in the playlist.

  # <CODE>
  # Create a list of dictionaries
  data = []
  for index, track in enumerate(playlist.tracks):
    data.append({
      'index': index,
      'name': track.name,
      'artist': ', '.join(track.artists),
      'album': track.album.name,
      'release_date': track.album.release_date,
      'label': track.album.label,
    })

  # Sort the list of dictionaries by the index of the track in the playlist
  data = sorted(data, key=lambda x: x['index'])

  # Print the JSON object
  print(json.dumps(data, indent=2))


class Playlist:
  def __init__(self, id: str, name: str, tracks: list['Track']):
    self.id = id
    self.name = name
    self.tracks = tracks

  @staticmethod
  def parse(data) -> 'Playlist':
    return Playlist(
      id=data['id'],
      name=data['name'],
      tracks=Track.parseMany(data),
    )

  @staticmethod
  def get(id: str) -> 'Playlist':
    return Playlist.parse(sp.playlist(
      id,
      fields=[
        "id,name,tracks.items.track(id,name,artists.name,album.id)"],
        ))

  def __str__(self):
    return f"{self.name} ({len(self.tracks)} tracks)"


class Copyright:
  def __init__(self, text: str, type: str):
    self.text = text
    self.type = type

  @staticmethod
  def parse(data) -> 'Copyright':
    return Copyright(
      text=data['text'],
      type=data['type'],
    )

  @staticmethod
  def parseMany(data) -> list['Copyright']:
    return [Copyright.parse(entry) for entry in data]

  def __str__(self):
    return self.text


class Album:
  def __init__(self, id: str, name: str, release_date: str, album_type: str, label: str, copyrights: list['Copyright']):
    self.id = id
    self.name = name
    self.release_date = release_date
    self.label = label
    self.copyright = copyrights

  @staticmethod
  def parse(data) -> 'Album':
    return Album(
      id=data['id'],
      name=data['name'],
      release_date=data['release_date'],
      album_type=data['album_type'],
      label=data['label'],
      copyrights=Copyright.parseMany(data['copyrights']),
    )

  @staticmethod
  def get(id: str) -> 'Album':
    return Album.parse(sp.album(id))


class Track:
  def __init__(self, id: str, name: str, artists: list[str], album: Album):
    self.id = id
    self.name = name
    self.artists = artists
    self.album = album

  @staticmethod
  def parseMany(data) -> list['Track']:
    return [Track.parse(entry['track']) for entry in data['tracks']['items']]

  @staticmethod
  def parse(data) -> 'Track':
    return Track(
      id=data['id'],
      name=data['name'],
      artists=[artist['name'] for artist in data['artists']],
      album=Album.get(data['album']['id']),
    )

  def __str__(self):
    return f"{self.name} by {', '.join(self.artists)}"


if __name__ == "__main__":
  main()
