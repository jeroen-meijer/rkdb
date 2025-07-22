import datetime
import yaml
import spotipy
from typing import List, Dict, Any, Set
from services import setup_spotify
from functions import exhaust_fetch


def extract_playlist_data(playlist_id: str):
  """Extract artists and labels from the 200 most recently added songs in a playlist."""
  print("🎵 Spotify Playlist Extractor")
  print("=" * 50)

  # Setup Spotify client
  sp = setup_spotify()
  if sp is None:
    print("❌ Failed to setup Spotify client")
    return

  try:
    # Get playlist info
    playlist_info = sp.playlist(playlist_id, fields='name,description')
    if playlist_info is None:
      print(f"❌ Could not fetch playlist {playlist_id}")
      return

    playlist_name = playlist_info.get('name', 'Unknown Playlist')
    print(f"📜 Extracting from playlist: {playlist_name}")
    print(f"🔗 Playlist ID: {playlist_id}")

    # Get playlist tracks with added_at field
    print("📥 Fetching playlist tracks...")
    tracks = exhaust_fetch(
        fetch=lambda offset, limit: sp.playlist_items(
            playlist_id,
            offset=offset,
            limit=limit,
            fields='items(added_at,track(id,uri,name,artists(id,name),album(id,name,release_date,label))),next'
        ),
        map_elements=lambda res: res['items']
    )

    print(f"📊 Found {len(tracks)} total tracks")

    # Sort by added_at date (most recent first) and take top 200
    sorted_tracks = sorted(
      tracks, key=lambda x: x.get('added_at', ''), reverse=True)
    recent_tracks = sorted_tracks[:200]

    print(f"🎯 Analyzing 200 most recently added tracks...")
    print(
      f"📅 First track added: {recent_tracks[0].get('added_at', 'Unknown') if recent_tracks else 'No tracks'}")
    print(
      f"📅 Last track added: {recent_tracks[-1].get('added_at', 'Unknown') if recent_tracks else 'No tracks'}")

    # Extract unique artists and labels
    artists = {}
    labels = set()
    album_cache = {}  # Cache to avoid fetching the same album multiple times

    for item in recent_tracks:
      if not item or not item.get('track'):
        continue

      track = item['track']

      # Extract artists
      if track.get('artists'):
        for artist in track['artists']:
          if artist.get('id') and artist.get('name'):
            artist_id = artist['id']
            artist_name = artist['name']

            # Generate simple key from artist name
            key = generate_simple_key(artist_name)

            if key not in artists:
              artists[key] = {
                'id': artist_id,
                'name': artist_name
              }

      # Extract label from album (with caching)
      if track.get('album') and track['album'].get('id'):
        album_id = track['album']['id']

        # Check if we've already fetched this album
        if album_id not in album_cache:
          try:
            album_cache[album_id] = sp.album(album_id)
          except Exception as e:
            print(f"⚠️  Error fetching album {album_id}: {e}")
            album_cache[album_id] = None

        # Extract label from cached album data
        if album_cache[album_id] and album_cache[album_id].get('label'):
          label_name = album_cache[album_id]['label']
          if label_name and label_name.strip():
            labels.add(label_name)

    # Convert labels to YAML format
    labels_dict = {}
    for label_name in sorted(labels):
      key = generate_simple_key(label_name)
      labels_dict[key] = label_name

    # Generate YAML output
    output_data = {
      'artists': artists,
      'labels': labels_dict
    }

    print(
      f"✅ Found {len(artists)} unique artists and {len(labels)} unique labels")
    print("\n📋 Generated YAML:")
    print("=" * 50)

    # Print YAML with proper formatting
    yaml_output = yaml.dump(
      output_data, default_flow_style=False, sort_keys=False, indent=2)
    print(yaml_output)

    # Also save to file
    output_filename = f"extracted_data_{playlist_id}.yaml"
    with open(output_filename, 'w') as f:
      yaml.dump(output_data, f, default_flow_style=False,
                sort_keys=False, indent=2)

    print(f"\n💾 Saved to: {output_filename}")

  except Exception as e:
    print(f"❌ Error extracting playlist data: {e}")


def generate_simple_key(name: str) -> str:
  """Generate a simple YAML key from a name."""
  # Convert to lowercase and replace spaces/special chars with underscores
  key = name.lower()
  key = key.replace(' ', '_')
  key = key.replace('-', '_')
  key = key.replace('.', '_')
  key = key.replace('&', 'and')
  key = key.replace('(', '')
  key = key.replace(')', '')
  key = key.replace('[', '')
  key = key.replace(']', '')
  key = key.replace('{', '')
  key = key.replace('}', '')

  # Remove any remaining special characters
  key = ''.join(c for c in key if c.isalnum() or c == '_')

  # Ensure it starts with a letter
  if key and not key[0].isalpha():
    key = 'artist_' + key

  # Limit length and ensure uniqueness
  if len(key) > 30:
    key = key[:30]

  return key
