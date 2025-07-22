# RKDB - Spotify & Rekordbox Sync Tool

A comprehensive tool for syncing Spotify playlists with Rekordbox collections, managing track purchases, and crawling new music releases.

# ⚠️ This library may destroy your Rekordbox collection. Use at your own risk.

**_This is a collection of scripts that I use to manage my Rekordbox collection. It is not a complete solution and is not guaranteed to work for you. It may on a whim decide to IRREVERSIBLY break your Rekordbox collection. Have a backup. I'm not responsible for any damage caused by this tool. Proceed at your own risk._**

## 🚀 Quick Start

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd rkdb_legacy
   ```

2. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   ```

3. **Setup configuration**

   ```bash
   cp src/secret_keys.py.example src/secret_keys.py
   # Edit src/secret_keys.py with your credentials
   ```

4. **Run commands**
   ```bash
   ./venv/bin/python3 src/main.py <command> [options]
   ```

## 📋 Available Commands

### `sync` - Sync Spotify Playlists to Rekordbox

Synchronizes Spotify playlists with your Rekordbox collection, creating a mapping between tracks and identifying missing tracks.

**Usage:**

```bash
./venv/bin/python3 src/main.py sync [playlist_id1 playlist_id2 ...]
```

**Examples:**

```bash
# Sync all configured playlists
./venv/bin/python3 src/main.py sync

# Sync specific playlists by ID
./venv/bin/python3 src/main.py sync 1UObZqUr1MtbveqsSw6sFP 2ABCdEfGhIjKlMnOpQrStU

# Sync specific playlists by URL
./venv/bin/python3 src/main.py sync "https://open.spotify.com/playlist/1UObZqUr1MtbveqsSw6sFP"
```

**What it does:**

- Fetches tracks from configured Spotify playlists
- Matches tracks with your Rekordbox collection using fuzzy matching
- Creates a track ID mapping database
- Identifies missing tracks and adds them to `missing_tracks.yaml`
- Generates sync reports with detailed statistics

**Configuration:**
The sync command uses playlists defined in `src/constants.py`:

- `SPOTIFY_PLAYLIST_PREFIXES`: Playlists starting with these prefixes
- `SPOTIFY_PLAYLISTS`: Specific playlist names to sync

### `search` - Search Rekordbox Collection

Interactive search tool for finding tracks in your Rekordbox collection.

**Usage:**

```bash
./venv/bin/python3 src/main.py search
```

**Features:**

- Search by track title, artist, or Rekordbox ID
- Fuzzy matching with configurable threshold
- Displays track details including cue points
- Interactive selection mode for integration with other commands

**Options:**

- `fuzzy_match_threshold`: Minimum match percentage (default: 60)
- `select_mode`: Enable selection mode for returning track IDs
- `result_limit`: Maximum number of results to display (default: 30)

### `buy` - Track Purchase Assistant

Interactive tool to help purchase missing tracks from the iTunes Store.

**Usage:**

```bash
./venv/bin/python3 src/main.py buy
```

**Features:**

- Reads missing tracks from `missing_tracks.yaml`
- Opens iTunes Store pages for each track
- Interactive workflow with keyboard shortcuts:
  - `Y` - Mark as purchased (removes from missing tracks)
  - `N` - Skip track
  - `S` - Search in Rekordbox collection
  - `I` - Ignore track (marks as ignored)
  - `R` - Refresh iTunes page
  - `Q` - Quit

**Workflow:**

1. Displays missing tracks sorted by date added
2. Opens iTunes Store page for each track
3. User chooses action via keyboard shortcuts
4. Updates track databases and removes purchased tracks
5. Generates summary report

### `crawl` - Spotify Playlist Crawler

Advanced tool for crawling Spotify playlists, artists, and labels to create new playlists with recent releases.

**Usage:**

```bash
./venv/bin/python3 src/main.py crawl
```

**Features:**

- Crawls multiple sources: playlists, artists, labels
- Filters tracks by release/added date
- Creates new playlists with template-based naming
- Generates detailed crawl reports
- Supports YAML configuration with job definitions

**Configuration:**
Requires `crawl_config.yaml` file with YAML anchors for ID references:

```yaml
# Define YAML anchors for IDs (required)
_notes:
  playlists:
    liquicity_festival_2025: &playlist_liquicity_festival_2025 '5GH6XFP11JTr9wzwsNESwY'
    fokuz_releases: &playlist_fokuz_releases '2hs6DgCw92aWOWFM935dLY'
  artists:
    koan_sound: &koan_sound 1NCLweIUpq8knzemBwAwoo
    gentlemens_club: &gentlemens_club 58MEqEE2029jp6KTWTt1hO
    telomic: &telomic 2uCrvTUHRA9kuW4IA67oDn
  labels:
    shogun_audio: &shogun_audio 4JDJd4eld2k95Vj7YTe8rU
    hospital_records: &hospital_records 0sklgkoO5JeS7YNhHS5EmH

jobs:
  - name: liquid_weekly
    inputs:
      playlists:
        - *playlist_liquicity_festival_2025 # Reference using YAML anchor
        - *playlist_fokuz_releases
      artists:
        - *koan_sound # Reference using YAML anchor
        - *gentlemens_club
        - *telomic
      labels:
        - *shogun_audio # Reference using YAML anchor
    filters:
      added_between_days: 7 # Look back 7 days
    options:
      deduplicate: true
      max_tracks: 100
    output_playlist:
      name: 'Liquid DnB - Week {week_num} ({date_range_start_short} - {date_range_end_short})'
      description: 'Fresh Liquid DnB tracks from {input_sources} • {track_count} tracks'
```

**Important:** All IDs in the `inputs` section must be actual Spotify IDs (22-character alphanumeric strings) or YAML anchor references (using `*anchor_name`). You cannot use arbitrary strings - they must be valid IDs or references to defined anchors.

**Finding Spotify IDs:**

- **Playlist IDs**: Extract from Spotify playlist URLs (e.g., `https://open.spotify.com/playlist/37i9dQZF1DX5Vy6DFOcx00` → `37i9dQZF1DX5Vy6DFOcx00`)
- **Artist IDs**: Use Spotify's API or third-party tools to find artist IDs
- **Label IDs**: Labels are identified by name, not ID, so use the label name as the ID

**YAML Anchor Best Practices:**

- Use descriptive names for anchors (e.g., `&playlist_liquicity_festival_2025`)
- Group anchors by type in the `_notes` section
- Reference anchors with `*anchor_name` in job inputs
- This allows you to reuse IDs across multiple jobs and keep your config organized

**Template Variables:**
See `TEMPLATE_VARIABLES.md` for complete list of available variables for playlist names and descriptions.

### `extract` - Extract Playlist Data

Extracts artist and label information from Spotify playlists for analysis.

**Usage:**

```bash
./venv/bin/python3 src/main.py extract <playlist_id>
```

**Example:**

```bash
./venv/bin/python3 src/main.py extract 37i9dQZF1DX5Vy6DFOcx00
```

**Features:**

- Analyzes the 200 most recently added tracks
- Extracts unique artists and labels
- Generates YAML output with structured data
- Saves results to `extracted_data_<playlist_id>.yaml`

**Output Format:**

```yaml
artists:
  koan_sound:
    id: '1NCLweIUpq8knzemBwAwoo'
    name: 'Koan Sound'
  gentlemens_club:
    id: '58MEqEE2029jp6KTWTt1hO'
    name: "Gentleman's Club"

labels:
  shogun_audio: 'Shogun Audio'
  hospital_records: 'Hospital Records'
```

## ⚙️ Configuration Files

### Required Setup

1. **Spotify API Credentials** (`src/secret_keys.py`)

   ```python
   SPOTIFY_CLIENT_ID = 'your_client_id'
   SPOTIFY_CLIENT_SECRET = 'your_client_secret'
   SPOTIFY_REDIRECT_URI = 'http://localhost:8888/callback'
   REKORDBOX_DB_KEY = 'your_rekordbox_db_key'
   ```

2. **Rekordbox Database**
   - Locate your Rekordbox database file
   - Extract the database key using the provided tools
   - Update `REKORDBOX_DB_KEY` in `src/secret_keys.py`

### Generated Files

The tool generates several YAML files for data persistence:

- `track_id_db.yaml` - Mapping between Spotify and Rekordbox track IDs
- `track_id_db_overrides.yaml` - Manual overrides for track mappings
- `missing_tracks.yaml` - Tracks not found in Rekordbox collection
- `custom_tracks.yaml` - Custom track configurations for playlists
- `sync_report_*.yaml` - Detailed sync operation reports
- `crawl_report_*.yaml` - Detailed crawl operation reports

### Custom Track Configuration

Use `custom_tracks.yaml` to insert or replace tracks in playlists:

```yaml
custom_tracks:
  spotify:
    '37i9dQZF1DX5Vy6DFOcx00': # Playlist ID
      - rekordbox_id: 12345
        type: insert
        index: 0 # Insert at beginning

      - rekordbox_id: 67890
        type: replace
        target: 11111 # Replace track with ID 11111
```

## 🔧 Advanced Usage

### Batch Operations

```bash
# Sync multiple playlists
./venv/bin/python3 src/main.py sync playlist1 playlist2 playlist3

# Extract data from multiple playlists
for playlist in playlist1 playlist2 playlist3; do
  ./venv/bin/python3 src/main.py extract $playlist
done
```

### Integration with Other Tools

```bash
# Search and buy workflow
./venv/bin/python3 src/main.py search  # Find track ID
./venv/bin/python3 src/main.py buy     # Purchase missing tracks
./venv/bin/python3 src/main.py sync    # Sync after purchases
```

### Automated Crawling

```bash
# Set up cron job for weekly crawling
0 9 * * 1 /path/to/venv/bin/python3 /path/to/src/main.py crawl
```

## 📊 Data Files

### Track ID Database (`track_id_db.yaml`)

```yaml
content:
  spotify:
    'spotify_track_id_1': 12345 # Rekordbox track ID
    'spotify_track_id_2': 67890
```

### Missing Tracks (`missing_tracks.yaml`)

```yaml
spotify_track_id:
  artist: 'Artist Name'
  title: 'Track Title'
  itunes_url: 'https://music.apple.com/...'
  date_added: '2024-01-15T10:30:00'
  ignored: false
```

### Custom Tracks Schema

See `custom_tracks_schema.yaml` for the complete schema definition.

## 🚨 Important Notes

- **Backup your Rekordbox collection** before using this tool
- **Test with small playlists** first to ensure proper operation
- **Review generated reports** to understand what changes were made
- **The tool is designed for personal use** - modify constants and configurations as needed
- **Spotify API rate limits** may affect large operations

## 🛠️ Troubleshooting

### Common Issues

1. **Spotify Authentication Failed**

   - Check your client ID and secret in `src/secret_keys.py`
   - Ensure redirect URI matches your Spotify app settings

2. **Rekordbox Connection Failed**

   - Verify your database key is correct
   - Ensure Rekordbox is not running during sync operations

3. **Missing Tracks Not Found**

   - Use the `search` command to find similar tracks
   - Check track ID overrides in `track_id_db_overrides.yaml`

4. **Crawl Configuration Errors**
   - Validate your `crawl_config.yaml` syntax
   - Check that all referenced IDs exist

### Debug Mode

Add debug logging by modifying the source code or checking generated report files for detailed information about operations.

## 📝 License

This tool is for personal use. Feel free to copy and modify for your own needs.

---

**Disclaimer**: This tool interacts with your music collection and streaming services. Use at your own risk and always backup your data before running operations.
