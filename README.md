# rkdb - Rekordbox and Spotify management tool

A Python tool for managing Rekordbox and Spotify playlists, with features for crawling, syncing, and organizing music.

## Features

- **Spotify Playlist Crawler**: Automatically create playlists from multiple sources (playlists, artists, labels) based on YAML configuration
- **Smart Caching**: Efficient caching system to reduce API calls and improve performance
- **Cross-Platform Sync**: Sync playlists between Spotify and Rekordbox
- **Track Management**: Search, buy, and manage tracks across platforms
- **Cover Generation**: Automatic playlist cover generation with custom templates

## Recent Optimizations

### API Call Optimization (v2.0)

The crawl system now includes intelligent optimization to reduce redundant API calls across multiple jobs:

- **Shared Artist Fetching**: If multiple jobs request the same artist, only one API call is made and results are filtered per job
- **Shared Label Fetching**: Label searches are batched and shared across jobs
- **Shared Playlist Fetching**: Playlist data is fetched once and filtered per job's time window
- **Centralized Album Management**: Album requests are collected and batched for efficient fetching

**Benefits:**
- Reduces API calls by up to 90% when multiple jobs share sources
- Faster execution times
- Better rate limit management
- Detailed optimization statistics

Example output:
```
📊 Optimization Statistics:
==================================================
🎤 Artists: 150 requests → 75 API calls (saved 75 calls)
🏷️  Labels: 20 requests → 10 API calls (saved 10 calls)
📜 Playlists: 10 requests → 5 API calls (saved 5 calls)
💾 Total: 180 requests → 90 API calls (saved 90 calls)
📈 Efficiency improvement: 50.0%
```

## Installation

1. Clone the repository
2. Create a virtual environment: `python3 -m venv venv`
3. Activate the virtual environment: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`
5. Copy `src/secret_keys.py.example` to `src/secret_keys.py` and add your API keys

## Usage

### Basic Commands

```bash
# Run crawl with current date
./venv/bin/python3 src/main.py crawl

# Run crawl with custom date
./venv/bin/python3 src/main.py crawl --date 2025-08-10

# Sync specific playlist
./venv/bin/python3 src/main.py sync playlist_id

# Search Rekordbox tracks
./venv/bin/python3 src/main.py search

# Buy tracks
./venv/bin/python3 src/main.py buy

# Extract playlist data
./venv/bin/python3 src/main.py extract playlist_id

# Test cover generation
./venv/bin/python3 src/main.py test-cover job_name

# Show cache statistics
./venv/bin/python3 src/main.py cache-stats

# Clear cache
./venv/bin/python3 src/main.py cache-clear
```

### Configuration

The crawl system uses `crawl_config.yaml` for configuration. See the file for examples of how to set up jobs with different sources and filters.

## Architecture

The system is built with modular components:

- **Commands**: Individual command modules for different operations
- **Services**: Spotify API integration and authentication
- **Cache**: Intelligent caching system for API responses
- **Functions**: Utility functions for data processing
- **Image Generator**: Playlist cover generation system

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
