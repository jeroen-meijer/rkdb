# Template Variables for Playlist Names and Descriptions

This document lists all available template variables that can be used in playlist names and descriptions in the `crawl_config.yaml` file.

## 📅 Date & Time Variables

### Basic Date Variables
- `{date}` - Current date in YYYY-MM-DD format (e.g., "2025-01-22")
- `{month}` - Full month name (e.g., "January")
- `{year}` - Full year (e.g., "2025")
- `{timestamp}` - Unix timestamp (e.g., "1737543423")

### Enhanced Date Variables
- `{month_num}` - Month number with leading zero (e.g., "01")
- `{month_name}` - Full month name (e.g., "January")
- `{month_name_short}` - Short month name (e.g., "Jan")
- `{quarter}` - Current quarter (e.g., "Q1")
- `{year_short}` - Short year (e.g., "25")

### Week Variables
- `{week_num}` - ISO week number of the year (e.g., "30")
- `{week_start_date}` - Start date of current week (e.g., "2025-01-20")
- `{week_end_date}` - End date of current week (e.g., "2025-01-26")

### Date Range Variables (Based on Search Window)
- `{date_range_start_date}` - Start date of search window (e.g., "2024-12-22")
- `{date_range_end_date}` - End date of search window (e.g., "2025-01-22")
- `{date_range_days}` - Number of days in search window (e.g., "31")
- `{date_range_start_short}` - Short start date (e.g., "Dec 22")
- `{date_range_end_short}` - Short end date (e.g., "Jan 22")
- `{date_range_month}` - Month range if within same month (e.g., "December 2024")
- `{date_range_cross_month}` - Cross-month range (e.g., "Dec 22 - Jan 22")

### Format Variables
- `{date_format_YYYY_MM_DD}` - Date in YYYY-MM-DD format
- `{date_format_DD_MM_YYYY}` - Date in DD-MM-YYYY format
- `{date_format_MMM_DD}` - Date in MMM DD format (e.g., "Jan 22")
- `{time_format_HH_MM}` - Time in HH:MM format

## 📊 Content & Statistics Variables

### Track Statistics
- `{track_count}` - Number of tracks in the final playlist
- `{source_count}` - Total number of input sources (playlists + artists + labels)

### Source Counts
- `{playlist_count}` - Number of input playlists
- `{artist_count}` - Number of input artists
- `{label_count}` - Number of input labels

## 🎵 Job & Configuration Variables

### Job Information
- `{job_name}` - Name of the job from config (e.g., "liquid_weekly")
- `{job_name_pretty}` - Pretty job name (e.g., "Liquid Weekly")

### Input Sources (Simplified)
- `{input_sources}` - Comma-separated list of input sources (e.g., "2 playlists, 1 artist, 2 labels")
- `{input_playlists}` - Comma-separated list of playlist counts (e.g., "2 playlists")
- `{input_artists}` - Comma-separated list of artist counts (e.g., "1 artist")
- `{input_labels}` - Comma-separated list of label counts (e.g., "2 labels")

## 💡 Example Configurations

### Weekly Liquid DnB Playlist
```yaml
output_playlist:
  name: 'Liquid DnB - Week {week_num} ({date_range_start_short} - {date_range_end_short})'
  description: 'Fresh Liquid DnB tracks from {input_sources} • {track_count} tracks • {date_range_days} days of releases'
```

**Result**: "Liquid DnB - Week 30 (Jul 02 - Jul 22)"
**Description**: "Fresh Liquid DnB tracks from 2 playlists, 1 artist, 2 labels • 155 tracks • 20 days of releases"

### Monthly Electronic Music Collection
```yaml
output_playlist:
  name: 'Electronic - {month_name} {year}'
  description: '{track_count} tracks from {source_count} sources • {date_range_days} days of electronic releases'
```

### Quarterly Summary
```yaml
output_playlist:
  name: 'Drum & Bass {quarter} {year} Summary'
  description: 'Best Drum & Bass tracks from {date_range_start_date} to {date_range_end_date} • {track_count} tracks'
```

### Artist-Focused Playlist
```yaml
output_playlist:
  name: 'Artist Spotlight - {month_name_short} {year}'
  description: 'Fresh tracks from {input_artists} • {track_count} tracks from {artist_count} artists'
```

### Label Discovery Playlist
```yaml
output_playlist:
  name: 'Label Discovery - {date_range_cross_month}'
  description: 'New releases from {input_labels} • {track_count} tracks from {label_count} labels'
```

## 🔧 Usage Tips

1. **Mix and Match**: Combine multiple variables for rich, descriptive names
2. **Keep it Short**: Playlist names have character limits, so be concise
3. **Descriptions**: Use descriptions for detailed information since they have more space
4. **Date Ranges**: Use date range variables to show the search window clearly
5. **Statistics**: Include track counts and source counts for transparency

## 📝 Notes

- All variables work in both playlist names and descriptions
- Variables are case-sensitive and must use curly braces `{}`
- Unknown variables are left as-is (not replaced)
- Date calculations are based on the current time when the job runs
- Genre detection is based on keywords in the job name 