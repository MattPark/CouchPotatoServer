# CouchPotato (Modernized Fork)

A modernized fork of [CouchPotatoServer](https://github.com/CouchPotato/CouchPotatoServer) — automatic NZB and torrent downloader for movies. Keep a "movies I want" list and CouchPotato will search for NZBs/torrents, then send them to SABnzbd, qBittorrent, or other download clients.

The original project was abandoned in 2020. This fork brings it back to life with Python 3, a modern database, Docker packaging, and fixes for broken external APIs.

## What Changed

- **Python 3.12+** — full port from Python 2.7
- **TinyDB replaces CodernityDB** — the old database engine was abandoned; TinyDB with in-memory caches provides a 180x speedup
- **Seamless migration** — point the container at your old `/config` volume and it auto-migrates the CodernityDB data on first boot
- **Docker container** — linuxserver.io-style image with s6-overlay, PUID/PGID support
- **IMDB fixed** — list/chart scraping replaced with IMDB GraphQL API (AWS WAF broke all HTML scraping)
- **OMDB budget protection** — 900 calls/day limit with 30-day cache to stay within free tier
- **TMDB-based suggestions** — movie suggestions now powered by TMDB recommendations
- **Graceful shutdown** — TinyDB is properly flushed before container stops
- **Dead code removed** — api.couchpota.to references, broken OAuth proxies, etc.

## Docker (Recommended)

### docker-compose

```yaml
services:
  couchpotato:
    image: ghcr.io/mattpark/couchpotatoserver:latest
    container_name: couchpotato
    environment:
      - PUID=1000
      - PGID=1000
      - TZ=America/New_York
    volumes:
      - /path/to/config:/config
      - /path/to/downloads:/downloads
      - /path/to/movies:/movies
    ports:
      - "5050:5050"
    restart: unless-stopped
```

### docker run

```bash
docker run -d \
  --name couchpotato \
  -e PUID=1000 \
  -e PGID=1000 \
  -e TZ=America/New_York \
  -p 5050:5050 \
  -v /path/to/config:/config \
  -v /path/to/downloads:/downloads \
  -v /path/to/movies:/movies \
  --restart unless-stopped \
  ghcr.io/mattpark/couchpotatoserver:latest
```

### Parameters

| Parameter | Description |
|-----------|-------------|
| `-e PUID=1000` | User ID for file permissions |
| `-e PGID=1000` | Group ID for file permissions |
| `-e TZ=America/New_York` | Timezone ([list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)) |
| `-p 5050:5050` | Web UI port |
| `-v /config` | Config directory (database, settings, cache, logs) |
| `-v /downloads` | Download directory (match your download client's output path) |
| `-v /movies` | Movie library directory |

### Migrating from the Original Container

If you were running the original linuxserver/couchpotato container:

1. Stop the old container
2. Point this container's `/config` volume at the same config directory
3. Start the new container — migration happens automatically on first boot
4. The old CodernityDB database is backed up to `/config/data/database/db_backup/` before migration

## Running from Source

```bash
git clone https://github.com/MattPark/CouchPotatoServer.git
cd CouchPotatoServer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # or: pip install tinydb requests tornado chardet beautifulsoup4 python-dateutil "apscheduler>=3.10,<4" html5lib pyopenssl lxml
python3 CouchPotato.py
```

Open `http://localhost:5050/` in your browser.

Requires Python 3.12 or newer.

## Development

CSS/JS build tools (Grunt, Compass) from the original project still apply for frontend changes. The browser loads combined minified JS files — if editing JavaScript, you must update both the source file and the corresponding `combined.*.min.js`.

```bash
npm install
grunt
```

Enable development mode in CP settings to get JS errors in the browser console instead of the server log.
