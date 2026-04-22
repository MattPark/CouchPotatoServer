"""Library audit tool for detecting mislabeled movies.

Quick scan (local, no network):
  1. Resolution mismatch — actual resolution vs filename-claimed resolution
  2. Runtime mismatch — actual duration vs TMDB runtime from CP database
  3. Container title mismatch — guessit-parsed metadata title/year vs folder title/year
  4. TV episode detection — S##E## / Season / Disc patterns in container titles

Full scan — identification (targeted for flagged files):
  A. Container title already identified it (from quick scan data)
  B. OpenSubtitles moviehash (fast, reads 128KB) → IMDB ID + title
  C. CRC32 reverse lookup on srrDB → release name + IMDB ID
  D. (future) TMDB search fallback

  Smart skip logic (full without force_full):
    - TV episode detected → skip identification, mark for deletion
    - Resolution-only mismatch → skip identification (right movie, wrong quality)
    - Everything else → run identification (suspect file needs it)
    - force_full=1 overrides all skip logic

Usage (standalone):
  python audit.py --movies-dir /movies --db /config/data/database/db.json
  python audit.py --movies-dir /movies --db /config/data/database/db.json --scan-path "Dead, The (1987)"
  python audit.py --movies-dir /movies --db /config/data/database/db.json --full
  python audit.py --movies-dir /movies --db /config/data/database/db.json --full --force-full
  python audit.py --movies-dir /movies --db /config/data/database/db.json --workers 8

API (when running inside CouchPotato):
  GET /api/{key}/audit.scan?full=0&force_full=0&workers=4&scan_path=
  GET /api/{key}/audit.cancel
  GET /api/{key}/audit.progress
  GET /api/{key}/audit.results?offset=0&limit=50&filter_check=&filter_severity=&filter_action=&sort=folder&sort_dir=asc
  GET /api/{key}/audit.stats
  GET /api/{key}/audit.fix.preview?item_id=&action=
  GET /api/{key}/audit.fix?item_id=&action=&confirm=1
  GET /api/{key}/audit.fix.batch?action=&filter_check=&confirm=1&dry_run=1
  GET /api/{key}/audit.fix.progress
  GET /api/{key}/audit.ignore?item_id=&reason=
  GET /api/{key}/audit.unignore?fingerprint=
  GET /api/{key}/audit.ignored
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed

from guessit import guessit as guessit_parse

try:
    import requests
except ImportError:
    requests = None

# Plugin integration — only available when running inside CouchPotato
try:
    from couchpotato import get_db
    from couchpotato.api import addApiView
    from couchpotato.core.event import addEvent, fireEvent, fireEventAsync
    from couchpotato.core.logger import CPLog
    from couchpotato.core.plugins.base import Plugin
    from couchpotato.core.db import RecordNotFound
    from couchpotato.environment import Env
    _CP_AVAILABLE = True
except ImportError:
    _CP_AVAILABLE = False

# Module-level log function — uses CPLog when inside CP, stderr when standalone
if _CP_AVAILABLE:
    log = CPLog(__name__)
    def _log_info(msg): log.info(msg)
    def _log_warn(msg): log.warning(msg)
    def _log_error(msg): log.error(msg)
else:
    def _log_info(msg): print(msg, file=sys.stderr)
    def _log_warn(msg): print(f'WARNING: {msg}', file=sys.stderr)
    def _log_error(msg): print(f'ERROR: {msg}', file=sys.stderr)

# Auto-load as a CP plugin when the module is imported by the loader.
# When running standalone (python audit.py), the loader never calls this.
autoload = 'Audit'


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VIDEO_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.m4v', '.wmv', '.flv', '.ts', '.m2ts'}

# Map claimed resolution labels to expected heights (the "p" in 1080p = lines)
RESOLUTION_HEIGHT_MAP = {
    '2160p': 2160,
    '4k': 2160,
    '1080p': 1080,
    '1080i': 1080,
    '720p': 720,
    '720i': 720,
    '480p': 480,
    '480i': 480,
}

# Runtime tolerance: flag if delta exceeds BOTH thresholds
RUNTIME_DELTA_MIN = 15       # minutes
RUNTIME_DELTA_PCT = 0.20     # 20%

# Junk container titles to ignore
JUNK_TITLE_PATTERNS = [
    re.compile(r'^(lib|x264|x265|hevc|avc|mpeg|divx|xvid)', re.I),
    re.compile(r'producties', re.I),
    re.compile(r'handbrake', re.I),
    re.compile(r'^encoded', re.I),
    re.compile(r'^untitled', re.I),
    re.compile(r'^RARBG', re.I),             # RARBG.COM tracker prefix
    re.compile(r'@', re.I),                  # user@tracker format (SNAKE@IPT, YiFan @ WiKi)
    re.compile(r'\.(com|org|net|rocks|me)\b', re.I),  # domain names in titles
    re.compile(r'^https?://', re.I),         # URLs
    re.compile(r'releases?\s+(at|by)\b', re.I),  # "releases at/by ..."
    re.compile(r'^(EVO|MIRCrew|g33k|CMRG|RPG|nmd|Manning|SbR)\b', re.I),  # known encoder/group names
    re.compile(r'\binternal\s+rls\b', re.I),  # "internal rls" tags
]

# Regex to extract IMDB ID from filename
IMDB_RE = re.compile(r'(tt\d{5,})')

# Regex to parse folder name: "Title (Year)" or "Title, The (Year)"
FOLDER_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$')

# srrDB API base
SRRDB_API = 'https://api.srrdb.com/v1'

# OpenSubtitles REST API v1
OPENSUBTITLES_API = 'https://api.opensubtitles.com/api/v1'

# TV episode patterns in container titles
TV_EPISODE_RE = re.compile(
    r'S\d{2}E\d{2}'           # S01E01
    r'|Season\s*\d+'          # Season 1
    r'|SEASON\s*\d+'          # SEASON 1
    r'|\bST\d{2}\b'          # ST01 (foreign season notation)
    r'|\bDisc\s*\d+'          # Disc 1
    r'|\bDISC\s*\d+',         # DISC 1
    re.I
)

# Default number of parallel scan workers
DEFAULT_WORKERS = 4
MAX_WORKERS = 16

# Edition detection (ported from scanner.py)
EDITION_MAP = {
    "Director's Cut": [('directors', 'cut'), ('directors', 'edition'), 'dc'],
    "Extended Director's Cut": [('extended', 'directors', 'cut'), ('extended', 'director', 'cut')],
    "Unrated Director's Cut": [('unrated', 'dc'), ('unrated', 'directors', 'cut'), ('unrated', 'director', 'cut')],
    'Extended Edition': [('extended', 'cut'), ('extended', 'edition'), 'extended'],
    'Unrated': ['unrated'],
    'Theatrical': [('theatrical', 'cut'), ('theatrical', 'edition'), 'theatrical'],
    'IMAX': [('imax', 'edition'), 'imax'],
    'Final Cut': [('final', 'cut')],
    'Remastered': ['remastered'],
    'Special Edition': [('special', 'edition')],
    'Anniversary Edition': [('anniversary', 'edition')],
    'Criterion': [('criterion', 'collection'), 'criterion'],
    'Redux': ['redux'],
    'Ultimate Cut': [('ultimate', 'cut'), ('ultimate', 'edition')],
    'Rogue Cut': [('rogue', 'cut')],
    "Black & Chrome": [('black', 'chrome'), ('black', 'and', 'chrome')],
    'Uncut': ['uncut'],
}

# Short single-word tags that are ambiguous and must only match AFTER
# the year position to avoid false positives (e.g. 'dc' matching DC Comics,
# 'uncut' matching movie titles like "Uncut Gems" or "Possessor Uncut")
EDITION_AFTER_YEAR_ONLY = {'dc', 'uncut'}

# Words that should not be treated as edition names when followed by Cut/Edition
EDITION_EXCLUDE = {
    'blu', 'ray', 'web', 'hd', 'sd', 'uhd', 'dvd', 'bd', 'hdr', 'tax',
    'pay', 'price', 'budget', 'the', 'a', 'an', 'no', 'rough', 'first',
    'clean',
}

# Technical/quality words that stop compound edition extension.
# If scanning forward from a known edition keyword and we hit one of these,
# the compound ends (e.g. "UNRATED.1080p" → Unrated, not "Unrated 1080p Cut").
_COMPOUND_STOP = {
    '1080p', '720p', '480p', '2160p', '4k',
    'bluray', 'brrip', 'webrip', 'web', 'dl', 'hdrip', 'dvdrip',
    'x264', 'x265', 'h264', 'h265', 'hevc', 'avc',
    'dts', 'ac3', 'aac', 'flac', 'remux',
    'mkv', 'mp4', 'avi',
    'hdr', 'hdr10', 'dv', 'dolby',
}

# ---------------------------------------------------------------------------
# Guessit → CouchPotato token mapping
# ---------------------------------------------------------------------------
# guessit returns different string values than CP's renamer expects.
# These dicts translate guessit output to the display names used in
# renamer template tokens (<video>, <audio>, <source>, <group>).

GUESSIT_VIDEO_MAP = {
    'H.264': 'H264',
    'H.265': 'x265',
    'MPEG-4 Visual': 'MPEG4',
    'MPEG-2': 'MPEG2',
    'MPEG-1': 'MPEG2',
    'VP8': 'VP8',
    'VP9': 'VP9',
    'AV1': 'AV1',
    'VC-1': 'VC1',
    'Theora': 'Theora',
    'x264': 'x264',
    'x265': 'x265',
    'XviD': 'Xvid',
    'DivX': 'DivX',
}

GUESSIT_AUDIO_MAP = {
    'AC3': 'AC3',
    'Dolby Digital': 'AC3',
    'E-AC-3': 'EAC3',
    'Dolby Digital Plus': 'EAC3',
    'DTS': 'DTS',
    'DTS-HD': 'DTS-HD',
    'DTS-HD MA': 'DTS-HD MA',
    'Dolby TrueHD': 'TrueHD',
    'Dolby Atmos': 'TrueHD',
    'AAC': 'AAC',
    'FLAC': 'FLAC',
    'MP3': 'MP3',
    'PCM': 'PCM',
    'Vorbis': 'Vorbis',
    'Opus': 'Opus',
    'WMA': 'WMA',
}

GUESSIT_SOURCE_MAP = {
    'Blu-ray': 'Blu-ray',
    'Ultra HD Blu-ray': 'Blu-ray',
    'HD DVD': 'HD DVD',
    'DVD': 'DVD',
    'HDTV': 'HDTV',
    'Web': 'WEB-DL',
    'Pay-per-view': 'HDTV',
    'TV': 'HDTV',
}

# Tokens that are "fillable" from audit data (folder, mediainfo, DB, guessit).
# Used to determine whether the template can be fully reconstructed.
FILLABLE_TOKENS = {
    'ext', 'thename', 'namethe', 'year', 'first', 'quality',
    'quality_type', 'video', 'audio', 'source', 'group',
    'audio_channels', 'imdb_id', 'cd', 'cd_nr',
    'edition', 'edition_plex',
    'imdb_id_plex', 'imdb_id_emby', 'imdb_id_kodi',
    '3d', '3d_type', '3d_type_short',
    'mpaa', 'mpaa_only', 'category',
    'resolution_width', 'resolution_height',
}


# ---------------------------------------------------------------------------
# Database loader
# ---------------------------------------------------------------------------

def load_cp_database(db_path):
    """Load CP's db.json and build lookup indices.

    Returns:
        media_by_imdb: dict mapping IMDB ID → {title, year, runtime, _id}
        files_by_media_id: dict mapping media _id → list of file paths
        media_by_title: dict mapping (normalized_title, year) → {title, year, runtime, _id, imdb_id}
        release_by_filepath: dict mapping file path → {release_id, media_id}
    """
    with open(db_path, 'r') as f:
        data = json.load(f).get('_default', {})

    media_by_imdb = {}
    files_by_media_id = {}
    release_by_filepath = {}

    # First pass: collect media records
    for doc in data.values():
        if doc.get('_t') == 'media' and doc.get('type') == 'movie':
            imdb_id = doc.get('identifiers', {}).get('imdb', '')
            info = doc.get('info', {})
            if imdb_id:
                media_by_imdb[imdb_id] = {
                    'title': doc.get('title', ''),
                    'year': info.get('year', 0),
                    'runtime': info.get('runtime', 0),
                    '_id': doc.get('_id', ''),
                }

    # Second pass: collect file paths from done releases
    for doc in data.values():
        if doc.get('_t') == 'release' and doc.get('status') == 'done':
            media_id = doc.get('media_id', '')
            release_id = doc.get('_id', '')
            movie_files = doc.get('files', {}).get('movie', [])
            if media_id and movie_files:
                files_by_media_id.setdefault(media_id, []).extend(movie_files)
                for fp in movie_files:
                    release_by_filepath[fp] = {
                        'release_id': release_id,
                        'media_id': media_id,
                    }

    # Build title+year index for IMDB enrichment
    media_by_title = {}
    for imdb_id, entry in media_by_imdb.items():
        key = (normalize_title(entry['title']), entry.get('year', 0))
        media_by_title[key] = {
            'title': entry['title'],
            'year': entry.get('year', 0),
            'runtime': entry.get('runtime', 0),
            '_id': entry.get('_id', ''),
            'imdb_id': imdb_id,
        }

    return media_by_imdb, files_by_media_id, media_by_title, release_by_filepath


# ---------------------------------------------------------------------------
# Title normalization
# ---------------------------------------------------------------------------

def normalize_title(title):
    """Normalize a movie title for comparison.

    Handles 'Title, The' → 'the title', strips punctuation, lowercases.
    """
    if not title:
        return ''
    t = title.strip()

    # Handle "Something, The" → "The Something"
    comma_article = re.match(r'^(.+),\s*(The|A|An)\s*$', t, re.I)
    if comma_article:
        t = comma_article.group(2) + ' ' + comma_article.group(1)

    # Lowercase, strip punctuation, collapse whitespace
    t = t.lower()
    t = re.sub(r'[^\w\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def titles_match(title_a, title_b):
    """Check if two normalized titles are substantially the same."""
    a = normalize_title(title_a)
    b = normalize_title(title_b)

    if not a or not b:
        return True  # can't compare, don't flag

    if a == b:
        return True

    # Check if one is a substring of the other
    # (e.g., "the dead" is in "dawn of the dead" — but this should NOT match)
    # Instead, check word overlap
    words_a = set(a.split())
    words_b = set(b.split())

    # Remove very common words for comparison
    stopwords = {'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'and', 'or', 'is'}
    sig_a = words_a - stopwords
    sig_b = words_b - stopwords

    if not sig_a or not sig_b:
        # One title is all stopwords — can't meaningfully compare
        return True

    # Jaccard similarity on significant words
    overlap = sig_a & sig_b
    union = sig_a | sig_b
    similarity = len(overlap) / len(union) if union else 1.0

    return similarity >= 0.7


# ---------------------------------------------------------------------------
# File metadata extraction
# ---------------------------------------------------------------------------

def _format_audio_channels(channels_str, channel_layout):
    """Convert mediainfo channel count + layout to a human-readable label.

    Returns e.g. '7.1', '5.1', '2.0', '1.0', or the raw count as fallback.
    """
    try:
        ch = int(channels_str)
    except (ValueError, TypeError):
        return ''
    layout = (channel_layout or '').upper()
    has_lfe = 'LFE' in layout
    if has_lfe:
        return '%s.1' % (ch - 1,)
    if ch <= 2:
        return '%s.0' % ch
    # No LFE detected but >2 channels — show raw count
    return str(ch)


def _format_audio_codec(track):
    """Derive a human-friendly audio codec name from a mediainfo Audio track.

    Prefers Format_Commercial_IfAny when it's informative (e.g. 'Dolby TrueHD
    with Dolby Atmos').  Falls back to a curated mapping of Format values, with
    Format_AdditionalFeatures used to distinguish variants like DTS-HD MA.
    """
    fmt = track.get('Format', '')
    commercial = track.get('Format_Commercial_IfAny', '')
    additional = track.get('Format_AdditionalFeatures', '')

    # Commercial name is often the best — but skip if it's just "Dolby Digital"
    # for plain AC-3 since we can be more concise.
    if commercial:
        c = commercial.lower()
        if 'truehd' in c and 'atmos' in c:
            return 'TrueHD Atmos'
        if 'truehd' in c:
            return 'TrueHD'
        if 'dolby digital plus' in c and 'atmos' in c:
            return 'DD+ Atmos'
        if 'dolby digital plus' in c:
            return 'DD+'
        if 'dts-hd master' in c or 'dts-hd ma' in c.replace(' ', '-'):
            return 'DTS-HD MA'
        if 'dts-hd' in c:
            return 'DTS-HD'
        if 'dts:x' in c.lower():
            return 'DTS:X'

    # Mapping from mediainfo Format field
    fmt_upper = fmt.upper()
    add_upper = additional.upper()
    if fmt_upper == 'MLP FBA':
        return 'TrueHD Atmos' if '16-CH' in add_upper else 'TrueHD'
    if fmt_upper == 'E-AC-3':
        return 'DD+ Atmos' if 'JOC' in add_upper else 'DD+'
    if fmt_upper == 'AC-3':
        return 'AC3'
    if fmt_upper == 'DTS':
        if 'XLL' in add_upper:
            return 'DTS-HD MA'
        if 'X' in add_upper:
            return 'DTS:X'
        return 'DTS'
    if fmt_upper == 'AAC':
        return 'AAC'
    if fmt_upper in ('FLAC',):
        return 'FLAC'
    if fmt_upper == 'PCM':
        return 'PCM'
    if fmt_upper == 'MPEG AUDIO':
        return 'MP3'
    if fmt_upper == 'VORBIS':
        return 'Vorbis'
    if fmt_upper == 'OPUS':
        return 'Opus'
    if fmt_upper == 'WMA':
        return 'WMA'
    # Fallback: return raw format or 'Unknown'
    return fmt or 'Unknown'


def _extract_audio_tracks(tracks):
    """Extract audio track info from mediainfo track list.

    Returns a list of dicts: [{codec, channels, language}, ...]
    """
    audio_tracks = []
    for t in tracks:
        if not isinstance(t, dict):
            continue
        if t.get('@type') != 'Audio':
            continue
        codec = _format_audio_codec(t)
        channels = _format_audio_channels(t.get('Channels', ''), t.get('ChannelLayout', ''))
        language = t.get('Language', '')
        audio_tracks.append({
            'codec': codec,
            'channels': channels,
            'language': language,
        })
    return audio_tracks


def extract_file_meta(filepath):
    """Extract metadata from a video file using the mediainfo CLI.

    Uses the CLI binary instead of the Python library to isolate crashes —
    if libmediainfo segfaults on a file, only the subprocess dies and the
    parent process continues scanning.

    Returns dict with: resolution_width, resolution_height, duration_min,
                       video_codec, container_title, audio_tracks
    """
    result = {
        'resolution_width': 0,
        'resolution_height': 0,
        'duration_min': 0.0,
        'video_codec': '',
        'container_title': None,
        'audio_tracks': [],
    }

    # Ask mediainfo for JSON output (available in mediainfo >= 18.03)
    try:
        proc = subprocess.run(
            ['mediainfo', '--Output=JSON', filepath],
            capture_output=True, text=True, timeout=60,
            encoding='utf-8', errors='replace',
        )
    except FileNotFoundError:
        _log_warn('mediainfo CLI not found, skipping')
        return result
    except subprocess.TimeoutExpired:
        _log_warn('mediainfo timed out on %s' % filepath)
        return result

    if proc.returncode != 0:
        _log_warn('mediainfo exited %s on %s' % (proc.returncode, filepath))
        return result

    try:
        data = json.loads(proc.stdout)
    except (json.JSONDecodeError, ValueError) as e:
        _log_warn('mediainfo JSON parse error on %s: %s' % (filepath, e))
        return result

    tracks = data.get('media', {}).get('track', [])
    general = None
    video = None
    for t in tracks:
        if not isinstance(t, dict):
            continue
        ttype = t.get('@type', '')
        if ttype == 'General' and general is None:
            general = t
        elif ttype == 'Video' and video is None:
            video = t

    if not video:
        return result

    # Resolution
    try:
        result['resolution_width'] = int(video.get('Width', 0))
    except (ValueError, TypeError):
        pass
    try:
        result['resolution_height'] = int(video.get('Height', 0))
    except (ValueError, TypeError):
        pass

    # Codec
    result['video_codec'] = video.get('Format', '')

    # Duration: prefer general track (container-level, value is in seconds)
    duration_sec = 0
    if general and general.get('Duration'):
        try:
            duration_sec = float(general['Duration'])
        except (ValueError, TypeError):
            pass
    if duration_sec == 0 and video.get('Duration'):
        try:
            duration_sec = float(video['Duration'])
        except (ValueError, TypeError):
            pass
    if duration_sec > 0:
        result['duration_min'] = duration_sec / 60.0

    # Container title
    if general and general.get('Title'):
        result['container_title'] = general['Title']

    # Audio tracks
    result['audio_tracks'] = _extract_audio_tracks(tracks)

    return result


def parse_folder_name(folder_name):
    """Parse a folder name like 'Dead, The (1987)' into title and year."""
    m = FOLDER_RE.match(folder_name)
    if m:
        return m.group(1).strip(), int(m.group(2))
    return folder_name, None


def parse_filename_resolution(filename):
    """Extract claimed resolution from filename (e.g., '1080p')."""
    lower = filename.lower()
    for label in ['2160p', '4k', '1080p', '1080i', '720p', '720i', '480p', '480i']:
        if label in lower:
            return label
    return None


def parse_filename_imdb(filename):
    """Extract IMDB ID from filename (e.g., 'tt0092843')."""
    m = IMDB_RE.search(filename)
    return m.group(1) if m else None


def is_junk_title(title):
    """Check if a container title is junk (encoder name, etc.)."""
    if not title or len(title.strip()) < 3:
        return True
    for pat in JUNK_TITLE_PATTERNS:
        if pat.search(title):
            return True
    return False


def resolution_label(width, height):
    """Convert pixel dimensions to a human-readable resolution label.

    Uses the higher of the two indicators (width-derived or height-derived)
    because both widescreen crops (1920x800 → 1080p by width) and narrow
    aspect ratios (1800x1080 → 1080p by height) are common in movie encodes.
    """
    # Derive label from width (handles widescreen crops like 1920x800)
    if width >= 3800:
        w_label = '2160p'
    elif width >= 1900:
        w_label = '1080p'
    elif width >= 1260:
        w_label = '720p'
    else:
        w_label = None

    # Derive label from height (handles narrow aspect ratios like 1800x1080)
    if height >= 2160:
        h_label = '2160p'
    elif height >= 1080:
        h_label = '1080p'
    elif height >= 720:
        h_label = '720p'
    elif height >= 480:
        h_label = '480p'
    else:
        h_label = f'{height}p (SD)' if height else None

    # Use whichever gives the higher resolution
    rank = {'2160p': 4, '1080p': 3, '720p': 2, '480p': 1}
    w_rank = rank.get(w_label, 0)
    h_rank = rank.get(h_label, 0)

    if w_rank >= h_rank:
        return w_label or h_label or f'{height}p (SD)'
    return h_label or w_label or f'{height}p (SD)'


# ---------------------------------------------------------------------------
# Edition detection (ported from scanner.py:getEdition)
# ---------------------------------------------------------------------------

def _detect_edition_from_words(words, after_year_only=True, year_idx=0):
    """Core edition detection logic using a multi-pass approach.

    Pass 1: Find EDITION_MAP tuple matches (multi-word known patterns like
            'directors.cut') and single-word matches, recording word positions.
    Pass 2: For each single-word match, try to extend into a compound edition
            ending in Cut/Edition (e.g. 'UNRATED.PRODUCERS.CUT').
    Pass 3: Pick the best candidate — earliest position wins; at same position
            tuple > compound > standalone.
    Pass 4: Fallback regex for unknown '<word(s)> Cut/Edition' patterns.

    Args:
        words: list of lowercase tokens from the filename/release name
        after_year_only: if True, restrict search to words at/after year_idx
        year_idx: index of the year token in words (0 if not found)

    Returns:
        edition string (e.g. "Director's Cut", "Unrated Producers Cut") or ''.
    """
    if after_year_only:
        search_words = words[year_idx:]
        offset = year_idx  # to map back to absolute positions in words
    else:
        search_words = words
        offset = 0

    search_joined = '.'.join(search_words)

    # Words after the year (for EDITION_AFTER_YEAR_ONLY gating)
    after_year_words = words[year_idx:] if year_idx else []

    # ---- Pass 1: collect all EDITION_MAP matches with positions ----
    # Each candidate: (start_pos, span, canonical_name, match_type)
    # match_type: 0=tuple (best), 1=compound, 2=standalone (worst)
    candidates = []

    for key, tags in EDITION_MAP.items():
        for tag in tags:
            if isinstance(tag, tuple):
                tag_str = '.'.join(tag)
                idx = search_joined.find(tag_str)
                if idx >= 0:
                    # Convert char position to word position
                    word_pos = search_joined[:idx].count('.') if idx > 0 else 0
                    span = len(tag)
                    candidates.append((word_pos + offset, span, key, 0))
            elif isinstance(tag, str):
                tag_lower = tag.lower()
                # EDITION_AFTER_YEAR_ONLY gating
                if tag_lower in EDITION_AFTER_YEAR_ONLY:
                    if not (after_year_words and tag_lower in after_year_words):
                        continue
                # Find all positions of this single-word tag
                for i, w in enumerate(search_words):
                    if w == tag_lower:
                        candidates.append((i + offset, 1, key, 2))

    # ---- Pass 2: compound extension for single-word matches ----
    # For each standalone match (type 2), look forward for Cut/Edition
    compound_candidates = []
    for start_pos, span, canon_name, mtype in candidates:
        if mtype != 2:
            continue
        # Scan forward up to 3 words looking for 'cut' or 'edition'
        for ahead in range(1, 4):
            look_idx = start_pos + ahead
            if look_idx >= len(words):
                break
            w = words[look_idx]
            if w in _COMPOUND_STOP:
                break  # hit a tech word, stop extending
            if w in ('cut', 'edition'):
                # Build compound from start_pos through look_idx
                compound_words = words[start_pos:look_idx + 1]
                compound_name = ' '.join(w2.title() for w2 in compound_words)
                compound_span = look_idx - start_pos + 1
                compound_candidates.append(
                    (start_pos, compound_span, compound_name, 1)
                )
                break  # take the first Cut/Edition found

    candidates.extend(compound_candidates)

    if candidates:
        # ---- Pass 3: pick the best candidate ----
        # Sort by: (start_pos ASC, match_type ASC, -span DESC)
        # match_type: 0=tuple > 1=compound > 2=standalone
        candidates.sort(key=lambda c: (c[0], c[3], -c[1]))
        return candidates[0][2]

    return ''


def _edition_fallback_regex(text, after_year_text=None):
    """Fallback: catch unknown '<word(s)> Cut/Edition' patterns via regex.

    Only called when _detect_edition_from_words found nothing.
    Searches after_year_text if provided, otherwise full text.

    Returns edition string or ''.
    """
    search_text = after_year_text if after_year_text is not None else text
    m = re.search(
        r'[\.\s_\-]((?:[a-z]+[\.\s_\-]){0,1}(?:[a-z]+))[\.\s_\-](cut|edition)'
        r'(?=[\.\s_\-]|$)',
        search_text, re.IGNORECASE
    )
    if m:
        name_part = re.sub(r'[\._\-]', ' ', m.group(1)).strip()
        kind = m.group(2)
        last_word = name_part.split()[-1].lower() if name_part else ''
        if last_word and last_word not in EDITION_EXCLUDE:
            return '%s %s' % (name_part.title(), kind.title())
    return ''


def get_edition(filename):
    """Detect edition/cut info from a filename or release name.

    Ported from Scanner.getEdition() in scanner.py.  Works as a standalone
    function (no class instance required).

    Only searches AFTER the year in the filename to avoid false positives
    when edition words appear in the movie title.

    Uses the multi-pass _detect_edition_from_words() algorithm for compound
    edition detection (e.g. "Unrated Producers Cut"), then falls back to
    _edition_fallback_regex() for unknown patterns.

    Returns the edition string (e.g. "Director's Cut") or empty string.
    """
    filename = str(filename)

    # Check for Plex {edition-X} tag first
    plex_match = re.search(r'\{edition-([^}]+)\}', filename, re.IGNORECASE)
    if plex_match:
        return plex_match.group(1)

    # Collapse possessive apostrophes before tokenizing so "Director's"
    # becomes "Directors" and matches EDITION_MAP tuples like ('directors', 'cut')
    text = re.sub(r"(\w)'s\b", r'\1s', filename.lower())
    words = re.split(r'\W+', text)

    # Find year position — editions only appear after the year in release names
    year_idx = 0
    for i, w in enumerate(words):
        if re.match(r'^(19|20)\d{2}$', w):
            year_idx = i
            break

    # Multi-pass detection: tuple matches, compound extension, best-pick
    result = _detect_edition_from_words(words, after_year_only=True,
                                        year_idx=year_idx)
    if result:
        return result

    # Fallback regex for unknown patterns — restrict to after the year
    basename = os.path.basename(filename)
    year_match = re.search(r'[\.\s_\-]((?:19|20)\d{2})[\.\s_\-]', basename)
    after_year_text = basename[year_match.start():] if year_match else basename
    return _edition_fallback_regex(basename, after_year_text=after_year_text)


# ---------------------------------------------------------------------------
# Item ID + recommended action
# ---------------------------------------------------------------------------

def compute_item_id(file_path):
    """Compute a stable ID for a flagged item: SHA256 of file_path, 12 hex chars."""
    return hashlib.sha256(file_path.encode('utf-8')).hexdigest()[:12]


def compute_file_fingerprint(file_path):
    """Compute a fingerprint for a file: file_size + SHA256 of first 64KB.

    Returns '<size>:<sha256hex16>' or None if file is unreadable.
    ~1ms per file.
    """
    try:
        size = os.path.getsize(file_path)
        h = hashlib.sha256()
        with open(file_path, 'rb') as f:
            h.update(f.read(65536))
        return '%d:%s' % (size, h.hexdigest()[:16])
    except (OSError, IOError):
        return None


def compute_recommended_action(flags, identification=None, expected=None):
    """Derive the recommended fix action from flags and identification data.

    Returns one of:
        'delete_wrong'       — TV episode or identified as non-movie
        'delete_duplicate'   — duplicate file (keep/delete decided by pick_best_duplicate)
        'verify_audio'       — audio language tag missing, needs whisper verification
        'delete_foreign'     — all audio tracks are non-English (no accepted language)
        'rename_template'    — filename doesn't match template (superset of resolution/edition)
        'rename_resolution'  — resolution-only flag (right movie, wrong quality label)
        'reassign_movie'     — identification found a different movie
        'rename_edition'     — edition detected in container but not filename
        'needs_full'        — title/runtime mismatch, needs identification
        'manual_review'      — identification ran but couldn't match
        'none'               — no action needed (identification confirmed same movie)

    Args:
        flags: list of flag dicts with 'check' keys
        identification: identification dict (optional)
        expected: item['expected'] dict with title/year/db_title (optional).
            When provided, allows same-title detection so year-only
            mismatches get 'rename_template' instead of 'reassign_movie'.
    """
    checks = {f['check'] for f in flags}

    # TV episode always gets delete
    if 'tv_episode' in checks:
        return 'delete_wrong'

    # Duplicate file — separate action from delete_wrong so UI can
    # distinguish duplicates from TV episodes
    if 'duplicate' in checks:
        return 'delete_duplicate'

    # Unknown audio — language tag missing, needs whisper verification
    if 'unknown_audio' in checks:
        return 'verify_audio'

    # Foreign audio — all tracks are non-English, recommend deletion
    if 'foreign_audio' in checks:
        return 'delete_foreign'

    # If identification has run, use it to decide — takes priority over
    # quick scan flag-based logic since it has more information
    if identification:
        method = identification.get('method', '')

        if method == 'tv_episode_detected':
            return 'delete_wrong'

        # OpenSubtitles identified as TV episode
        if method == 'opensubtitles_hash' and identification.get('feature_type') == 'episode':
            return 'delete_wrong'

        if method in ('container_title', 'srrdb_crc', 'opensubtitles_hash', 'manual'):
            id_title = identification.get('identified_title', '')
            id_imdb = identification.get('identified_imdb')

            # When we have expected data AND an identified title, compare them
            # to distinguish "same movie, wrong year" from "different movie"
            if id_title and expected:
                exp_title = expected.get('db_title') or expected.get('title', '')
                if titles_match(id_title, exp_title):
                    # Same movie title — check year
                    id_year = identification.get('identified_year')
                    exp_year = expected.get('year')
                    if (id_year and exp_year
                            and str(id_year) != str(exp_year)):
                        # Year differs — only treat as rename if within ±1
                        # (beyond that is likely a remake = different movie)
                        if abs(int(id_year) - int(exp_year)) <= 1:
                            return 'rename_template'
                        else:
                            return 'reassign_movie'
                    # Same title, same year (or can't compare) — identification
                    # confirmed identity.  Fall through to flag-based logic
                    # which will handle any remaining template/res/edition flags.
                    pass
                else:
                    # Different title — genuinely a different movie
                    return 'reassign_movie'
            elif id_imdb or id_title:
                # No expected data to compare (backward compat) or IMDB-only
                # match — assume reassign
                return 'reassign_movie'

        if method == 'crc_not_found':
            return 'manual_review'

        if method == 'skipped':
            # High-confidence quick scan — fall through to flag-based logic below
            pass

    # Template mismatch is a naming-only fix — use rename_template
    # It subsumes resolution and edition rename when template check is present
    if 'template' in checks:
        # If there are also identity flags (title/runtime) and no identification data,
        # those need identification first
        identity_checks = checks & {'title', 'runtime'}
        if identity_checks and not identification:
            return 'needs_full'
        return 'rename_template'

    # No identification data — decide from quick scan flags alone
    if checks == {'resolution'}:
        return 'rename_resolution'

    if checks == {'edition'}:
        return 'rename_edition'

    if 'edition' in checks and checks - {'edition', 'resolution'} == set():
        return 'rename_resolution'

    # Title or runtime mismatch without identification — needs identification.
    # But if identification already ran and confirmed the same movie,
    # there's nothing to fix (e.g., container metadata has a different
    # year but the filename is already correct).
    if 'title' in checks or 'runtime' in checks:
        if not identification:
            return 'needs_full'
        return 'none'

    return 'manual_review'


def pick_best_duplicate(a, b):
    """Given two duplicate items, return (keep, delete) — the better one first.

    Preference order:
    1. HEVC/h265 codec wins over non-HEVC (even if smaller)
    2. Otherwise, larger file wins

    Args:
        a, b: result dicts with 'actual' (video_codec) and 'file_size_bytes'

    Returns:
        (keep, delete) tuple of the two input dicts
    """
    a_codec = (a.get('actual', {}).get('video_codec') or '').upper()
    b_codec = (b.get('actual', {}).get('video_codec') or '').upper()

    a_hevc = 'HEVC' in a_codec or 'H265' in a_codec or 'X265' in a_codec or 'H.265' in a_codec
    b_hevc = 'HEVC' in b_codec or 'H265' in b_codec or 'X265' in b_codec or 'H.265' in b_codec

    # If one is HEVC and the other isn't, HEVC wins
    if a_hevc and not b_hevc:
        return (a, b)
    if b_hevc and not a_hevc:
        return (b, a)

    # Same codec family — larger file wins
    a_size = a.get('file_size_bytes', 0)
    b_size = b.get('file_size_bytes', 0)
    if a_size >= b_size:
        return (a, b)
    return (b, a)


# ---------------------------------------------------------------------------
# Quick scan checks
# ---------------------------------------------------------------------------

def check_resolution(claimed_label, actual_width, actual_height):
    """Check 1: Resolution mismatch.

    Uses both width and height to classify the actual resolution.  Width is
    the primary indicator because widescreen crops (e.g. 1920x800) reduce
    height but the encode is still 1080p.

    Returns a flag dict or None.
    """
    if not claimed_label or not actual_height:
        return None

    expected_height = RESOLUTION_HEIGHT_MAP.get(claimed_label.lower())
    if expected_height is None:
        return None

    actual_label = resolution_label(actual_width, actual_height)
    expected_label = resolution_label(0, expected_height)

    if actual_label != expected_label:
        return {
            'check': 'resolution',
            'severity': 'HIGH',
            'detail': f'Claimed {claimed_label}, actual {actual_width}x{actual_height} (maps to {actual_label})',
        }

    return None


def check_runtime(actual_duration_min, expected_runtime_min, edition='',
                   container_title=''):
    """Check 2: Runtime mismatch.

    Edition-aware: when a non-theatrical edition is detected (Extended,
    Director's Cut, Unrated, etc.) and the file is LONGER than expected,
    suppress the flag — extended editions are expected to run longer than
    the standard TMDB runtime.  If the file is shorter than expected even
    with an edition tag, still flag it (likely truncated).

    The edition can come from the filename or from the container title.

    Returns a flag dict or None.
    """
    if not expected_runtime_min or not actual_duration_min:
        return None

    delta = abs(actual_duration_min - expected_runtime_min)
    pct = delta / expected_runtime_min if expected_runtime_min else 0

    # Flag only if BOTH absolute and percentage thresholds are exceeded
    if delta > RUNTIME_DELTA_MIN and pct > RUNTIME_DELTA_PCT:
        # Edition-aware suppression: if file has a non-theatrical edition
        # and runs LONGER than TMDB, suppress the flag
        is_longer = actual_duration_min > expected_runtime_min
        if is_longer:
            effective_edition = _get_effective_edition(edition, container_title)
            if effective_edition and effective_edition.lower() != 'theatrical':
                return None

        return {
            'check': 'runtime',
            'severity': 'HIGH',
            'detail': (
                f'Expected {expected_runtime_min} min, '
                f'actual {actual_duration_min:.1f} min '
                f'(delta: {delta:+.1f} min, {pct:+.0%})'
            ),
        }

    return None


# Regex for edition hints in container titles — used when no edition was
# detected from the filename itself.
_CONTAINER_EDITION_RE = re.compile(
    r'\b(extended|director.?s?\s*cut|unrated|uncut|special\s+edition|'
    r'assembly\s*cut|ultimate|redux|final\s+cut|international\s+cut|'
    r'alternate\s+cut|uncensored|criterion|remaster(?:ed)?|'
    r'theatrical(?:\s+cut)?|deluxe|superbit|imax)\b',
    re.IGNORECASE,
)


def _get_effective_edition(edition, container_title):
    """Return the best-guess edition from filename edition or container title.

    Args:
        edition: Edition string detected from the filename (may be empty).
        container_title: Raw container title from mediainfo (may be empty).

    Returns:
        Edition string or '' if no edition detected.
    """
    if edition:
        return edition
    if container_title:
        m = _CONTAINER_EDITION_RE.search(container_title)
        if m:
            return m.group(1)
    return ''


def _parse_edition_from_release(release_name):
    """Parse edition info from a srrDB release name or container title source.

    Unlike get_edition(), this searches the ENTIRE string (not just after the
    year) because release names like 'Apocalypse.Now.Redux.1979...' have the
    edition keyword before the year.  Uses the same EDITION_MAP keywords.

    Tags in EDITION_AFTER_YEAR_ONLY (e.g. 'dc', 'uncut') are only matched
    after the year to avoid false positives like 'DC' in 'DC Comics' titles
    or 'Uncut' in 'Uncut Gems'.

    Uses the multi-pass _detect_edition_from_words() algorithm for compound
    edition detection (e.g. "Unrated Producers Cut"), then falls back to
    _edition_fallback_regex() for unknown patterns.

    Returns the edition string (e.g. "Extended Edition") or ''.
    """
    if not release_name:
        return ''
    # Collapse possessive apostrophes: "Director's" → "Directors"
    text = re.sub(r"(\w)'s\b", r'\1s', release_name.lower())
    words = re.split(r'\W+', text)

    # Find year position for gating ambiguous tags
    year_idx = 0
    for i, w in enumerate(words):
        if re.match(r'^(19|20)\d{2}$', w):
            year_idx = i
            break

    # Multi-pass detection: search full string (after_year_only=False)
    # but still pass year_idx so EDITION_AFTER_YEAR_ONLY gating works
    result = _detect_edition_from_words(words, after_year_only=False,
                                        year_idx=year_idx)
    if result:
        return result

    # Fallback regex — search entire release name
    return _edition_fallback_regex(release_name)


def _backfill_edition_from_identification(result):
    """Backfill detected_edition from identification source.

    After identification, the srrDB release name or container title
    source may contain edition info that wasn't available during the quick scan.
    This function:
      1. Parses the identification source for edition keywords
      2. Sets detected_edition on the item if not already set
      3. If the edition is non-theatrical and the file runs longer than
         expected, removes the runtime flag (false positive for extended cuts)
      4. Adds an edition mismatch flag if the edition isn't in the filename

    Args:
        result: The audit item dict (modified in-place).

    Returns:
        The backfilled edition string, or '' if nothing was backfilled.
    """
    ident = result.get('identification')
    if not ident:
        return ''

    # Only backfill if we don't already have an edition
    if result.get('detected_edition'):
        return ''

    source = ident.get('source', '')
    if not source:
        return ''

    edition = _parse_edition_from_release(source)
    if not edition:
        return ''

    result['detected_edition'] = edition

    # If non-theatrical edition and file runs LONGER, suppress runtime flag
    if edition.lower() != 'theatrical':
        actual_duration = result.get('actual', {}).get('duration_min', 0)
        expected_runtime = result.get('expected', {}).get('runtime_min', 0)
        if (actual_duration and expected_runtime
                and actual_duration > expected_runtime):
            old_count = len(result.get('flags', []))
            result['flags'] = [
                f for f in result.get('flags', [])
                if f['check'] != 'runtime'
            ]
            if len(result['flags']) != old_count:
                result['flag_count'] = len(result['flags'])

    # Add edition mismatch flag if edition not in filename
    filename = result.get('file', '')
    filename_edition = get_edition(filename)
    if not filename_edition:
        # Check that we don't already have an edition flag
        has_edition_flag = any(
            f['check'] == 'edition' for f in result.get('flags', [])
        )
        if not has_edition_flag:
            result['flags'].append({
                'check': 'edition',
                'severity': 'LOW',
                'detail': (
                    'Edition "%s" found in release name but not in filename'
                    % edition
                ),
            })
            result['flag_count'] = len(result['flags'])

    return edition


def check_container_title(container_title, folder_title, folder_year,
                          imdb_year=None):
    """Check 3: Container title mismatch.

    Args:
        container_title: The title embedded in the video container metadata.
        folder_title: The movie title parsed from the folder name.
        folder_year: The year parsed from the folder name.
        imdb_year: The authoritative year from the CP database (sourced from
            IMDB/TMDB).  When provided and the title matches but the year is
            off by exactly ±1, the IMDB year is used to adjudicate:
            - folder year matches IMDB → suppress the flag (false positive)
            - container year matches IMDB → keep as HIGH (folder may be wrong)
            - neither matches → downgrade to LOW

    Returns a flag dict or None.  Also returns parsed metadata (title, year)
    for use in identification.
    """
    if not container_title or is_junk_title(container_title):
        return None, None

    parsed = guessit_parse(container_title)
    meta_title = parsed.get('title')
    meta_year = parsed.get('year')

    if not meta_title:
        return None, None

    # Short titles with no year are almost certainly junk encoder/group names
    # (e.g. "EVO", "g33k", "Manning", "Dread-Team", "ultimate-force").
    # Real scene names include a year.  Allow longer titles through since they
    # could be actual movie names (e.g. "What keeps you alive").
    # Don't flag as a title mismatch, but DO return the parsed metadata so
    # Identification Strategy A can still attempt identification from it.
    if not meta_year and len(meta_title.split()) <= 3:
        parsed_meta = {
            'title': meta_title,
            'year': meta_year,
            'screen_size': parsed.get('screen_size'),
            'raw': container_title,
        }
        return None, parsed_meta

    parsed_meta = {
        'title': meta_title,
        'year': meta_year,
        'screen_size': parsed.get('screen_size'),
        'raw': container_title,
    }

    title_same = titles_match(meta_title, folder_title)
    year_same = (meta_year is None) or (folder_year is None) or (meta_year == folder_year)

    if not title_same and not year_same:
        return {
            'check': 'title',
            'severity': 'HIGH',
            'detail': (
                f"Container title '{meta_title} ({meta_year})' "
                f"vs folder '{folder_title} ({folder_year})'"
            ),
        }, parsed_meta

    if not title_same:
        return {
            'check': 'title',
            'severity': 'MEDIUM',
            'detail': (
                f"Container title '{meta_title}' "
                f"vs folder '{folder_title}' (year matches)"
            ),
        }, parsed_meta

    if not year_same:
        year_diff = abs(meta_year - folder_year)
        if year_diff == 1:
            verdict = _check_year_against_imdb(meta_year, folder_year, imdb_year)
            if verdict == 'suppress':
                # Folder year matches IMDB — container just uses a different
                # year convention (production vs. theatrical release).
                return None, parsed_meta
            elif verdict == 'downgrade':
                return {
                    'check': 'title',
                    'severity': 'LOW',
                    'detail': (
                        f"Container year {meta_year} "
                        f"vs folder year {folder_year} "
                        f"(title matches, ±1 year — ambiguous)"
                    ),
                }, parsed_meta
            # verdict == 'keep' — fall through to HIGH flag below

        return {
            'check': 'title',
            'severity': 'HIGH',
            'detail': (
                f"Container year {meta_year} "
                f"vs folder year {folder_year} (title matches)"
            ),
        }, parsed_meta

    return None, parsed_meta


def _check_year_against_imdb(container_year, folder_year, imdb_year):
    """Decide whether a ±1 year title flag should be kept, removed, or downgraded.

    When the container title and folder agree on the movie but disagree on the
    year by exactly 1 (production year vs. theatrical release), use the IMDB/
    TMDB year from the CP database to adjudicate:

    Returns:
        'suppress'  — folder year matches IMDB → flag is a false positive
        'keep'      — container year matches IMDB → folder may be wrong
        'downgrade' — neither matches, or no IMDB data → ambiguous, LOW severity
    """
    if not imdb_year:
        return 'downgrade'
    if folder_year == imdb_year:
        return 'suppress'
    if container_year == imdb_year:
        return 'keep'
    return 'downgrade'


def _revalidate_year_flags(item, imdb_year):
    """Re-evaluate ±1 year title flags using newly available IMDB data.

    Called after identification provides an IMDB year that wasn't
    available during the quick scan.  Mutates the item's flags list in place:
    removes suppressed flags and downgrades ambiguous ones.

    Args:
        item: The scan result dict (must have 'flags', 'expected', 'flag_count').
        imdb_year: The authoritative year from IMDB/TMDB identification.

    Returns True if any flag was removed or modified, False otherwise.
    """
    if not imdb_year:
        return False

    flags = item.get('flags', [])
    folder_year = item.get('expected', {}).get('year')
    if not folder_year:
        return False

    modified = False
    to_remove = []

    for i, flag in enumerate(flags):
        if flag.get('check') != 'title':
            continue
        detail = flag.get('detail', '')
        if 'title matches' not in detail:
            continue

        # Extract container year from detail string like
        # "Container year 2018 vs folder year 2019 (title matches)"
        m = re.search(r'Container year (\d{4}) vs folder year (\d{4})', detail)
        if not m:
            continue

        container_year = int(m.group(1))
        flag_folder_year = int(m.group(2))
        year_diff = abs(container_year - flag_folder_year)

        if year_diff != 1:
            continue

        verdict = _check_year_against_imdb(container_year, flag_folder_year,
                                           imdb_year)
        if verdict == 'suppress':
            to_remove.append(i)
            modified = True
        elif verdict == 'downgrade':
            flag['severity'] = 'LOW'
            flag['detail'] = (
                f"Container year {container_year} "
                f"vs folder year {flag_folder_year} "
                f"(title matches, ±1 year — ambiguous)"
            )
            modified = True
        # 'keep' — no change

    # Remove suppressed flags in reverse order to maintain indices
    for i in reversed(to_remove):
        flags.pop(i)

    if modified:
        item['flag_count'] = len(flags)

    return modified


def check_tv_episode(container_title):
    """Check 4: TV episode filed as a movie.

    Detects TV episode patterns (S01E01, Season, Disc) in container titles.
    Returns a flag dict or None.
    """
    if not container_title:
        return None

    m = TV_EPISODE_RE.search(container_title)
    if m:
        return {
            'check': 'tv_episode',
            'severity': 'HIGH',
            'detail': f"Container title looks like a TV episode: '{container_title}'",
        }

    return None


# ---------------------------------------------------------------------------
# Language normalization
# ---------------------------------------------------------------------------

# Maps non-standard language codes to ISO 639-1 base forms.
# Covers ISO 639-2 (3-letter bibliographic & terminological), ISO 639-3
# (extended), non-standard abbreviations, and full English language names.
_LANGUAGE_ALIASES = {
    # ISO 639-2 bibliographic / terminological → ISO 639-1
    'eng': 'en', 'fra': 'fr', 'fre': 'fr', 'deu': 'de', 'ger': 'de',
    'spa': 'es', 'ita': 'it', 'por': 'pt', 'rus': 'ru', 'jpn': 'ja',
    'kor': 'ko', 'zho': 'zh', 'chi': 'zh', 'nld': 'nl', 'dut': 'nl',
    'ara': 'ar', 'hin': 'hi', 'swe': 'sv', 'nor': 'no', 'dan': 'da',
    'fin': 'fi', 'pol': 'pl', 'tur': 'tr', 'heb': 'he', 'tha': 'th',
    'ron': 'ro', 'rum': 'ro', 'hun': 'hu', 'ces': 'cs', 'cze': 'cs',
    'ell': 'el', 'gre': 'el', 'fas': 'fa', 'per': 'fa', 'ind': 'id',
    'slv': 'sl', 'srp': 'sr', 'bos': 'bs', 'hrv': 'hr', 'ukr': 'uk',
    'cat': 'ca', 'glg': 'gl', 'eus': 'eu', 'baq': 'eu',
    'vie': 'vi', 'msa': 'ms', 'may': 'ms', 'tgl': 'tl',
    # ISO 639-3 macro-language → ISO 639-1 parent
    'cmn': 'zh',  # Mandarin → Chinese
    'yue': 'zh',  # Cantonese → Chinese
    # Non-standard abbreviations seen in the wild
    'jap': 'ja',
    # Full English language names (lowercased for lookup)
    'english': 'en', 'french': 'fr', 'german': 'de', 'spanish': 'es',
    'italian': 'it', 'portuguese': 'pt', 'russian': 'ru', 'japanese': 'ja',
    'korean': 'ko', 'chinese': 'zh', 'dutch': 'nl', 'nederlands': 'nl',
    'arabic': 'ar', 'hindi': 'hi', 'swedish': 'sv', 'norwegian': 'no',
    'danish': 'da', 'finnish': 'fi', 'polish': 'pl', 'turkish': 'tr',
    'hebrew': 'he', 'thai': 'th', 'hungarian': 'hu', 'czech': 'cs',
    'greek': 'el', 'persian': 'fa', 'romanian': 'ro',
}

# Special ISO 639-2 codes that are NOT spoken languages.
# zxx = no linguistic content (instrumental, silent film, music video).
_NO_LINGUISTIC_CONTENT = {'zxx'}
# und = undetermined, mul = multiple languages — treat as unknown (needs
# whisper verification to determine actual language).
_UNDETERMINED_LANGUAGE = {'und', 'mul'}


def normalize_language(raw):
    """Normalize a language tag to its ISO 639-1 base form.

    Handles BCP-47 locale codes (en-US → en), ISO 639-2/3 codes
    (eng → en, cmn → zh), non-standard abbreviations (jap → ja),
    and full language names (Nederlands → nl).

    Returns the normalized 2-letter code, or the original base lowercased
    if no alias is found.
    """
    base = raw.split('-')[0].lower()
    return _LANGUAGE_ALIASES.get(base, base)


def check_audio_language(audio_tracks, accepted_languages=('en',)):
    """Check 4b: Non-English / unknown audio.

    Flags files where no audio track contains an accepted language.
    - No tracks at all → flag as foreign_audio.
    - Track tagged 'zxx' (no linguistic content) → skip (not a problem).
    - Track tagged 'und'/'mul' or empty → flag as unknown_audio (needs
      whisper verification).
    - All tracks have known languages, none accepted → flag as foreign_audio.
    - Any track in accepted_languages → no flag.

    Language matching normalizes codes via normalize_language() so that
    BCP-47 locale codes (en-US), ISO 639-2/3 (eng, cmn), non-standard
    abbreviations (jap), and full names (Nederlands) all resolve correctly.

    Returns a flag dict or None.
    """
    if not audio_tracks:
        return {
            'check': 'foreign_audio',
            'severity': 'LOW',
            'detail': 'No audio tracks detected',
        }

    accepted = {a.lower() for a in accepted_languages}
    has_unknown = False
    languages = []  # list of (raw_tag, normalized) tuples

    for t in audio_tracks:
        raw = t.get('language', '')
        if not raw:
            has_unknown = True
            continue

        base = raw.split('-')[0].lower()

        # zxx = no linguistic content — intentional, not a problem
        if base in _NO_LINGUISTIC_CONTENT:
            return None

        # und/mul = undetermined/multiple — treat as unknown
        if base in _UNDETERMINED_LANGUAGE:
            has_unknown = True
            continue

        normalized = normalize_language(raw)
        languages.append((raw, normalized))

        # Early exit: found an accepted language track
        if normalized in accepted:
            return None

    # If any track had unknown language and no accepted track was found
    if has_unknown:
        return {
            'check': 'unknown_audio',
            'severity': 'LOW',
            'detail': 'Audio track(s) have no language tag — needs verification',
        }

    # All tracks had known languages but none were empty/unknown
    if not languages:
        # All tracks were zxx/und/mul but zxx returns early above,
        # so this means all were und/mul → already handled above.
        # Safety fallback:
        return {
            'check': 'unknown_audio',
            'severity': 'LOW',
            'detail': 'Audio track(s) have no language tag — needs verification',
        }

    # All tracks are known non-accepted languages
    unique_langs = list(dict.fromkeys(raw for raw, _ in languages))

    return {
        'check': 'foreign_audio',
        'severity': 'LOW',
        'detail': 'All audio tracks are non-English: %s' % ', '.join(unique_langs),
    }


# ---------------------------------------------------------------------------
# Whisper audio language verification
# ---------------------------------------------------------------------------

_WHISPER_LANG_RE = re.compile(
    r'auto-detected language:\s*(\w+)\s*\(p\s*=\s*([\d.]+)\)'
)

DEFAULT_WHISPER_MODEL = '/models/ggml-tiny.bin'
WHISPER_SAMPLE_SECONDS = 30
WHISPER_MIN_CONFIDENCE = 0.70


def _extract_audio_sample(file_path, offset_seconds, duration, output_path,
                          track_index=None):
    """Extract a mono 16kHz WAV audio sample from a video file using ffmpeg.

    If track_index is given, extracts from that specific audio stream
    (0-based index among audio streams).  Otherwise extracts the default
    audio stream.

    Returns True on success, False on failure.
    """
    cmd = [
        'ffmpeg', '-y',
        '-ss', str(offset_seconds),
        '-i', file_path,
    ]
    if track_index is not None:
        cmd += ['-map', '0:a:%d' % track_index]
    cmd += [
        '-t', str(duration),
        '-vn', '-ar', '16000', '-ac', '1', '-f', 'wav',
        output_path,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, timeout=60,
        )
        if result.returncode != 0:
            log.warning('ffmpeg sample extraction failed (rc=%d) at offset %ds track=%s: %s',
                        (result.returncode, int(offset_seconds),
                         track_index if track_index is not None else 'default',
                         result.stderr[-200:] if result.stderr else ''))
            return False
        return True
    except subprocess.TimeoutExpired:
        log.warning('ffmpeg timed out extracting sample at offset %ds from %s',
                    (int(offset_seconds), os.path.basename(file_path)))
        return False
    except Exception as e:
        log.error('ffmpeg error extracting sample: %s', (e,))
        return False


def _run_whisper_detection(wav_path, model_path):
    """Run whisper-cli on a WAV file and parse the detected language.

    Uses --detect-language (-dl) to exit immediately after detection,
    avoiding unnecessary full transcription.

    Returns (language, confidence) or (None, 0.0) on failure.
    """
    cmd = [
        'whisper-cli',
        '-m', model_path,
        '-f', wav_path,
        '-l', 'auto',
        '-dl',
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=60,
        )
        # Language detection is printed to stderr
        output = result.stderr + result.stdout
        match = _WHISPER_LANG_RE.search(output)
        if match:
            return match.group(1), float(match.group(2))
        log.debug('Whisper: no language match in output for %s (rc=%d, '
                   'stdout=%d bytes, stderr=%d bytes)',
                   (os.path.basename(wav_path), result.returncode,
                    len(result.stdout), len(result.stderr)))
        return None, 0.0
    except subprocess.TimeoutExpired:
        log.warning('Whisper: timed out on %s', (wav_path,))
        return None, 0.0
    except Exception as e:
        log.error('Whisper: unexpected error on %s: %s', (wav_path, e))
        return None, 0.0


def _get_media_duration(file_path):
    """Get the duration of a media file in seconds using ffprobe.

    Returns duration as float, or 0.0 on failure.
    """
    cmd = [
        'ffprobe', '-v', 'quiet',
        '-show_entries', 'format=duration',
        '-of', 'default=noprint_wrappers=1:nokey=1',
        file_path,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=30,
        )
        return float(result.stdout.strip())
    except (ValueError, subprocess.TimeoutExpired, Exception):
        return 0.0


def _verify_single_track(file_path, duration, track_index, model_path, tmp_dir,
                         basename):
    """Verify one audio track using multi-sample whisper detection.

    Returns {'language': str, 'confidence': float, 'samples': list}
    """
    prefix = 't%d' % track_index if track_index is not None else 'default'

    # First try: middle of file (50%)
    offset = max(0, (duration * 0.5) - (WHISPER_SAMPLE_SECONDS / 2))
    wav_path = os.path.join(tmp_dir, '%s_50.wav' % prefix)

    if not _extract_audio_sample(file_path, offset, WHISPER_SAMPLE_SECONDS,
                                 wav_path, track_index=track_index):
        return {'language': None, 'confidence': 0.0, 'samples': [],
                'error': 'Failed to extract audio'}

    lang, conf = _run_whisper_detection(wav_path, model_path)
    samples = [{'offset_pct': 50, 'language': lang, 'confidence': conf}]
    log.info('Whisper track %s @50%%: lang=%s conf=%.3f for %s',
             (prefix, lang, conf, basename))

    if lang and conf >= WHISPER_MIN_CONFIDENCE:
        return {'language': lang, 'confidence': conf, 'samples': samples}

    # Multi-sample retry at 25% and 75%
    for pct in (25, 75):
        offset = max(0, (duration * pct / 100) - (WHISPER_SAMPLE_SECONDS / 2))
        wav_path = os.path.join(tmp_dir, '%s_%d.wav' % (prefix, pct))

        if not _extract_audio_sample(file_path, offset, WHISPER_SAMPLE_SECONDS,
                                     wav_path, track_index=track_index):
            continue

        lang2, conf2 = _run_whisper_detection(wav_path, model_path)
        samples.append({'offset_pct': pct, 'language': lang2, 'confidence': conf2})
        log.info('Whisper track %s @%d%%: lang=%s conf=%.3f for %s',
                 (prefix, pct, lang2, conf2, basename))

    best = max(samples, key=lambda s: s['confidence'])
    return {'language': best['language'], 'confidence': best['confidence'],
            'samples': samples}


def whisper_verify_audio(file_path, audio_tracks=None,
                         model_path=DEFAULT_WHISPER_MODEL):
    """Verify the spoken language of each audio track using whisper.cpp.

    If audio_tracks is provided (list of dicts with at least 'language' key),
    each track is verified independently and the result includes per-track
    details.  Otherwise falls back to verifying the default audio stream.

    Returns a dict:
        {
            'tracks': [
                {'track_index': 0, 'tagged_language': 'fr',
                 'language': 'en', 'confidence': 0.95, 'samples': [...]},
                ...
            ],
            'language': 'en',       # best overall detection
            'confidence': 0.95,
        }
    or on failure:
        {'error': 'reason', 'language': None, 'confidence': 0.0}
    """
    basename = os.path.basename(file_path)

    if not os.path.isfile(file_path):
        log.warning('Whisper verify: file not found: %s', (basename,))
        return {'error': 'File not found', 'language': None, 'confidence': 0.0}

    if not os.path.isfile(model_path):
        log.error('Whisper verify: model not found: %s', (model_path,))
        return {'error': 'Whisper model not found: %s' % model_path,
                'language': None, 'confidence': 0.0}

    duration = _get_media_duration(file_path)
    if duration < 10:
        log.warning('Whisper verify: file too short (%.1fs): %s',
                    (duration, basename))
        return {'error': 'File too short for analysis (%.1fs)' % duration,
                'language': None, 'confidence': 0.0}

    num_tracks = len(audio_tracks) if audio_tracks else 0
    log.info('Whisper verify starting: %s (%.0fs duration, %d audio tracks)',
             (basename, duration, num_tracks or 1))

    tmp_dir = tempfile.mkdtemp(prefix='whisper_')
    try:
        if audio_tracks and len(audio_tracks) > 0:
            # Verify each track independently
            track_results = []
            for idx, track_info in enumerate(audio_tracks):
                tagged = track_info.get('language', '')
                result = _verify_single_track(
                    file_path, duration, idx, model_path, tmp_dir, basename)
                result['track_index'] = idx
                result['tagged_language'] = tagged
                track_results.append(result)
                log.info('Whisper track %d (%s): detected %s (%.1f%%) for %s',
                         (idx, tagged, result['language'],
                          result['confidence'] * 100, basename))

            # Overall best = highest confidence across all tracks
            best = max(track_results, key=lambda t: t['confidence'])
            return {
                'tracks': track_results,
                'language': best['language'],
                'confidence': best['confidence'],
            }
        else:
            # Single default track (no track info available)
            result = _verify_single_track(
                file_path, duration, None, model_path, tmp_dir, basename)
            if 'error' in result:
                return {'error': result['error'],
                        'language': None, 'confidence': 0.0}
            log.info('Whisper verify done: %s → %s (%.1f%%)',
                     (basename, result['language'],
                      result['confidence'] * 100))
            return {
                'tracks': [{
                    'track_index': 0,
                    'tagged_language': '',
                    'language': result['language'],
                    'confidence': result['confidence'],
                    'samples': result.get('samples', []),
                }],
                'language': result['language'],
                'confidence': result['confidence'],
            }
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def check_edition(container_title, filename):
    """Check 5: Edition mismatch between container title and filename.

    Detects when the container title metadata contains an edition keyword
    (e.g., "Director's Cut", "Extended") but the filename does not, or
    when they contain different editions.

    Returns a flag dict or None.  Also returns the detected edition string.
    """
    if not container_title:
        return None, ''

    container_edition = get_edition(container_title)
    if not container_edition:
        return None, ''

    filename_edition = get_edition(filename)

    if not filename_edition:
        # Container has edition, filename doesn't — missing from name.
        # LOW severity: this is a cosmetic naming issue; the file is the
        # right movie, just missing an edition tag.
        return {
            'check': 'edition',
            'severity': 'LOW',
            'detail': (
                f"Container title has edition '{container_edition}' "
                f"but filename does not"
            ),
        }, container_edition

    if container_edition.lower() != filename_edition.lower():
        # Different editions
        return {
            'check': 'edition',
            'severity': 'LOW',
            'detail': (
                f"Container edition '{container_edition}' "
                f"differs from filename edition '{filename_edition}'"
            ),
        }, container_edition

    return None, container_edition


def parse_guessit_tokens(filename):
    """Parse a filename with guessit and map results to CP renamer token values.

    Returns a dict with keys matching renamer tokens: video, audio, source,
    group, audio_channels, quality_type.  Values are CP-format strings or
    empty string if not detected.
    """
    try:
        g = guessit_parse(filename, {'type': 'movie'})
    except Exception:
        return {
            'video': '', 'audio': '', 'source': '', 'group': '',
            'audio_channels': '', 'quality_type': '',
        }

    # Video codec
    vc = g.get('video_codec', '')
    if isinstance(vc, list):
        vc = vc[0] if vc else ''
    video = GUESSIT_VIDEO_MAP.get(str(vc), str(vc)) if vc else ''

    # Audio codec (handle DTS-HD MA via audio_profile)
    ac = g.get('audio_codec', '')
    if isinstance(ac, list):
        ac = ac[0] if ac else ''
    ac_str = str(ac) if ac else ''
    audio_profile = g.get('audio_profile', '')
    if isinstance(audio_profile, list):
        audio_profile = audio_profile[0] if audio_profile else ''
    # DTS with "Master Audio" profile → DTS-HD MA
    if ac_str == 'DTS' and audio_profile and 'master' in str(audio_profile).lower():
        audio = 'DTS-HD MA'
    elif ac_str == 'DTS' and audio_profile and 'high' in str(audio_profile).lower():
        audio = 'DTS-HD'
    else:
        audio = GUESSIT_AUDIO_MAP.get(ac_str, ac_str) if ac_str else ''

    # Source
    src = g.get('source', '')
    if isinstance(src, list):
        src = src[0] if src else ''
    source = GUESSIT_SOURCE_MAP.get(str(src), str(src)) if src else ''

    # Release group
    grp = g.get('release_group', '')
    group = str(grp) if grp else ''

    # Audio channels
    channels = g.get('audio_channels', '')
    if isinstance(channels, list):
        channels = channels[0] if channels else ''
    audio_channels = str(channels) if channels else ''

    # Quality type (HD vs SD) — derive from screen_size
    screen = g.get('screen_size', '')
    if screen and screen in ('2160p', '1080p', '1080i', '720p', '720i'):
        quality_type = 'HD'
    elif screen and screen in ('480p', '480i', '576p', '576i'):
        quality_type = 'SD'
    else:
        quality_type = ''

    return {
        'video': video,
        'audio': audio,
        'source': source,
        'group': group,
        'audio_channels': audio_channels,
        'quality_type': quality_type,
    }


def _title_to_thename(title):
    """Convert a title from namethe format back to thename format.

    Reverses the article-at-end convention used in folder names:
        'Monster, The'              → 'The Monster'
        'Affair To Remember, An'    → 'An Affair To Remember'
        'Christmas Carol, A'        → 'A Christmas Carol'
        'The Monster'               → 'The Monster'  (already correct)
        'Some Movie, No Article'    → 'Some Movie, No Article'  (unchanged)

    Only moves 'The', 'An', or 'A' — other trailing comma phrases are left as-is.
    """
    for article in ['The', 'An', 'A']:
        suffix = ', ' + article
        if title.endswith(suffix):
            return article + ' ' + title[:-len(suffix)]
    return title


def build_expected_filename(item, template, replace_doubles=True, separator='',
                           cd_number=None):
    """Build the expected filename from renamer template and item data.

    This is a scan-time variant of _apply_renamer_template() that works
    without Env.setting() by accepting template/settings as arguments.
    Uses guessit_tokens from the item to fill video/audio/source/group.

    Args:
        item: Audit result dict with expected/actual/guessit_tokens data
        template: Renamer file_name template string
        replace_doubles: Whether to clean up double separators
        separator: Character to replace spaces with
        cd_number: CD number for multi-CD files (int), or None

    Returns the expected filename string or None on error.
    """
    old_file = item['file']
    _, ext = os.path.splitext(old_file)
    ext = ext.lstrip('.')

    # Use db_title (original form) when available; reverse-transform namethe
    # folder titles as fallback so <thename> and <namethe> resolve correctly
    movie_name = (item['expected'].get('db_title')
                  or _title_to_thename(item['expected'].get('title', '')))
    movie_name = re.sub(r'[\x00/\\:*?"<>|]', '', movie_name)

    name_the = movie_name
    for prefix in ['the ', 'an ', 'a ']:
        if prefix == movie_name[:len(prefix)].lower():
            name_the = movie_name[len(prefix):] + ', ' + prefix.strip().capitalize()
            break

    quality = item['expected'].get('resolution', '')
    if not quality:
        actual_res = item['actual'].get('resolution', '')
        if 'x' in actual_res:
            try:
                w = int(actual_res.split('x')[0])
                h = int(actual_res.split('x')[1])
                quality = resolution_label(w, h)
            except (ValueError, IndexError):
                pass

    edition = item.get('detected_edition', '') or ''
    # Fallback: detect edition from filename when stored result has none
    if not edition:
        edition = get_edition(old_file)
    imdb_id = item.get('imdb_id', '') or ''
    if not imdb_id:
        imdb_id = (item.get('identification') or {}).get('identified_imdb', '') or ''
    year = item['expected'].get('year') or ''

    # Guessit-derived tokens
    gt = item.get('guessit_tokens', {})

    actual_res = item['actual'].get('resolution', '')
    actual_width = ''
    actual_height = ''
    if 'x' in actual_res:
        parts = actual_res.split('x')
        actual_width = parts[0]
        actual_height = parts[1] if len(parts) > 1 else ''

    replacements = {
        'ext': ext,
        'namethe': name_the.strip(),
        'thename': movie_name.strip(),
        'year': str(year) if year else '',
        'first': name_the[0].upper() if name_the else '',
        'quality': quality,
        'quality_type': gt.get('quality_type', ''),
        'video': gt.get('video', ''),
        'audio': gt.get('audio', ''),
        'group': gt.get('group', ''),
        'source': gt.get('source', ''),
        'resolution_width': actual_width,
        'resolution_height': actual_height,
        'audio_channels': gt.get('audio_channels', ''),
        'imdb_id': imdb_id,
        'cd': ' cd%d' % cd_number if cd_number else '',
        'cd_nr': str(cd_number) if cd_number else '',
        'mpaa': '',
        'mpaa_only': 'Not Rated',
        'category': '',
        '3d': '',
        '3d_type': '',
        '3d_type_short': '',
        'edition': edition,
        'edition_plex': '{edition-%s}' % edition if edition else '',
        'imdb_id_plex': '{imdb-%s}' % imdb_id if imdb_id else '',
        'imdb_id_emby': '[imdbid-%s]' % imdb_id if imdb_id else '',
        'imdb_id_kodi': '{imdb=%s}' % imdb_id if imdb_id else '',
    }

    # Apply template — same logic as renamer.doReplace()
    replaced = template
    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            continue
        if val is not None:
            replaced = replaced.replace('<%s>' % key, str(val))
        else:
            replaced = replaced.replace('<%s>' % key, '')

    if replace_doubles:
        replaced = replaced.lstrip('. ')
        double_replaces = [
            (r'\(\s*\)', ''),   # remove empty parentheses from missing tokens
            (r'\[\s*\]', ''),   # remove empty brackets from missing tokens
            (r'\{\s*\}', ''),   # remove empty braces from missing tokens
            (r'\.+', '.'), (r'_+', '_'), (r'-+', '-'), (r'\s+', ' '), (r' \\', r'\\'), (' /', '/'),
            (r'(\s\.)+', '.'), (r'(-\.)+', '.'), (r'(\s-[^\s])+', '-'), (' ]', ']'),
        ]
        for pattern, repl in double_replaces:
            replaced = re.sub(pattern, repl, replaced)
        replaced = replaced.rstrip(',_-/\\ ')

    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            replaced = replaced.replace('<%s>' % key, str(val))

    replaced = re.sub(r'[\x00:*?"<>|]', '', replaced)

    if separator:
        replaced = replaced.replace(' ', separator)

    return replaced


def check_template(item, template, replace_doubles=True, separator='',
                   cd_number=None):
    """Check 6: Filename doesn't match renamer template.

    Builds the expected filename from the template and all available item
    data (folder title/year, mediainfo resolution, IMDB, edition, guessit
    tokens from the filename).  Compares strictly against the actual filename.

    Args:
        item: Audit result dict with expected/actual/guessit_tokens data
        template: Renamer file_name template string
        replace_doubles: Whether to clean up double separators
        separator: Character to replace spaces with
        cd_number: CD number for multi-CD files (int), or None

    Returns a flag dict or None.
    """
    if not template:
        return None

    expected = build_expected_filename(item, template, replace_doubles, separator,
                                       cd_number=cd_number)
    if not expected:
        return None

    actual = item['file']

    if expected == actual:
        return None

    # Build detail message listing specific differences
    differences = []

    # Check IMDB presence
    imdb_id = item.get('imdb_id', '') or ''
    if not imdb_id:
        imdb_id = (item.get('identification') or {}).get('identified_imdb', '') or ''
    if imdb_id:
        # Check if any IMDB token is in the template
        has_imdb_token = any(
            ('<%s>' % t) in template
            for t in ('imdb_id_plex', 'imdb_id_emby', 'imdb_id_kodi', 'imdb_id')
        )
        if has_imdb_token:
            # Check if IMDB is actually in the filename
            if imdb_id not in actual and '{imdb-' not in actual and '[imdbid-' not in actual and '{imdb=' not in actual:
                differences.append('missing IMDB tag')

    # Check quality/resolution
    quality = item['expected'].get('resolution', '')
    if not quality:
        actual_res = item['actual'].get('resolution', '')
        if 'x' in actual_res:
            try:
                w = int(actual_res.split('x')[0])
                h = int(actual_res.split('x')[1])
                quality = resolution_label(w, h)
            except (ValueError, IndexError):
                pass
    if quality and '<quality>' in template:
        if quality.lower() not in actual.lower():
            differences.append('wrong/missing quality')

    # Check edition
    edition = item.get('detected_edition', '') or ''
    has_edition_token = '<edition>' in template or '<edition_plex>' in template
    if edition and has_edition_token:
        if edition.lower() not in actual.lower() and '{edition-' not in actual:
            differences.append('missing edition')

    # Check year
    year = item['expected'].get('year')
    if year and '<year>' in template:
        if str(year) not in actual:
            differences.append('missing year')

    detail_parts = ', '.join(differences) if differences else 'filename format mismatch'

    # LOW = purely cosmetic formatting (title/year/quality all present, has IMDB)
    #       or only difference is a missing edition tag
    # MEDIUM = missing substantive tokens or no IMDB to confirm identity
    edition_only = differences == ['missing edition']
    severity = 'LOW' if ((not differences or edition_only) and imdb_id) else 'MEDIUM'

    return {
        'check': 'template',
        'severity': severity,
        'detail': '%s (expected: %s)' % (detail_parts.capitalize(), expected),
    }


# ---------------------------------------------------------------------------
# Identification skip logic
# ---------------------------------------------------------------------------

def needs_identification(flags):
    """Determine if a flagged file needs identification.

    Smart skip logic — avoid expensive CRC32/srrDB lookups when the quick scan
    already gives us enough information to act:

      - TV episode detected → skip (already identified from container title,
        queue for deletion)
      - Resolution-only mismatch → skip (right movie, wrong quality — not
        suspect, just needs re-download)
      - Edition-only → skip (right movie, just missing edition in filename)
      - Resolution + edition only → skip (quality label + edition fix, no ID needed)
      - Template-only → skip (naming fix, no ID needed)
      - Template + resolution/edition only → skip (naming fixes, no ID needed)
      - Audio-only flags (foreign_audio, unknown_audio) → skip (language issue,
        not identity issue — identification won't help resolve these)
      - Audio flags + naming-fix flags → skip (combination of above)
      - Everything else → run (title mismatch, runtime mismatch, or
        multi-flag combinations are suspect and need identification)

    Returns True if identification should run, False to skip.
    """
    checks = {f['check'] for f in flags}

    # TV episode: already identified from container title, no CRC needed
    if 'tv_episode' in checks:
        return False

    # Checks that never require identification — naming fixes and audio issues
    skip_checks = {'resolution', 'edition', 'template',
                   'foreign_audio', 'unknown_audio'}
    if checks <= skip_checks:
        return False

    # Everything else is suspect — run identification
    return True


# ---------------------------------------------------------------------------
# Identification
# ---------------------------------------------------------------------------

def compute_crc32(filepath, progress_callback=None):
    """Compute CRC32 of a file, reading in chunks.

    Returns uppercase hex string (e.g., 'AE45D279').
    """
    crc = 0
    total_size = os.path.getsize(filepath)
    bytes_read = 0
    chunk_size = 4 * 1024 * 1024  # 4MB chunks

    with open(filepath, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            crc = zlib.crc32(chunk, crc)
            bytes_read += len(chunk)
            if progress_callback:
                progress_callback(bytes_read, total_size)

    return format(crc & 0xFFFFFFFF, '08X')


def srrdb_lookup_crc(crc_hex):
    """Look up a CRC32 on srrDB to identify a release.

    Returns dict with release info or None.
    """
    if not requests:
        _log_warn('requests not available, skipping srrDB lookup')
        return None

    url = f'{SRRDB_API}/search/archive-crc:{crc_hex}'
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _log_warn('srrDB lookup failed: %s' % e)
        return None

    results = data.get('results', [])
    if not results:
        return None

    # Return the first (best) match
    hit = results[0]
    return {
        'release': hit.get('release', ''),
        'imdb_id': 'tt' + hit.get('imdbId', '') if hit.get('imdbId') else None,
        'size': hit.get('size', 0),
    }


def compute_opensubtitles_hash(filepath):
    """Compute the OpenSubtitles movie hash for a file.

    The algorithm sums 64-bit little-endian integers from the first 64KB and
    last 64KB of the file, seeded with the file size.  Only reads 128KB
    regardless of file size, making it orders of magnitude faster than CRC32.

    Returns a 16-character lowercase hex string, or None if the file is
    too small (< 128KB).
    """
    bytesize = struct.calcsize(b'<q')  # 8 bytes
    filesize = os.path.getsize(filepath)
    if filesize < 65536 * 2:
        return None

    filehash = filesize
    with open(filepath, 'rb') as f:
        # Read first 64KB
        for _ in range(65536 // bytesize):
            buf = f.read(bytesize)
            (val,) = struct.unpack(b'<q', buf)
            filehash += val
            filehash &= 0xFFFFFFFFFFFFFFFF

        # Read last 64KB
        f.seek(max(0, filesize - 65536), 0)
        for _ in range(65536 // bytesize):
            buf = f.read(bytesize)
            (val,) = struct.unpack(b'<q', buf)
            filehash += val
            filehash &= 0xFFFFFFFFFFFFFFFF

    return f'{filehash:016x}'


def opensubtitles_lookup_hash(moviehash, api_key, filepath=None):
    """Look up an OpenSubtitles movie hash to identify a file.

    Queries the /subtitles endpoint with the moviehash parameter.
    Returns an identification dict or None if no match found.

    The search endpoint is unlimited (no download quota consumed).
    Only subtitle downloads count against the daily quota.

    Args:
        moviehash: 16-char hex string from compute_opensubtitles_hash()
        api_key: OpenSubtitles REST API key
        filepath: optional, used for file_name hint in the request

    Returns:
        dict with identification info, or None if no match.
    """
    if not requests:
        _log_warn('requests not available, skipping OpenSubtitles lookup')
        return None

    if not api_key:
        return None

    headers = {
        'Api-Key': api_key,
        'User-Agent': 'CouchPotato',
        'Accept': 'application/json',
    }

    params = {'moviehash': moviehash}

    try:
        resp = requests.get(
            f'{OPENSUBTITLES_API}/subtitles',
            headers=headers,
            params=params,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _log_warn('OpenSubtitles lookup failed: %s' % e)
        return None

    results = data.get('data', [])
    if not results:
        return None

    # Find the first result with moviehash_match and feature_details
    for entry in results:
        attrs = entry.get('attributes', {})
        if not attrs.get('moviehash_match', False):
            continue
        feature = attrs.get('feature_details', {})
        if not feature:
            continue

        imdb_id = feature.get('imdb_id')
        if imdb_id:
            # Ensure tt prefix and zero-padding
            imdb_id = 'tt' + str(int(imdb_id)).rjust(7, '0')

        feature_type = (feature.get('feature_type') or '').lower()

        result = {
            'title': feature.get('title', ''),
            'year': int(feature.get('year')) if feature.get('year') else None,
            'imdb_id': imdb_id,
            'tmdb_id': feature.get('tmdb_id'),
            'feature_type': feature_type,
            'release': attrs.get('release', ''),
        }
        return result

    # No moviehash_match entries — try the first result anyway
    first = results[0]
    attrs = first.get('attributes', {})
    feature = attrs.get('feature_details', {})
    if feature:
        imdb_id = feature.get('imdb_id')
        if imdb_id:
            imdb_id = 'tt' + str(int(imdb_id)).rjust(7, '0')

        feature_type = (feature.get('feature_type') or '').lower()

        return {
            'title': feature.get('title', ''),
            'year': int(feature.get('year')) if feature.get('year') else None,
            'imdb_id': imdb_id,
            'tmdb_id': feature.get('tmdb_id'),
            'feature_type': feature_type,
            'release': attrs.get('release', ''),
            'hash_match': False,
        }

    return None


def identify_flagged_file(filepath, flags, container_title_parsed,
                          cached_crc32=None, cached_opensubtitles_hash=None):
    """Try to identify what a flagged file actually is.

    Strategy A: Container title (already parsed)
    Strategy B: OpenSubtitles moviehash (fast, reads only 128KB)
    Strategy C: CRC32 → srrDB reverse lookup (slow, reads entire file)

    Args:
        filepath: Path to the video file.
        flags: List of flag dicts from the scan.
        container_title_parsed: Parsed container title metadata (or None).
        cached_crc32: Pre-computed CRC32 hex string from file_knowledge DB.
            If provided, Strategy C skips recomputing the hash.
        cached_opensubtitles_hash: Pre-computed OpenSubtitles hash from
            file_knowledge DB.  If provided, Strategy B skips recomputing.
    """
    identification = None

    # Strategy A: container title already told us
    if container_title_parsed and container_title_parsed.get('title'):
        meta = container_title_parsed
        title = meta['title']
        # Use the container title for identification if it looks like a real
        # movie name (not junk) and doesn't match the folder title.  We check
        # title_flag presence OR a direct mismatch because the short-no-year
        # heuristic may suppress the title flag while still passing parsed
        # metadata through for identification.
        has_title_flag = any(f['check'] == 'title' for f in flags)
        if not has_title_flag:
            # No title flag — check if the container title actually differs
            # from the folder name (the flag may have been suppressed)
            folder_name = os.path.basename(os.path.dirname(filepath))
            m = FOLDER_RE.match(folder_name)
            folder_title = m.group(1).strip() if m else folder_name
            has_title_flag = not titles_match(title, folder_title)
        # Skip single-word titles without a year — almost always encoder/group
        # names (Dread-Team, ultimate-force) rather than real movie titles.
        # Multi-word titles without a year (e.g. "Jimmy Vestvood") are more
        # likely to be real movie names and worth trying.
        if not meta.get('year') and len(title.split()) <= 1:
            has_title_flag = False
        if has_title_flag and not is_junk_title(title) and len(title) > 3:
            identification = {
                'method': 'container_title',
                'identified_title': meta['title'],
                'identified_year': meta.get('year'),
                'confidence': 'high' if meta.get('year') else 'medium',
                'source': meta.get('raw', ''),
            }
            # Parse the container title for srrDB lookup to get IMDB
            release_name = meta.get('raw', '').replace(' ', '.')
            if release_name and requests:
                try:
                    resp = requests.get(
                        f'{SRRDB_API}/search/{release_name}',
                        timeout=15,
                    )
                    if resp.ok:
                        results = resp.json().get('results', [])
                        if results:
                            imdb = results[0].get('imdbId')
                            if imdb:
                                identification['identified_imdb'] = f'tt{imdb}'
                except Exception:
                    pass

            return identification

    # Strategy B: OpenSubtitles moviehash (fast — reads only 128KB)
    # Get API key: via event inside CP, direct import for standalone CLI
    _os_api_key = None
    if _CP_AVAILABLE:
        try:
            _os_api_key = fireEvent('opensubtitles.api_key', single=True)
        except Exception:
            pass
    else:
        try:
            from couchpotato.core.media.movie.providers.info.opensubtitles import OS_APP_API_KEY
            _os_api_key = OS_APP_API_KEY
        except ImportError:
            pass
    if _os_api_key:
        if cached_opensubtitles_hash:
            _log_info('Using cached OpenSubtitles hash for %s' % os.path.basename(filepath))
            moviehash = cached_opensubtitles_hash
        else:
            _log_info('Computing OpenSubtitles hash for %s...' % os.path.basename(filepath))
            moviehash = compute_opensubtitles_hash(filepath)
        if moviehash:
            _log_info('OpenSubtitles hash: %s' % moviehash)
            hit = opensubtitles_lookup_hash(moviehash, _os_api_key,
                                            filepath=filepath)
            if hit:
                feature_type = hit.get('feature_type', '')

                # TV episode detection from OpenSubtitles
                if feature_type == 'episode':
                    return {
                        'method': 'opensubtitles_hash',
                        'identified_title': hit.get('title', ''),
                        'identified_year': hit.get('year'),
                        'identified_imdb': hit.get('imdb_id'),
                        'confidence': 'high',
                        'source': 'OpenSubtitles hash %s' % moviehash,
                        'moviehash': moviehash,
                        'feature_type': 'episode',
                        'action': 'queue_deletion',
                        'detail': 'OpenSubtitles identifies this as a TV episode',
                    }

                identification = {
                    'method': 'opensubtitles_hash',
                    'identified_title': hit.get('title', ''),
                    'identified_year': hit.get('year'),
                    'identified_imdb': hit.get('imdb_id'),
                    'confidence': 'high',
                    'source': 'OpenSubtitles hash %s' % moviehash,
                    'moviehash': moviehash,
                }
                # Include release name if available (for edition backfill)
                if hit.get('release'):
                    identification['source'] = hit['release']
                return identification
        else:
            _log_info('File too small for OpenSubtitles hash, skipping')

    # Strategy C: CRC32 reverse lookup on srrDB
    if cached_crc32:
        _log_info('Using cached CRC32 for %s: %s' % (os.path.basename(filepath), cached_crc32))
        crc_hex = cached_crc32
    else:
        _log_info('Computing CRC32 of %s...' % os.path.basename(filepath))
        file_size = os.path.getsize(filepath)
        file_size_gb = file_size / (1024 ** 3)

        def progress(done, total):
            pct = done / total * 100 if total else 0
            print(f'\r  CRC32: {pct:.1f}% ({done / (1024**3):.1f}/{total / (1024**3):.1f} GB)',
                  end='', file=sys.stderr)

        crc_hex = compute_crc32(filepath, progress_callback=progress)
    _log_info('CRC32: %s' % crc_hex)

    hit = srrdb_lookup_crc(crc_hex)
    if hit:
        # Parse the release name with guessit
        parsed = guessit_parse(hit['release'])
        identification = {
            'method': 'srrdb_crc',
            'identified_title': parsed.get('title', hit['release']),
            'identified_year': parsed.get('year'),
            'identified_imdb': hit.get('imdb_id'),
            'confidence': 'high',
            'source': hit['release'],
            'crc32': crc_hex,
        }
    else:
        identification = {
            'method': 'crc_not_found',
            'confidence': 'none',
            'detail': f'CRC32 {crc_hex} not found in srrDB (may be P2P release)',
            'crc32': crc_hex,
        }

    return identification


# TMDB API key fallback (base64-encoded, same as CP's built-in default)
_TMDB_API_KEYS = [
    'ZTIyNGZlNGYzZmVjNWY3YjU1NzA2NDFmN2NkM2RmM2E=',
    'ZjZiZDY4N2ZmYTYzY2QyODJiNmZmMmM2ODc3ZjI2Njk=',
]


def lookup_imdb_id(imdb_id, db_path=None):
    """Look up a movie by IMDB ID.

    First checks the local CP database, then falls back to the TMDB API.

    Returns a dict with 'title', 'year', 'imdb_id' or None if not found.
    """
    import base64
    import random

    # Strategy 1: local CP database
    if db_path:
        try:
            media_by_imdb, _, _, _ = load_cp_database(db_path)
            if imdb_id in media_by_imdb:
                entry = media_by_imdb[imdb_id]
                return {
                    'title': entry['title'],
                    'year': entry.get('year'),
                    'imdb_id': imdb_id,
                    'source': 'cp_database',
                }
        except Exception:
            pass

    # Strategy 2: TMDB find API
    if requests:
        try:
            api_key = base64.b64decode(
                random.choice(_TMDB_API_KEYS)
            ).decode('utf-8')
            resp = requests.get(
                'https://api.themoviedb.org/3/find/%s' % imdb_id,
                params={
                    'api_key': api_key,
                    'external_source': 'imdb_id',
                },
                timeout=15,
            )
            if resp.ok:
                data = resp.json()
                movies = data.get('movie_results', [])
                if movies:
                    movie = movies[0]
                    year = None
                    release_date = movie.get('release_date', '')
                    if release_date and len(release_date) >= 4:
                        try:
                            year = int(release_date[:4])
                        except ValueError:
                            pass
                    return {
                        'title': movie.get('title', ''),
                        'year': year,
                        'imdb_id': imdb_id,
                        'tmdb_id': movie.get('id'),
                        'source': 'tmdb',
                    }
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def find_video_files(folder_path):
    """Find all video files in a folder (non-recursive within the folder)."""
    files = []
    try:
        for entry in os.listdir(folder_path):
            full_path = os.path.join(folder_path, entry)
            ext = os.path.splitext(entry)[1].lower()
            if ext in VIDEO_EXTENSIONS and os.path.isfile(full_path):
                files.append(full_path)
    except OSError:
        pass
    return files


# Regex matching cd/CD markers in filenames: "cd1", "cd 2", "cd.3", "cd-4"
_CD_NUMBER_RE = re.compile(r'[ _.\-]cd[ _.\-]*(\d+)', re.IGNORECASE)


def parse_cd_number(filename):
    """Extract a CD number from a filename.

    Matches patterns like 'cd1', 'cd 2', 'cd.3', 'cd-4' (case-insensitive).
    Returns the cd number as an int, or None if not found.
    """
    m = _CD_NUMBER_RE.search(filename)
    return int(m.group(1)) if m else None


def classify_video_files(video_files):
    """Classify video files in a folder into logical groups.

    Determines whether a folder contains a single file, a multi-CD set
    (sequential cd1, cd2, ...), or multiple variants (editions, qualities,
    duplicates).

    Args:
        video_files: List of full file paths to video files.

    Returns:
        {
            'type': 'single' | 'multi_cd' | 'variants',
            'cd_files': [(cd_num, filepath), ...],  # sorted by cd_num
            'non_cd_files': [filepath, ...],         # files without cd tags
        }
    """
    if len(video_files) <= 1:
        return {
            'type': 'single',
            'cd_files': [],
            'non_cd_files': list(video_files),
        }

    cd_files = []
    non_cd_files = []

    for filepath in video_files:
        filename = os.path.basename(filepath)
        cd_num = parse_cd_number(filename)
        if cd_num is not None:
            cd_files.append((cd_num, filepath))
        else:
            non_cd_files.append(filepath)

    cd_files.sort(key=lambda x: x[0])

    # Multi-CD: ALL files have cd tags AND form a complete sequence starting at 1
    if cd_files and not non_cd_files:
        cd_nums = [num for num, _ in cd_files]
        expected_seq = list(range(1, len(cd_files) + 1))
        if cd_nums == expected_seq:
            return {
                'type': 'multi_cd',
                'cd_files': cd_files,
                'non_cd_files': [],
            }

    # Everything else: variants (each file scanned individually)
    # This includes:
    # - Multiple non-cd files (different editions, qualities, duplicates)
    # - Mix of cd-tagged and non-cd files (cd sub-group + standalone files)
    # - Incomplete cd sequences (cd1+cd3 missing cd2)

    # Check if the CD files form a valid sequential sub-group (cd1..cdN)
    has_cd_subgroup = False
    if cd_files and non_cd_files:
        cd_nums = [num for num, _ in cd_files]
        expected_seq = list(range(1, len(cd_files) + 1))
        has_cd_subgroup = (cd_nums == expected_seq)

    return {
        'type': 'variants',
        'cd_files': cd_files,
        'non_cd_files': non_cd_files,
        'has_cd_subgroup': has_cd_subgroup,
    }


def detect_duplicates(file_results):
    """Detect duplicate pairs among scanned file results.

    Two files are considered duplicates if:
    - They have the exact same file size (bytes), OR
    - They have the same runtime (within 0.1 min) AND same resolution label

    Args:
        file_results: List of dicts, each with at minimum:
            - 'file_size_bytes': int
            - 'actual': {'resolution': str, 'duration_min': float}
            - 'expected': {'resolution': str}

    Returns:
        List of (index_a, index_b) tuples identifying duplicate pairs.
    """
    pairs = []
    n = len(file_results)

    for i in range(n):
        for j in range(i + 1, n):
            a = file_results[i]
            b = file_results[j]

            # Check 1: exact same file size
            if a.get('file_size_bytes') and b.get('file_size_bytes'):
                if a['file_size_bytes'] == b['file_size_bytes']:
                    pairs.append((i, j))
                    continue

            # Check 2: same runtime AND same resolution
            a_dur = a.get('actual', {}).get('duration_min', 0)
            b_dur = b.get('actual', {}).get('duration_min', 0)
            a_res = a.get('expected', {}).get('resolution', '')
            b_res = b.get('expected', {}).get('resolution', '')

            if (a_dur and b_dur and a_res and b_res
                    and a_res == b_res
                    and abs(a_dur - b_dur) <= 0.1):
                pairs.append((i, j))

    return pairs


def _scan_single_file(filepath, folder_title, folder_year, imdb_id, db_entry,
                      expected_runtime, renamer_template, renamer_replace_doubles,
                      renamer_separator, full=False, force_full=False,
                      cd_number=None, seen_fingerprints=None,
                      knowledge_callback=None):
    """Scan one video file and return an audit result dict (or None if clean).

    This is the core per-file scanning logic extracted from scan_movie_folder().
    It runs all six checks (resolution, runtime, container title, TV episode,
    edition, template) and optionally identification.

    Args:
        filepath: Full path to the video file
        folder_title: Parsed title from the folder name
        folder_year: Parsed year from the folder name
        imdb_id: IMDB ID (from filename or DB lookup), may be None
        db_entry: CP database entry for this movie, may be None
        expected_runtime: Expected runtime in minutes from DB
        renamer_template: Renamer file_name template string
        renamer_replace_doubles: Whether to clean up double separators
        renamer_separator: Character to replace spaces with
        full: Run full scan with identification on flagged files
        force_full: Force identification even for high-confidence flags
        cd_number: CD number for multi-CD files (int), or None

    Returns:
        dict with scan results, or None if no issues found.
    """
    filename = os.path.basename(filepath)

    # Parse expected values from filename
    claimed_res = parse_filename_resolution(filename)
    file_imdb_id = parse_filename_imdb(filename)
    # Prefer IMDB ID from filename if available, else use the one from caller
    effective_imdb_id = file_imdb_id or imdb_id

    # Extract actual metadata from file
    meta = extract_file_meta(filepath)

    # Parse guessit tokens from filename for template comparison and rename
    guessit_tokens = parse_guessit_tokens(filename)

    # Determine if renamer template has edition tokens
    has_edition_in_template = (
        renamer_template and
        ('<edition>' in renamer_template or '<edition_plex>' in renamer_template)
    )

    # Run checks
    flags = []
    container_title_parsed = None

    # Check 1: Resolution (width + height)
    flag = check_resolution(claimed_res, meta['resolution_width'], meta['resolution_height'])
    if flag:
        flags.append(flag)

    # Detect edition early — needed by runtime check for edition-aware suppression
    detected_edition = ''
    if has_edition_in_template or not renamer_template:
        edition_flag, detected_edition = check_edition(meta['container_title'], filename)
    else:
        # Still detect edition for metadata even if we don't flag it
        edition_flag, detected_edition = check_edition(meta['container_title'], filename)
        edition_flag = None  # suppress the flag when template has no edition token

    # Fallback: detect edition from filename when container title had none
    if not detected_edition:
        detected_edition = get_edition(filename)

    # Check 2: Runtime (edition-aware)
    flag = check_runtime(meta['duration_min'], expected_runtime,
                         edition=detected_edition,
                         container_title=meta['container_title'])
    if flag:
        flags.append(flag)

    # Check 3: Container title
    flag, container_title_parsed = check_container_title(
        meta['container_title'], folder_title, folder_year,
        imdb_year=db_entry.get('year') if db_entry else None,
    )
    if flag:
        flags.append(flag)

    # Check 4: TV episode in container title
    flag = check_tv_episode(meta['container_title'])
    if flag:
        flags.append(flag)

    # Check 5: Edition mismatch
    if edition_flag:
        flags.append(edition_flag)

    # Build partial item for template check
    partial_item = {
        'file': filename,
        'file_path': filepath,
        'imdb_id': effective_imdb_id,
        'actual': {
            'resolution': f"{meta['resolution_width']}x{meta['resolution_height']}",
        },
        'expected': {
            'resolution': claimed_res,
            'title': folder_title,
            'year': folder_year,
        },
        'detected_edition': detected_edition if detected_edition else None,
        'guessit_tokens': guessit_tokens,
    }

    # Check 6: Template conformance
    if renamer_template:
        template_flag = check_template(
            partial_item, renamer_template,
            renamer_replace_doubles, renamer_separator,
            cd_number=cd_number,
        )
        if template_flag:
            flags.append(template_flag)

    # Check 7: Non-English audio
    flag = check_audio_language(meta.get('audio_tracks', []))
    if flag:
        flags.append(flag)

    # Compute fingerprint for all files (clean or flagged) so that
    # file_knowledge records can be created/updated and stale entries pruned.
    fingerprint = compute_file_fingerprint(filepath)
    if seen_fingerprints is not None and fingerprint:
        seen_fingerprints[fingerprint] = filepath
    if knowledge_callback and fingerprint:
        knowledge_doc = knowledge_callback(fingerprint, filepath)
    else:
        knowledge_doc = None

    if not flags:
        return None

    result = {
        'item_id': compute_item_id(filepath),
        'file_fingerprint': fingerprint,
        'folder': os.path.basename(os.path.dirname(filepath)),
        'file': filename,
        'file_path': filepath,
        'imdb_id': effective_imdb_id,
        'file_size_bytes': os.path.getsize(filepath),
        'actual': {
            'resolution': f"{meta['resolution_width']}x{meta['resolution_height']}",
                'duration_min': round(meta['duration_min'], 1),
                'video_codec': meta['video_codec'],
                'audio_tracks': meta.get('audio_tracks', []),
                'container_title': meta['container_title'],
                'container_title_parsed': container_title_parsed,
            },
        'expected': {
            'resolution': claimed_res,
            'runtime_min': expected_runtime,
            'title': folder_title,
            'year': folder_year,
            'db_title': db_entry['title'] if db_entry else None,
        },
        'flags': flags,
        'flag_count': len(flags),
        'detected_edition': detected_edition if detected_edition else None,
        'guessit_tokens': guessit_tokens,
        'identification': None,
        'recommended_action': None,
        'fixed': None,
    }

    if cd_number is not None:
        result['cd_number'] = cd_number

    # Identification with smart skip logic
    if full:
        has_tv = any(f['check'] == 'tv_episode' for f in flags)
        if has_tv and not force_full:
            result['identification'] = {
                'method': 'tv_episode_detected',
                'action': 'queue_deletion',
                'detail': 'Container title indicates TV episode content',
            }
        elif force_full or needs_identification(flags):
            # Check knowledge doc for cached positive identification
            cached_ident = knowledge_doc.get('identification') if knowledge_doc else None
            if cached_ident and cached_ident.get('method') != 'crc_not_found':
                result['identification'] = cached_ident
                result['identification_cached'] = True
            else:
                # Pass cached hashes to avoid recomputing from file
                cached_crc = knowledge_doc.get('crc32') if knowledge_doc else None
                cached_osh = (knowledge_doc.get('opensubtitles_hash')
                              if knowledge_doc else None)
                result['identification'] = identify_flagged_file(
                    filepath, flags, container_title_parsed,
                    cached_crc32=cached_crc,
                    cached_opensubtitles_hash=cached_osh,
                )
        else:
            result['identification'] = {
                'method': 'skipped',
                'reason': 'high_confidence_quick_scan',
                'detail': 'Quick scan flags sufficient; use force_full=1 to override',
            }

        # Backfill edition from identification source
        _backfill_edition_from_identification(result)

        # Post-identification: re-evaluate ±1 year title flags
        if not db_entry:
            ident = result.get('identification') or {}
            id_year = ident.get('identified_year')
            if id_year:
                removed = _revalidate_year_flags(result, id_year)
                if removed and not result['flags']:
                    return None

    # Compute recommended action from flags + identification
    result['recommended_action'] = compute_recommended_action(
        flags, result['identification'], result.get('expected')
    )

    return result


def _scan_multi_cd(cd_files, folder_title, folder_year, imdb_id, db_entry,
                   expected_runtime, renamer_template, renamer_replace_doubles,
                   renamer_separator, full=False, force_full=False,
                   seen_fingerprints=None, knowledge_callback=None):
    """Scan a multi-CD folder (all files have sequential cd tags).

    Scans each CD file individually with its cd_number, then aggregates
    into a single result card.  The aggregate result uses:
    - cd1's metadata for resolution/codec/container title checks
    - Sum of all CD runtimes for the runtime check
    - All flags from all files (deduplicated)
    - A 'cd_files' list with per-file details

    Args:
        cd_files: List of (cd_num, filepath) tuples, sorted by cd_num
        (remaining args same as _scan_single_file)

    Returns:
        dict with aggregated scan result, or None if no issues.
    """
    # Scan each CD file
    per_file_results = []
    for cd_num, filepath in cd_files:
        result = _scan_single_file(
            filepath, folder_title, folder_year, imdb_id, db_entry,
            expected_runtime=0,  # skip runtime check per-file; aggregate below
            renamer_template=renamer_template,
            renamer_replace_doubles=renamer_replace_doubles,
            renamer_separator=renamer_separator,
            full=full, force_full=force_full,
            cd_number=cd_num,
            seen_fingerprints=seen_fingerprints,
            knowledge_callback=knowledge_callback,
        )
        per_file_results.append((cd_num, filepath, result))

    # Collect results that have flags
    flagged_results = [(cd, fp, r) for cd, fp, r in per_file_results if r is not None]

    # Aggregate runtime from all files for a runtime check
    total_runtime = 0
    for cd_num, filepath, result in per_file_results:
        if result and result.get('actual', {}).get('duration_min'):
            total_runtime += result['actual']['duration_min']
        else:
            # File was clean — still need its runtime
            meta = extract_file_meta(filepath)
            total_runtime += round(meta.get('duration_min', 0), 1)

    # Check aggregate runtime against expected
    runtime_flag = None
    if expected_runtime:
        # Detect edition from cd1 for edition-aware runtime suppression
        cd1_filepath = cd_files[0][1]
        cd1_filename = os.path.basename(cd1_filepath)
        cd1_meta = extract_file_meta(cd1_filepath)
        detected_edition = get_edition(cd1_filename)
        if not detected_edition:
            _, detected_edition = check_edition(
                cd1_meta.get('container_title', ''), cd1_filename
            )
        runtime_flag = check_runtime(
            total_runtime, expected_runtime,
            edition=detected_edition,
            container_title=cd1_meta.get('container_title', ''),
        )

    if not flagged_results and not runtime_flag:
        return None

    # Use cd1 as the primary result, augment with multi-CD info
    primary_cd, primary_path, primary_result = per_file_results[0]

    if primary_result is None:
        # cd1 was clean but we have aggregate runtime or other CD flags
        # Build a minimal result from cd1
        meta = extract_file_meta(primary_path)
        filename = os.path.basename(primary_path)
        guessit_tokens = parse_guessit_tokens(filename)
        claimed_res = parse_filename_resolution(filename)
        primary_result = {
            'item_id': compute_item_id(primary_path),
            'file_fingerprint': compute_file_fingerprint(primary_path),
            'folder': os.path.basename(os.path.dirname(primary_path)),
            'file': filename,
            'file_path': primary_path,
            'imdb_id': imdb_id,
            'file_size_bytes': os.path.getsize(primary_path),
            'actual': {
                'resolution': f"{meta['resolution_width']}x{meta['resolution_height']}",
                'duration_min': round(meta['duration_min'], 1),
                'video_codec': meta['video_codec'],
                'audio_tracks': meta.get('audio_tracks', []),
                'container_title': meta['container_title'],
                'container_title_parsed': None,
            },
            'expected': {
                'resolution': claimed_res,
                'runtime_min': expected_runtime,
                'title': folder_title,
                'year': folder_year,
                'db_title': db_entry['title'] if db_entry else None,
            },
            'flags': [],
            'flag_count': 0,
            'detected_edition': None,
            'guessit_tokens': guessit_tokens,
            'identification': None,
            'recommended_action': None,
            'fixed': None,
        }

    # Add aggregate runtime flag if present
    if runtime_flag:
        primary_result['flags'].append(runtime_flag)
        primary_result['flag_count'] = len(primary_result['flags'])

    # Merge flags from other CD files
    seen_checks = {(f['check'], f.get('detail', '')) for f in primary_result['flags']}
    for cd_num, filepath, result in flagged_results:
        if result is primary_result:
            continue
        for flag in result.get('flags', []):
            key = (flag['check'], flag.get('detail', ''))
            if key not in seen_checks:
                primary_result['flags'].append(flag)
                seen_checks.add(key)

    primary_result['flag_count'] = len(primary_result['flags'])

    # Add multi-CD metadata
    primary_result['multi_cd'] = True
    primary_result['cd_count'] = len(cd_files)
    primary_result['cd_files'] = [
        {
            'cd_number': cd_num,
            'file': os.path.basename(fp),
            'file_path': fp,
            'file_size_bytes': os.path.getsize(fp),
            'has_flags': r is not None,
        }
        for cd_num, fp, r in per_file_results
    ]

    # Update aggregate runtime
    primary_result['actual']['duration_min'] = round(total_runtime, 1)
    primary_result['expected']['runtime_min'] = expected_runtime

    # Compute recommended action from aggregated flags
    primary_result['recommended_action'] = compute_recommended_action(
        primary_result['flags'],
        primary_result.get('identification'),
        primary_result.get('expected'),
    )

    return primary_result


def _scan_variants(video_files, classification, folder_title, folder_year,
                   imdb_id, db_entry, expected_runtime, renamer_template,
                   renamer_replace_doubles, renamer_separator, full=False,
                   force_full=False, seen_fingerprints=None,
                   knowledge_callback=None):
    """Scan a folder with multiple file variants (editions, qualities, dupes).

    When the folder contains a CD sub-group (sequential cd1..cdN alongside
    non-CD files), the CD files are scanned as a single logical unit via
    _scan_multi_cd() and their combined runtime is compared against the
    expected runtime.  Non-CD standalone files are scanned individually.

    Duplicate detection runs across all logical units (the CD set counts
    as one unit).

    Args:
        video_files: All video files in the folder (sorted by size descending)
        classification: Result from classify_video_files()
        (remaining args same as _scan_single_file)

    Returns:
        list[dict] — list of result dicts for flagged files (may be empty → None)
        None — if no files have issues
    """
    scan_kwargs = dict(
        folder_title=folder_title,
        folder_year=folder_year,
        imdb_id=imdb_id,
        db_entry=db_entry,
        expected_runtime=expected_runtime,
        renamer_template=renamer_template,
        renamer_replace_doubles=renamer_replace_doubles,
        renamer_separator=renamer_separator,
        full=full,
        force_full=force_full,
        knowledge_callback=knowledge_callback,
    )

    has_cd_subgroup = classification.get('has_cd_subgroup', False)
    cd_files = classification.get('cd_files', [])

    # -- Phase 1: Scan logical units --
    # Each "unit" is either a CD sub-group result or a standalone file result.
    # full_results holds one entry per unit for duplicate detection.
    full_results = []
    # Track which filepaths belong to the CD sub-group (for variant_files display)
    cd_subgroup_paths = set()

    if has_cd_subgroup and cd_files:
        # Scan the CD sub-group as one logical unit
        cd_subgroup_paths = {fp for _, fp in cd_files}
        cd_result = _scan_multi_cd(cd_files, **scan_kwargs,
                                   seen_fingerprints=seen_fingerprints)
        if cd_result is not None:
            cd_result['_is_cd_subgroup'] = True
            full_results.append(cd_result)
        else:
            # CD sub-group is clean — still need it for duplicate detection
            # Build a minimal aggregate result
            total_size = sum(os.path.getsize(fp) for _, fp in cd_files)
            total_runtime = 0
            for _, fp in cd_files:
                meta = extract_file_meta(fp)
                total_runtime += round(meta.get('duration_min', 0), 1)
            cd1_path = cd_files[0][1]
            cd1_filename = os.path.basename(cd1_path)
            claimed_res = parse_filename_resolution(cd1_filename)
            full_results.append({
                'file': cd1_filename,
                'file_path': cd1_path,
                'file_size_bytes': total_size,
                'actual': {
                    'duration_min': round(total_runtime, 1),
                },
                'expected': {
                    'resolution': claimed_res,
                },
                '_clean': True,
                '_is_cd_subgroup': True,
                'multi_cd': True,
                'cd_count': len(cd_files),
            })

    # Scan standalone files (non-CD, or CD files when there's no valid sub-group)
    standalone_files = classification.get('non_cd_files', [])
    if not has_cd_subgroup:
        # No valid CD sub-group — scan ALL files as individual variants
        standalone_files = list(video_files)

    for filepath in standalone_files:
        result = _scan_single_file(filepath, **scan_kwargs,
                                   seen_fingerprints=seen_fingerprints)
        if result is not None:
            full_results.append(result)
        else:
            # Build a minimal dict for duplicate detection
            meta = extract_file_meta(filepath)
            filename = os.path.basename(filepath)
            claimed_res = parse_filename_resolution(filename)
            full_results.append({
                'file': filename,
                'file_path': filepath,
                'file_size_bytes': os.path.getsize(filepath),
                'actual': {
                    'duration_min': round(meta['duration_min'], 1),
                },
                'expected': {
                    'resolution': claimed_res,
                },
                '_clean': True,
            })

    # -- Phase 2: Duplicate detection across all units --
    dupe_pairs = detect_duplicates(full_results)

    # Add duplicate flags to detected pairs with keep/delete recommendation
    for idx_a, idx_b in dupe_pairs:
        # Promote clean items first so we have full metadata for comparison
        for idx in (idx_a, idx_b):
            r = full_results[idx]
            if r.get('_clean'):
                _promote_clean_to_full(r, imdb_id, expected_runtime,
                                       folder_title, folder_year, db_entry)

        # Determine which item to keep
        a = full_results[idx_a]
        b = full_results[idx_b]
        keep, delete = pick_best_duplicate(a, b)

        for idx in (idx_a, idx_b):
            r = full_results[idx]
            has_dupe = any(f['check'] == 'duplicate' for f in r.get('flags', []))
            if not has_dupe:
                partner_idx = idx_b if idx == idx_a else idx_a
                partner = full_results[partner_idx]
                partner_label = partner['file']
                if partner.get('multi_cd'):
                    partner_label = '%s (%d-CD set)' % (partner['file'], partner.get('cd_count', 0))

                is_keeper = (r is keep)
                if is_keeper:
                    detail = 'Possible duplicate of %s (recommend keeping this copy)' % partner_label
                else:
                    detail = 'Possible duplicate of %s (recommend deleting this copy)' % partner_label

                r.setdefault('flags', []).append({
                    'check': 'duplicate',
                    'severity': 'MEDIUM',
                    'detail': detail,
                    'duplicate_action': 'keep' if is_keeper else 'delete',
                    'duplicate_of': partner['file'],
                })
                r['flag_count'] = len(r['flags'])

    # -- Phase 3: Assemble final results --
    variant_files = [os.path.basename(fp) for fp in video_files]

    results = []
    for r in full_results:
        if r.get('_clean'):
            continue  # no flags and not a duplicate
        r['variant_files'] = variant_files
        r['variant_count'] = len(video_files)
        # Recompute recommended_action with updated flags
        if r.get('flags'):
            r['recommended_action'] = compute_recommended_action(
                r['flags'], r.get('identification'), r.get('expected')
            )
        results.append(r)

    return results if results else None


def _promote_clean_to_full(r, imdb_id, expected_runtime, folder_title,
                           folder_year, db_entry):
    """Promote a minimal '_clean' result dict to a full result in-place.

    Used when a previously-clean file gets a duplicate flag added.
    Skips CD sub-group results (they already have full structure from
    _scan_multi_cd).
    """
    if r.get('_is_cd_subgroup'):
        # CD sub-group results already have most fields from _scan_multi_cd
        r.pop('_clean', None)
        r.setdefault('flags', [])
        r.setdefault('flag_count', 0)
        return

    filepath = r['file_path']
    meta = extract_file_meta(filepath)
    filename = os.path.basename(filepath)
    guessit_tokens = parse_guessit_tokens(filename)
    claimed_res = parse_filename_resolution(filename)

    r.update({
        'item_id': compute_item_id(filepath),
        'file_fingerprint': compute_file_fingerprint(filepath),
        'folder': os.path.basename(os.path.dirname(filepath)),
        'file': filename,
        'file_path': filepath,
        'imdb_id': imdb_id,
        'file_size_bytes': os.path.getsize(filepath),
        'actual': {
            'resolution': f"{meta['resolution_width']}x{meta['resolution_height']}",
            'duration_min': round(meta['duration_min'], 1),
            'video_codec': meta['video_codec'],
            'audio_tracks': meta.get('audio_tracks', []),
            'container_title': meta['container_title'],
            'container_title_parsed': None,
        },
        'expected': {
            'resolution': claimed_res,
            'runtime_min': expected_runtime,
            'title': folder_title,
            'year': folder_year,
            'db_title': db_entry['title'] if db_entry else None,
        },
        'flags': [],
        'flag_count': 0,
        'detected_edition': None,
        'guessit_tokens': guessit_tokens,
        'identification': None,
        'recommended_action': None,
        'fixed': None,
    })
    r.pop('_clean', None)


def scan_movie_folder(folder_path, folder_name, media_by_imdb,
                      full=False, force_full=False, media_by_title=None,
                      renamer_template=None, renamer_replace_doubles=True,
                      renamer_separator='', seen_fingerprints=None,
                      knowledge_callback=None):
    """Scan a single movie folder and return audit results.

    Handles single-file, multi-CD, and variant (multi-file) folders.

    Args:
        folder_path: Full path to the movie folder
        folder_name: Folder basename (e.g., "Dead, The (1987)")
        media_by_imdb: Dict mapping IMDB ID → movie info from CP database
        full: Run full scan with identification on flagged files
        force_full: Force identification even for high-confidence quick scan flags
        media_by_title: Dict mapping (normalized_title, year) → movie info
        renamer_template: Renamer file_name template string (e.g. '<thename> (<year>) <quality>.<ext>')
        renamer_replace_doubles: Whether to clean up double separators
        renamer_separator: Character to replace spaces with (empty = spaces)

    Returns:
        dict — single result for single-file or multi-CD folders
        list[dict] — list of results for variant folders (one per flagged file)
        None — if no issues found
    """
    video_files = find_video_files(folder_path)
    if not video_files:
        return None

    # Parse expected values from folder name
    folder_title, folder_year = parse_folder_name(folder_name)

    # Look up DB entry — try IMDB ID from the largest file first, then
    # fall back to title+year lookup.  This shared context is used by
    # all files in the folder.
    video_files.sort(key=lambda f: os.path.getsize(f), reverse=True)
    imdb_id = parse_filename_imdb(os.path.basename(video_files[0]))
    db_entry = None
    expected_runtime = 0

    if imdb_id and imdb_id in media_by_imdb:
        db_entry = media_by_imdb[imdb_id]
        expected_runtime = db_entry.get('runtime', 0)

    # Fallback: look up by title+year when IMDB ID not in filename
    if not db_entry and media_by_title and folder_title and folder_year:
        key = (normalize_title(folder_title), folder_year)
        match = media_by_title.get(key)
        if match:
            if not imdb_id:
                imdb_id = match['imdb_id']
            db_entry = match
            expected_runtime = match.get('runtime', 0)

    # Common scan kwargs shared by all dispatch paths
    scan_kwargs = dict(
        folder_title=folder_title,
        folder_year=folder_year,
        imdb_id=imdb_id,
        db_entry=db_entry,
        expected_runtime=expected_runtime,
        renamer_template=renamer_template,
        renamer_replace_doubles=renamer_replace_doubles,
        renamer_separator=renamer_separator,
        full=full,
        force_full=force_full,
        knowledge_callback=knowledge_callback,
    )

    # Classify files and dispatch
    classification = classify_video_files(video_files)

    if classification['type'] == 'single':
        # Single file — original behavior
        filepath = video_files[0]
        return _scan_single_file(filepath, **scan_kwargs,
                                 seen_fingerprints=seen_fingerprints)

    elif classification['type'] == 'multi_cd':
        # Multi-CD: scan each cd file, aggregate into one result
        return _scan_multi_cd(classification['cd_files'], **scan_kwargs,
                              seen_fingerprints=seen_fingerprints)

    else:
        # Variants: scan each file individually, return list of flagged results
        return _scan_variants(video_files, classification, **scan_kwargs,
                              seen_fingerprints=seen_fingerprints)


# Sentinel value for _scan_one: folder was not a directory, skip it
_SKIP = object()


def scan_library(movies_dir, db_path, scan_path=None, full=False,
                 force_full=False, workers=DEFAULT_WORKERS,
                 progress_callback=None, cancel_flag=None,
                 knowledge_callback=None):
    """Scan movie library for mislabeled files.

    Args:
        movies_dir: Path to the movies directory
        db_path: Path to CP's db.json
        scan_path: If set, only scan this specific folder name
        full: Run full scan with identification on flagged files
        force_full: Force identification even for high-confidence quick scan flags
        workers: Number of parallel scan threads (1 = sequential)
        progress_callback: If set, called with (scanned, total, flagged_count)
                           after each folder
        cancel_flag: If set, a list whose first element is checked each
                     iteration — if truthy, the scan stops early and returns
                     partial results.

    Returns:
        dict with scan results
    """
    _log_info('Loading CP database from %s...' % db_path)
    media_by_imdb, files_by_media_id, media_by_title, release_by_filepath = load_cp_database(db_path)
    _log_info('Loaded %s movies from database' % len(media_by_imdb))

    # Wrap knowledge_callback to inject release/media IDs from DB mapping
    if knowledge_callback:
        _raw_kb_callback = knowledge_callback

        def _kb_callback(fingerprint, filepath):
            rel_info = release_by_filepath.get(filepath, {})
            return _raw_kb_callback(fingerprint, filepath,
                                    release_id=rel_info.get('release_id'),
                                    media_id=rel_info.get('media_id'))

        knowledge_callback = _kb_callback

    # Read renamer template once for template conformance check
    renamer_template = None
    renamer_replace_doubles = True
    renamer_separator = ''
    if _CP_AVAILABLE:
        try:
            renamer_template = Env.setting('file_name', section='renamer',
                                           default='<thename><cd>.<ext>')
            renamer_replace_doubles = Env.setting('replace_doubles',
                                                   section='renamer', default=True)
            renamer_separator = Env.setting('separator', section='renamer',
                                            default='')
            _log_info('Renamer template: %s' % renamer_template)
        except Exception as e:
            _log_warn('Could not read renamer settings: %s' % e)
            renamer_template = None

    # Enumerate movie folders
    if scan_path:
        folders = [scan_path]
    else:
        try:
            folders = sorted(os.listdir(movies_dir))
        except OSError as e:
            _log_error('Cannot list %s: %s' % (movies_dir, e))
            return {'error': str(e)}

    total = len(folders)
    flagged = []
    scanned = 0
    errors = 0
    seen_fingerprints = {}  # fingerprint → filepath, for DB upsert + pruning
    lock = threading.Lock()

    # Clamp workers to sane range
    workers = max(1, min(workers, MAX_WORKERS))

    def _scan_one(folder_name):
        """Scan a single folder. Called from thread pool or main loop.

        Returns:
            (_SKIP, False) — not a directory, skip
            (result_or_None, False) — scanned successfully (result is None if clean)
            (None, True) — error during scan
        """
        folder_path = os.path.join(movies_dir, folder_name)
        if not os.path.isdir(folder_path):
            return _SKIP, False
        try:
            result = scan_movie_folder(
                folder_path, folder_name, media_by_imdb,
                full=full, force_full=force_full,
                media_by_title=media_by_title,
                renamer_template=renamer_template,
                renamer_replace_doubles=renamer_replace_doubles,
                renamer_separator=renamer_separator,
                seen_fingerprints=seen_fingerprints,
                knowledge_callback=knowledge_callback,
            )
            return result, False
        except Exception as e:
            _log_error('Error scanning %s: %s' % (folder_name, e))
            return None, True

    def _collect_result(folder_name, result, is_error):
        """Process a scan result. Must be called under lock (or single-threaded).

        result can be:
          - None (clean folder)
          - dict (single flagged item)
          - list[dict] (multiple flagged items from variant folder)
        """
        nonlocal scanned, errors
        if is_error:
            errors += 1
            return
        scanned += 1
        if result is not None:
            if isinstance(result, list):
                # Variant folder: multiple flagged files
                for item in result:
                    flagged.append(item)
                    severity = max(f['severity'] for f in item['flags'])
                    checks = ', '.join(f['check'] for f in item['flags'])
                    _log_info('FLAGGED [%s] %s/%s: %s' % (severity, folder_name, item['file'], checks))
            else:
                flagged.append(result)
                severity = max(f['severity'] for f in result['flags'])
                checks = ', '.join(f['check'] for f in result['flags'])
                _log_info('FLAGGED [%s] %s: %s' % (severity, folder_name, checks))
        if progress_callback:
            progress_callback(scanned, total, len(flagged))

    _log_info('Scanning %s folders with %s worker(s)...' % (total, workers))

    if workers <= 1:
        # ---- Sequential scan (original behavior) ----
        for folder_name in folders:
            if cancel_flag and cancel_flag[0]:
                _log_info('Scan cancelled at %s/%s (%s flagged)' % (scanned, total, len(flagged)))
                break

            result, is_error = _scan_one(folder_name)
            if result is _SKIP:
                continue
            _collect_result(folder_name, result, is_error)
    else:
        # ---- Multi-threaded scan ----
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all work up front — the executor queues internally
            # and only runs `workers` tasks concurrently
            future_to_folder = {}
            for folder_name in folders:
                if cancel_flag and cancel_flag[0]:
                    break
                future = executor.submit(_scan_one, folder_name)
                future_to_folder[future] = folder_name

            for future in as_completed(future_to_folder):
                if cancel_flag and cancel_flag[0]:
                    # Cancel pending futures (already-running ones will finish)
                    for f in future_to_folder:
                        f.cancel()
                    _log_info('Scan cancelled at %s/%s (%s flagged)' % (scanned, total, len(flagged)))
                    break

                folder_name = future_to_folder[future]
                try:
                    result, is_error = future.result()
                except Exception as e:
                    _log_error('Unexpected error scanning %s: %s' % (folder_name, e))
                    with lock:
                        errors += 1
                    continue

                if result is _SKIP:
                    continue

                with lock:
                    _collect_result(folder_name, result, is_error)

    was_cancelled = bool(cancel_flag and cancel_flag[0])
    if was_cancelled:
        _log_info('Scan complete: %s scanned, %s flagged, %s errors (CANCELLED)' % (scanned, len(flagged), errors))
    else:
        _log_info('Scan complete: %s scanned, %s flagged, %s errors' % (scanned, len(flagged), errors))

    # Sort flagged by flag count (most flags first), then by severity
    severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}
    flagged.sort(key=lambda r: (
        -r['flag_count'],
        min(severity_order.get(f['severity'], 99) for f in r['flags']),
    ))

    return {
        'total_scanned': scanned,
        'total_flagged': len(flagged),
        'total_errors': errors,
        'cancelled': was_cancelled,
        'flagged': flagged,
        'seen_fingerprints': seen_fingerprints,
        'release_by_filepath': release_by_filepath,
    }


# ---------------------------------------------------------------------------
# Fix helpers
# ---------------------------------------------------------------------------

VALID_FIX_ACTIONS = {
    'rename_resolution',
    'reassign_movie',
    'delete_wrong',
    'delete_duplicate',
    'delete_foreign',
    'verify_audio',
    'rename_edition',
    'rename_template',
}


def _build_resolution_rename(item):
    """Build the new filename for a resolution rename.

    Uses the renamer template with the corrected quality label.
    Returns (new_filename, new_path, actual_label) or raises ValueError.
    """
    claimed = item['expected'].get('resolution')
    if not claimed:
        raise ValueError('No claimed resolution in item')

    actual_width = 0
    actual_height = 0
    actual_res = item['actual'].get('resolution', '')
    if 'x' in actual_res:
        try:
            actual_width = int(actual_res.split('x')[0])
            actual_height = int(actual_res.split('x')[1])
        except (ValueError, IndexError):
            pass

    if not actual_height:
        raise ValueError('Cannot determine actual resolution from: %s' % actual_res)

    actual_label = resolution_label(actual_width, actual_height)
    if actual_label == claimed:
        raise ValueError('Resolution already matches: %s' % claimed)

    # Preserve existing edition from filename if present
    old_file = item['file']
    existing_edition = get_edition(old_file)
    edition = existing_edition if existing_edition else (item.get('detected_edition') or '')

    new_file, new_path = _apply_renamer_template(
        item, quality_override=actual_label, edition_override=edition
    )
    return new_file, new_path, actual_label


def _apply_renamer_template(item, quality_override=None, edition_override=None,
                            cd_number=None):
    """Rebuild a filename from the renamer's file_name template.

    Uses the renamer config settings (file_name template, replace_doubles,
    separator) to construct the filename, exactly as the renamer would.

    Args:
        item: audit item dict with expected/actual/imdb_id fields
        quality_override: if set, use this instead of the item's claimed quality
        edition_override: if set, use this as the edition value
        cd_number: CD number for multi-CD files (int), or None

    Returns (new_filename, new_path) or raises ValueError.
    """
    if not _CP_AVAILABLE:
        raise ValueError('Renamer template requires CouchPotato environment')

    template = Env.setting('file_name', section='renamer', default='<thename><cd>.<ext>')
    replace_doubles = Env.setting('replace_doubles', section='renamer', default=True)
    separator = Env.setting('separator', section='renamer', default='')

    old_path = item['file_path']
    old_dir = os.path.dirname(old_path)
    old_file = item['file']
    _, ext = os.path.splitext(old_file)
    ext = ext.lstrip('.')

    # Build replacements dict matching renamer.py lines 430-457
    # Use db_title (original form) when available; reverse-transform namethe
    # folder titles as fallback so <thename> and <namethe> resolve correctly
    movie_name = (item['expected'].get('db_title')
                  or _title_to_thename(item['expected'].get('title', '')))
    # Remove chars illegal in filenames
    movie_name = re.sub(r'[\x00/\\:*?"<>|]', '', movie_name)

    # Build "name_the" — put leading article at the end
    name_the = movie_name
    for prefix in ['the ', 'an ', 'a ']:
        if prefix == movie_name[:len(prefix)].lower():
            name_the = movie_name[len(prefix):] + ', ' + prefix.strip().capitalize()
            break

    quality = quality_override if quality_override else item['expected'].get('resolution', '')
    # Derive quality from actual resolution when not claimed in filename
    if not quality:
        actual_res = item['actual'].get('resolution', '')
        if 'x' in actual_res:
            try:
                w = int(actual_res.split('x')[0])
                h = int(actual_res.split('x')[1])
                quality = resolution_label(w, h)
            except (ValueError, IndexError):
                pass
    edition = edition_override if edition_override is not None else item.get('detected_edition', '')
    # Fallback: detect edition from filename when stored result has none
    if not edition:
        edition = get_edition(old_file)
    imdb_id = item.get('imdb_id', '') or ''
    if not imdb_id:
        imdb_id = (item.get('identification') or {}).get('identified_imdb', '') or ''
    year = item['expected'].get('year') or ''

    # Guessit-derived tokens — fill from item if available, otherwise parse
    gt = item.get('guessit_tokens')
    if not gt:
        gt = parse_guessit_tokens(old_file)

    actual_res = item['actual'].get('resolution', '')
    actual_width = ''
    actual_height = ''
    if 'x' in actual_res:
        parts = actual_res.split('x')
        actual_width = parts[0]
        actual_height = parts[1] if len(parts) > 1 else ''

    replacements = {
        'ext': ext,
        'namethe': name_the.strip(),
        'thename': movie_name.strip(),
        'year': str(year) if year else '',
        'first': name_the[0].upper() if name_the else '',
        'quality': quality,
        'quality_type': gt.get('quality_type', ''),
        'video': gt.get('video', ''),
        'audio': gt.get('audio', ''),
        'group': gt.get('group', ''),
        'source': gt.get('source', ''),
        'resolution_width': actual_width,
        'resolution_height': actual_height,
        'audio_channels': gt.get('audio_channels', ''),
        'imdb_id': imdb_id,
        'cd': ' cd%d' % cd_number if cd_number else '',
        'cd_nr': str(cd_number) if cd_number else '',
        'mpaa': '',
        'mpaa_only': 'Not Rated',
        'category': '',
        '3d': '',
        '3d_type': '',
        '3d_type_short': '',
        'edition': edition,
        'edition_plex': '{edition-%s}' % edition if edition else '',
        'imdb_id_plex': '{imdb-%s}' % imdb_id if imdb_id else '',
        'imdb_id_emby': '[imdbid-%s]' % imdb_id if imdb_id else '',
        'imdb_id_kodi': '{imdb=%s}' % imdb_id if imdb_id else '',
    }

    # Apply template — same logic as renamer.doReplace()
    replaced = template
    # First pass: replace all tokens except thename/namethe
    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            continue
        if val is not None:
            replaced = replaced.replace('<%s>' % key, str(val))
        else:
            replaced = replaced.replace('<%s>' % key, '')

    # Clean up double separators if enabled
    if replace_doubles:
        replaced = replaced.lstrip('. ')
        double_replaces = [
            (r'\(\s*\)', ''),   # remove empty parentheses from missing tokens
            (r'\[\s*\]', ''),   # remove empty brackets from missing tokens
            (r'\{\s*\}', ''),   # remove empty braces from missing tokens
            (r'\.+', '.'), (r'_+', '_'), (r'-+', '-'), (r'\s+', ' '), (r' \\', r'\\'), (' /', '/'),
            (r'(\s\.)+', '.'), (r'(-\.)+', '.'), (r'(\s-[^\s])+', '-'), (' ]', ']'),
        ]
        for pattern, repl in double_replaces:
            replaced = re.sub(pattern, repl, replaced)
        replaced = replaced.rstrip(',_-/\\ ')

    # Second pass: replace thename/namethe (after doubles cleanup)
    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            replaced = replaced.replace('<%s>' % key, str(val))

    # Remove illegal chars
    replaced = re.sub(r'[\x00:*?"<>|]', '', replaced)

    # Apply separator
    if separator:
        replaced = replaced.replace(' ', separator)

    new_file = replaced
    new_path = os.path.join(old_dir, new_file)
    return new_file, new_path


def _apply_folder_template(title, year):
    """Build a folder name from the renamer's folder_name template.

    Uses the same token replacement logic as the renamer to produce folder names
    that match the library convention (e.g. '<namethe> (<year>)').

    Args:
        title: Movie title (raw, will be sanitized)
        year: Movie year (int or str, may be None)

    Returns:
        Sanitized folder name string.
    """
    if not _CP_AVAILABLE:
        safe = re.sub(r'[\x00/\\:*?"<>|]', '', title)
        return '%s (%s)' % (safe, year) if year else safe

    template = Env.setting('folder_name', section='renamer', default='<namethe> (<year>)')
    replace_doubles = Env.setting('replace_doubles', section='renamer', default=True)
    foldersep = Env.setting('foldersep', section='renamer', default='')

    # Sanitize title for filesystem
    safe_title = re.sub(r'[\x00/\\:*?"<>|]', '', title)

    # Build "name_the" — put leading article at the end
    name_the = safe_title
    for prefix in ['the ', 'an ', 'a ']:
        if prefix == safe_title[:len(prefix)].lower():
            name_the = safe_title[len(prefix):] + ', ' + prefix.strip().capitalize()
            break

    replacements = {
        'thename': safe_title.strip(),
        'namethe': name_the.strip(),
        'year': str(year) if year else '',
        'first': name_the[0].upper() if name_the else '',
        'quality': '',
        'quality_type': '',
        'video': '',
        'audio': '',
        'group': '',
        'source': '',
        'resolution_width': '',
        'resolution_height': '',
        'audio_channels': '',
        'imdb_id': '',
        'cd': '',
        'cd_nr': '',
        'mpaa': '',
        'mpaa_only': '',
        'category': '',
        '3d': '',
        '3d_type': '',
        '3d_type_short': '',
        'edition': '',
        'edition_plex': '',
        'imdb_id_plex': '',
        'imdb_id_emby': '',
        'imdb_id_kodi': '',
    }

    replaced = template
    # First pass: replace all tokens except thename/namethe
    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            continue
        if val is not None:
            replaced = replaced.replace('<%s>' % key, str(val))
        else:
            replaced = replaced.replace('<%s>' % key, '')

    # Clean up double separators
    if replace_doubles:
        replaced = replaced.lstrip('. ')
        double_replaces = [
            (r'\(\s*\)', ''),
            (r'\[\s*\]', ''),
            (r'\{\s*\}', ''),
            (r'\.+', '.'), (r'_+', '_'), (r'-+', '-'), (r'\s+', ' '), (r' \\', r'\\'), (' /', '/'),
            (r'(\s\.)+', '.'), (r'(-\.)+', '.'), (r'(\s-[^\s])+', '-'), (' ]', ']'),
        ]
        for pattern, repl in double_replaces:
            replaced = re.sub(pattern, repl, replaced)
        replaced = replaced.rstrip(',_-/\\ ')

    # Second pass: replace thename/namethe
    for key, val in replacements.items():
        if key in ('thename', 'namethe'):
            replaced = replaced.replace('<%s>' % key, str(val))

    # Remove any remaining illegal chars
    replaced = re.sub(r'[\x00:*?"<>|]', '', replaced)

    # Apply folder separator
    if foldersep:
        replaced = replaced.replace(' ', foldersep)

    return replaced.strip()


def _build_edition_rename(item):
    """Build the new filename for an edition rename.

    Uses the renamer template to reconstruct the filename with the edition included.
    Returns (new_filename, new_path) or raises ValueError.
    """
    edition = item.get('detected_edition')
    if not edition:
        raise ValueError('No detected edition in item')

    old_file = item['file']
    # Check if edition is already in the filename
    filename_edition = get_edition(old_file)
    if filename_edition:
        raise ValueError('Filename already has edition: %s' % filename_edition)

    new_file, new_path = _apply_renamer_template(item, edition_override=edition)

    if new_path == item['file_path']:
        raise ValueError('Rename would produce identical filename')

    return new_file, new_path


def _preview_rename_resolution(item):
    """Generate preview for rename_resolution action."""
    try:
        new_file, new_path, actual_label = _build_resolution_rename(item)
    except ValueError as e:
        return {'error': str(e)}

    return {
        'item_id': item['item_id'],
        'action': 'rename_resolution',
        'changes': {
            'filesystem': {
                'old_path': item['file_path'],
                'new_path': new_path,
            },
            'database': {
                'update_release_quality': {
                    'from': item['expected'].get('resolution', ''),
                    'to': actual_label,
                },
            },
        },
        'warnings': [],
    }


def _preview_rename_edition(item):
    """Generate preview for rename_edition action."""
    try:
        new_file, new_path = _build_edition_rename(item)
    except ValueError as e:
        return {'error': str(e)}

    return {
        'item_id': item['item_id'],
        'action': 'rename_edition',
        'changes': {
            'filesystem': {
                'old_path': item['file_path'],
                'new_path': new_path,
            },
            'database': None,
        },
        'warnings': [],
    }


def _preview_delete_wrong(item):
    """Generate preview for delete_wrong action."""
    old_path = item['file_path']
    folder_path = os.path.dirname(old_path)

    return {
        'item_id': item['item_id'],
        'action': 'delete_wrong',
        'changes': {
            'filesystem': {
                'delete_path': old_path,
                'folder_cleanup': folder_path,
            },
            'database': {
                'reset_status': {
                    'movie': item['expected'].get('title', ''),
                    'default': 'wanted',
                    'options': ['wanted', 'done', 'nochange', 'remove'],
                },
            },
        },
        'warnings': [],
    }


def _preview_reassign_movie(item):
    """Generate preview for reassign_movie action.

    Requires identification with an identified IMDB ID.
    """
    ident = item.get('identification')
    if not ident:
        return {'error': 'No identification data — run a full scan first'}

    method = ident.get('method', '')
    if method not in ('container_title', 'srrdb_crc', 'opensubtitles_hash', 'manual'):
        return {'error': 'Identification method "%s" cannot determine correct movie' % method}

    id_title = ident.get('identified_title', '')
    id_year = ident.get('identified_year')
    id_imdb = ident.get('identified_imdb', '')

    if not id_title:
        return {'error': 'Identification did not find a title'}

    # Determine the movies root from the old path
    # old_path: /media/Movies/FolderName/file.mkv → movies_dir = /media/Movies
    old_path = item['file_path']
    old_folder_path = os.path.dirname(old_path)
    movies_dir = os.path.dirname(old_folder_path)

    # Build new filename using the renamer template with the identified movie's
    # title/year/IMDB, preserving guessit tokens from the original filename
    reassign_item = dict(item)
    reassign_item['expected'] = dict(item.get('expected', {}))
    reassign_item['expected']['title'] = id_title
    reassign_item['expected']['db_title'] = id_title
    reassign_item['expected']['year'] = id_year
    reassign_item['imdb_id'] = id_imdb

    try:
        new_file, _ = _apply_renamer_template(reassign_item)
    except (ValueError, Exception) as e:
        # Fallback to simple format if template fails
        old_file = item['file']
        ext = os.path.splitext(old_file)[1]
        actual_res = item['actual'].get('resolution', '')
        actual_width = actual_height = 0
        if 'x' in actual_res:
            try:
                actual_width = int(actual_res.split('x')[0])
                actual_height = int(actual_res.split('x')[1])
            except (ValueError, IndexError):
                pass
        res_label = resolution_label(actual_width, actual_height) if actual_height else ''
        safe_title = re.sub(r'[\x00/\\:*?"<>|]', '', id_title)
        parts = [safe_title]
        if id_year:
            parts[0] = '%s (%s)' % (safe_title, id_year)
        if res_label:
            parts.append(res_label)
        if id_imdb:
            parts.append(id_imdb)
        new_file = ' '.join(parts) + ext

    # Build destination folder name using renamer's folder_name template
    new_folder = _apply_folder_template(id_title, id_year)

    new_folder_path = os.path.join(movies_dir, new_folder)
    new_path = os.path.join(new_folder_path, new_file)

    warnings = []
    if not id_imdb:
        warnings.append('No IMDB ID identified — movie may not be linked in CP database')
    if not id_year:
        warnings.append('No year identified — folder name may be ambiguous')

    return {
        'item_id': item['item_id'],
        'action': 'reassign_movie',
        'changes': {
            'filesystem': {
                'old_path': old_path,
                'new_path': new_path,
                'old_folder_cleanup': True,
            },
            'database': {
                'remove_from': {
                    'movie': item['expected'].get('title', ''),
                    'year': item['expected'].get('year'),
                    'imdb': item.get('imdb_id', ''),
                },
                'add_to': {
                    'movie': id_title,
                    'year': id_year,
                    'imdb': id_imdb,
                },
                'reset_status': {
                    'movie': item['expected'].get('title', ''),
                    'default': 'wanted',
                    'options': ['wanted', 'done', 'nochange', 'remove'],
                },
            },
        },
        'warnings': warnings,
    }


def _preview_rename_template(item):
    """Generate preview for rename_template action.

    Rebuilds filename from the renamer template using all available data
    including guessit-parsed tokens from the current filename.

    When the expected year (e.g. corrected by identification) produces a different
    folder name than the current folder, a folder rename is included in the
    preview so the entire path is corrected in one operation.

    For multi-CD items, generates renames for all cd files.
    """
    cd_number = item.get('cd_number')

    try:
        new_file, new_path = _apply_renamer_template(item, cd_number=cd_number)
    except ValueError as e:
        return {'error': str(e)}

    old_path = item['file_path']
    old_folder_path = os.path.dirname(old_path)
    old_folder_name = os.path.basename(old_folder_path)

    # Check if the folder also needs renaming (e.g. year correction)
    exp = item.get('expected', {})
    exp_title = exp.get('db_title') or exp.get('title', '')
    exp_year = exp.get('year')
    folder_rename = False
    new_folder_path = None

    if exp_title:
        expected_folder = _apply_folder_template(exp_title, exp_year)
        if expected_folder != old_folder_name:
            folder_rename = True
            movies_dir = os.path.dirname(old_folder_path)
            new_folder_path = os.path.join(movies_dir, expected_folder)
            new_path = os.path.join(new_folder_path, new_file)

    if new_path == old_path:
        return {'error': 'Rename would produce identical filename'}

    result = {
        'item_id': item['item_id'],
        'action': 'rename_template',
        'changes': {
            'filesystem': {
                'old_path': old_path,
                'new_path': new_path,
            },
            'database': None,
        },
        'warnings': [],
    }

    if folder_rename:
        result['changes']['filesystem']['folder_rename'] = {
            'old_folder': old_folder_path,
            'new_folder': new_folder_path,
        }
        result['changes']['filesystem']['old_folder_cleanup'] = True

    # Multi-CD: include rename info for all cd files
    if item.get('multi_cd') and item.get('cd_files'):
        cd_renames = []
        for cd_info in item['cd_files']:
            cd_num = cd_info['cd_number']
            cd_filepath = cd_info['file_path']
            cd_item = dict(item)
            cd_item['file'] = cd_info['file']
            cd_item['file_path'] = cd_filepath
            cd_item['guessit_tokens'] = parse_guessit_tokens(cd_info['file'])
            try:
                cd_new_file, cd_new_path = _apply_renamer_template(
                    cd_item, cd_number=cd_num
                )
            except ValueError:
                continue
            if folder_rename and new_folder_path:
                cd_new_path = os.path.join(new_folder_path, cd_new_file)
            cd_renames.append({
                'cd_number': cd_num,
                'old_path': cd_filepath,
                'new_path': cd_new_path,
            })
        if cd_renames:
            result['changes']['filesystem']['cd_renames'] = cd_renames
            result['changes']['filesystem']['old_path'] = cd_renames[0]['old_path']
            result['changes']['filesystem']['new_path'] = cd_renames[0]['new_path']

    return result


def generate_fix_preview(item, action):
    """Generate a fix preview for a flagged item.

    Returns a preview dict describing what changes would be made.
    """
    if action == 'rename_template':
        return _preview_rename_template(item)
    elif action == 'rename_resolution':
        return _preview_rename_resolution(item)
    elif action == 'rename_edition':
        return _preview_rename_edition(item)
    elif action in ('delete_wrong', 'delete_duplicate', 'delete_foreign'):
        return _preview_delete_wrong(item)
    elif action == 'verify_audio':
        return {
            'action': 'verify_audio',
            'current_path': item.get('file_path', ''),
            'description': 'Run whisper.cpp language detection on audio track(s)',
        }
    elif action == 'reassign_movie':
        return _preview_reassign_movie(item)
    else:
        return {'error': 'Unknown action: %s' % action}


def execute_fix_rename_resolution(item):
    """Execute a resolution rename fix.

    Returns (success, details_dict).
    """
    try:
        new_file, new_path, actual_label = _build_resolution_rename(item)
    except ValueError as e:
        return False, {'error': str(e)}

    old_path = item['file_path']

    # Safety: check file exists
    if not os.path.isfile(old_path):
        return False, {'error': 'File not found: %s' % old_path}

    # Safety: check destination doesn't exist
    if os.path.exists(new_path):
        return False, {'error': 'Destination already exists: %s' % new_path}

    try:
        os.rename(old_path, new_path)
    except OSError as e:
        return False, {'error': 'Rename failed: %s' % e}

    return True, {
        'old_path': old_path,
        'new_path': new_path,
        'old_resolution': item['expected'].get('resolution', ''),
        'new_resolution': actual_label,
    }


def execute_fix_rename_edition(item):
    """Execute an edition rename fix.

    Returns (success, details_dict).
    """
    try:
        new_file, new_path = _build_edition_rename(item)
    except ValueError as e:
        return False, {'error': str(e)}

    old_path = item['file_path']

    if not os.path.isfile(old_path):
        return False, {'error': 'File not found: %s' % old_path}

    if os.path.exists(new_path):
        return False, {'error': 'Destination already exists: %s' % new_path}

    try:
        os.rename(old_path, new_path)
    except OSError as e:
        return False, {'error': 'Rename failed: %s' % e}

    return True, {
        'old_path': old_path,
        'new_path': new_path,
        'edition': item.get('detected_edition', ''),
    }


def execute_fix_rename_template(item):
    """Execute a template rename fix.

    Rebuilds filename from the renamer template, preserving guessit-parsed
    tokens from the current filename.  This is a superset of
    rename_resolution and rename_edition — it fixes everything in one rename.

    When the expected data produces a different folder name (e.g. year
    correction from identification), the folder is also renamed/moved.

    For multi-CD items, renames all cd files in one operation.

    Returns (success, details_dict).
    """
    cd_number = item.get('cd_number')

    try:
        new_file, new_path = _apply_renamer_template(item, cd_number=cd_number)
    except ValueError as e:
        return False, {'error': str(e)}

    old_path = item['file_path']
    old_folder_path = os.path.dirname(old_path)
    old_folder_name = os.path.basename(old_folder_path)

    # Determine if folder also needs renaming
    exp = item.get('expected', {})
    exp_title = exp.get('db_title') or exp.get('title', '')
    exp_year = exp.get('year')
    folder_rename = False
    new_folder_path = None

    if exp_title:
        expected_folder = _apply_folder_template(exp_title, exp_year)
        if expected_folder != old_folder_name:
            folder_rename = True
            movies_dir = os.path.dirname(old_folder_path)
            new_folder_path = os.path.join(movies_dir, expected_folder)
            new_path = os.path.join(new_folder_path, new_file)

    if new_path == old_path:
        return False, {'error': 'Rename would produce identical filename'}

    if not os.path.isfile(old_path):
        return False, {'error': 'File not found: %s' % old_path}

    if os.path.exists(new_path):
        return False, {'error': 'Destination already exists: %s' % new_path}

    # Multi-CD: rename all cd files
    if item.get('multi_cd') and item.get('cd_files'):
        cd_renames = []
        for cd_info in item['cd_files']:
            cd_num = cd_info['cd_number']
            cd_filepath = cd_info['file_path']
            cd_item = dict(item)
            cd_item['file'] = cd_info['file']
            cd_item['file_path'] = cd_filepath
            cd_item['guessit_tokens'] = parse_guessit_tokens(cd_info['file'])
            try:
                cd_new_file, cd_new_path = _apply_renamer_template(
                    cd_item, cd_number=cd_num
                )
            except ValueError as e:
                return False, {'error': 'CD%d template failed: %s' % (cd_num, e)}
            if folder_rename and new_folder_path:
                cd_new_path = os.path.join(new_folder_path, cd_new_file)
            cd_renames.append((cd_filepath, cd_new_path))

        # Validate all cd files exist and destinations don't
        for cd_old, cd_new in cd_renames:
            if not os.path.isfile(cd_old):
                return False, {'error': 'CD file not found: %s' % cd_old}
            if os.path.exists(cd_new) and cd_new != cd_old:
                return False, {'error': 'CD destination already exists: %s' % cd_new}

        if folder_rename:
            try:
                os.makedirs(new_folder_path, exist_ok=True)
            except OSError as e:
                return False, {'error': 'Cannot create folder %s: %s' % (new_folder_path, e)}

        # Execute all renames
        completed = []
        for cd_old, cd_new in cd_renames:
            if cd_old == cd_new:
                continue
            try:
                if folder_rename:
                    shutil.move(cd_old, cd_new)
                else:
                    os.rename(cd_old, cd_new)
                completed.append((cd_old, cd_new))
            except (OSError, shutil.Error) as e:
                return False, {
                    'error': 'Rename failed for %s: %s' % (os.path.basename(cd_old), e),
                    'partial': completed,
                }

        details = {
            'old_path': cd_renames[0][0],
            'new_path': cd_renames[0][1],
            'cd_renames': [{'old_path': o, 'new_path': n} for o, n in cd_renames],
        }

        if folder_rename:
            details['folder_renamed'] = True
            details['old_folder'] = old_folder_path
            details['new_folder'] = new_folder_path
            # Clean up old folder if no video files remain
            try:
                remaining = os.listdir(old_folder_path)
                video_remaining = [f for f in remaining
                                  if os.path.splitext(f)[1].lower() in VIDEO_EXTENSIONS]
                if not video_remaining:
                    shutil.rmtree(old_folder_path, ignore_errors=True)
                    details['folder_cleaned'] = True
            except OSError:
                pass

        return True, details

    # Single file path (original behavior)
    if folder_rename:
        try:
            os.makedirs(new_folder_path, exist_ok=True)
        except OSError as e:
            return False, {'error': 'Cannot create folder %s: %s' % (new_folder_path, e)}

        try:
            shutil.move(old_path, new_path)
        except (OSError, shutil.Error) as e:
            return False, {'error': 'Move failed: %s' % e}

        folder_cleaned = False
        try:
            remaining = os.listdir(old_folder_path)
            video_remaining = [f for f in remaining
                              if os.path.splitext(f)[1].lower() in VIDEO_EXTENSIONS]
            if not video_remaining:
                shutil.rmtree(old_folder_path, ignore_errors=True)
                folder_cleaned = True
        except OSError:
            pass

        return True, {
            'old_path': old_path,
            'new_path': new_path,
            'folder_renamed': True,
            'old_folder': old_folder_path,
            'new_folder': new_folder_path,
            'folder_cleaned': folder_cleaned,
        }
    else:
        try:
            os.rename(old_path, new_path)
        except OSError as e:
            return False, {'error': 'Rename failed: %s' % e}

        return True, {
            'old_path': old_path,
            'new_path': new_path,
        }


def execute_fix_delete_wrong(item):
    """Execute a delete-wrong-file fix.

    Returns (success, details_dict).
    """
    old_path = item['file_path']

    if not os.path.isfile(old_path):
        return False, {'error': 'File not found: %s' % old_path}

    folder_path = os.path.dirname(old_path)

    try:
        os.remove(old_path)
    except OSError as e:
        return False, {'error': 'Delete failed: %s' % e}

    # Clean up empty folder
    folder_cleaned = False
    try:
        remaining = os.listdir(folder_path)
        # Only delete if empty or only contains small files (nfo, srt, jpg, etc)
        video_remaining = [f for f in remaining
                          if os.path.splitext(f)[1].lower() in VIDEO_EXTENSIONS]
        if not video_remaining:
            shutil.rmtree(folder_path, ignore_errors=True)
            folder_cleaned = True
    except OSError:
        pass

    return True, {
        'deleted_path': old_path,
        'folder_cleaned': folder_cleaned,
    }


def execute_fix_reassign_movie(item):
    """Execute a reassign-movie fix (move file to correct folder).

    Returns (success, details_dict).
    """
    preview = _preview_reassign_movie(item)
    if 'error' in preview:
        return False, preview

    old_path = item['file_path']
    changes = preview['changes']
    new_path = changes['filesystem']['new_path']
    new_folder = os.path.dirname(new_path)
    old_folder = os.path.dirname(old_path)

    if not os.path.isfile(old_path):
        return False, {'error': 'File not found: %s' % old_path}

    if os.path.exists(new_path):
        return False, {'error': 'Destination already exists: %s' % new_path}

    # Create destination folder
    try:
        os.makedirs(new_folder, exist_ok=True)
    except OSError as e:
        return False, {'error': 'Cannot create folder %s: %s' % (new_folder, e)}

    # Move the file
    try:
        shutil.move(old_path, new_path)
    except (OSError, shutil.Error) as e:
        return False, {'error': 'Move failed: %s' % e}

    # Clean up old folder if empty
    folder_cleaned = False
    try:
        remaining = os.listdir(old_folder)
        video_remaining = [f for f in remaining
                          if os.path.splitext(f)[1].lower() in VIDEO_EXTENSIONS]
        if not video_remaining:
            shutil.rmtree(old_folder, ignore_errors=True)
            folder_cleaned = True
    except OSError:
        pass

    return True, {
        'old_path': old_path,
        'new_path': new_path,
        'folder_cleaned': folder_cleaned,
    }


# ---------------------------------------------------------------------------
# CouchPotato plugin
# ---------------------------------------------------------------------------

class Audit(Plugin if _CP_AVAILABLE else object):
    """Library audit plugin — exposes scan/progress/results/stats via the CP API."""

    in_progress = False
    last_report = None
    _cancel = [False]   # mutable list so the scan loop can observe changes

    # Fix progress tracking
    fix_in_progress = False

    # Whisper verification progress tracking
    verify_in_progress = False

    def __init__(self):
        if not _CP_AVAILABLE:
            return

        addApiView('audit.scan', self.scanView, docs={
            'desc': 'Start a library audit scan',
            'params': {
                'full': {'desc': 'Run full scan with identification (hash + srrDB). Default 0.'},
                'force_full': {'desc': 'Force identification even for high-confidence flags. Default 0.'},
                'workers': {'desc': 'Number of parallel scan threads (1-16). Default 4.'},
                'scan_path': {'desc': 'Scan only this folder name (optional).'},
            },
        })

        addApiView('audit.cancel', self.cancelView, docs={
            'desc': 'Cancel a running audit scan',
        })

        addApiView('audit.progress', self.progressView, docs={
            'desc': 'Get progress of current audit scan',
            'return': {'type': 'object', 'example': """{
    'progress': False || {'total': 8526, 'scanned': 1200, 'flagged': 45}
}"""},
        })

        addApiView('audit.results', self.resultsView, docs={
            'desc': 'Get results of the last completed audit scan (paginated)',
            'params': {
                'offset': {'desc': 'Skip N items (default 0)'},
                'limit': {'desc': 'Return N items (default 50, max 500)'},
                'filter_check': {'desc': 'Filter by check type (comma-sep): resolution,title,runtime,tv_episode,edition,template,foreign_audio'},
                'filter_severity': {'desc': 'Filter by severity: HIGH, MEDIUM, LOW'},
                'filter_action': {'desc': 'Filter by recommended action'},
                'filter_fixed': {'desc': 'true, false, all (default false)'},
                'sort': {'desc': 'Sort by: folder, severity, flag_count, file_size (default folder)'},
                'sort_dir': {'desc': 'asc or desc (default asc)'},
            },
        })

        addApiView('audit.stats', self.statsView, docs={
            'desc': 'Get summary statistics of the last audit scan (no item data)',
        })

        addApiView('audit.fix.preview', self.fixPreviewView, docs={
            'desc': 'Preview what a fix action would change (dry run)',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
                'action': {'desc': 'Fix action: rename_template, rename_resolution, reassign_movie, delete_wrong, delete_duplicate, delete_foreign, verify_audio, rename_edition'},
            },
        })

        addApiView('audit.fix', self.fixView, docs={
            'desc': 'Execute a fix action on a flagged item',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
                'action': {'desc': 'Fix action: rename_template, rename_resolution, reassign_movie, delete_wrong, delete_duplicate, delete_foreign, verify_audio, rename_edition'},
                'confirm': {'desc': 'Must be 1 to confirm execution'},
            },
        })

        addApiView('audit.fix.batch', self.fixBatchView, docs={
            'desc': 'Execute a fix action on multiple items (async)',
            'params': {
                'action': {'desc': 'Fix action to apply'},
                'filter_check': {'desc': 'Only apply to items with this check type'},
                'filter_severity': {'desc': 'Only apply to items with this severity'},
                'confirm': {'desc': 'Must be 1 to confirm execution'},
                'dry_run': {'desc': '1 = preview only, 0 = execute (default 1)'},
            },
        })

        addApiView('audit.fix.progress', self.fixProgressView, docs={
            'desc': 'Get progress of a running batch fix operation',
        })

        addApiView('audit.identify', self.identifyView, docs={
            'desc': 'Run identification on a single flagged item',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
            },
        })

        addApiView('audit.reassign', self.reassignView, docs={
            'desc': 'Manually reassign a flagged item to a different movie by IMDB ID',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
                'imdb_id': {'desc': 'IMDB ID of the correct movie (e.g. tt1234567)'},
            },
        })

        addApiView('audit.ignore', self.ignoreView, docs={
            'desc': 'Ignore a flagged item so it no longer appears in results',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
                'reason': {'desc': 'Optional reason for ignoring'},
            },
        })

        addApiView('audit.unignore', self.unignoreView, docs={
            'desc': 'Un-ignore a previously ignored item',
            'params': {
                'fingerprint': {'desc': 'File fingerprint of the ignored item'},
            },
        })

        addApiView('audit.ignored', self.ignoredView, docs={
            'desc': 'List all currently ignored items',
        })

        addApiView('audit.verify', self.verifyView, docs={
            'desc': 'Run whisper language verification on a single item',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
            },
        })

        addApiView('audit.verify.batch', self.verifyBatchView, docs={
            'desc': 'Run whisper verification on all unknown_audio items',
        })

        addApiView('audit.verify.progress', self.verifyProgressView, docs={
            'desc': 'Get progress of a running batch verification',
        })

        addEvent('audit.run_scan', self._run_scan)
        addEvent('audit.run_batch_fix', self._run_batch_fix)
        addEvent('audit.run_verify_batch', self._run_verify_batch)

        # Load persisted results on startup
        self._load_results()

    def _get_results_path(self):
        """Get the path to the persisted audit results file."""
        data_dir = Env.get('data_dir')
        return os.path.join(data_dir, 'audit_results.json')

    def _get_actions_path(self):
        """Get the path to the append-only audit actions log."""
        data_dir = Env.get('data_dir')
        return os.path.join(data_dir, 'audit_actions.jsonl')

    # ------------------------------------------------------------------
    # File knowledge — DB-backed (file_knowledge documents in TinyDB)
    # ------------------------------------------------------------------

    def _get_knowledge(self, fingerprint):
        """Look up a file_knowledge doc by current_fingerprint.

        Returns the doc dict, or None if not found.
        """
        if not fingerprint:
            return None
        try:
            db = get_db()
            result = db.get('file_knowledge', fingerprint, with_doc=True)
            return result['doc']
        except RecordNotFound:
            return None

    def _get_or_create_knowledge(self, fingerprint, file_path,
                                 release_id=None, media_id=None):
        """Get existing or create new file_knowledge doc.

        On existing docs, updates last_seen and file_path if changed.
        On new docs, sets original_fingerprint = current_fingerprint.
        Returns the doc dict.
        """
        if not fingerprint:
            return None
        doc = self._get_knowledge(fingerprint)
        now = time.strftime('%Y-%m-%dT%H:%M:%S')
        if doc:
            changed = False
            if doc.get('file_path') != file_path:
                doc['file_path'] = file_path
                changed = True
            if doc.get('last_seen') != now:
                doc['last_seen'] = now
                changed = True
            # Backfill release/media IDs if they were missing
            if release_id and not doc.get('release_id'):
                doc['release_id'] = release_id
                changed = True
            if media_id and not doc.get('media_id'):
                doc['media_id'] = media_id
                changed = True
            if changed:
                self._update_knowledge(doc)
            return doc
        else:
            doc = {
                '_t': 'file_knowledge',
                'release_id': release_id,
                'media_id': media_id,
                'file_path': file_path,
                'original_fingerprint': fingerprint,
                'current_fingerprint': fingerprint,
                'crc32': None,
                'opensubtitles_hash': None,
                'srrdb': None,
                'opensubtitles': None,
                'identification': None,
                'ignored': None,
                'whisper': None,
                'modified': False,
                'modifications': [],
                'first_seen': now,
                'last_seen': now,
            }
            return get_db().insert(doc)

    def _update_knowledge(self, doc):
        """Update a file_knowledge doc in the DB."""
        try:
            get_db().update(doc)
        except RecordNotFound:
            log.error('Failed to update file_knowledge doc: %s not found',
                      (doc.get('_id'),))

    def _cache_identification(self, fingerprint, identification):
        """Write identification result (and extracted hashes) to knowledge doc.

        Called after identify_flagged_file() to cache the result so subsequent
        scans can skip expensive CRC32/hash computation and network lookups.

        Only caches "positive" identifications (not crc_not_found), so that
        unresolved files get retried on the next scan in case srrDB was updated.
        """
        if not fingerprint or not identification:
            return
        method = identification.get('method', '')
        if method == 'crc_not_found':
            # Don't cache negative results — retry on next scan
            # But DO cache the CRC32 hash to avoid recomputing it
            doc = self._get_knowledge(fingerprint)
            if doc and identification.get('crc32') and not doc.get('crc32'):
                doc['crc32'] = identification['crc32']
                self._update_knowledge(doc)
            return
        doc = self._get_knowledge(fingerprint)
        if not doc:
            return
        changed = False
        if doc.get('identification') != identification:
            doc['identification'] = identification
            changed = True
        # Extract and cache hashes from the identification result
        crc = identification.get('crc32')
        if crc and not doc.get('crc32'):
            doc['crc32'] = crc
            changed = True
        osh = identification.get('moviehash')
        if osh and not doc.get('opensubtitles_hash'):
            doc['opensubtitles_hash'] = osh
            changed = True
        if changed:
            self._update_knowledge(doc)

    def _is_ignored(self, fingerprint):
        """Check whether a file fingerprint is marked as ignored."""
        doc = self._get_knowledge(fingerprint)
        return doc is not None and doc.get('ignored') is not None

    def _get_knowledge_stats(self):
        """Return summary stats about file_knowledge docs in the DB."""
        try:
            db = get_db()
            all_docs = db._docs_for_type('file_knowledge')
            total = len(all_docs)
            n_ignored = sum(1 for d in all_docs.values()
                           if d.get('ignored') is not None)
            n_whisper = sum(1 for d in all_docs.values()
                           if d.get('whisper') is not None)
            n_identified = sum(1 for d in all_docs.values()
                               if d.get('identification') is not None)
            n_modified = sum(1 for d in all_docs.values()
                             if d.get('modified'))
            return {
                'total': total,
                'ignored': n_ignored,
                'whisper_verified': n_whisper,
                'identified': n_identified,
                'modified': n_modified,
            }
        except Exception:
            return {'total': 0, 'ignored': 0, 'whisper_verified': 0,
                    'identified': 0, 'modified': 0}

    def _upsert_scan_knowledge(self, seen_fingerprints, release_by_filepath,
                               flagged_items):
        """Create/update file_knowledge DB docs for all scanned files.

        NOTE: No longer called during normal scan flow — knowledge records are
        now created incrementally via the knowledge_callback passed through the
        scan pipeline.  This method is retained as a utility for manual
        re-upserts or debugging.

        Args:
            seen_fingerprints: dict mapping fingerprint → file_path (all files)
            release_by_filepath: dict mapping file_path → {release_id, media_id}
            flagged_items: list of flagged item dicts (for enriching knowledge)
        """
        created = 0
        updated = 0
        for fingerprint, file_path in seen_fingerprints.items():
            rel_info = release_by_filepath.get(file_path, {})
            release_id = rel_info.get('release_id')
            media_id = rel_info.get('media_id')
            doc = self._get_knowledge(fingerprint)
            if doc:
                updated += 1
            else:
                created += 1
            self._get_or_create_knowledge(
                fingerprint, file_path,
                release_id=release_id,
                media_id=media_id,
            )
        log.info('File knowledge upsert: %s created, %s updated (%s total)',
                 (created, updated, created + updated))

    def _prune_file_knowledge(self, seen_fingerprints):
        """Remove file_knowledge DB docs whose fingerprints were not seen
        during the last complete scan.  This cleans up entries for files
        that have been deleted, re-encoded, or replaced."""
        if not seen_fingerprints:
            return
        db = get_db()
        all_docs = db._docs_for_type('file_knowledge')
        if not all_docs:
            return
        seen_set = set(seen_fingerprints)
        stale = []
        for _id, doc in all_docs.items():
            fp = doc.get('current_fingerprint')
            if fp and fp not in seen_set:
                stale.append(doc)
        if not stale:
            log.info('File knowledge cleanup: no stale entries found')
            return
        for doc in stale:
            try:
                db.delete(doc)
            except RecordNotFound:
                pass
        log.info('File knowledge cleanup: removed %s stale entries',
                 (len(stale),))

    def _ensure_original_hashes(self, knowledge_doc, file_path):
        """Ensure CRC32 and OpenSubtitles hash are stored before file modification.

        Computes any missing hashes from the CURRENT file (which must still be
        the original if modified==False).  Must be called BEFORE any file
        modification.

        Returns the updated doc.
        """
        if not knowledge_doc or not file_path:
            return knowledge_doc
        changed = False
        if knowledge_doc.get('crc32') is None:
            log.info('Computing CRC32 for %s (pre-modification guard)...',
                     (os.path.basename(file_path),))
            crc_hex = compute_crc32(file_path)
            knowledge_doc['crc32'] = crc_hex
            changed = True
        if knowledge_doc.get('opensubtitles_hash') is None:
            log.info('Computing OpenSubtitles hash for %s (pre-modification guard)...',
                     (os.path.basename(file_path),))
            os_hash = compute_opensubtitles_hash(file_path)
            knowledge_doc['opensubtitles_hash'] = os_hash
            changed = True
        if changed:
            self._update_knowledge(knowledge_doc)
        return knowledge_doc

    def _post_modification_update(self, knowledge_doc, file_path,
                                  mod_type, detail=''):
        """Update file_knowledge after modifying a file.

        Computes a new fingerprint for the modified file and updates
        current_fingerprint while preserving original_fingerprint and
        the original hashes (CRC32, OpenSubtitles hash).

        Args:
            knowledge_doc: The file_knowledge doc (must have original hashes)
            file_path: Path to the modified file
            mod_type: Type of modification (e.g. 'relabel_audio')
            detail: Human-readable detail (e.g. 'Track 0: de -> en')
        """
        new_fp = compute_file_fingerprint(file_path)
        knowledge_doc['current_fingerprint'] = new_fp
        knowledge_doc['modified'] = True
        # Clear cached identification — file content changed, so hash-based
        # identification (CRC32/OpenSubtitles) may no longer be valid.
        knowledge_doc['identification'] = None
        knowledge_doc.setdefault('modifications', []).append({
            'type': mod_type,
            'date': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'detail': detail,
        })
        self._update_knowledge(knowledge_doc)
        log.info('Updated fingerprint after %s: %s -> %s',
                 (mod_type, knowledge_doc.get('original_fingerprint', '?')[:20],
                  new_fp[:20] if new_fp else '?'))

    def _save_results(self):
        """Persist last_report to disk."""
        if not self.last_report:
            return
        results_path = self._get_results_path()
        try:
            tmp_path = results_path + '.tmp'
            with open(tmp_path, 'w') as f:
                json.dump(self.last_report, f)
            os.replace(tmp_path, results_path)
            log.info('Audit results saved to %s', (results_path,))
        except Exception as e:
            log.error('Failed to save audit results: %s', (e,))

    def _append_action(self, item, action, details, success=True):
        """Append a single fix action to the audit actions log.

        Uses append-only writes (O(1) regardless of total report size)
        instead of re-serializing the entire audit_results.json on each fix.
        Actions are reconciled into the main report on startup and at the
        end of batch operations.
        """
        actions_path = self._get_actions_path()
        record = {
            'item_id': item.get('item_id', ''),
            'folder': item.get('folder', ''),
            'action': action,
            'success': success,
            'timestamp': time.time(),
            'details': details,
        }
        try:
            with open(actions_path, 'a') as f:
                f.write(json.dumps(record) + '\n')
        except Exception as e:
            log.error('Failed to append audit action: %s', (e,))

    def _reconcile_actions(self):
        """Replay the append-only actions log into the in-memory report.

        Called on startup (after _load_results) and at the end of batch
        operations.  Applies any un-reconciled fix actions to the in-memory
        last_report, saves the full report once, then truncates the actions
        log file.
        """
        actions_path = self._get_actions_path()
        if not os.path.isfile(actions_path):
            return
        if not self.last_report or 'flagged' not in self.last_report:
            return

        # Build lookup by item_id for O(1) matching
        items_by_id = {}
        for item in self.last_report['flagged']:
            iid = item.get('item_id', '')
            if iid:
                items_by_id[iid] = item

        applied = 0
        try:
            with open(actions_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, ValueError):
                        continue
                    if not record.get('success', False):
                        continue  # Skip failed actions during reconciliation
                    item_id = record.get('item_id', '')
                    if item_id in items_by_id:
                        item = items_by_id[item_id]
                        if not item.get('fixed'):
                            item['fixed'] = {
                                'action': record.get('action', ''),
                                'timestamp': record.get('timestamp', 0),
                                'details': record.get('details', {}),
                            }
                            new_path = record.get('details', {}).get('new_path')
                            if new_path:
                                item['file_path'] = new_path
                            applied += 1
        except Exception as e:
            log.error('Failed to read audit actions log: %s', (e,))
            return

        if applied:
            log.info('Reconciled %s actions from action log', (applied,))
            self._save_results()

        # Truncate the actions file
        self._truncate_actions()

    def _truncate_actions(self):
        """Truncate the audit actions log file."""
        actions_path = self._get_actions_path()
        try:
            open(actions_path, 'w').close()
        except OSError:
            pass

    def _load_results(self):
        """Load persisted results from disk on startup."""
        results_path = self._get_results_path()
        if not os.path.isfile(results_path):
            return
        try:
            with open(results_path, 'r') as f:
                self.last_report = json.load(f)
            count = self.last_report.get('total_flagged', 0)
            log.info('Loaded %s flagged items from %s', (count, results_path))
        except Exception as e:
            log.error('Failed to load audit results: %s', (e,))
            self.last_report = None

        # Reconcile any pending actions from the append-only log
        self._reconcile_actions()

        # Migrate: separate delete_duplicate from delete_wrong for existing results
        self._migrate_duplicate_actions()

        # Log file knowledge stats from DB
        stats = self._get_knowledge_stats()
        if stats['total']:
            log.info('File knowledge: %s entries (%s ignored, %s whisper, %s identified)',
                     (stats['total'], stats['ignored'], stats['whisper_verified'],
                      stats['identified']))

    def _migrate_duplicate_actions(self):
        """One-time migration: change recommended_action from delete_wrong to
        delete_duplicate for items flagged as duplicates (not TV episodes).

        Prior to this change, both duplicates and TV episodes used delete_wrong.
        """
        if not self.last_report:
            return
        migrated = 0
        for item in self.last_report.get('flagged', []):
            if item.get('recommended_action') != 'delete_wrong':
                continue
            checks = {f['check'] for f in item.get('flags', [])}
            if 'duplicate' in checks and 'tv_episode' not in checks:
                item['recommended_action'] = 'delete_duplicate'
                migrated += 1
        if migrated:
            log.info('Migrated %s items from delete_wrong to delete_duplicate', (migrated,))
            self._save_results()

    def _get_movies_dir(self):
        """Get the first library directory from manage settings."""
        dirs = Env.setting('library', section='manage', default=[])
        if dirs:
            return dirs[0]
        return None

    def _get_db_path(self):
        """Get the path to CP's db.json."""
        data_dir = Env.get('data_dir')
        return os.path.join(data_dir, 'database', 'db.json')

    def _on_progress(self, scanned, total, flagged_count):
        """Progress callback called by scan_library after each folder."""
        self.in_progress = {
            'total': total,
            'scanned': scanned,
            'flagged': flagged_count,
        }

    def _run_scan(self, full=False, force_full=False, scan_path=None,
                  workers=DEFAULT_WORKERS):
        """Run the audit scan (called in background thread)."""
        self._cancel[0] = False

        movies_dir = self._get_movies_dir()
        if not movies_dir:
            log.error('No library directory configured in manage settings')
            self.in_progress = False
            return

        db_path = self._get_db_path()
        if not os.path.isfile(db_path):
            log.error('Database not found at %s', (db_path,))
            self.in_progress = False
            return

        self.in_progress = {'total': 0, 'scanned': 0, 'flagged': 0}

        try:
            report = scan_library(
                movies_dir=movies_dir,
                db_path=db_path,
                scan_path=scan_path,
                full=full,
                force_full=force_full,
                workers=workers,
                progress_callback=self._on_progress,
                cancel_flag=self._cancel,
                knowledge_callback=self._get_or_create_knowledge,
            )
            self.last_report = report
            self.last_report['completed_at'] = time.time()
            self.last_report['scan_timestamp'] = time.strftime(
                '%Y-%m-%dT%H:%M:%S'
            )
            # Extract fingerprints and release mapping before saving
            # (dict is not JSON-serializable and mapping not needed on disk)
            seen_fps = self.last_report.pop('seen_fingerprints', {})
            self.last_report.pop('release_by_filepath', None)
            # Persist to disk
            self._save_results()

            # Cache new identification results in file_knowledge DB docs.
            # Only writes for items that were freshly identified (not cached).
            flagged = self.last_report.get('flagged', [])
            cached_count = 0
            for item in flagged:
                if item.get('identification_cached'):
                    cached_count += 1
                    continue
                ident = item.get('identification')
                fp = item.get('file_fingerprint')
                if ident and fp and ident.get('method') not in (
                    'skipped', 'tv_episode_detected',
                ):
                    self._cache_identification(fp, ident)
            new_idents = sum(
                1 for i in flagged
                if i.get('identification')
                and not i.get('identification_cached')
                and i['identification'].get('method') not in (
                    'skipped', 'tv_episode_detected',
                )
            )
            if cached_count or new_idents:
                log.info('Identification cache: %s cached hits, %s new results written',
                         (cached_count, new_idents))

            # Prune stale file_knowledge entries after a complete full-library
            # scan (not cancelled, not a single-folder scan).
            if not report.get('cancelled') and not scan_path:
                self._prune_file_knowledge(seen_fps)
        except Exception as e:
            log.error('Audit scan failed: %s', (e,))
        finally:
            self.in_progress = False
            self._cancel[0] = False

    def _filter_and_sort(self, filter_check=None, filter_severity=None,
                         filter_action=None, filter_fixed='false',
                         sort='folder', sort_dir='asc'):
        """Filter and sort flagged items from last_report.

        Returns a list of flagged items matching the filter criteria, sorted.
        """
        if not self.last_report:
            return []

        items = self.last_report.get('flagged', [])

        # Filter out ignored items (by fingerprint in file knowledge DB)
        items = [i for i in items
                 if not self._is_ignored(i.get('file_fingerprint'))]

        # Filter by fixed status
        if filter_fixed == 'true':
            items = [i for i in items if i.get('fixed')]
        elif filter_fixed == 'false':
            items = [i for i in items if not i.get('fixed')]
        # 'all' returns everything

        # Filter by check type (comma-separated)
        if filter_check:
            check_set = {c.strip() for c in filter_check.split(',')}
            items = [
                i for i in items
                if check_set & {f['check'] for f in i.get('flags', [])}
            ]

        # Filter by severity (max severity semantics — an item's severity
        # is the highest severity across all its flags, so filtering by LOW
        # only returns items where every flag is LOW)
        if filter_severity:
            sev_set = {s.strip().upper() for s in filter_severity.split(',')}
            sev_order = {'LOW': 0, 'MEDIUM': 1, 'HIGH': 2}
            target_levels = {sev_order.get(s, 1) for s in sev_set}
            items = [
                i for i in items
                if max((sev_order.get(f['severity'], 1)
                        for f in i.get('flags', [])), default=1)
                   in target_levels
            ]

        # Filter by recommended action
        if filter_action:
            action_set = {a.strip() for a in filter_action.split(',')}
            items = [
                i for i in items
                if i.get('recommended_action') in action_set
            ]

        # Sort
        severity_order = {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}

        if sort == 'severity':
            items.sort(
                key=lambda r: min(
                    severity_order.get(f['severity'], 99)
                    for f in r.get('flags', [{'severity': 'LOW'}])
                ),
                reverse=(sort_dir == 'desc'),
            )
        elif sort == 'flag_count':
            items.sort(
                key=lambda r: r.get('flag_count', 0),
                reverse=(sort_dir == 'desc'),
            )
        elif sort == 'file_size':
            items.sort(
                key=lambda r: r.get('file_size_bytes', 0),
                reverse=(sort_dir == 'desc'),
            )
        else:
            # Default: sort by folder name
            items.sort(
                key=lambda r: r.get('folder', '').lower(),
                reverse=(sort_dir == 'desc'),
            )

        return items

    def scanView(self, full='0', force_full='0', workers='4',
                 scan_path=None, **kwargs):
        """API handler: start an audit scan."""
        if self.in_progress:
            return {
                'success': False,
                'message': 'Scan already in progress',
                'progress': self.in_progress,
            }

        do_full = str(full) == '1'
        do_force = str(force_full) == '1'

        try:
            num_workers = int(workers)
        except (ValueError, TypeError):
            num_workers = DEFAULT_WORKERS
        num_workers = max(1, min(num_workers, MAX_WORKERS))

        self.in_progress = {'total': 0, 'scanned': 0, 'flagged': 0}

        fireEventAsync(
            'audit.run_scan',
            full=do_full,
            force_full=do_force,
            scan_path=scan_path if scan_path else None,
            workers=num_workers,
        )

        return {
            'success': True,
            'message': 'Audit scan started (workers=%s, full=%s, force_full=%s)' % (
                num_workers, do_full, do_force),
            'progress': self.in_progress,
        }

    def cancelView(self, **kwargs):
        """API handler: cancel a running audit scan."""
        if not self.in_progress:
            return {
                'success': False,
                'message': 'No scan is running',
            }

        self._cancel[0] = True
        return {
            'success': True,
            'message': 'Cancel signal sent — scan will stop after current folder',
            'progress': self.in_progress,
        }

    def progressView(self, **kwargs):
        """API handler: return current scan progress."""
        return {
            'progress': self.in_progress,
        }

    def resultsView(self, offset='0', limit='50', filter_check=None,
                    filter_severity=None, filter_action=None,
                    filter_fixed='false', sort='folder', sort_dir='asc',
                    **kwargs):
        """API handler: return last completed scan report (paginated).

        Supports filtering by check type, severity, action, and fixed status.
        Supports sorting by folder, severity, flag_count, file_size.
        """
        if not self.last_report:
            return {'results': None}

        # Parse pagination params
        try:
            offset_int = max(0, int(offset))
        except (ValueError, TypeError):
            offset_int = 0
        try:
            limit_int = max(1, min(500, int(limit)))
        except (ValueError, TypeError):
            limit_int = 50

        # Filter and sort
        filtered = self._filter_and_sort(
            filter_check=filter_check,
            filter_severity=filter_severity,
            filter_action=filter_action,
            filter_fixed=filter_fixed or 'false',
            sort=sort or 'folder',
            sort_dir=sort_dir or 'asc',
        )

        total_filtered = len(filtered)
        page = filtered[offset_int:offset_int + limit_int]

        return {
            'results': {
                'total_scanned': self.last_report.get('total_scanned', 0),
                'total_flagged': self.last_report.get('total_flagged', 0),
                'total_errors': self.last_report.get('total_errors', 0),
                'scan_timestamp': self.last_report.get('scan_timestamp', ''),
                'total_filtered': total_filtered,
                'offset': offset_int,
                'limit': limit_int,
                'items': page,
            },
        }

    def statsView(self, **kwargs):
        """API handler: return summary statistics (no item data)."""
        if not self.last_report:
            return {'stats': None}

        all_flagged = self.last_report.get('flagged', [])

        # Separate ignored items
        total_ignored = 0
        flagged = []
        for item in all_flagged:
            if self._is_ignored(item.get('file_fingerprint')):
                total_ignored += 1
            else:
                flagged.append(item)

        # Count by check type
        check_counts = {}
        for item in flagged:
            for flag in item.get('flags', []):
                check = flag.get('check', 'unknown')
                check_counts[check] = check_counts.get(check, 0) + 1

        # Count by severity
        severity_counts = {}
        for item in flagged:
            for flag in item.get('flags', []):
                sev = flag.get('severity', 'UNKNOWN')
                severity_counts[sev] = severity_counts.get(sev, 0) + 1

        # Count by recommended action
        action_counts = {}
        for item in flagged:
            action = item.get('recommended_action', 'none')
            action_counts[action] = action_counts.get(action, 0) + 1

        # Count fixed items
        total_fixed = sum(1 for i in flagged if i.get('fixed'))

        # Identification stats
        ident_identified = 0
        ident_unidentified = 0
        ident_skipped = 0
        for item in flagged:
            ident = item.get('identification')
            if not ident:
                continue
            method = ident.get('method', '')
            if method in ('container_title', 'srrdb_crc', 'opensubtitles_hash', 'manual'):
                ident_identified += 1
            elif method == 'crc_not_found':
                ident_unidentified += 1
            elif method in ('skipped', 'tv_episode_detected'):
                ident_skipped += 1

        total_scanned = self.last_report.get('total_scanned', 0)
        total_flagged = self.last_report.get('total_flagged', 0)

        return {
            'stats': {
                'scan_timestamp': self.last_report.get('scan_timestamp', ''),
                'total_scanned': total_scanned,
                'total_flagged': total_flagged,
                'total_clean': total_scanned - total_flagged,
                'total_errors': self.last_report.get('total_errors', 0),
                'total_fixed': total_fixed,
                'total_ignored': total_ignored,
                'checks': check_counts,
                'severity': severity_counts,
                'actions': action_counts,
                'identification': {
                    'identified': ident_identified,
                    'unidentified': ident_unidentified,
                     'skipped': ident_skipped,
                },
                'file_knowledge': self._get_knowledge_stats(),
            },
        }

    def ignoreView(self, item_id=None, reason='', **kwargs):
        """API handler: ignore a flagged item by its file fingerprint."""
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        fingerprint = item.get('file_fingerprint')
        if not fingerprint:
            # Compute fingerprint on-the-fly for items from older scans
            filepath = item.get('file_path', '')
            if filepath and os.path.isfile(filepath):
                fingerprint = compute_file_fingerprint(filepath)
                item['file_fingerprint'] = fingerprint
            if not fingerprint:
                return {'success': False, 'error': 'Could not compute fingerprint'}

        doc = self._get_or_create_knowledge(fingerprint, item.get('file_path', ''))
        if not doc:
            return {'success': False, 'error': 'Failed to create knowledge record'}
        doc['ignored'] = {
            'reason': reason or '',
            'ignored_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
            'item_id': item_id,
            'title': '%s / %s' % (item.get('folder', ''), item.get('file', '')),
        }
        self._update_knowledge(doc)

        log.info('Ignored audit item %s (fingerprint %s): %s',
                 (item_id, fingerprint, item.get('folder', '')))

        stats = self._get_knowledge_stats()
        return {
            'success': True,
            'fingerprint': fingerprint,
            'total_ignored': stats['ignored'],
        }

    def unignoreView(self, fingerprint=None, **kwargs):
        """API handler: un-ignore a previously ignored item."""
        if not fingerprint:
            return {'success': False, 'error': 'fingerprint is required'}

        doc = self._get_knowledge(fingerprint)
        if not doc or doc.get('ignored') is None:
            return {'success': False, 'error': 'Fingerprint not found in ignored list'}

        removed = doc.get('ignored', {})
        doc['ignored'] = None
        self._update_knowledge(doc)

        log.info('Un-ignored audit item (fingerprint %s): %s',
                 (fingerprint, removed.get('title', '')))

        stats = self._get_knowledge_stats()
        return {
            'success': True,
            'total_ignored': stats['ignored'],
        }

    def ignoredView(self, **kwargs):
        """API handler: list all currently ignored items."""
        items = []
        try:
            db = get_db()
            all_docs = db._docs_for_type('file_knowledge')
            for _id, doc in all_docs.items():
                info = doc.get('ignored')
                if not info:
                    continue
                items.append({
                    'fingerprint': doc.get('current_fingerprint', ''),
                    'title': info.get('title', ''),
                    'reason': info.get('reason', ''),
                    'ignored_at': info.get('ignored_at', ''),
                    'item_id': info.get('item_id', ''),
                })
        except Exception as e:
            log.error('Failed to list ignored items: %s', (e,))
        items.sort(key=lambda x: x.get('ignored_at', ''), reverse=True)
        return {
            'ignored': items,
            'total': len(items),
        }

    # -------------------------------------------------------------------
    # Whisper audio language verification
    # -------------------------------------------------------------------

    def _apply_whisper_result(self, item, result):
        """Apply whisper verification to an item: update flags and knowledge.

        Examines per-track results.  If any track is in an accepted language,
        the foreign/unknown flags are cleared.  Otherwise, a foreign_audio
        flag is added with per-track detail.

        Stores the result in the file_knowledge DB doc for future reference.
        """
        fingerprint = item.get('file_fingerprint')
        if fingerprint:
            doc = self._get_or_create_knowledge(fingerprint,
                                                item.get('file_path', ''))
            if doc:
                doc['whisper'] = {
                    'language': result.get('language'),
                    'confidence': result.get('confidence', 0.0),
                    'verified_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
                    'tracks': result.get('tracks', []),
                }
                self._update_knowledge(doc)

        tracks = result.get('tracks', [])
        if not tracks:
            return  # no results, leave flags as-is

        # Check if ANY track detected an accepted language
        accepted = {'en'}  # TODO: read from config
        any_accepted = False
        track_details = []
        for t in tracks:
            lang = t.get('language')
            if not lang:
                continue
            normalized = normalize_language(lang)
            if normalized in accepted:
                any_accepted = True
            conf = t.get('confidence', 0)
            tagged = t.get('tagged_language', '?')
            track_details.append(
                'Track %d (%s): %s %.0f%%' % (
                    t.get('track_index', 0), tagged, lang, conf * 100))

        # Update flags: remove unknown_audio and existing foreign_audio,
        # then re-add foreign_audio only if no track is accepted
        flags = item.get('flags', [])
        new_flags = [f for f in flags
                     if f['check'] not in ('unknown_audio', 'foreign_audio')]

        if any_accepted:
            # At least one track is accepted — clear the flag
            pass
        else:
            detail = 'Whisper: ' + '; '.join(track_details) if track_details \
                else 'Whisper: no language detected'
            new_flags.append({
                'check': 'foreign_audio',
                'severity': 'LOW',
                'detail': detail,
            })

        item['flags'] = new_flags
        item['flag_count'] = len(new_flags)

        # Recompute recommended action
        item['recommended_action'] = compute_recommended_action(
            new_flags, item.get('identification'), item.get('expected')
        )

    def verifyView(self, item_id=None, **kwargs):
        """API handler: run whisper verification on a single item."""
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        file_path = item.get('file_path', '')
        if not file_path or not os.path.isfile(file_path):
            return {'success': False, 'error': 'File not accessible: %s' % file_path}

        # Check if already verified via file_knowledge DB
        fingerprint = item.get('file_fingerprint')
        if fingerprint:
            doc = self._get_knowledge(fingerprint)
            cached = doc.get('whisper') if doc else None
            if cached and cached.get('language'):
                return {
                    'success': True,
                    'item_id': item_id,
                    'cached': True,
                    'whisper': cached,
                    'recommended_action': item.get('recommended_action'),
                }

        # Run whisper
        audio_tracks = item.get('actual', {}).get('audio_tracks', [])
        result = whisper_verify_audio(file_path, audio_tracks=audio_tracks)

        if 'error' in result:
            return {
                'success': False,
                'error': result['error'],
                'item_id': item_id,
            }

        # Apply result (update flags, knowledge table)
        self._apply_whisper_result(item, result)
        self._save_results()

        return {
            'success': True,
            'item_id': item_id,
            'cached': False,
            'whisper': {
                'language': result['language'],
                'confidence': result['confidence'],
                'tracks': result.get('tracks', []),
            },
            'recommended_action': item.get('recommended_action'),
        }

    def verifyBatchView(self, **kwargs):
        """API handler: run whisper verification on all unknown_audio items."""
        if self.verify_in_progress and self.verify_in_progress.get('active'):
            return {
                'success': False,
                'error': 'A batch verification is already in progress',
                'verify_progress': self.verify_in_progress,
            }

        if not self.last_report:
            return {'success': False, 'error': 'No scan results available'}

        # Find all unfixed items with unknown_audio flags
        items = [
            i for i in self.last_report.get('flagged', [])
            if not i.get('fixed')
            and any(f['check'] == 'unknown_audio' for f in i.get('flags', []))
            and not self._is_ignored(i.get('file_fingerprint'))
        ]

        if not items:
            return {'success': False, 'error': 'No unknown_audio items to verify'}

        # Start async verification
        fireEventAsync(
            'audit.run_verify_batch',
            items=items,
        )

        return {
            'success': True,
            'total': len(items),
            'message': 'Batch verification started — check audit.verify.progress for status',
        }

    def _run_verify_batch(self, items):
        """Run whisper verification on a batch of items (background thread)."""
        total = len(items)
        completed = 0
        failed = 0

        self.verify_in_progress = {
            'active': True,
            'total': total,
            'completed': 0,
            'failed': 0,
            'current_item': '',
        }

        for item in items:
            file_path = item.get('file_path', '')
            self.verify_in_progress['current_item'] = item.get('folder', '')

            if not file_path or not os.path.isfile(file_path):
                failed += 1
                log.error('Verify batch: file not accessible: %s', (file_path,))
                self.verify_in_progress['completed'] = completed + failed
                self.verify_in_progress['failed'] = failed
                continue

            # Check cache
            fingerprint = item.get('file_fingerprint')
            cached = None
            if fingerprint:
                doc = self._get_knowledge(fingerprint)
                cached = doc.get('whisper') if doc else None

            if cached and cached.get('language'):
                # Already verified — apply cached result
                self._apply_whisper_result(item, cached)
                completed += 1
            else:
                audio_tracks = item.get('actual', {}).get('audio_tracks', [])
                result = whisper_verify_audio(file_path,
                                              audio_tracks=audio_tracks)
                if 'error' in result:
                    failed += 1
                    log.error('Verify batch failed for %s: %s',
                              (item.get('folder', ''), result['error']))
                else:
                    self._apply_whisper_result(item, result)
                    completed += 1

            self.verify_in_progress['completed'] = completed + failed
            self.verify_in_progress['failed'] = failed

        # Save results
        self._save_results()

        self.verify_in_progress = {
            'active': False,
            'total': total,
            'completed': completed,
            'failed': failed,
            'current_item': '',
        }

        log.info('Batch verification complete: %s/%s verified, %s failed',
                 (completed, total, failed))

    def verifyProgressView(self, **kwargs):
        """API handler: return current batch verification progress."""
        return {
            'verify_progress': self.verify_in_progress if self.verify_in_progress else {
                'active': False,
            },
        }

    def _find_item(self, item_id):
        """Find a flagged item by its item_id.

        Returns the item dict or None.
        """
        if not self.last_report:
            return None
        for item in self.last_report.get('flagged', []):
            if item.get('item_id') == item_id:
                return item
        return None

    def _mark_fixed(self, item, action, details):
        """Mark a flagged item as fixed and persist via append-only log."""
        item['fixed'] = {
            'action': action,
            'timestamp': time.time(),
            'details': details,
        }
        # Update the file_path if it changed (for subsequent operations)
        if 'new_path' in details:
            item['file_path'] = details['new_path']
            item['file'] = os.path.basename(details['new_path'])
        # Update folder name if it changed (e.g. year correction)
        if details.get('folder_renamed') and details.get('new_folder'):
            item['folder'] = os.path.basename(details['new_folder'])
        # Append to actions log (O(1)) instead of rewriting full report
        self._append_action(item, action, details)

    def _recalculate_folder_duplicates(self, fixed_item):
        """Recalculate duplicate flags for siblings after a delete/reassign.

        When a file is deleted or moved out of a folder, its sibling items
        may have stale 'duplicate' flags referencing the removed file.
        This method:
        1. Finds all unfixed siblings in the same folder
        2. Re-runs detect_duplicates() on them (no disk I/O — uses stored metadata)
        3. Removes stale duplicate flags, adds new ones if still warranted
        4. Recomputes recommended_action for affected siblings
        5. Drops siblings that have no remaining flags (now clean)
        6. Updates variant_files/variant_count to reflect the removed file
        """
        if not self.last_report:
            return

        folder = fixed_item.get('folder', '')
        fixed_file = fixed_item.get('file', '')
        flagged = self.last_report.get('flagged', [])

        # Find unfixed siblings in the same folder (excluding the just-fixed item)
        siblings = [it for it in flagged
                    if it.get('folder') == folder
                    and not it.get('fixed')
                    and it is not fixed_item]

        if not siblings:
            return

        # Check if any sibling has a duplicate flag — if not, nothing to do
        has_any_dupe = any(
            any(f['check'] == 'duplicate' for f in it.get('flags', []))
            for it in siblings
        )
        if not has_any_dupe:
            return

        # Re-run duplicate detection on surviving siblings
        new_dupe_pairs = detect_duplicates(siblings)
        # Build set of indices that are still duplicates
        still_duped = set()
        for idx_a, idx_b in new_dupe_pairs:
            still_duped.add(idx_a)
            still_duped.add(idx_b)

        # Build partner map: index → partner filename
        partner_map = {}
        for idx_a, idx_b in new_dupe_pairs:
            # Each index gets mapped to its first partner
            if idx_a not in partner_map:
                partner_map[idx_a] = siblings[idx_b].get('file', '')
            if idx_b not in partner_map:
                partner_map[idx_b] = siblings[idx_a].get('file', '')

        # Update each sibling
        items_to_remove = []
        for i, sib in enumerate(siblings):
            old_flags = sib.get('flags', [])
            # Remove all existing duplicate flags
            new_flags = [f for f in old_flags if f['check'] != 'duplicate']

            # Re-add duplicate flag if still warranted
            if i in still_duped:
                partner_file = partner_map.get(i, '?')
                # Find the partner item for keep/delete recommendation
                partner_item = None
                for j, s in enumerate(siblings):
                    if s.get('file') == partner_file:
                        partner_item = s
                        break
                if partner_item:
                    keep, delete = pick_best_duplicate(sib, partner_item)
                    is_keeper = (sib is keep)
                    if is_keeper:
                        detail = 'Possible duplicate of %s (recommend keeping this copy)' % partner_file
                    else:
                        detail = 'Possible duplicate of %s (recommend deleting this copy)' % partner_file
                    new_flags.append({
                        'check': 'duplicate',
                        'severity': 'MEDIUM',
                        'detail': detail,
                        'duplicate_action': 'keep' if is_keeper else 'delete',
                        'duplicate_of': partner_file,
                    })
                else:
                    new_flags.append({
                        'check': 'duplicate',
                        'severity': 'MEDIUM',
                        'detail': 'Possible duplicate of %s' % partner_file,
                    })

            sib['flags'] = new_flags
            sib['flag_count'] = len(new_flags)

            if not new_flags:
                # No flags left — this item is now clean
                items_to_remove.append(sib)
            else:
                # Recompute recommended_action with updated flags
                sib['recommended_action'] = compute_recommended_action(
                    new_flags, sib.get('identification'), sib.get('expected')
                )

            # Update variant_files to remove the deleted/moved file
            vf = sib.get('variant_files', [])
            if fixed_file in vf:
                vf = [f for f in vf if f != fixed_file]
                sib['variant_files'] = vf
                sib['variant_count'] = len(vf)

        # Remove now-clean items from the flagged list
        if items_to_remove:
            for item in items_to_remove:
                flagged.remove(item)
            self.last_report['total_flagged'] = len(flagged)
            log.info('Duplicate recalculation: removed %s now-clean items from %s',
                     (len(items_to_remove), folder))

    def _apply_reset_status(self, item, reset_status):
        """Apply a status change to the original movie in the CP database.

        Args:
            item: The audit item dict (has imdb_id, expected title/year).
            reset_status: 'wanted' to set movie to active (wanted),
                          'done' to set movie to done, or
                          'remove' to fully delete the movie from the database.

        Returns a dict describing what happened.
        """
        if not _CP_AVAILABLE:
            return {'applied': False, 'reason': 'Not running inside CouchPotato'}

        imdb_id = item.get('imdb_id', '')
        movie_title = item.get('expected', {}).get('title', 'Unknown')

        if not imdb_id:
            log.warning('Cannot apply reset_status — no IMDB ID for %s', (movie_title,))
            return {'applied': False, 'reason': 'No IMDB ID for movie'}

        try:
            # Find the movie in CP's database by IMDB ID
            media = fireEvent(
                'media.with_identifiers',
                {'imdb': imdb_id},
                with_doc=True,
                single=True,
            )

            if not media:
                log.warning('Cannot apply reset_status — movie not in CP database: %s (%s)',
                            (movie_title, imdb_id))
                return {'applied': False, 'reason': 'Movie not found in CP database'}

            doc = media.get('doc', media) if isinstance(media, dict) else media
            media_id = doc.get('_id')
            old_status = doc.get('status', 'unknown')

            # Full removal — delete movie and all release records
            if reset_status == 'remove':
                fireEvent('media.delete', media_id, delete_from='all', single=True)
                log.info('Removed movie %s (%s) from database',
                         (movie_title, imdb_id))
                return {'applied': True, 'old_status': old_status,
                        'new_status': 'removed', 'changed': True}

            if reset_status == 'wanted':
                new_db_status = 'active'
            elif reset_status == 'done':
                new_db_status = 'done'
            else:
                return {'applied': False, 'reason': 'Unknown reset_status: %s' % reset_status}

            if old_status == new_db_status:
                log.info('Movie %s already has status %s, no change needed',
                         (movie_title, old_status))
                return {'applied': True, 'old_status': old_status,
                        'new_status': new_db_status, 'changed': False}

            # Update the movie status directly via get_db
            db = get_db()
            m = db.get('id', media_id)
            m['status'] = new_db_status
            db.update(m)

            log.info('Reset status for %s (%s): %s -> %s',
                     (movie_title, imdb_id, old_status, new_db_status))

            return {'applied': True, 'old_status': old_status,
                    'new_status': new_db_status, 'changed': True}

        except Exception as e:
            log.error('Failed to apply reset_status for %s: %s', (movie_title, e))
            return {'applied': False, 'reason': 'Error: %s' % e}

    def fixPreviewView(self, item_id=None, action=None, **kwargs):
        """API handler: preview what a fix action would change."""
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}
        if not action or action not in VALID_FIX_ACTIONS:
            return {
                'success': False,
                'error': 'action must be one of: %s' % ', '.join(sorted(VALID_FIX_ACTIONS)),
            }

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        if item.get('fixed'):
            return {'success': False, 'error': 'Item already fixed'}

        preview = generate_fix_preview(item, action)
        if 'error' in preview:
            return {'success': False, 'error': preview['error']}

        return {'success': True, 'preview': preview}

    def fixView(self, item_id=None, action=None, confirm='0', reset_status=None, **kwargs):
        """API handler: execute a fix action on a flagged item.

        Args:
            reset_status: What to do with the original movie's status after
                delete/reassign.  One of 'wanted' (set to active/wanted),
                'done' (keep as done), or 'nochange' (leave as-is).  Only
                relevant for delete_wrong and reassign_movie actions.
        """
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}
        if not action or action not in VALID_FIX_ACTIONS:
            return {
                'success': False,
                'error': 'action must be one of: %s' % ', '.join(sorted(VALID_FIX_ACTIONS)),
            }
        if str(confirm) != '1':
            return {
                'success': False,
                'error': 'confirm=1 is required to execute a fix (use audit.fix.preview first)',
            }

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        if item.get('fixed'):
            return {'success': False, 'error': 'Item already fixed'}

        # Execute the fix
        if action == 'rename_template':
            success, details = execute_fix_rename_template(item)
        elif action == 'rename_resolution':
            success, details = execute_fix_rename_resolution(item)
        elif action == 'rename_edition':
            success, details = execute_fix_rename_edition(item)
        elif action in ('delete_wrong', 'delete_duplicate', 'delete_foreign'):
            success, details = execute_fix_delete_wrong(item)
        elif action == 'verify_audio':
            return self.verifyView(item_id=item.get('item_id'))
        elif action == 'reassign_movie':
            success, details = execute_fix_reassign_movie(item)
        else:
            return {'success': False, 'error': 'Unknown action: %s' % action}

        if not success:
            log.error('Fix %s failed for %s: %s', (action, item_id, details.get('error', '')))
            return {'success': False, 'error': details.get('error', 'Unknown error')}

        # Apply status change for delete/reassign actions
        if action in ('delete_wrong', 'delete_duplicate', 'delete_foreign', 'reassign_movie') and reset_status and reset_status != 'nochange':
            status_result = self._apply_reset_status(item, reset_status)
            details['status_change'] = status_result

        # Mark as fixed
        self._mark_fixed(item, action, details)
        log.info('Fix %s applied to %s: %s', (action, item.get('folder', item_id), details))

        # Recalculate duplicate flags for remaining siblings when a file
        # is removed from a folder (delete or reassign/move)
        if action in ('delete_wrong', 'delete_duplicate', 'delete_foreign', 'reassign_movie'):
            self._recalculate_folder_duplicates(item)

        return {
            'success': True,
            'action': action,
            'item_id': item_id,
            'details': details,
        }

    def identifyView(self, item_id=None, **kwargs):
        """API handler: run identification on a single flagged item.

        Runs identify_flagged_file() on the item, updates identification and
        recommended_action in-place, persists results, and returns the updated
        item data.
        """
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        filepath = item.get('file_path', '')
        if not filepath or not os.path.isfile(filepath):
            return {
                'success': False,
                'error': 'File not found: %s' % filepath,
            }

        # Get container_title_parsed from item data
        actual = item.get('actual', {})
        container_title_parsed = actual.get('container_title_parsed')

        flags = item.get('flags', [])

        log.info('Running identification on %s', (item.get('folder', item_id),))

        # Look up cached hashes from knowledge doc to avoid recomputing
        # (even though we always re-identify, we can skip CRC32 file reads)
        fp = item.get('file_fingerprint')
        knowledge_doc = self._get_knowledge(fp) if fp else None
        cached_crc = knowledge_doc.get('crc32') if knowledge_doc else None
        cached_osh = (knowledge_doc.get('opensubtitles_hash')
                      if knowledge_doc else None)

        try:
            identification = identify_flagged_file(
                filepath, flags, container_title_parsed,
                cached_crc32=cached_crc,
                cached_opensubtitles_hash=cached_osh,
            )
        except Exception as e:
            log.error('Identification failed for %s: %s', (item_id, e))
            return {'success': False, 'error': 'Identification failed: %s' % str(e)}

        # Update item in-place
        item['identification'] = identification

        # Backfill imdb_id so template rendering has it
        id_imdb = identification.get('identified_imdb', '')
        if id_imdb and not item.get('imdb_id'):
            item['imdb_id'] = id_imdb

        # Backfill edition from identification source (srrDB release name or
        # container title).  This can suppress false-positive runtime flags
        # for extended/director's cuts and add edition mismatch flags.
        _backfill_edition_from_identification(item)

        # Re-evaluate ±1 year title flags with the IMDB year from
        # identification.  This may suppress false-positive year flags that
        # were created during the quick scan when no IMDB data was available.
        id_year = identification.get('identified_year')
        if id_year:
            _revalidate_year_flags(item, id_year)

        # Recompute recommended action with the new identification data
        item['recommended_action'] = compute_recommended_action(
            item.get('flags', []), identification, item.get('expected')
        )

        # When identification confirms same title but different year (±1),
        # compute_recommended_action returns 'rename_template'.  Update the
        # expected year so _apply_renamer_template builds the correct filename
        # and add a template flag so the UI shows what changed.
        if item['recommended_action'] == 'rename_template':
            id_year = identification.get('identified_year')
            exp = item.get('expected', {})
            exp_year = exp.get('year')
            if id_year and exp_year and str(id_year) != str(exp_year):
                item['expected']['year'] = id_year
                existing_checks = {f['check'] for f in item.get('flags', [])}
                if 'template' not in existing_checks:
                    item['flags'].append({
                        'check': 'template',
                        'severity': 'MEDIUM',
                        'detail': 'Year corrected from %s to %s based on identification' % (
                            exp_year, id_year),
                    })
                    item['flag_count'] = len(item['flags'])

        # Persist
        self._save_results()

        # Cache identification result in knowledge doc
        if fp:
            self._cache_identification(fp, identification)

        log.info('Identification result for %s: method=%s action=%s', (
            item.get('folder', item_id),
            identification.get('method', ''),
            item['recommended_action'],
        ))

        return {
            'success': True,
            'item_id': item_id,
            'identification': identification,
            'recommended_action': item['recommended_action'],
            'item': item,
        }

    def reassignView(self, item_id=None, imdb_id=None, **kwargs):
        """API handler: manually reassign a flagged item by IMDB ID.

        Looks up the movie by IMDB ID (CP database, then TMDB fallback),
        sets the identification data on the item, recomputes the recommended
        action, and returns a reassign_movie preview.
        """
        if not item_id:
            return {'success': False, 'error': 'item_id is required'}
        if not imdb_id:
            return {'success': False, 'error': 'imdb_id is required'}

        # Validate IMDB ID format
        imdb_id = imdb_id.strip()
        if not re.match(r'^tt\d{5,}$', imdb_id):
            return {
                'success': False,
                'error': 'Invalid IMDB ID format (expected tt1234567): %s' % imdb_id,
            }

        item = self._find_item(item_id)
        if not item:
            return {'success': False, 'error': 'Item not found: %s' % item_id}

        if item.get('fixed'):
            return {'success': False, 'error': 'Item already fixed'}

        # Look up movie info by IMDB ID
        db_path = self._get_db_path()
        movie_info = lookup_imdb_id(imdb_id, db_path=db_path)
        if not movie_info:
            return {
                'success': False,
                'error': 'Could not find movie for IMDB ID %s (not in CP database or TMDB)' % imdb_id,
            }

        log.info('Manual reassign for %s -> %s (%s) via %s', (
            item.get('folder', item_id),
            movie_info['title'],
            movie_info.get('year', '?'),
            movie_info.get('source', 'unknown'),
        ))

        # Set identification data
        identification = {
            'method': 'manual',
            'identified_title': movie_info['title'],
            'identified_year': movie_info.get('year'),
            'identified_imdb': imdb_id,
            'confidence': 'high',
            'source': 'manual (%s)' % movie_info.get('source', 'lookup'),
        }
        item['identification'] = identification

        # Backfill imdb_id so template rendering has it
        if imdb_id and not item.get('imdb_id'):
            item['imdb_id'] = imdb_id

        # Recompute recommended action
        item['recommended_action'] = compute_recommended_action(
            item.get('flags', []), identification, item.get('expected')
        )

        # Same-title year correction: update expected year for template rename
        if item['recommended_action'] == 'rename_template':
            id_year = identification.get('identified_year')
            exp = item.get('expected', {})
            exp_year = exp.get('year')
            if id_year and exp_year and str(id_year) != str(exp_year):
                item['expected']['year'] = id_year
                existing_checks = {f['check'] for f in item.get('flags', [])}
                if 'template' not in existing_checks:
                    item['flags'].append({
                        'check': 'template',
                        'severity': 'MEDIUM',
                        'detail': 'Year corrected from %s to %s based on identification' % (
                            exp_year, id_year),
                    })
                    item['flag_count'] = len(item['flags'])

        # Persist
        self._save_results()

        # Generate preview
        preview = _preview_reassign_movie(item)
        if 'error' in preview:
            return {'success': False, 'error': preview['error']}

        return {
            'success': True,
            'item_id': item_id,
            'identification': identification,
            'recommended_action': item['recommended_action'],
            'preview': preview,
            'movie_info': movie_info,
        }

    def _run_batch_fix(self, action, items, dry_run=False):
        """Run a batch fix operation (called in background thread).

        Updates self.fix_in_progress with running stats.
        """
        total = len(items)
        completed = 0
        failed = 0
        results = []

        self.fix_in_progress = {
            'active': True,
            'action': action,
            'total': total,
            'completed': 0,
            'failed': 0,
            'current_item': '',
        }

        for item in items:
            if item.get('fixed'):
                completed += 1
                self.fix_in_progress['completed'] = completed
                continue

            self.fix_in_progress['current_item'] = item.get('folder', '')

            if dry_run:
                preview = generate_fix_preview(item, action)
                results.append({
                    'item_id': item.get('item_id', ''),
                    'folder': item.get('folder', ''),
                    'preview': preview,
                })
                completed += 1
            else:
                # Execute the fix
                if action == 'rename_template':
                    success, details = execute_fix_rename_template(item)
                elif action == 'rename_resolution':
                    success, details = execute_fix_rename_resolution(item)
                elif action == 'rename_edition':
                    success, details = execute_fix_rename_edition(item)
                elif action in ('delete_wrong', 'delete_duplicate', 'delete_foreign'):
                    success, details = execute_fix_delete_wrong(item)
                elif action == 'verify_audio':
                    # Run whisper inline during batch
                    file_path = item.get('file_path', '')
                    if file_path and os.path.isfile(file_path):
                        wr = whisper_verify_audio(file_path)
                        if 'error' not in wr:
                            self._apply_whisper_result(item, wr)
                            success = True
                            details = {'language': wr['language'], 'confidence': wr['confidence']}
                        else:
                            success, details = False, {'error': wr['error']}
                    else:
                        success, details = False, {'error': 'File not accessible'}
                elif action == 'reassign_movie':
                    success, details = execute_fix_reassign_movie(item)
                else:
                    success, details = False, {'error': 'Unknown action'}

                if success:
                    self._mark_fixed(item, action, details)
                    # Recalculate duplicate flags for remaining siblings
                    if action in ('delete_wrong', 'delete_duplicate', 'delete_foreign', 'reassign_movie'):
                        self._recalculate_folder_duplicates(item)
                    completed += 1
                else:
                    failed += 1
                    log.error('Batch fix failed for %s: %s',
                              (item.get('folder', ''), details.get('error', '')))
                    self._append_action(item, action, details, success=False)

                results.append({
                    'item_id': item.get('item_id', ''),
                    'folder': item.get('folder', ''),
                    'success': success,
                    'details': details,
                })

            self.fix_in_progress['completed'] = completed
            self.fix_in_progress['failed'] = failed

        self.fix_in_progress = {
            'active': False,
            'action': action,
            'total': total,
            'completed': completed,
            'failed': failed,
            'current_item': '',
        }

        if not dry_run:
            # Save results after batch completes and clear the actions log
            self._save_results()
            self._truncate_actions()

        return results

    def fixBatchView(self, action=None, filter_check=None,
                     filter_severity=None, confirm='0', dry_run='1',
                     **kwargs):
        """API handler: execute a fix action on multiple items."""
        if not action or action not in VALID_FIX_ACTIONS:
            return {
                'success': False,
                'error': 'action must be one of: %s' % ', '.join(sorted(VALID_FIX_ACTIONS)),
            }
        if str(confirm) != '1':
            return {
                'success': False,
                'error': 'confirm=1 is required',
            }

        is_dry_run = str(dry_run) != '0'

        if self.fix_in_progress and self.fix_in_progress.get('active'):
            return {
                'success': False,
                'error': 'A batch fix is already in progress',
                'fix_progress': self.fix_in_progress,
            }

        if not self.last_report:
            return {'success': False, 'error': 'No scan results available'}

        # Get matching items using the filter
        items = self._filter_and_sort(
            filter_check=filter_check,
            filter_severity=filter_severity,
            filter_action=action,
            filter_fixed='false',
        )

        if not items:
            return {
                'success': False,
                'error': 'No matching unfixed items found for action %s' % action,
            }

        if is_dry_run:
            # Synchronous dry run — return previews immediately
            previews = []
            for item in items:
                preview = generate_fix_preview(item, action)
                previews.append({
                    'item_id': item.get('item_id', ''),
                    'folder': item.get('folder', ''),
                    'preview': preview,
                })
            return {
                'success': True,
                'dry_run': True,
                'action': action,
                'total': len(previews),
                'previews': previews,
            }
        else:
            # Async execution
            fireEventAsync(
                'audit.run_batch_fix',
                action=action,
                items=items,
                dry_run=False,
            )
            return {
                'success': True,
                'dry_run': False,
                'action': action,
                'total': len(items),
                'message': 'Batch fix started — check audit.fix.progress for status',
            }

    def fixProgressView(self, **kwargs):
        """API handler: return current batch fix progress."""
        return {
            'fix_progress': self.fix_in_progress if self.fix_in_progress else {
                'active': False,
            },
        }




# ---------------------------------------------------------------------------
# Settings config — registers the "Audit" subtab under Settings > Manage
# ---------------------------------------------------------------------------

config = [{
    'name': 'audit',
    'groups': [
        {
            'tab': 'manage',
            'subtab': 'audit',
            'subtab_label': 'Audit',
            'label': 'Library Audit',
            'description': 'Scan your library for quality and identity issues.',
            'options': [
                {
                    'name': 'default_workers',
                    'default': 4,
                    'type': 'int',
                    'label': 'Scan Workers',
                    'description': 'Number of parallel scan threads (1-16).',
                },
            ],
        },
    ],
}]


# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Audit movie library for mislabeled files'
    )
    parser.add_argument(
        '--movies-dir', required=True,
        help='Path to the movies directory',
    )
    parser.add_argument(
        '--db', required=True,
        help='Path to CP db.json',
    )
    parser.add_argument(
        '--scan-path',
        help='Scan only this specific folder name (within movies-dir)',
    )
    parser.add_argument(
        '--full', action='store_true',
        help='Run full scan with identification on flagged files (hash + srrDB)',
    )
    parser.add_argument(
        '--force-full', action='store_true',
        help='Force identification even for high-confidence quick scan flags',
    )
    parser.add_argument(
        '--workers', type=int, default=DEFAULT_WORKERS,
        help='Number of parallel scan threads (default: %s)' % DEFAULT_WORKERS,
    )
    parser.add_argument(
        '--output', '-o',
        help='Write JSON report to this file (default: stdout)',
    )
    args = parser.parse_args()

    report = scan_library(
        movies_dir=args.movies_dir,
        db_path=args.db,
        scan_path=args.scan_path,
        full=args.full,
        force_full=args.force_full,
        workers=args.workers,
    )

    json_output = json.dumps(report, indent=2)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(json_output)
        print(f'\nReport written to {args.output}', file=sys.stderr)
    else:
        print(json_output)


if __name__ == '__main__':
    main()
