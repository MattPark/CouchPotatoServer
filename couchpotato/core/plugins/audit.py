"""Library audit tool for detecting mislabeled movies.

Tier 1 (local, no network):
  1. Resolution mismatch — actual resolution vs filename-claimed resolution
  2. Runtime mismatch — actual duration vs TMDB runtime from CP database
  3. Container title mismatch — guessit-parsed metadata title/year vs folder title/year
  4. TV episode detection — S##E## / Season / Disc patterns in container titles

Tier 2 (targeted identification for flagged files):
  A. Container title already identified it (from Tier 1 data)
  B. CRC32 reverse lookup on srrDB → release name + IMDB ID
  C. (future) TMDB search fallback

  Smart skip logic (tier2 without force_tier2):
    - TV episode detected → skip tier 2, mark for deletion
    - Resolution-only mismatch → skip tier 2 (right movie, wrong quality)
    - Everything else → run tier 2 (suspect file needs identification)
    - force_tier2=1 overrides all skip logic

Usage (standalone):
  python audit.py --movies-dir /movies --db /config/data/database/db.json
  python audit.py --movies-dir /movies --db /config/data/database/db.json --scan-path "Dead, The (1987)"
  python audit.py --movies-dir /movies --db /config/data/database/db.json --tier2
  python audit.py --movies-dir /movies --db /config/data/database/db.json --tier2 --force-tier2
  python audit.py --movies-dir /movies --db /config/data/database/db.json --workers 8

API (when running inside CouchPotato):
  GET /api/{key}/audit.scan?tier2=0&force_tier2=0&workers=4&scan_path=
  GET /api/{key}/audit.cancel
  GET /api/{key}/audit.progress
  GET /api/{key}/audit.results?offset=0&limit=50&filter_check=&filter_severity=&filter_action=&sort=folder&sort_dir=asc
  GET /api/{key}/audit.stats
  GET /api/{key}/audit.fix.preview?item_id=&action=
  GET /api/{key}/audit.fix?item_id=&action=&confirm=1
  GET /api/{key}/audit.fix.batch?action=&filter_check=&confirm=1&dry_run=1
  GET /api/{key}/audit.fix.progress
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
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
    from couchpotato.api import addApiView
    from couchpotato.core.event import addEvent, fireEventAsync
    from couchpotato.core.logger import CPLog
    from couchpotato.core.plugins.base import Plugin
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
]

# Regex to extract IMDB ID from filename
IMDB_RE = re.compile(r'(tt\d{5,})')

# Regex to parse folder name: "Title (Year)" or "Title, The (Year)"
FOLDER_RE = re.compile(r'^(.+?)\s*\((\d{4})\)\s*$')

# srrDB API base
SRRDB_API = 'https://api.srrdb.com/v1'

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
}

# Words that should not be treated as edition names when followed by Cut/Edition
EDITION_EXCLUDE = {
    'blu', 'ray', 'web', 'hd', 'sd', 'uhd', 'dvd', 'bd', 'hdr', 'tax',
    'pay', 'price', 'budget', 'the', 'a', 'an', 'no', 'rough', 'first',
    'clean',
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
    """
    with open(db_path, 'r') as f:
        data = json.load(f).get('_default', {})

    media_by_imdb = {}
    files_by_media_id = {}

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
            movie_files = doc.get('files', {}).get('movie', [])
            if media_id and movie_files:
                files_by_media_id.setdefault(media_id, []).extend(movie_files)

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

    return media_by_imdb, files_by_media_id, media_by_title


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

def extract_file_meta(filepath):
    """Extract metadata from a video file using the mediainfo CLI.

    Uses the CLI binary instead of the Python library to isolate crashes —
    if libmediainfo segfaults on a file, only the subprocess dies and the
    parent process continues scanning.

    Returns dict with: resolution_width, resolution_height, duration_min,
                       video_codec, container_title
    """
    result = {
        'resolution_width': 0,
        'resolution_height': 0,
        'duration_min': 0.0,
        'video_codec': '',
        'container_title': None,
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

    Width takes priority because widescreen/letterbox crops reduce height
    while the encode resolution stays the same (e.g. 1920x800 is 1080p,
    not 720p).  Height is the fallback for non-standard widths (e.g.
    1440x1080 4:3 content is still 1080p).
    """
    if width >= 3800:
        return '2160p'
    if width >= 1900:
        return '1080p'
    if width >= 1260:
        return '720p'
    if height >= 2160:
        return '2160p'
    if height >= 1080:
        return '1080p'
    if height >= 720:
        return '720p'
    if height >= 480:
        return '480p'
    return f'{height}p (SD)'


# ---------------------------------------------------------------------------
# Edition detection (ported from scanner.py:getEdition)
# ---------------------------------------------------------------------------

def get_edition(filename):
    """Detect edition/cut info from a filename or release name.

    Ported from Scanner.getEdition() in scanner.py.  Works as a standalone
    function (no class instance required).

    Only searches AFTER the year in the filename to avoid false positives
    when edition words appear in the movie title.

    Returns the edition string (e.g. "Director's Cut") or empty string.
    """
    filename = str(filename)

    # Check for Plex {edition-X} tag first
    plex_match = re.search(r'\{edition-([^}]+)\}', filename, re.IGNORECASE)
    if plex_match:
        return plex_match.group(1)

    words = re.split(r'\W+', filename.lower())

    # Find year position — editions only appear after the year in release names
    year_idx = 0
    for i, w in enumerate(words):
        if re.match(r'^(19|20)\d{2}$', w):
            year_idx = i
            break

    # Restrict search to words at/after year position
    search_words = words[year_idx:]
    search_joined = '.'.join(search_words)

    # Check known editions first
    for key, tags in EDITION_MAP.items():
        for tag in tags:
            if isinstance(tag, tuple) and '.'.join(tag) in search_joined:
                return key
            elif isinstance(tag, str) and tag.lower() in search_words:
                return key

    # Fallback: catch arbitrary "<Word(s)> Cut" or "<Word(s)> Edition" patterns
    basename = os.path.basename(filename)
    # Restrict fallback to after the year too
    year_match = re.search(r'[\.\s_\-]((?:19|20)\d{2})[\.\s_\-]', basename)
    search_basename = basename[year_match.start():] if year_match else basename
    m = re.search(
        r'[\.\s_\-]((?:[a-z]+[\.\s_\-]){0,2}(?:[a-z]+))[\.\s_\-](cut|edition)(?=[\.\s_\-]|$)',
        search_basename, re.IGNORECASE
    )
    if m:
        name_part = re.sub(r'[\._\-]', ' ', m.group(1)).strip()
        kind = m.group(2)
        last_word = name_part.split()[-1].lower() if name_part else ''
        if last_word and last_word not in EDITION_EXCLUDE:
            return '%s %s' % (name_part.title(), kind.title())

    return ''


# ---------------------------------------------------------------------------
# Item ID + recommended action
# ---------------------------------------------------------------------------

def compute_item_id(file_path):
    """Compute a stable ID for a flagged item: SHA256 of file_path, 12 hex chars."""
    return hashlib.sha256(file_path.encode('utf-8')).hexdigest()[:12]


def compute_recommended_action(flags, identification=None):
    """Derive the recommended fix action from flags and identification data.

    Returns one of:
        'delete_wrong'       — TV episode or identified as non-movie
        'rename_resolution'  — resolution-only flag (right movie, wrong quality label)
        'reassign_movie'     — tier 2 identified as a different movie
        'rename_edition'     — edition detected in container but not filename
        'needs_tier2'        — title/runtime mismatch, needs tier 2 identification
        'manual_review'      — tier 2 ran but couldn't identify
        'none'               — no action needed (tier 2 confirmed same movie)
    """
    checks = {f['check'] for f in flags}

    # TV episode always gets delete
    if 'tv_episode' in checks:
        return 'delete_wrong'

    # If tier 2 has run, use identification to decide
    if identification:
        method = identification.get('method', '')

        if method == 'tv_episode_detected':
            return 'delete_wrong'

        if method == 'skipped':
            # High-confidence tier 1 (resolution-only skip)
            if checks == {'resolution'}:
                return 'rename_resolution'
            if checks == {'edition'}:
                return 'rename_edition'
            if 'edition' in checks and checks - {'edition', 'resolution'} == set():
                # Only edition + resolution flags
                return 'rename_resolution'

        if method in ('container_title', 'srrdb_crc'):
            # Tier 2 found a match — is it the same movie or different?
            id_imdb = identification.get('identified_imdb')
            if id_imdb:
                # We have an IMDB — if it differs from expected, reassign
                return 'reassign_movie'
            # No IMDB from tier 2 — title-based guess
            id_title = identification.get('identified_title', '')
            if id_title:
                return 'reassign_movie'

        if method == 'crc_not_found':
            return 'manual_review'

    # No tier 2 data — decide from tier 1 flags alone
    if checks == {'resolution'}:
        return 'rename_resolution'

    if checks == {'edition'}:
        return 'rename_edition'

    if 'edition' in checks and checks - {'edition', 'resolution'} == set():
        return 'rename_resolution'

    # Title or runtime mismatch without tier 2 — needs identification
    if 'title' in checks or 'runtime' in checks:
        return 'needs_tier2'

    return 'manual_review'


# ---------------------------------------------------------------------------
# Tier 1 checks
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


def check_runtime(actual_duration_min, expected_runtime_min):
    """Check 2: Runtime mismatch.

    Returns a flag dict or None.
    """
    if not expected_runtime_min or not actual_duration_min:
        return None

    delta = abs(actual_duration_min - expected_runtime_min)
    pct = delta / expected_runtime_min if expected_runtime_min else 0

    # Flag only if BOTH absolute and percentage thresholds are exceeded
    if delta > RUNTIME_DELTA_MIN and pct > RUNTIME_DELTA_PCT:
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


def check_container_title(container_title, folder_title, folder_year):
    """Check 3: Container title mismatch.

    Returns a flag dict or None.  Also returns parsed metadata (title, year)
    for use in Tier 2 identification.
    """
    if not container_title or is_junk_title(container_title):
        return None, None

    parsed = guessit_parse(container_title)
    meta_title = parsed.get('title')
    meta_year = parsed.get('year')

    # If guessit couldn't extract a year, it's probably not a scene name
    # Still check the title if we got one
    if not meta_title:
        return None, None

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
        return {
            'check': 'title',
            'severity': 'HIGH',
            'detail': (
                f"Container year {meta_year} "
                f"vs folder year {folder_year} (title matches)"
            ),
        }, parsed_meta

    return None, parsed_meta


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
        # Container has edition, filename doesn't — missing from name
        return {
            'check': 'edition',
            'severity': 'MEDIUM',
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


# ---------------------------------------------------------------------------
# Tier 2 skip logic
# ---------------------------------------------------------------------------

def needs_identification(flags):
    """Determine if a flagged file needs Tier 2 identification.

    Smart skip logic — avoid expensive CRC32/srrDB lookups when Tier 1
    already gives us enough information to act:

      - TV episode detected → skip (already identified from container title,
        queue for deletion)
      - Resolution-only mismatch → skip (right movie, wrong quality — not
        suspect, just needs re-download)
      - Edition-only → skip (right movie, just missing edition in filename)
      - Resolution + edition only → skip (quality label + edition fix, no ID needed)
      - Everything else → run (title mismatch, runtime mismatch, or
        multi-flag combinations are suspect and need identification)

    Returns True if Tier 2 should run, False to skip.
    """
    checks = {f['check'] for f in flags}

    # TV episode: already identified from container title, no CRC needed
    if 'tv_episode' in checks:
        return False

    # Resolution-only: right movie, wrong quality — not suspect
    if checks == {'resolution'}:
        return False

    # Edition-only: right movie, just missing edition in filename
    if checks == {'edition'}:
        return False

    # Resolution + edition: both are simple renames, no ID needed
    if checks == {'resolution', 'edition'}:
        return False

    # Everything else is suspect — run tier 2
    return True


# ---------------------------------------------------------------------------
# Tier 2 identification
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


def identify_flagged_file(filepath, flags, container_title_parsed):
    """Try to identify what a flagged file actually is.

    Strategy A: Container title (already parsed)
    Strategy B: CRC32 → srrDB reverse lookup
    """
    identification = None

    # Strategy A: container title already told us
    if container_title_parsed and container_title_parsed.get('title'):
        meta = container_title_parsed
        title = meta['title']
        # Only use if title or year differs from what was expected,
        # AND the title isn't a junk encoder/group name
        has_title_flag = any(f['check'] == 'title' for f in flags)
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

    # Strategy B: CRC32 reverse lookup on srrDB
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


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

def find_video_files(folder_path):
    """Find all video files in a folder (non-recursive within the folder)."""
    files = []
    try:
        for entry in os.listdir(folder_path):
            ext = os.path.splitext(entry)[1].lower()
            if ext in VIDEO_EXTENSIONS:
                files.append(os.path.join(folder_path, entry))
    except OSError:
        pass
    return files


def scan_movie_folder(folder_path, folder_name, media_by_imdb,
                      tier2=False, force_tier2=False, media_by_title=None):
    """Scan a single movie folder and return audit results.

    Args:
        folder_path: Full path to the movie folder
        folder_name: Folder basename (e.g., "Dead, The (1987)")
        media_by_imdb: Dict mapping IMDB ID → movie info from CP database
        tier2: Run Tier 2 identification on flagged files
        force_tier2: Force Tier 2 even for high-confidence Tier 1 flags
        media_by_title: Dict mapping (normalized_title, year) → movie info

    Returns a dict with scan results or None if no issues found.
    """
    video_files = find_video_files(folder_path)
    if not video_files:
        return None

    # Use the largest video file (skip samples)
    video_files.sort(key=lambda f: os.path.getsize(f), reverse=True)
    filepath = video_files[0]
    filename = os.path.basename(filepath)

    # Parse expected values from folder/filename
    folder_title, folder_year = parse_folder_name(folder_name)
    claimed_res = parse_filename_resolution(filename)
    imdb_id = parse_filename_imdb(filename)

    # Look up TMDB runtime from CP database
    expected_runtime = 0
    db_entry = None
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

    # Extract actual metadata from file
    meta = extract_file_meta(filepath)

    # Run checks
    flags = []
    container_title_parsed = None

    # Check 1: Resolution (width + height)
    flag = check_resolution(claimed_res, meta['resolution_width'], meta['resolution_height'])
    if flag:
        flags.append(flag)

    # Check 2: Runtime
    flag = check_runtime(meta['duration_min'], expected_runtime)
    if flag:
        flags.append(flag)

    # Check 3: Container title
    flag, container_title_parsed = check_container_title(
        meta['container_title'], folder_title, folder_year
    )
    if flag:
        flags.append(flag)

    # Check 4: TV episode in container title
    flag = check_tv_episode(meta['container_title'])
    if flag:
        flags.append(flag)

    # Check 5: Edition mismatch
    edition_flag, detected_edition = check_edition(meta['container_title'], filename)
    if edition_flag:
        flags.append(edition_flag)

    if not flags:
        return None

    result = {
        'item_id': compute_item_id(filepath),
        'folder': folder_name,
        'file': filename,
        'file_path': filepath,
        'imdb_id': imdb_id,
        'file_size_bytes': os.path.getsize(filepath),
        'actual': {
            'resolution': f"{meta['resolution_width']}x{meta['resolution_height']}",
            'duration_min': round(meta['duration_min'], 1),
            'video_codec': meta['video_codec'],
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
        'identification': None,
        'recommended_action': None,
        'fixed': None,
    }

    # Tier 2: identification with smart skip logic
    if tier2:
        has_tv = any(f['check'] == 'tv_episode' for f in flags)
        if has_tv and not force_tier2:
            # TV episode detected — mark for deletion, skip CRC identification
            result['identification'] = {
                'method': 'tv_episode_detected',
                'action': 'queue_deletion',
                'detail': 'Container title indicates TV episode content',
            }
        elif force_tier2 or needs_identification(flags):
            result['identification'] = identify_flagged_file(
                filepath, flags, container_title_parsed
            )
        else:
            # High-confidence tier 1 flag (resolution-only) — skip
            result['identification'] = {
                'method': 'skipped',
                'reason': 'high_confidence_tier1',
                'detail': 'Tier 1 flags sufficient; use force_tier2=1 to override',
            }

    # Compute recommended action from flags + identification
    result['recommended_action'] = compute_recommended_action(
        flags, result['identification']
    )

    return result


# Sentinel value for _scan_one: folder was not a directory, skip it
_SKIP = object()


def scan_library(movies_dir, db_path, scan_path=None, tier2=False,
                 force_tier2=False, workers=DEFAULT_WORKERS,
                 progress_callback=None, cancel_flag=None):
    """Scan movie library for mislabeled files.

    Args:
        movies_dir: Path to the movies directory
        db_path: Path to CP's db.json
        scan_path: If set, only scan this specific folder name
        tier2: Run Tier 2 identification on flagged files
        force_tier2: Force Tier 2 even for high-confidence Tier 1 flags
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
    media_by_imdb, files_by_media_id, media_by_title = load_cp_database(db_path)
    _log_info('Loaded %s movies from database' % len(media_by_imdb))

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
                tier2=tier2, force_tier2=force_tier2,
                media_by_title=media_by_title,
            )
            return result, False
        except Exception as e:
            _log_error('Error scanning %s: %s' % (folder_name, e))
            return None, True

    def _collect_result(folder_name, result, is_error):
        """Process a scan result. Must be called under lock (or single-threaded)."""
        nonlocal scanned, errors
        if is_error:
            errors += 1
            return
        scanned += 1
        if result is not None:
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
    }


# ---------------------------------------------------------------------------
# Fix helpers
# ---------------------------------------------------------------------------

VALID_FIX_ACTIONS = {
    'rename_resolution',
    'reassign_movie',
    'delete_wrong',
    'rename_edition',
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


def _apply_renamer_template(item, quality_override=None, edition_override=None):
    """Rebuild a filename from the renamer's file_name template.

    Uses the renamer config settings (file_name template, replace_doubles,
    separator) to construct the filename, exactly as the renamer would.

    Args:
        item: audit item dict with expected/actual/imdb_id fields
        quality_override: if set, use this instead of the item's claimed quality
        edition_override: if set, use this as the edition value

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
    movie_name = item['expected'].get('title', '')
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
    imdb_id = item.get('imdb_id', '') or ''
    year = item['expected'].get('year') or ''

    replacements = {
        'ext': ext,
        'namethe': name_the.strip(),
        'thename': movie_name.strip(),
        'year': str(year) if year else '',
        'first': name_the[0].upper() if name_the else '',
        'quality': quality,
        'quality_type': '',
        'video': '',
        'audio': '',
        'group': '',
        'source': '',
        'resolution_width': '',
        'resolution_height': '',
        'audio_channels': '',
        'imdb_id': imdb_id,
        'cd': '',
        'cd_nr': '',
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
                'remove_release': {
                    'movie': item['expected'].get('title', ''),
                    'year': item['expected'].get('year'),
                    'imdb': item.get('imdb_id', ''),
                },
                'reset_status': {
                    'movie': item['expected'].get('title', ''),
                    'new_status': 'wanted',
                },
            },
        },
        'warnings': [],
    }


def _preview_reassign_movie(item):
    """Generate preview for reassign_movie action.

    Requires tier 2 identification with an identified IMDB ID.
    """
    ident = item.get('identification')
    if not ident:
        return {'error': 'No tier 2 identification data — run a tier 2 scan first'}

    method = ident.get('method', '')
    if method not in ('container_title', 'srrdb_crc'):
        return {'error': 'Tier 2 identification method "%s" cannot determine correct movie' % method}

    id_title = ident.get('identified_title', '')
    id_year = ident.get('identified_year')
    id_imdb = ident.get('identified_imdb', '')

    if not id_title:
        return {'error': 'Tier 2 did not identify a title'}

    # Build destination folder and filename
    # Format: "Title (Year)" or "Title, The (Year)"
    if id_year:
        new_folder = '%s (%s)' % (id_title, id_year)
    else:
        new_folder = id_title

    # Build new filename: "Title (Year) Resolution IMDBid.ext"
    old_file = item['file']
    ext = os.path.splitext(old_file)[1]

    # Use actual resolution for the new filename
    actual_res = item['actual'].get('resolution', '')
    actual_width = 0
    actual_height = 0
    if 'x' in actual_res:
        try:
            actual_width = int(actual_res.split('x')[0])
            actual_height = int(actual_res.split('x')[1])
        except (ValueError, IndexError):
            pass
    res_label = resolution_label(actual_width, actual_height) if actual_height else ''

    parts = [new_folder]
    if res_label:
        parts.append(res_label)
    if id_imdb:
        parts.append(id_imdb)
    new_file = ' '.join(parts) + ext

    # Determine the movies root from the old path
    # old_path: /media/Movies/FolderName/file.mkv → movies_dir = /media/Movies
    old_path = item['file_path']
    old_folder_path = os.path.dirname(old_path)
    movies_dir = os.path.dirname(old_folder_path)

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
                    'new_status': 'wanted',
                },
            },
        },
        'warnings': warnings,
    }


def generate_fix_preview(item, action):
    """Generate a fix preview for a flagged item.

    Returns a preview dict describing what changes would be made.
    """
    if action == 'rename_resolution':
        return _preview_rename_resolution(item)
    elif action == 'rename_edition':
        return _preview_rename_edition(item)
    elif action == 'delete_wrong':
        return _preview_delete_wrong(item)
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

    def __init__(self):
        if not _CP_AVAILABLE:
            return

        addApiView('audit.scan', self.scanView, docs={
            'desc': 'Start a library audit scan',
            'params': {
                'tier2': {'desc': 'Run Tier 2 identification (CRC + srrDB). Default 0.'},
                'force_tier2': {'desc': 'Force Tier 2 even for high-confidence flags. Default 0.'},
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
                'filter_check': {'desc': 'Filter by check type (comma-sep): resolution,title,runtime,tv_episode,edition'},
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
                'action': {'desc': 'Fix action: rename_resolution, reassign_movie, delete_wrong, rename_edition'},
            },
        })

        addApiView('audit.fix', self.fixView, docs={
            'desc': 'Execute a fix action on a flagged item',
            'params': {
                'item_id': {'desc': '12-char hex ID of the flagged item'},
                'action': {'desc': 'Fix action: rename_resolution, reassign_movie, delete_wrong, rename_edition'},
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

        addEvent('audit.run_scan', self._run_scan)
        addEvent('audit.run_batch_fix', self._run_batch_fix)

        # Load persisted results on startup
        self._load_results()

    def _get_results_path(self):
        """Get the path to the persisted audit results file."""
        data_dir = Env.get('data_dir')
        return os.path.join(data_dir, 'audit_results.json')

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

    def _run_scan(self, tier2=False, force_tier2=False, scan_path=None,
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
                tier2=tier2,
                force_tier2=force_tier2,
                workers=workers,
                progress_callback=self._on_progress,
                cancel_flag=self._cancel,
            )
            self.last_report = report
            self.last_report['completed_at'] = time.time()
            self.last_report['scan_timestamp'] = time.strftime(
                '%Y-%m-%dT%H:%M:%S'
            )
            # Persist to disk
            self._save_results()
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

        # Filter by severity
        if filter_severity:
            sev_set = {s.strip().upper() for s in filter_severity.split(',')}
            items = [
                i for i in items
                if sev_set & {f['severity'] for f in i.get('flags', [])}
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

    def scanView(self, tier2='0', force_tier2='0', workers='4',
                 scan_path=None, **kwargs):
        """API handler: start an audit scan."""
        if self.in_progress:
            return {
                'success': False,
                'message': 'Scan already in progress',
                'progress': self.in_progress,
            }

        do_tier2 = str(tier2) == '1'
        do_force = str(force_tier2) == '1'

        try:
            num_workers = int(workers)
        except (ValueError, TypeError):
            num_workers = DEFAULT_WORKERS
        num_workers = max(1, min(num_workers, MAX_WORKERS))

        self.in_progress = {'total': 0, 'scanned': 0, 'flagged': 0}

        fireEventAsync(
            'audit.run_scan',
            tier2=do_tier2,
            force_tier2=do_force,
            scan_path=scan_path if scan_path else None,
            workers=num_workers,
        )

        return {
            'success': True,
            'message': 'Audit scan started (workers=%s, tier2=%s, force_tier2=%s)' % (
                num_workers, do_tier2, do_force),
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

        flagged = self.last_report.get('flagged', [])

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

        # Tier 2 stats
        tier2_identified = 0
        tier2_unidentified = 0
        tier2_skipped = 0
        for item in flagged:
            ident = item.get('identification')
            if not ident:
                continue
            method = ident.get('method', '')
            if method in ('container_title', 'srrdb_crc'):
                tier2_identified += 1
            elif method == 'crc_not_found':
                tier2_unidentified += 1
            elif method in ('skipped', 'tv_episode_detected'):
                tier2_skipped += 1

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
                'checks': check_counts,
                'severity': severity_counts,
                'actions': action_counts,
                'tier2': {
                    'identified': tier2_identified,
                    'unidentified': tier2_unidentified,
                    'skipped': tier2_skipped,
                },
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
        """Mark a flagged item as fixed and persist."""
        item['fixed'] = {
            'action': action,
            'timestamp': time.time(),
            'details': details,
        }
        # Update the file_path if it changed (for subsequent operations)
        if 'new_path' in details:
            item['file_path'] = details['new_path']
        self._save_results()

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

    def fixView(self, item_id=None, action=None, confirm='0', **kwargs):
        """API handler: execute a fix action on a flagged item."""
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
        if action == 'rename_resolution':
            success, details = execute_fix_rename_resolution(item)
        elif action == 'rename_edition':
            success, details = execute_fix_rename_edition(item)
        elif action == 'delete_wrong':
            success, details = execute_fix_delete_wrong(item)
        elif action == 'reassign_movie':
            success, details = execute_fix_reassign_movie(item)
        else:
            return {'success': False, 'error': 'Unknown action: %s' % action}

        if not success:
            log.error('Fix %s failed for %s: %s', (action, item_id, details.get('error', '')))
            return {'success': False, 'error': details.get('error', 'Unknown error')}

        # Mark as fixed
        self._mark_fixed(item, action, details)
        log.info('Fix %s applied to %s: %s', (action, item.get('folder', item_id), details))

        return {
            'success': True,
            'action': action,
            'item_id': item_id,
            'details': details,
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
                if action == 'rename_resolution':
                    success, details = execute_fix_rename_resolution(item)
                elif action == 'rename_edition':
                    success, details = execute_fix_rename_edition(item)
                elif action == 'delete_wrong':
                    success, details = execute_fix_delete_wrong(item)
                elif action == 'reassign_movie':
                    success, details = execute_fix_reassign_movie(item)
                else:
                    success, details = False, {'error': 'Unknown action'}

                if success:
                    self._mark_fixed(item, action, details)
                    completed += 1
                else:
                    failed += 1
                    log.error('Batch fix failed for %s: %s',
                              (item.get('folder', ''), details.get('error', '')))

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
            # Save results after batch completes
            self._save_results()

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
        '--tier2', action='store_true',
        help='Run Tier 2 identification on flagged files (CRC32 + srrDB)',
    )
    parser.add_argument(
        '--force-tier2', action='store_true',
        help='Force Tier 2 even for high-confidence Tier 1 flags',
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
        tier2=args.tier2,
        force_tier2=args.force_tier2,
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
