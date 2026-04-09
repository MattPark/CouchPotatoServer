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
  GET /api/{key}/audit.results
"""

import argparse
import json
import os
import re
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

# Tolerance: actual height can be within this percentage of expected
# (slight crops like 1920x1072 are still "1080p")
RESOLUTION_TOLERANCE_PCT = 0.05

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


# ---------------------------------------------------------------------------
# Database loader
# ---------------------------------------------------------------------------

def load_cp_database(db_path):
    """Load CP's db.json and build lookup indices.

    Returns:
        media_by_imdb: dict mapping IMDB ID → {title, year, runtime, _id}
        files_by_media_id: dict mapping media _id → list of file paths
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

    return media_by_imdb, files_by_media_id


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


def resolution_label_for_height(height):
    """Convert a pixel height to a human-readable resolution label."""
    if height >= 2160:
        return '2160p'
    elif height >= 1080:
        return '1080p'
    elif height >= 720:
        return '720p'
    elif height >= 480:
        return '480p'
    else:
        return f'{height}p (SD)'


# ---------------------------------------------------------------------------
# Tier 1 checks
# ---------------------------------------------------------------------------

def check_resolution(claimed_label, actual_height):
    """Check 1: Resolution mismatch (height-based).

    The 'p' in 1080p refers to vertical lines, so classification should
    be based on height, not width.  This avoids false positives from 4:3
    content (e.g. 1440x1080 is still 1080p).

    Returns a flag dict or None.
    """
    if not claimed_label or not actual_height:
        return None

    expected_height = RESOLUTION_HEIGHT_MAP.get(claimed_label.lower())
    if expected_height is None:
        return None

    # Allow small tolerance for slight crops (e.g. 1072 is close enough to 1080)
    lower = expected_height * (1 - RESOLUTION_TOLERANCE_PCT)

    if actual_height < lower:
        actual_label = resolution_label_for_height(actual_height)
        expected_label = resolution_label_for_height(expected_height)

        if actual_label != expected_label:
            return {
                'check': 'resolution',
                'severity': 'HIGH',
                'detail': f'Claimed {claimed_label}, actual height {actual_height}px (maps to {actual_label})',
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
                      tier2=False, force_tier2=False):
    """Scan a single movie folder and return audit results.

    Args:
        folder_path: Full path to the movie folder
        folder_name: Folder basename (e.g., "Dead, The (1987)")
        media_by_imdb: Dict mapping IMDB ID → movie info from CP database
        tier2: Run Tier 2 identification on flagged files
        force_tier2: Force Tier 2 even for high-confidence Tier 1 flags

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

    # Extract actual metadata from file
    meta = extract_file_meta(filepath)

    # Run checks
    flags = []
    container_title_parsed = None

    # Check 1: Resolution (height-based)
    flag = check_resolution(claimed_res, meta['resolution_height'])
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

    if not flags:
        return None

    result = {
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
        'identification': None,
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
    media_by_imdb, files_by_media_id = load_cp_database(db_path)
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
# CouchPotato plugin
# ---------------------------------------------------------------------------

class Audit(Plugin if _CP_AVAILABLE else object):
    """Library audit plugin — exposes scan/progress/results via the CP API."""

    in_progress = False
    last_report = None
    _cancel = [False]   # mutable list so the scan loop can observe changes

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
            'desc': 'Get results of the last completed audit scan',
            'return': {'type': 'object', 'example': """{
    'results': {'total_scanned': 8526, 'total_flagged': 127, 'flagged': [...]}
}"""},
        })

        addEvent('audit.run_scan', self._run_scan)

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
        except Exception as e:
            log.error('Audit scan failed: %s', (e,))
        finally:
            self.in_progress = False
            self._cancel[0] = False

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

    def resultsView(self, **kwargs):
        """API handler: return last completed scan report."""
        return {
            'results': self.last_report,
        }


# ---------------------------------------------------------------------------
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
