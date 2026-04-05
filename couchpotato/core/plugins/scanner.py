import os
import queue
import re
import sys
import platform
import ctypes
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from couchpotato import get_db
from couchpotato.core.event import fireEvent, addEvent
from couchpotato.core.helpers.encoding import toUnicode, simplifyString, sp, ss
from couchpotato.core.helpers.variable import getExt, getImdb, tryInt, \
    splitString, getIdentifier
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin
from guessit import guess_movie_info
from subliminal.videos import Video
import enzyme


log = CPLog(__name__)

autoload = 'Scanner'

# Thread pool sizes for parallel scanning
WALK_WORKERS = 16    # Parallel directory walkers (I/O-bound stat calls on NAS)
PROCESS_WORKERS = 8  # Parallel group processors (enzyme + TMDB API calls)

# --- I/O priority helper (Linux ionice via ioprio_set syscall) ---
_ionice_set = threading.local()
_ioprio_syscall_nr = None


def _set_thread_io_idle():
    """Set current thread to idle I/O scheduling class (Linux only).
    Falls back to lowest best-effort (class 2, data 7) if idle class not permitted.
    Silent no-op on non-Linux platforms. Called once per thread (cached via thread-local)."""
    if getattr(_ionice_set, 'done', False):
        return
    _ionice_set.done = True
    if sys.platform != 'linux':
        return

    global _ioprio_syscall_nr
    if _ioprio_syscall_nr is None:
        arch = platform.machine()
        _ioprio_syscall_nr = {
            'x86_64': 251, 'aarch64': 30, 'i386': 289, 'i686': 289,
        }.get(arch, 0)
    if _ioprio_syscall_nr == 0:
        return

    try:
        tid = threading.get_native_id()
        libc = ctypes.CDLL('libc.so.6', use_errno=True)
        IOPRIO_WHO_PROCESS = 1
        IOPRIO_CLASS_IDLE = 3
        IOPRIO_CLASS_BE = 2
        # Try idle class first (may require CAP_SYS_NICE)
        ioprio = IOPRIO_CLASS_IDLE << 13
        ret = libc.syscall(_ioprio_syscall_nr, IOPRIO_WHO_PROCESS, tid, ioprio)
        if ret != 0:
            # Fall back to best-effort, lowest data priority (7)
            ioprio = (IOPRIO_CLASS_BE << 13) | 7
            libc.syscall(_ioprio_syscall_nr, IOPRIO_WHO_PROCESS, tid, ioprio)
    except Exception:
        pass


class Scanner(Plugin):

    ignored_in_path = [os.path.sep + 'extracted' + os.path.sep, 'extracting', '_unpack', '_failed_', '_unknown_', '_exists_', '_failed_remove_',
                       '_failed_rename_', '.appledouble', '.appledb', '.appledesktop', os.path.sep + '._', '.ds_store', 'cp.cpnfo',
                       'thumbs.db', 'ehthumbs.db', 'desktop.ini']  # unpacking, smb-crap, hidden files
    ignore_names = ['extract', 'extracting', 'extracted', 'movie', 'movies', 'film', 'films', 'download', 'downloads', 'video_ts', 'audio_ts', 'bdmv', 'certificate']
    ignored_extensions = ['ignore', 'lftp-pget-status']
    extensions = {
        'movie': ['mkv', 'wmv', 'avi', 'mpg', 'mpeg', 'mp4', 'm2ts', 'iso', 'img', 'mdf', 'ts', 'm4v', 'flv'],
        'movie_extra': ['mds'],
        'dvd': ['vts_*', 'vob'],
        'nfo': ['nfo', 'txt', 'tag'],
        'subtitle': ['sub', 'srt', 'ssa', 'ass'],
        'subtitle_extra': ['idx'],
        'trailer': ['mov', 'mp4', 'flv']
    }

    threed_types = {
        'Half SBS': [('half', 'sbs'), ('h', 'sbs'), 'hsbs'],
        'Full SBS': [('full', 'sbs'), ('f', 'sbs'), 'fsbs'],
        'SBS': ['sbs'],
        'Half OU': [('half', 'ou'), ('h', 'ou'), ('half', 'tab'), ('h', 'tab'), 'htab', 'hou'],
        'Full OU': [('full', 'ou'), ('f', 'ou'), ('full', 'tab'), ('f', 'tab'), 'ftab', 'fou'],
        'OU': ['ou', 'tab'],
        'Frame Packed': ['mvc', ('complete', 'bluray')],
        '3D': ['3d']
    }

    file_types = {
        'subtitle': ('subtitle', 'subtitle'),
        'subtitle_extra': ('subtitle', 'subtitle_extra'),
        'trailer': ('video', 'trailer'),
        'nfo': ('nfo', 'nfo'),
        'movie': ('video', 'movie'),
        'movie_extra': ('movie', 'movie_extra'),
        'backdrop': ('image', 'backdrop'),
        'poster': ('image', 'poster'),
        'thumbnail': ('image', 'thumbnail'),
        'leftover': ('leftover', 'leftover'),
    }

    file_sizes = {  # in MB
        'movie': {'min': 200},
        'trailer': {'min': 2, 'max': 199},
        'backdrop': {'min': 0, 'max': 5},
    }

    codecs = {
        'audio': ['DTS', 'AC3', 'AC3D', 'MP3'],
        'video': ['x264', 'H264', 'x265', 'H265', 'DivX', 'Xvid']
    }

    resolutions = {
        '2160p': {'resolution_width': 3840, 'resolution_height': 2160, 'aspect': 1.78},
        '1080p': {'resolution_width': 1920, 'resolution_height': 1080, 'aspect': 1.78},
        '1080i': {'resolution_width': 1920, 'resolution_height': 1080, 'aspect': 1.78},
        '720p': {'resolution_width': 1280, 'resolution_height': 720, 'aspect': 1.78},
        '720i': {'resolution_width': 1280, 'resolution_height': 720, 'aspect': 1.78},
        '480p': {'resolution_width': 640, 'resolution_height': 480, 'aspect': 1.33},
        '480i': {'resolution_width': 640, 'resolution_height': 480, 'aspect': 1.33},
        'default': {'resolution_width': 0, 'resolution_height': 0, 'aspect': 1},
    }

    audio_codec_map = {
        0x2000: 'AC3',
        0x2001: 'DTS',
        0x0055: 'MP3',
        0x0050: 'MP2',
        0x0001: 'PCM',
        0x003: 'WAV',
        0x77a1: 'TTA1',
        0x5756: 'WAV',
        0x6750: 'Vorbis',
        0xF1AC: 'FLAC',
        0x00ff: 'AAC',
    }

    source_media = {
        'Blu-ray': ['bluray', 'blu-ray', 'brrip', 'br-rip'],
        'HD DVD': ['hddvd', 'hd-dvd'],
        'DVD': ['dvd'],
        'HDTV': ['hdtv']
    }

    clean = r'([ _\,\.\(\)\[\]\-]|^)(3d|hsbs|sbs|half.sbs|full.sbs|ou|half.ou|full.ou|extended|extended.cut|directors.cut|french|fr|swedisch|sw|danish|dutch|nl|swesub|subs|spanish|german|ac3|dts|custom|dc|divx|divx5|dsr|dsrip|dutch|dvd|dvdr|dvdrip|dvdscr|dvdscreener|screener|dvdivx|cam|fragment|fs|hdtv|hdrip' \
            r'|hdtvrip|webdl|web.dl|webrip|web.rip|internal|limited|multisubs|ntsc|ogg|ogm|pal|pdtv|proper|repack|rerip|retail|r3|r5|bd5|se|svcd|swedish|german|read.nfo|nfofix|unrated|ws|telesync|ts|telecine|tc|brrip|bdrip|video_ts|audio_ts|480p|480i|576p|576i|720p|720i|1080p|1080i|hrhd|hrhdtv|hddvd|bluray|x264|h264|x265|h265|xvid|xvidvd|xxx|www.www|hc|\[.*\])(?=[ _\,\.\(\)\[\]\-]|$)'
    multipart_regex = [
        r'[ _\.-]+cd[ _\.-]*([0-9a-d]+)',  #*cd1
        r'[ _\.-]+dvd[ _\.-]*([0-9a-d]+)',  #*dvd1
        r'[ _\.-]+part[ _\.-]*([0-9a-d]+)',  #*part1
        r'[ _\.-]+dis[ck][ _\.-]*([0-9a-d]+)',  #*disk1
        r'cd[ _\.-]*([0-9a-d]+)$',  #cd1.ext
        r'dvd[ _\.-]*([0-9a-d]+)$',  #dvd1.ext
        r'part[ _\.-]*([0-9a-d]+)$',  #part1.mkv
        r'dis[ck][ _\.-]*([0-9a-d]+)$',  #disk1.mkv
        r'()[ _\.-]+([0-9]*[abcd]+)(\.....?)$',
        r'([a-z])([0-9]+)(\.....?)$',
        r'()([ab])(\.....?)$'  #*a.mkv
    ]

    cp_imdb = r'\.cp\((?P<id>tt[0-9]+),?\s?(?P<random>[A-Za-z0-9]+)?\)'

    def __init__(self):

        addEvent('scanner.create_file_identifier', self.createStringIdentifier)
        addEvent('scanner.remove_cptag', self.removeCPTag)

        addEvent('scanner.scan', self.scan)
        addEvent('scanner.name_year', self.getReleaseNameYear)
        addEvent('scanner.partnumber', self.getPartNumber)

    def scan(self, folder = None, files = None, release_download = None, simple = False, newer_than = 0, return_ignored = True, check_file_date = True, on_found = None, on_walk_progress = None):

        folder = sp(folder)

        if not folder or not os.path.isdir(folder):
            log.error('Folder doesn\'t exists: %s', folder)
            return {}

        # Phase A: Collect and categorize files
        if not files:
            log.info('Walking folder with %d parallel workers: %s' % (WALK_WORKERS, folder))
            movie_files, leftovers = self._parallel_walk_and_categorize(
                folder, newer_than, on_walk_progress)
            log.info('Walk complete: %d movie groups, %d leftover files' % (len(movie_files), len(leftovers)))
        else:
            check_file_date = False
            files = [sp(x) for x in files]
            log.info('Scanner received %s file(s) to process' % len(files))
            movie_files, leftovers = self._categorize_files(files, folder)

        # Phase B: Group leftovers with movie groups
        # Sort reverse so "Iron man 2" groups before "Iron man"
        leftovers = set(sorted(leftovers, reverse = True))

        # Group files minus extension
        ignored_identifiers = {}
        for identifier, group in movie_files.items():
            if identifier not in group['identifiers'] and len(identifier) > 0: group['identifiers'].append(identifier)

            log.debug('Grouping files: %s', identifier)

            has_ignored = 0
            ignore_files = []
            for file_path in list(group['unsorted_files']):
                ext = getExt(file_path)
                wo_ext = file_path[:-(len(ext) + 1)]
                found_files = set([i for i in leftovers if wo_ext in i])
                group['unsorted_files'].extend(found_files)
                leftovers = leftovers - found_files

                if ext in self.ignored_extensions:
                    has_ignored += 1
                    ignore_files.append(file_path)

            if has_ignored == 0:
                for file_path in list(group['unsorted_files']):
                    ext = getExt(file_path)
                    if ext in self.ignored_extensions:
                        has_ignored += 1
                        ignore_files.append(file_path)

            if has_ignored > 0:
                ignored_identifiers[identifier] = ignore_files

            # Break if CP wants to shut down
            if self.shuttingDown():
                break


        # Create identifiers for all leftover files
        path_identifiers = {}
        for file_path in leftovers:
            identifier = self.createStringIdentifier(file_path, folder)

            if not path_identifiers.get(identifier):
                path_identifiers[identifier] = []

            path_identifiers[identifier].append(file_path)


        # Group the files based on the identifier
        delete_identifiers = []
        for identifier, found_files in path_identifiers.items():
            log.debug('Grouping files on identifier: %s', identifier)

            group = movie_files.get(identifier)
            if group:
                group['unsorted_files'].extend(found_files)
                delete_identifiers.append(identifier)

                # Remove the found files from the leftover stack
                leftovers = leftovers - set(found_files)

            # Break if CP wants to shut down
            if self.shuttingDown():
                break

        # Cleaning up used
        for identifier in delete_identifiers:
            if path_identifiers.get(identifier):
                del path_identifiers[identifier]
        del delete_identifiers

        # Group based on folder
        delete_identifiers = []
        for identifier, found_files in path_identifiers.items():
            log.debug('Grouping files on foldername: %s', identifier)

            for ff in found_files:
                new_identifier = self.createStringIdentifier(os.path.dirname(ff), folder)

                group = movie_files.get(new_identifier)
                if group:
                    group['unsorted_files'].extend([ff])
                    delete_identifiers.append(identifier)

                    # Remove the found files from the leftover stack
                    leftovers -= leftovers - set([ff])

            # Break if CP wants to shut down
            if self.shuttingDown():
                break

        # leftovers should be empty
        if leftovers:
            log.debug('Some files are still left over: %s', leftovers)

        # Cleaning up used
        for identifier in delete_identifiers:
            if path_identifiers.get(identifier):
                del path_identifiers[identifier]
        del delete_identifiers

        # Phase C: Filter still-unpacking files (check_file_date)
        # newer_than is already handled in Phase A (parallel walk workers)
        valid_files = {}
        while True and not self.shuttingDown():
            try:
                identifier, group = movie_files.popitem()
            except:
                break

            # Check if movie is fresh and maybe still unpacking, ignore files newer than 1 minute
            if check_file_date:
                files_too_new, time_string = self.checkFilesChanged(group['unsorted_files'])
                if files_too_new:
                    log.info('Files seem to be still unpacking or just unpacked (created on %s), ignoring for now: %s', (time_string, identifier))

                    # Delete the unsorted list
                    del group['unsorted_files']

                    continue

            valid_files[identifier] = group

        del movie_files

        total_found = len(valid_files)

        # Make sure only one movie was found if a download ID is provided
        if release_download and total_found == 0:
            log.info('Download ID provided (%s), but no groups found! Make sure the download contains valid media files (fully extracted).', release_download.get('imdb_id'))
        elif release_download and total_found > 1:
            log.info('Download ID provided (%s), but more than one group found (%s). Ignoring Download ID...', (release_download.get('imdb_id'), len(valid_files)))
            release_download = None

        # Phase D: Process groups in parallel
        processed_movies = self._parallel_process_groups(
            valid_files, folder, release_download, simple,
            return_ignored, ignored_identifiers, on_found, total_found)

        if len(processed_movies) > 0:
            log.info('Found %s movies in the folder %s', (len(processed_movies), folder))
        else:
            log.debug('Found no movies in the folder %s', folder)

        return processed_movies

    # ---- Parallel scanning infrastructure ----

    def _scandir_recursive(self, top, followlinks=True):
        """Recursively walk directory using os.scandir, yielding (path, stat_result).
        DirEntry objects avoid redundant stat syscalls for type checking."""
        try:
            with os.scandir(top) as entries:
                dirs = []
                for entry in entries:
                    try:
                        if entry.is_file(follow_symlinks=followlinks):
                            st = entry.stat(follow_symlinks=followlinks)
                            yield (entry.path, st)
                        elif entry.is_dir(follow_symlinks=followlinks):
                            dirs.append(entry.path)
                    except OSError:
                        pass
                for d in dirs:
                    yield from self._scandir_recursive(d, followlinks)
        except OSError:
            pass

    def _walk_and_categorize_subdir(self, subdir_path, folder, newer_than):
        """Walk one top-level subdirectory, categorize files into movie groups.
        Called from worker threads with idle I/O priority.
        Returns (movie_files_dict, leftovers_list, stats_dict)."""
        _set_thread_io_idle()

        movie_files = {}
        leftovers = []
        stats = {'sample': 0, 'ignored': 0, 'toosmall': 0, 'accepted': 0}

        # Collect all files via scandir (one stat per file instead of 4-5)
        all_files = []
        for file_path, stat_result in self._scandir_recursive(subdir_path):
            all_files.append((sp(file_path), stat_result))

        if self.shuttingDown() or not all_files:
            return movie_files, leftovers, stats

        # Quick scan optimization: if newer_than is set, check if ANY movie-sized
        # file has mtime or ctime newer than threshold. If none do, skip the entire
        # subdirectory — avoids expensive quality.guess calls for unchanged movies.
        if newer_than and newer_than > 0:
            min_movie_bytes = self.file_sizes['movie'].get('min', 0) * 1024 * 1024
            has_new = False
            for file_path, st in all_files:
                if st.st_size >= min_movie_bytes:
                    if st.st_mtime > newer_than or st.st_ctime > newer_than:
                        has_new = True
                        break
            if not has_new:
                return movie_files, leftovers, stats

        # Categorize files
        for file_path, st in all_files:
            if self.shuttingDown():
                break

            # Remove ignored files
            if self.isSampleFile(file_path):
                leftovers.append(file_path)
                stats['sample'] += 1
                continue
            elif not self.keepFile(file_path):
                stats['ignored'] += 1
                continue

            size_mb = st.st_size / 1024 / 1024
            is_dvd_file = self.isDVDFile(file_path)
            if self.filesizeBetween(file_path, self.file_sizes['movie'], cached_size_mb=size_mb) or is_dvd_file:

                # Normal identifier
                identifier = self.createStringIdentifier(file_path, folder, exclude_filename = is_dvd_file)
                identifiers = [identifier]

                # Identifier with quality
                quality = fireEvent('quality.guess', files = [file_path], size = size_mb, single = True) if not is_dvd_file else {'identifier':'dvdr'}
                if quality:
                    identifier_with_quality = '%s %s' % (identifier, quality.get('identifier', ''))
                    identifiers = [identifier_with_quality, identifier]

                if not movie_files.get(identifier):
                    movie_files[identifier] = {
                        'unsorted_files': [],
                        'identifiers': identifiers,
                        'is_dvd': is_dvd_file,
                        'primary_count': 0,
                    }

                movie_files[identifier]['unsorted_files'].append(file_path)
                movie_files[identifier]['primary_count'] += 1
                stats['accepted'] += 1
            else:
                leftovers.append(file_path)
                stats['toosmall'] += 1

        return movie_files, leftovers, stats

    def _parallel_walk_and_categorize(self, folder, newer_than, on_walk_progress):
        """Walk library folder in parallel, categorize files into movie groups.
        Splits top-level subdirectories across WALK_WORKERS threads, each with
        idle I/O priority. Returns (all_movie_files, all_leftovers)."""
        folder_path = folder.rstrip(os.sep)

        # List top-level entries
        top_level_subdirs = []
        top_level_files = []
        try:
            with os.scandir(folder_path) as entries:
                for entry in entries:
                    try:
                        if entry.is_dir(follow_symlinks=True):
                            top_level_subdirs.append(entry.path)
                        elif entry.is_file(follow_symlinks=True):
                            top_level_files.append((sp(entry.path), entry.stat(follow_symlinks=True)))
                    except OSError:
                        pass
        except OSError:
            log.error('Failed listing directory: %s', folder)
            return {}, []

        total_dirs = len(top_level_subdirs)
        if on_walk_progress:
            on_walk_progress(0, total_dirs)

        log.info('Scanning %d top-level folders in %s with %d workers' % (total_dirs, folder, WALK_WORKERS))

        # Accumulators (only touched from main thread during merge)
        all_movie_files = {}
        all_leftovers = []
        total_stats = {'sample': 0, 'ignored': 0, 'toosmall': 0, 'accepted': 0}

        # Handle files directly in the scan root (rare but possible)
        for file_path, st in top_level_files:
            if self.shuttingDown():
                break
            if self.isSampleFile(file_path):
                all_leftovers.append(file_path)
                continue
            if not self.keepFile(file_path):
                continue
            size_mb = st.st_size / 1024 / 1024
            is_dvd = self.isDVDFile(file_path)
            if self.filesizeBetween(file_path, self.file_sizes['movie'], cached_size_mb=size_mb) or is_dvd:
                identifier = self.createStringIdentifier(file_path, folder, exclude_filename=is_dvd)
                identifiers = [identifier]
                quality = fireEvent('quality.guess', files=[file_path], size=size_mb, single=True) if not is_dvd else {'identifier': 'dvdr'}
                if quality:
                    identifier_with_quality = '%s %s' % (identifier, quality.get('identifier', ''))
                    identifiers = [identifier_with_quality, identifier]
                if not all_movie_files.get(identifier):
                    all_movie_files[identifier] = {
                        'unsorted_files': [], 'identifiers': identifiers,
                        'is_dvd': is_dvd, 'primary_count': 0,
                    }
                all_movie_files[identifier]['unsorted_files'].append(file_path)
                all_movie_files[identifier]['primary_count'] += 1
            else:
                all_leftovers.append(file_path)

        # Parallel walk of subdirectories
        dirs_done = [0]
        progress_lock = threading.Lock()

        try:
            with ThreadPoolExecutor(max_workers=WALK_WORKERS) as pool:
                future_to_subdir = {}
                for subdir in top_level_subdirs:
                    if self.shuttingDown():
                        break
                    future = pool.submit(self._walk_and_categorize_subdir, subdir, folder, newer_than)
                    future_to_subdir[future] = subdir

                for future in as_completed(future_to_subdir):
                    subdir = future_to_subdir[future]
                    try:
                        subdir_movies, subdir_leftovers, subdir_stats = future.result()

                        # Merge movie_files
                        for identifier, group in subdir_movies.items():
                            if identifier in all_movie_files:
                                all_movie_files[identifier]['unsorted_files'].extend(group['unsorted_files'])
                                all_movie_files[identifier]['primary_count'] += group['primary_count']
                                for ident in group['identifiers']:
                                    if ident not in all_movie_files[identifier]['identifiers']:
                                        all_movie_files[identifier]['identifiers'].append(ident)
                            else:
                                all_movie_files[identifier] = group

                        # Merge leftovers
                        all_leftovers.extend(subdir_leftovers)

                        # Merge stats
                        for k, v in subdir_stats.items():
                            total_stats[k] = total_stats.get(k, 0) + v

                    except Exception:
                        log.error('Error scanning subdir %s: %s', (os.path.basename(subdir), traceback.format_exc()))

                    # Progress
                    with progress_lock:
                        dirs_done[0] += 1
                        if on_walk_progress:
                            on_walk_progress(dirs_done[0])

        except RuntimeError:
            # Thread pool shut down (app exiting) — fall back to serial
            log.warning('Thread pool unavailable, falling back to serial scan')
            for subdir in top_level_subdirs:
                if self.shuttingDown():
                    break
                try:
                    subdir_movies, subdir_leftovers, subdir_stats = self._walk_and_categorize_subdir(subdir, folder, newer_than)
                    for identifier, group in subdir_movies.items():
                        if identifier in all_movie_files:
                            all_movie_files[identifier]['unsorted_files'].extend(group['unsorted_files'])
                            all_movie_files[identifier]['primary_count'] += group['primary_count']
                        else:
                            all_movie_files[identifier] = group
                    all_leftovers.extend(subdir_leftovers)
                    for k, v in subdir_stats.items():
                        total_stats[k] = total_stats.get(k, 0) + v
                except Exception:
                    log.error('Error scanning subdir %s: %s', (os.path.basename(subdir), traceback.format_exc()))
                dirs_done[0] += 1
                if on_walk_progress:
                    on_walk_progress(dirs_done[0])

        if total_stats['accepted'] == 0:
            total_files = sum(total_stats.values())
            log.info('Scanner file disposition: %d total, %d sample, %d ignored, %d too-small (<200MB), 0 accepted as movie' %
                     (total_files, total_stats['sample'], total_stats['ignored'], total_stats['toosmall']))

        return all_movie_files, all_leftovers

    def _categorize_files(self, files, folder):
        """Categorize a list of provided files into movie groups and leftovers.
        Used when scanner is called with specific files (e.g. from renamer)."""
        movie_files = {}
        leftovers = []

        for file_path in files:
            if not os.path.exists(file_path):
                continue

            # Remove ignored files
            if self.isSampleFile(file_path):
                leftovers.append(file_path)
                continue
            elif not self.keepFile(file_path):
                continue

            is_dvd_file = self.isDVDFile(file_path)
            if self.filesizeBetween(file_path, self.file_sizes['movie']) or is_dvd_file:

                # Normal identifier
                identifier = self.createStringIdentifier(file_path, folder, exclude_filename = is_dvd_file)
                identifiers = [identifier]

                # Identifier with quality
                quality = fireEvent('quality.guess', files = [file_path], size = self.getFileSize(file_path), single = True) if not is_dvd_file else {'identifier':'dvdr'}
                if quality:
                    identifier_with_quality = '%s %s' % (identifier, quality.get('identifier', ''))
                    identifiers = [identifier_with_quality, identifier]

                if not movie_files.get(identifier):
                    movie_files[identifier] = {
                        'unsorted_files': [],
                        'identifiers': identifiers,
                        'is_dvd': is_dvd_file,
                        'primary_count': 0,
                    }

                movie_files[identifier]['unsorted_files'].append(file_path)
                movie_files[identifier]['primary_count'] += 1
            else:
                leftovers.append(file_path)

            # Break if CP wants to shut down
            if self.shuttingDown():
                break

        return movie_files, leftovers

    def _process_single_group(self, identifier, group, folder, release_download, simple,
                               return_ignored, ignored_identifiers):
        """Process a single movie group: classify files, get metadata, determine media.
        Called from worker threads with idle I/O priority.
        Returns (identifier, group) or None if group should be skipped."""
        _set_thread_io_idle()

        if self.shuttingDown():
            return None

        if return_ignored is False and identifier in ignored_identifiers:
            ignore_files = ignored_identifiers[identifier]
            for f in ignore_files:
                # Filename format: <name>.<tag>.ignore — extract the tag
                parts = os.path.basename(f).rsplit('.', 2)
                tag = parts[-2] if len(parts) >= 3 else 'unknown'
                # Read the file for the reason line
                reason = ''
                try:
                    with open(f, 'r') as fh:
                        for line in fh:
                            if line.startswith('Reason:'):
                                reason = line[len('Reason:'):].strip()
                                break
                except:
                    pass
                if reason:
                    log.warning('Skipping release "%s": tagged "%s" — %s (file: %s)',
                                (identifier, tag, reason, os.path.basename(f)))
                else:
                    log.warning('Skipping release "%s": tagged "%s" (file: %s)',
                                (identifier, tag, os.path.basename(f)))
            return None

        # Group extra (and easy) files first
        group['files'] = {
            'movie_extra': self.getMovieExtras(group['unsorted_files']),
            'subtitle': self.getSubtitles(group['unsorted_files']),
            'subtitle_extra': self.getSubtitlesExtras(group['unsorted_files']),
            'nfo': self.getNfo(group['unsorted_files']),
            'trailer': self.getTrailers(group['unsorted_files']),
            'leftover': set(group['unsorted_files']),
        }

        # Media files
        if group['is_dvd']:
            group['files']['movie'] = self.getDVDFiles(group['unsorted_files'])
        else:
            group['files']['movie'] = self.getMediaFiles(group['unsorted_files'])

        if len(group['files']['movie']) == 0:
            log.error('Couldn\'t find any movie files for %s', identifier)
            return None

        log.debug('Getting metadata for %s', identifier)
        group['meta_data'] = self.getMetaData(group, folder = folder, release_download = release_download)

        # Subtitle meta
        group['subtitle_language'] = self.getSubtitleLanguage(group) if not simple else {}

        # Get parent dir from movie files
        for movie_file in group['files']['movie']:
            group['parentdir'] = os.path.dirname(movie_file)
            group['dirname'] = None

            folder_names = group['parentdir'].replace(folder, '').split(os.path.sep)
            folder_names.reverse()

            # Try and get a proper dirname, so no "A", "Movie", "Download" etc
            for folder_name in folder_names:
                if folder_name.lower() not in self.ignore_names and len(folder_name) > 2:
                    group['dirname'] = folder_name
                    break

            break

        # Leftover "sorted" files
        for file_type in group['files']:
            if not file_type == 'leftover':
                group['files']['leftover'] -= set(group['files'][file_type])
                group['files'][file_type] = list(group['files'][file_type])
        group['files']['leftover'] = list(group['files']['leftover'])

        # Delete the unsorted list
        del group['unsorted_files']

        # Determine movie
        group['media'] = self.determineMedia(group, release_download = release_download)
        if not group['media']:
            log.error('Unable to determine media: %s', group['identifiers'])
        else:
            group['identifier'] = getIdentifier(group['media']) or group['media']['info'].get('imdb')

        return (identifier, group)

    # Number of threads draining the on_found notification queue.
    # on_found does release.add + movie.update (DB writes + TMDB API) which is
    # the heaviest part of scanning. Using fewer notifiers than PROCESS_WORKERS
    # reduces DB/API contention while still allowing parallel updates.
    NOTIFY_WORKERS = 4

    def _parallel_process_groups(self, valid_files, folder, release_download, simple,
                                  return_ignored, ignored_identifiers, on_found, total_found):
        """Process movie groups in parallel using PROCESS_WORKERS threads.
        Workers do the heavy lifting (classify + enzyme + TMDB) and push results
        to a queue. NOTIFY_WORKERS threads drain the queue and call on_found
        (release.add + movie.update) in parallel, decoupled from the workers."""
        processed_movies = {}

        if total_found == 0:
            return processed_movies

        remaining = [total_found]
        lock = threading.Lock()
        notify_queue = queue.Queue()

        # --- Notifier threads: drain queue and call on_found in parallel ---
        def notifier():
            while True:
                item = notify_queue.get()
                if item is None:
                    # Sentinel — this notifier thread should exit
                    break
                grp, found, to_go = item
                try:
                    on_found(grp, found, to_go)
                except Exception:
                    log.error('Error in on_found callback: %s', traceback.format_exc())

        notifier_threads = []
        if on_found:
            for i in range(self.NOTIFY_WORKERS):
                t = threading.Thread(target=notifier, name='scan-notifier-%d' % i, daemon=True)
                t.start()
                notifier_threads.append(t)

        def process_one(identifier, group):
            result = self._process_single_group(
                identifier, group, folder, release_download, simple,
                return_ignored, ignored_identifiers)

            if result is None:
                with lock:
                    remaining[0] -= 1
                return None

            ident, grp = result

            with lock:
                remaining[0] -= 1
                to_go = remaining[0]
                processed_movies[ident] = grp

            # Push to notifier queue instead of calling on_found directly
            if on_found:
                notify_queue.put((grp, total_found, to_go))

            return result

        try:
            with ThreadPoolExecutor(max_workers=PROCESS_WORKERS) as pool:
                futures = []
                while not self.shuttingDown():
                    try:
                        identifier, group = valid_files.popitem()
                    except KeyError:
                        break
                    futures.append(pool.submit(process_one, identifier, group))

                # Wait for all workers to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception:
                        log.error('Error processing movie group: %s', traceback.format_exc())

        except RuntimeError:
            # Thread pool shut down — fall back to serial
            log.warning('Thread pool unavailable, processing groups serially')
            while not self.shuttingDown():
                try:
                    identifier, group = valid_files.popitem()
                except KeyError:
                    break
                try:
                    process_one(identifier, group)
                except Exception:
                    log.error('Error processing movie group: %s', traceback.format_exc())

        # Signal all notifier threads to stop and wait for queue to drain
        for _ in notifier_threads:
            notify_queue.put(None)
        for t in notifier_threads:
            t.join(timeout=300)
            if t.is_alive():
                log.warning('Notifier thread %s did not finish within 5 minutes' % t.name)

        return processed_movies

    # ---- Existing methods below (unchanged except filesizeBetween) ----

    def getMetaData(self, group, folder = '', release_download = None):

        data = {}
        files = list(group['files']['movie'])

        for cur_file in files:
            if not self.filesizeBetween(cur_file, self.file_sizes['movie']): continue  # Ignore smaller files

            if not data.get('audio'): # Only get metadata from first media file
                meta = self.getMeta(cur_file)

                try:
                    data['titles'] = meta.get('titles', [])
                    data['video'] = meta.get('video', self.getCodec(cur_file, self.codecs['video']))
                    data['audio'] = meta.get('audio', self.getCodec(cur_file, self.codecs['audio']))
                    data['audio_channels'] = meta.get('audio_channels', 2.0)
                    if meta.get('resolution_width'):
                        data['resolution_width'] = meta.get('resolution_width')
                        data['resolution_height'] = meta.get('resolution_height')
                        data['aspect'] = round(float(meta.get('resolution_width')) / meta.get('resolution_height', 1), 2)
                    else:
                        data.update(self.getResolution(cur_file))
                except:
                    log.info('Error parsing metadata: %s', cur_file)
                    pass

            data['size'] = data.get('size', 0) + self.getFileSize(cur_file)

        data['quality'] = None
        quality = fireEvent('quality.guess', size = data.get('size'), files = files, extra = data, single = True)

        # Use the quality that we snatched but check if it matches our guess
        if release_download and release_download.get('quality'):
            data['quality'] = fireEvent('quality.single', release_download.get('quality'), single = True)
            data['quality']['is_3d'] = release_download.get('is_3d', 0)
            if data['quality']['identifier'] != quality['identifier']:
                log.info('Different quality snatched than detected for %s: %s vs. %s. Assuming snatched quality is correct.', (files[0], data['quality']['identifier'], quality['identifier']))
            if data['quality']['is_3d'] != quality['is_3d']:
                log.info('Different 3d snatched than detected for %s: %s vs. %s. Assuming snatched 3d is correct.', (files[0], data['quality']['is_3d'], quality['is_3d']))

        if not data['quality']:
            data['quality'] = quality

            if not data['quality']:
                data['quality'] = fireEvent('quality.single', 'sd', single = True)

        data['quality_type'] = 'HD' if data.get('resolution_width', 0) >= 1280 or data['quality'].get('hd') else 'SD'

        filename = re.sub(self.cp_imdb, '', files[0])
        data['group'] = self.getGroup(filename[len(folder):])
        data['source'] = self.getSourceMedia(filename)
        if data['quality'].get('is_3d', 0):
            data['3d_type'] = self.get3dType(filename)
        return data

    def get3dType(self, filename):
        filename = ss(filename)

        words = re.split(r'\W+', filename.lower())

        for key in self.threed_types:
            tags = self.threed_types.get(key, [])

            for tag in tags:
                if (isinstance(tag, tuple) and '.'.join(tag) in '.'.join(words)) or (isinstance(tag, str) and ss(tag.lower()) in words):
                    log.debug('Found %s in %s', (tag, filename))
                    return key

        return ''

    def getMeta(self, filename):

        try:
            p = enzyme.parse(filename)

            # Video codec
            vc = ('H264' if p.video[0].codec == 'AVC1' else 'x265' if p.video[0].codec == 'HEVC' else p.video[0].codec)

            # Audio codec
            ac = p.audio[0].codec
            try: ac = self.audio_codec_map.get(p.audio[0].codec)
            except: pass

            # Find title in video headers
            titles = []

            try:
                if p.title and self.findYear(p.title):
                    titles.append(ss(p.title))
            except:
                log.error('Failed getting title from meta: %s', traceback.format_exc())

            for video in p.video:
                try:
                    if video.title and self.findYear(video.title):
                        titles.append(ss(video.title))
                except:
                    log.error('Failed getting title from meta: %s', traceback.format_exc())

            return {
                'titles': list(set(titles)),
                'video': vc,
                'audio': ac,
                'resolution_width': tryInt(p.video[0].width),
                'resolution_height': tryInt(p.video[0].height),
                'audio_channels': p.audio[0].channels,
            }
        except enzyme.exceptions.ParseError:
            log.info('Failed to parse MKV metadata (EBML): %s', filename)
        except enzyme.exceptions.NoParserError:
            log.debug('No parser found for %s', filename)
        except:
            log.info('Failed parsing metadata: %s', filename)

        return {}

    def getSubtitleLanguage(self, group):
        detected_languages = {}

        # Subliminal scanner
        paths = None
        try:
            paths = group['files']['movie']
            scan_result = []
            for p in paths:
                if not group['is_dvd']:
                    video = Video.from_path(toUnicode(sp(p)))
                    video_result = [(video, video.scan())]
                    scan_result.extend(video_result)

            for video, detected_subtitles in scan_result:
                for s in detected_subtitles:
                    if s.language and s.path not in paths:
                        detected_languages[s.path] = [s.language]
        except:
            log.debug('Failed parsing subtitle languages for %s: %s', (paths, traceback.format_exc()))

        # IDX
        for extra in group['files']['subtitle_extra']:
            try:
                if os.path.isfile(extra):
                    output = open(extra, 'r')
                    txt = output.read()
                    output.close()

                    idx_langs = re.findall(r'\nid: (\w+)', txt)

                    sub_file = '%s.sub' % os.path.splitext(extra)[0]
                    if len(idx_langs) > 0 and os.path.isfile(sub_file):
                        detected_languages[sub_file] = idx_langs
            except:
                log.error('Failed parsing subtitle idx for %s: %s', (extra, traceback.format_exc()))

        return detected_languages

    def determineMedia(self, group, release_download = None):

        # Get imdb id from downloader
        imdb_id = release_download and release_download.get('imdb_id')
        if imdb_id:
            log.debug('Found movie via imdb id from it\'s download id: %s', release_download.get('imdb_id'))

        files = group['files']

        # Check for CP(imdb_id) string in the file paths
        if not imdb_id:
            for cur_file in files['movie']:
                imdb_id = self.getCPImdb(cur_file)
                if imdb_id:
                    log.debug('Found movie via CP tag: %s', cur_file)
                    break

        # Check and see if nfo contains the imdb-id
        nfo_file = None
        if not imdb_id:
            try:
                for nf in files['nfo']:
                    imdb_id = getImdb(nf, check_inside = True)
                    if imdb_id:
                        log.debug('Found movie via nfo file: %s', nf)
                        nfo_file = nf
                        break
            except:
                pass

        # Check and see if filenames contains the imdb-id
        if not imdb_id:
            try:
                for filetype in files:
                    for filetype_file in files[filetype]:
                        imdb_id = getImdb(filetype_file)
                        if imdb_id:
                            log.debug('Found movie via imdb in filename: %s', nfo_file)
                            break
            except:
                pass

        # Search based on identifiers
        if not imdb_id:
            for identifier in group['identifiers']:

                if len(identifier) > 2:
                    try: filename = list(group['files'].get('movie'))[0]
                    except: filename = None

                    name_year = self.getReleaseNameYear(identifier, file_name = filename if not group['is_dvd'] else None)
                    if name_year.get('name') and name_year.get('year'):

                        movie_name = name_year['name']
                        movie_year = name_year['year']

                        # Split CamelCase words (e.g. MetallicaSlayerMegadeth -> Metallica Slayer Megadeth)
                        if re.search(r'[a-z][A-Z]', movie_name):
                            movie_name = re.sub(r'([a-z])([A-Z])', r'\1 \2', movie_name)

                        # Strip leftover source/quality words that survived self.clean
                        _junk_words = {'rip', 'dvd', 'br', 'bd', 'web', 'dl', 'hd', 'sd'}
                        name_words = [w for w in movie_name.split() if w.lower() not in _junk_words]
                        if name_words:
                            movie_name = ' '.join(name_words)

                        # Build search queries in order of specificity
                        search_attempts = []

                        # 1. Name + year (strict phrase search)
                        if movie_year and int(movie_year) > 0:
                            search_attempts.append(('%s %s' % (movie_name, movie_year), 1))

                        # 2. Name-only with wider limit (ngram search, year=0 or strict failed)
                        search_attempts.append((movie_name, 5))

                        # 3. Guessit alternative if available and different
                        if name_year.get('other') and name_year['other'].get('name') and name_year['other'].get('year'):
                            alt_q = '%(name)s %(year)s' % name_year.get('other')
                            search_attempts.append((alt_q, 1))

                        for search_q, limit in search_attempts:
                            if not search_q.strip():
                                continue
                            movie = fireEvent('movie.search', q = search_q, merge = True, limit = limit)

                            if limit > 1 and len(movie) > 1 and movie_year and int(movie_year) > 0:
                                # Multiple results: prefer one matching year ±1
                                yr = int(movie_year)
                                for m in movie:
                                    m_year = m.get('year', 0)
                                    if m_year and abs(m_year - yr) <= 1 and m.get('imdb'):
                                        imdb_id = m.get('imdb')
                                        break

                            if not imdb_id and len(movie) > 0:
                                imdb_id = movie[0].get('imdb')

                            if imdb_id:
                                log.debug('Found movie via search: %s', identifier)
                                break

                        if imdb_id: break
                else:
                    log.debug('Identifier to short to use for search: %s', identifier)

        if imdb_id:
            try:
                db = get_db()
                return db.get('media', 'imdb-%s' % imdb_id, with_doc = True)['doc']
            except:
                log.debug('Movie "%s" not in library, just getting info', imdb_id)
                return {
                    'identifier': imdb_id,
                    'info': fireEvent('movie.info', identifier = imdb_id, merge = True, extended = False)
                }

        log.error('No imdb_id found for %s. Add a NFO file with IMDB id or add the year to the filename.', group['identifiers'])
        return {}

    def getCPImdb(self, string):

        try:
            m = re.search(self.cp_imdb, string.lower())
            id = m.group('id')
            if id: return id
        except AttributeError:
            pass

        return False

    def removeCPTag(self, name):
        try:
            return re.sub(self.cp_imdb, '', name).strip()
        except:
            pass
        return name

    def getSamples(self, files):
        return set(filter(lambda s: self.isSampleFile(s), files))

    def getMediaFiles(self, files):

        def test(s):
            return self.filesizeBetween(s, self.file_sizes['movie']) and getExt(s.lower()) in self.extensions['movie'] and not self.isSampleFile(s)

        return set(filter(test, files))

    def getMovieExtras(self, files):
        return set(filter(lambda s: getExt(s.lower()) in self.extensions['movie_extra'], files))

    def getDVDFiles(self, files):
        def test(s):
            return self.isDVDFile(s)

        return set(filter(test, files))

    def getSubtitles(self, files):
        return set(filter(lambda s: getExt(s.lower()) in self.extensions['subtitle'], files))

    def getSubtitlesExtras(self, files):
        return set(filter(lambda s: getExt(s.lower()) in self.extensions['subtitle_extra'], files))

    def getNfo(self, files):
        return set(filter(lambda s: getExt(s.lower()) in self.extensions['nfo'], files))

    def getTrailers(self, files):

        def test(s):
            return re.search(r'(^|[\W_])trailer\d*[\W_]', s.lower()) and self.filesizeBetween(s, self.file_sizes['trailer'])

        return set(filter(test, files))

    def getImages(self, files):

        def test(s):
            return getExt(s.lower()) in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tbn']
        files = set(filter(test, files))

        images = {
            'backdrop': set(filter(lambda s: re.search(r'(^|[\W_])fanart|backdrop\d*[\W_]', s.lower()) and self.filesizeBetween(s, self.file_sizes['backdrop']), files))
        }

        # Rest
        images['rest'] = files - images['backdrop']

        return images


    def isDVDFile(self, file_name):

        if list(set(file_name.lower().split(os.path.sep)) & set(['video_ts', 'audio_ts'])):
            return True

        for needle in ['vts_', 'video_ts', 'audio_ts', 'bdmv', 'certificate']:
            if needle in file_name.lower():
                return True

        return False

    def keepFile(self, filename):

        # ignoredpaths
        for i in self.ignored_in_path:
            if i in filename.lower():
                log.debug('Ignored "%s" contains "%s".', (filename, i))
                return False

        # All is OK
        return True

    def isSampleFile(self, filename):
        is_sample = re.search(r'(^|[\W_])sample\d*[\W_]', filename.lower())
        if is_sample: log.debug('Is sample file: %s', filename)
        return is_sample

    def filesizeBetween(self, file, file_size = None, cached_size_mb = None):
        if not file_size: file_size = []

        try:
            sz = cached_size_mb if cached_size_mb is not None else self.getFileSize(file)
            return file_size.get('min', 0) < sz < file_size.get('max', 100000)
        except:
            log.error('Couldn\'t get filesize of %s.', file)

        return False

    def getFileSize(self, file):
        try:
            return os.path.getsize(file) / 1024 / 1024
        except:
            return None

    def createStringIdentifier(self, file_path, folder = '', exclude_filename = False):

        identifier = file_path.replace(folder, '').lstrip(os.path.sep) # root folder
        identifier = os.path.splitext(identifier)[0] # ext

        # Exclude file name path if needed (f.e. for DVD files)
        if exclude_filename:
            identifier = identifier[:len(identifier) - len(os.path.split(identifier)[-1])]

        # Make sure the identifier is lower case as all regex is with lower case tags
        identifier = identifier.lower()

        try:
            path_split = splitString(identifier, os.path.sep)
            identifier = path_split[-2] if len(path_split) > 1 and len(path_split[-2]) > len(path_split[-1]) else path_split[-1] # Only get filename
        except: pass

        # multipart
        identifier = self.removeMultipart(identifier)

        # remove cptag
        identifier = self.removeCPTag(identifier)

        # simplify the string
        identifier = simplifyString(identifier)

        year = self.findYear(file_path)

        # groups, release tags, scenename cleaner
        identifier = re.sub(self.clean, '::', identifier).strip(':')

        # Year
        if year and identifier[:4] != year:
            split_by = ':::' if ':::' in identifier else year
            identifier = '%s %s' % (identifier.split(split_by)[0].strip(), year)
        else:
            identifier = identifier.split('::')[0]

        # Remove duplicates
        out = []
        for word in identifier.split():
            if not word in out:
                out.append(word)

        identifier = ' '.join(out)

        return simplifyString(identifier)


    def removeMultipart(self, name):
        for regex in self.multipart_regex:
            try:
                found = re.sub(regex, '', name)
                if found != name:
                    name = found
            except:
                pass
        return name

    def getPartNumber(self, name):
        for regex in self.multipart_regex:
            try:
                found = re.search(regex, name)
                if found:
                    return found.group(1)
                return 1
            except:
                pass
        return 1

    def getCodec(self, filename, codecs):
        codecs = map(re.escape, codecs)
        try:
            codec = re.search('[^A-Z0-9](?P<codec>' + '|'.join(codecs) + ')[^A-Z0-9]', filename, re.I)
            return (codec and codec.group('codec')) or ''
        except:
            return ''

    def getResolution(self, filename):
        try:
            for key in self.resolutions:
                if key in filename.lower() and key != 'default':
                    return self.resolutions[key]
        except:
            pass

        return self.resolutions['default']

    def getGroup(self, file):
        try:
            match = re.findall(r'\-([A-Z0-9]+)[\.\/]', file, re.I)
            return match[-1] or ''
        except:
            return ''

    def getSourceMedia(self, file):
        for media in self.source_media:
            for alias in self.source_media[media]:
                if alias in file.lower():
                    return media

        return None

    def findYear(self, text):

        # Search year inside () or [] first
        matches = re.findall(r'(\(|\[)(?P<year>19[0-9]{2}|20[0-9]{2})(\]|\))', text)
        if matches:
            return matches[-1][1]

        # Search normal
        matches = re.findall('(?P<year>19[0-9]{2}|20[0-9]{2})', text)
        if matches:
            return matches[-1]

        return ''

    def getReleaseNameYear(self, release_name, file_name = None):

        release_name = release_name.strip(' .-_')

        # Use guessit first
        guess = {}
        if file_name:
            try:
                guessit = guess_movie_info(toUnicode(file_name))
                if guessit.get('title') and guessit.get('year'):
                    guess = {
                        'name': guessit.get('title'),
                        'year': guessit.get('year'),
                    }
            except:
                log.debug('Could not detect via guessit "%s": %s', (file_name, traceback.format_exc()))

        # Backup to simple
        release_name = os.path.basename(release_name.replace('\\', '/'))
        cleaned = ' '.join(re.split(r'\W+', simplifyString(release_name)))
        cleaned = re.sub(self.clean, ' ', cleaned)

        year = None
        for year_str in [file_name, release_name, cleaned]:
            if not year_str: continue
            year = self.findYear(year_str)
            if year:
                break

        cp_guess = {}

        if year:  # Split name on year
            try:
                movie_name = cleaned.rsplit(year, 1).pop(0).strip()
                if movie_name:
                    cp_guess = {
                        'name': movie_name,
                        'year': int(year),
                    }
            except:
                pass

        if not cp_guess:  # Split name on multiple spaces
            try:
                movie_name = cleaned.split('  ').pop(0).strip()
                cp_guess = {
                    'name': movie_name,
                    'year': int(year) if movie_name[:4] != year else 0,
                }
            except:
                pass

        if cp_guess.get('year') == guess.get('year') and len(cp_guess.get('name', '')) > len(guess.get('name', '')):
            cp_guess['other'] = guess
            return cp_guess
        elif guess == {}:
            cp_guess['other'] = guess
            return cp_guess

        guess['other'] = cp_guess
        return guess
