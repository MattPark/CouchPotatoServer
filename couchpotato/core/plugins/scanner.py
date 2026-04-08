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
from guessit import guessit as guessit_parse
from pymediainfo import MediaInfo


log = CPLog(__name__)

autoload = 'Scanner'

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
        # pymediainfo format strings -> display names
        'AC-3': 'AC3',
        'E-AC-3': 'EAC3',
        'DTS': 'DTS',
        'DTS-HD MA': 'DTS-HD MA',
        'DTS-HD': 'DTS-HD',
        'AAC': 'AAC',
        'AAC LC': 'AAC',
        'FLAC': 'FLAC',
        'MLP FBA': 'TrueHD',  # Dolby TrueHD
        'MPEG Audio': 'MP3',
        'PCM': 'PCM',
        'Vorbis': 'Vorbis',
        'Opus': 'Opus',
        'WMA': 'WMA',
        'TTA': 'TTA1',
    }

    video_codec_map = {
        # pymediainfo format strings -> display names
        'AVC': 'H264',
        'HEVC': 'x265',
        'MPEG-4 Visual': 'MPEG4',
        'MPEG Video': 'MPEG2',
        'VP8': 'VP8',
        'VP9': 'VP9',
        'AV1': 'AV1',
        'VC-1': 'VC1',
        'Theora': 'Theora',
    }

    source_media = {
        'Blu-ray': ['bluray', 'blu-ray', 'brrip', 'br-rip'],
        'HD DVD': ['hddvd', 'hd-dvd'],
        'DVD': ['dvd'],
        'HDTV': ['hdtv']
    }

    edition_map = {
        "Director's Cut": [('directors', 'cut'), ('directors', 'edition')],
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

        # Thread pool sizes (configurable via Settings > Manage > Scanner Threading)
        self.walk_workers = tryInt(self.conf('walk_workers', default=16))
        self.process_workers = tryInt(self.conf('process_workers', default=8))
        self.notify_workers = tryInt(self.conf('notify_workers', default=4))

    def scan(self, folder = None, files = None, release_download = None, simple = False, newer_than = 0, return_ignored = True, check_file_date = True, on_found = None, on_walk_progress = None):

        folder = sp(folder)

        if not folder or not os.path.isdir(folder):
            log.error('Folder doesn\'t exists: %s', folder)
            return {}

        # Phase A: Collect and categorize files
        if not files:
            log.info('Walking folder with %d parallel workers: %s' % (self.walk_workers, folder))
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

        # Phase C: Filter still-unpacking files (check_file_date) and
        # apply newer_than as a safety net.  Phase A (parallel walk) already
        # skips unchanged directories, but this second filter catches any
        # groups that slipped through (e.g. dir mtime was updated by a
        # non-content change).  This is the same approach the original
        # pre-parallelization code used — filter AFTER grouping but BEFORE
        # the expensive Phase D (TMDB lookups / metadata).
        valid_files = {}
        skipped_by_newer = 0
        while True and not self.shuttingDown():
            try:
                identifier, group = movie_files.popitem()
            except KeyError:
                break

            # Check if movie is fresh and maybe still unpacking, ignore files newer than 1 minute
            if check_file_date:
                files_too_new, time_string = self.checkFilesChanged(group['unsorted_files'])
                if files_too_new:
                    log.info('Files seem to be still unpacking or just unpacked (created on %s), ignoring for now: %s', (time_string, identifier))

                    # Delete the unsorted list
                    del group['unsorted_files']

                    continue

            # Only process movies newer than the last scan time.
            # Check only primary movie files (not companion subs/nfos/images
            # which may be updated by external media servers like Plex/Emby).
            if newer_than and newer_than > 0:
                has_new_files = False
                primary_count = group.get('primary_count', len(group['unsorted_files']))
                for cur_file in group['unsorted_files'][:primary_count]:
                    file_time = self.getFileTimes(cur_file)
                    if file_time[0] > newer_than:
                        has_new_files = True
                        break

                if not has_new_files:
                    del group['unsorted_files']
                    skipped_by_newer += 1
                    continue

            valid_files[identifier] = group

        if skipped_by_newer > 0:
            log.info('Quick scan Phase C: skipped %d groups with no files newer than %s' % (skipped_by_newer, time.ctime(newer_than)))

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

        # Quick scan fast-path: when newer_than is set, do a lightweight scan
        # to check if ANY movie-sized file is newer BEFORE collecting full stat data.
        # This avoids the expensive quality.guess / categorize work for 99%+ of
        # unchanged folders.  The scandir walk itself is still needed (we must stat
        # each file to check timestamps), but we bail out of the _categorize_ phase
        # if nothing qualifies — which is the real saving since quality.guess fires
        # events and does regex work.
        if newer_than and newer_than > 0:
            min_movie_bytes = self.file_sizes['movie'].get('min', 0) * 1024 * 1024
            has_new = False
            all_files = []
            for file_path, st in self._scandir_recursive(subdir_path):
                all_files.append((sp(file_path), st))
                if not has_new and st.st_size >= min_movie_bytes:
                    # Only check mtime — ctime changes on chmod/chown/ACL which
                    # are irrelevant and cause false positives on NAS volumes.
                    if st.st_mtime > newer_than:
                        has_new = True
            if not has_new:
                return movie_files, leftovers, stats
            # Fall through to categorize using already-collected all_files
        else:
            # Full scan — collect all files via scandir
            all_files = []
            for file_path, stat_result in self._scandir_recursive(subdir_path):
                all_files.append((sp(file_path), stat_result))

        if self.shuttingDown() or not all_files:
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
        Splits top-level subdirectories across walk_workers threads, each with
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

        log.info('Scanning %d top-level folders in %s with %d workers' % (total_dirs, folder, self.walk_workers))

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
            with ThreadPoolExecutor(max_workers=self.walk_workers) as pool:
                future_to_subdir = {}
                dirs_skipped = 0
                for subdir in top_level_subdirs:
                    if self.shuttingDown():
                        break

                    # Quick scan optimization: check directory mtime/ctime before
                    # submitting to the thread pool.  A directory's mtime updates when
                    # files are added, removed, or renamed inside it — which covers all
                    # relevant changes in a movie library.  This avoids the expensive
                    # recursive scandir+stat of every file inside unchanged directories.
                    if newer_than and newer_than > 0:
                        try:
                            dir_st = os.stat(subdir)
                            # Only check mtime — ctime (inode change time) updates on
                            # chmod/chown/ACL changes which are irrelevant for content.
                            # A bulk permission change would make ctime recent on every
                            # directory, defeating the quick-scan skip entirely.
                            if dir_st.st_mtime < newer_than:
                                dirs_skipped += 1
                                with progress_lock:
                                    dirs_done[0] += 1
                                    if on_walk_progress:
                                        on_walk_progress(dirs_done[0])
                                continue
                        except OSError:
                            pass  # Can't stat — let the worker handle it

                    future = pool.submit(self._walk_and_categorize_subdir, subdir, folder, newer_than)
                    future_to_subdir[future] = subdir

                if dirs_skipped > 0:
                    log.info('Quick scan: skipped %d/%d unchanged directories' % (dirs_skipped, total_dirs))

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
                # Same quick-scan dir-level skip as the parallel path
                if newer_than and newer_than > 0:
                    try:
                        dir_st = os.stat(subdir)
                        if dir_st.st_mtime < newer_than:
                            dirs_done[0] += 1
                            if on_walk_progress:
                                on_walk_progress(dirs_done[0])
                            continue
                    except OSError:
                        pass
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
                except Exception:
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

    def _parallel_process_groups(self, valid_files, folder, release_download, simple,
                                  return_ignored, ignored_identifiers, on_found, total_found):
        """Process movie groups in parallel using self.process_workers threads.
        Workers do the heavy lifting (classify + pymediainfo + TMDB) and push results
        to a queue. self.notify_workers threads drain the queue and call on_found
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
            for i in range(self.notify_workers):
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
            with ThreadPoolExecutor(max_workers=self.process_workers) as pool:
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
                except Exception:
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
        data['edition'] = self.getEdition(filename)
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

    # Words that should not be treated as edition names when followed by Cut/Edition
    _edition_exclude = {
        'blu', 'ray', 'web', 'hd', 'sd', 'uhd', 'dvd', 'bd', 'hdr', 'tax',
        'pay', 'price', 'budget', 'the', 'a', 'an', 'no', 'rough', 'first',
        'clean',
    }

    def getEdition(self, filename):
        """Detect edition/cut info from filename (e.g. Director's Cut, Extended, IMAX).

        Only searches AFTER the year in the filename to avoid false positives
        when edition words appear in the movie title (e.g. "Redux Redux (2024)").
        """
        filename = ss(filename)
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
        for key in self.edition_map:
            tags = self.edition_map.get(key, [])
            for tag in tags:
                if isinstance(tag, tuple) and '.'.join(tag) in search_joined:
                    log.debug('Found edition %s in %s', (key, filename))
                    return key
                elif isinstance(tag, str) and tag.lower() in search_words:
                    log.debug('Found edition %s in %s', (key, filename))
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
            # Reject if the word right before Cut/Edition is a known non-edition word
            last_word = name_part.split()[-1].lower() if name_part else ''
            if last_word and last_word not in self._edition_exclude:
                edition = '%s %s' % (name_part.title(), kind.title())
                log.debug('Found edition (fallback) %s in %s', (edition, filename))
                return edition

        return ''

    def getMeta(self, filename):

        try:
            mi = MediaInfo.parse(filename)
            video_tracks = [t for t in mi.tracks if t.track_type == 'Video']
            audio_tracks = [t for t in mi.tracks if t.track_type == 'Audio']
            general_tracks = [t for t in mi.tracks if t.track_type == 'General']

            if not video_tracks:
                log.debug('No video tracks found in %s', filename)
                return {}

            vt = video_tracks[0]

            # Video codec
            vc = self.video_codec_map.get(vt.format, vt.format or '')

            # Audio codec
            ac = ''
            if audio_tracks:
                at = audio_tracks[0]
                ac = self.audio_codec_map.get(at.format or '', at.format or '')

            # Find title in video/general headers
            titles = []

            # Check general (container-level) title
            try:
                gen_title = general_tracks[0].title if general_tracks else None
                if gen_title and self.findYear(gen_title):
                    titles.append(ss(gen_title))
            except Exception:
                log.error('Failed getting title from meta: %s', traceback.format_exc())

            # Check video track titles
            for video in video_tracks:
                try:
                    if video.title and self.findYear(video.title):
                        titles.append(ss(video.title))
                except Exception:
                    log.error('Failed getting title from meta: %s', traceback.format_exc())

            return {
                'titles': list(set(titles)),
                'video': vc,
                'audio': ac,
                'resolution_width': tryInt(vt.width),
                'resolution_height': tryInt(vt.height),
                'audio_channels': float(audio_tracks[0].channel_s) if audio_tracks and audio_tracks[0].channel_s else 0,
            }
        except Exception:
            log.debug('Failed parsing metadata: %s %s', (filename, traceback.format_exc()))

        return {}

    def getSubtitleLanguage(self, group):
        detected_languages = {}

        # Detect external subtitle languages from filenames
        # e.g., "Movie.en.srt" -> lang code "en", "Movie.eng.srt" -> "eng"
        paths = None
        try:
            paths = group['files']['movie']
            movie_dir = os.path.dirname(sp(paths[0])) if paths else None

            if movie_dir and not group['is_dvd']:
                subtitle_exts = {'.srt', '.sub', '.ass', '.ssa', '.smi', '.vtt'}
                try:
                    dir_files = os.listdir(movie_dir)
                except OSError:
                    dir_files = []

                for fname in dir_files:
                    fpath = os.path.join(movie_dir, fname)
                    ext = os.path.splitext(fname)[1].lower()
                    if ext in subtitle_exts and fpath not in paths:
                        # Try to extract language code: "movie.en.srt" -> "en"
                        name_no_ext = os.path.splitext(fname)[0]
                        parts = name_no_ext.rsplit('.', 1)
                        if len(parts) == 2:
                            lang_code = parts[1].lower()
                            if len(lang_code) in (2, 3) and lang_code.isalpha():
                                detected_languages[fpath] = [lang_code]
        except Exception:
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
            except Exception:
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
            except Exception:
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
            except Exception:
                pass

        # Search based on identifiers
        if not imdb_id:
            for identifier in group['identifiers']:

                if len(identifier) > 2:
                    try: filename = list(group['files'].get('movie'))[0]
                    except (TypeError, IndexError): filename = None

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
            except Exception:
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
        except Exception:
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
        except Exception:
            log.error('Couldn\'t get filesize of %s.', file)

        return False

    def getFileSize(self, file):
        try:
            return os.path.getsize(file) / 1024 / 1024
        except OSError:
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
        except Exception: pass

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
            if word not in out:
                out.append(word)

        identifier = ' '.join(out)

        return simplifyString(identifier)


    def removeMultipart(self, name):
        for regex in self.multipart_regex:
            try:
                found = re.sub(regex, '', name)
                if found != name:
                    name = found
            except Exception:
                pass
        return name

    def getPartNumber(self, name):
        for regex in self.multipart_regex:
            try:
                found = re.search(regex, name)
                if found:
                    return found.group(1)
                return 1
            except Exception:
                pass
        return 1

    def getCodec(self, filename, codecs):
        codecs = map(re.escape, codecs)
        try:
            codec = re.search('[^A-Z0-9](?P<codec>' + '|'.join(codecs) + ')[^A-Z0-9]', filename, re.I)
            return (codec and codec.group('codec')) or ''
        except Exception:
            return ''

    def getResolution(self, filename):
        try:
            for key in self.resolutions:
                if key in filename.lower() and key != 'default':
                    return self.resolutions[key]
        except Exception:
            pass

        return self.resolutions['default']

    def getGroup(self, file):
        try:
            match = re.findall(r'\-([A-Z0-9]+)[\.\/]', file, re.I)
            return match[-1] or ''
        except Exception:
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
                guessit = guessit_parse(toUnicode(file_name), {'type': 'movie'})
                if guessit.get('title') and guessit.get('year'):
                    guess = {
                        'name': guessit.get('title'),
                        'year': guessit.get('year'),
                    }
            except Exception:
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
            except Exception:
                pass

        if not cp_guess:  # Split name on multiple spaces
            try:
                movie_name = cleaned.split('  ').pop(0).strip()
                cp_guess = {
                    'name': movie_name,
                    'year': int(year) if movie_name[:4] != year else 0,
                }
            except Exception:
                pass

        if cp_guess.get('year') == guess.get('year') and len(cp_guess.get('name', '')) > len(guess.get('name', '')):
            cp_guess['other'] = guess
            return cp_guess
        elif guess == {}:
            cp_guess['other'] = guess
            return cp_guess

        guess['other'] = cp_guess
        return guess


config = [{
    'name': 'scanner',
    'groups': [
        {
            'tab': 'manage',
            'name': 'scanner',
            'label': 'Scanner Threading',
            'description': 'Thread pool sizes for library scanning.',
            'options': [
                {
                    'name': 'walk_workers',
                    'label': 'Walk Workers',
                    'type': 'int',
                    'default': 16,
                    'advanced': True,
                    'description': 'Parallel threads for directory walking (I/O-bound). Higher values speed up initial file discovery on NAS.',
                },
                {
                    'name': 'process_workers',
                    'label': 'Process Workers',
                    'type': 'int',
                    'default': 8,
                    'advanced': True,
                    'description': 'Parallel threads for movie identification (metadata + TMDB lookups).',
                },
                {
                    'name': 'notify_workers',
                    'label': 'Notify Workers',
                    'type': 'int',
                    'default': 4,
                    'advanced': True,
                    'description': 'Parallel threads for DB writes and API updates after identification.',
                },
            ],
        },
    ],
}]
