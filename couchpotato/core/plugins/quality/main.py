from math import fabs, ceil
import traceback
import re
import os

from couchpotato.core.db import RecordNotFound
from couchpotato import get_db
from couchpotato.api import addApiView
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import toUnicode, ss
from couchpotato.core.helpers.variable import mergeDicts, getExt, tryInt, splitString, tryFloat
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin


log = CPLog(__name__)


class QualityPlugin(Plugin):

    _database = {}

    qualities = [
        {'identifier': '2160p', 'hd': True, 'allow_3d': True, 'size': (10000, 650000), 'median_size': 20000, 'label': '2160p', 'width': 3840, 'height': 2160, 'alternative': [], 'allow': [], 'ext': ['mkv'], 'tags': ['x264', 'h264', 'x265', 'h265', 'hevc', '2160', '4k', 'uhd']},
        {'identifier': '1080p', 'hd': True, 'allow_3d': True, 'size': (4000, 20000), 'median_size': 10000, 'label': '1080p', 'width': 1920, 'height': 1080, 'alternative': [], 'allow': [], 'ext': ['mkv', 'm2ts', 'ts'], 'tags': ['m2ts', 'x264', 'h264', 'x265', 'h265', 'hevc', '1080']},
        {'identifier': '720p', 'hd': True, 'allow_3d': True, 'size': (3000, 10000), 'median_size': 5500, 'label': '720p', 'width': 1280, 'height': 720, 'alternative': [], 'allow': [], 'ext': ['mkv', 'ts'], 'tags': ['x264', 'h264', 'x265', 'h265', '720']},
        {'identifier': 'sd', 'hd': False, 'allow_3d': False, 'size': (600, 2400), 'median_size': 1500, 'label': 'SD', 'width': 720, 'alternative': [], 'allow': [], 'ext': ['avi', 'mp4', 'iso', 'img', 'vob'], 'tags': ['xvid', 'divx', 'pal', 'ntsc', 'video_ts', 'audio_ts', 'dvd9']},
        {'identifier': 'screener', 'hd': False, 'allow_3d': False, 'size': (600, 1600), 'median_size': 700, 'label': 'Screener', 'alternative': ['dvdscr', 'dvdscreener', 'hdscr', 'ppvrip'], 'allow': [], 'ext': [], 'tags': []},
        {'identifier': 'r5', 'hd': False, 'allow_3d': False, 'size': (600, 1000), 'median_size': 700, 'label': 'R5', 'alternative': ['r6'], 'allow': [], 'ext': [], 'tags': []},
        {'identifier': 'tc', 'hd': False, 'allow_3d': False, 'size': (600, 1000), 'median_size': 700, 'label': 'TeleCine', 'alternative': ['telecine'], 'allow': [], 'ext': [], 'tags': []},
        {'identifier': 'ts', 'hd': False, 'allow_3d': False, 'size': (600, 1000), 'median_size': 700, 'label': 'TeleSync', 'alternative': ['telesync', 'hdts'], 'allow': [], 'ext': [], 'tags': []},
        {'identifier': 'cam', 'hd': False, 'allow_3d': False, 'size': (600, 1000), 'median_size': 700, 'label': 'Cam', 'alternative': ['camrip', 'hdcam'], 'allow': [], 'ext': [], 'tags': []},
    ]

    # Pre-release identifiers: if detected, they OVERRIDE resolution
    pre_releases = ['cam', 'ts', 'tc', 'r5', 'screener']

    # Pre-release tags: map filename tokens to quality identifier
    pre_release_tags = {
        'cam': ['cam', 'camrip', 'hdcam'],
        'ts': ['ts', 'telesync', 'hdts'],
        'tc': ['tc', 'telecine'],
        'r5': ['r5', 'r6'],
        'screener': ['screener', 'dvdscr', 'dvdscreener', 'hdscr', 'scr', 'ppvrip'],
    }

    # Source rankings within a resolution tier (lower rank = better source)
    # Tags are checked: tuples first (across ALL sources for specificity), then single words in rank order
    # NOTE: bare 'web' is NOT here — handled as fallback after all sources checked
    source_rankings = {
        'remux':  {'rank': 1, 'tags': ['remux', 'bdremux']},
        'bluray': {'rank': 2, 'tags': ['bluray', 'bdrip', 'brrip', 'bd50', 'bd25', 'bdmv', ('blu', 'ray'), ('br', 'disk'), ('br', 'disc')]},
        'webdl':  {'rank': 3, 'tags': ['webdl', ('web', 'dl')]},
        'webrip': {'rank': 4, 'tags': ['webrip', ('web', 'rip'), 'webcap']},
        'hdtv':   {'rank': 5, 'tags': ['hdtv', 'pdtv', 'dsr', 'satrip', 'dvbrip', 'hdrip']},
        'dvd':    {'rank': 6, 'tags': ['dvdrip', ('dvd', 'rip'), 'dvdr', ('dvd', 'r'), 'dvd9', 'video_ts', 'audio_ts', 'pal', 'ntsc']},
    }

    # Codec detection tags
    codec_tags = {
        'x265': ['x265', 'h265', 'hevc'],
        'x264': ['x264', 'h264', 'avc'],
        'av1': ['av1'],
        'xvid': ['xvid'],
        'divx': ['divx'],
        'mpeg2': ['mpeg2'],
        'vc1': ['vc1', 'vc-1'],
    }

    threed_tags = {
        'sbs': [('half', 'sbs'), 'hsbs', ('full', 'sbs'), 'fsbs'],
        'ou': [('half', 'ou'), 'hou', ('full', 'ou'), 'fou'],
        '3d': ['2d3d', '3d2d', '3d'],
    }

    # Old quality identifiers that need migration
    _old_quality_identifiers = {'brrip', 'dvdr', 'dvdrip', 'bd50', 'scr'}

    cached_qualities = None
    cached_order = None

    def __init__(self):
        addEvent('quality.all', self.all)
        addEvent('quality.single', self.single)
        addEvent('quality.guess', self.guess)
        addEvent('quality.pre_releases', self.preReleases)
        addEvent('quality.order', self.getOrder)
        addEvent('quality.ishigher', self.isHigher)
        addEvent('quality.isfinish', self.isFinish)
        addEvent('quality.fill', self.fill)

        addApiView('quality.size.save', self.saveSize)
        addApiView('quality.list', self.allView, docs = {
            'desc': 'List all available qualities',
            'return': {'type': 'object', 'example': """{
            'success': True,
            'list': array, qualities
}"""}
        })
        addApiView('quality.test', self.testView)

        addEvent('app.initialize', self.fill, priority = 10)
        addEvent('app.load', self.migrateQualities, priority = 100)
        addEvent('app.load', self.fillBlank, priority = 120)

        addEvent('app.test', self.doTest)

        self.order = []
        self.addOrder()

    def addOrder(self):
        self.order = []
        for q in self.qualities:
            self.order.append(q.get('identifier'))

    def getOrder(self):
        return self.order

    def preReleases(self):
        return self.pre_releases

    def allView(self, **kwargs):

        return {
            'success': True,
            'list': self.all()
        }

    def all(self):

        if self.cached_qualities:
            return self.cached_qualities

        db = get_db()

        temp = []
        for quality in self.qualities:
            try:
                quality_doc = db.get('quality', quality.get('identifier'), with_doc = True)['doc']
                q = mergeDicts(quality, quality_doc)
                temp.append(q)
            except RecordNotFound:
                # Quality doc doesn't exist yet, use defaults
                temp.append(quality.copy())

        if len(temp) == len(self.qualities):
            self.cached_qualities = temp

        return temp

    def single(self, identifier = ''):

        db = get_db()
        quality_dict = {}

        try:
            quality = db.get('quality', identifier, with_doc = True)['doc']
        except RecordNotFound:
            log.error("Unable to find '%s' in the quality DB", identifier)
            quality = None

        if quality:
            quality_dict = mergeDicts(self.getQuality(quality['identifier']) or {}, quality)

        return quality_dict

    def getQuality(self, identifier):

        for q in self.qualities:
            if identifier == q.get('identifier'):
                return q

    def saveSize(self, **kwargs):

        try:
            db = get_db()
            quality = db.get('quality', kwargs.get('identifier'), with_doc = True)

            if quality:
                quality['doc'][kwargs.get('value_type')] = tryInt(kwargs.get('value'))
                db.update(quality['doc'])

            self.cached_qualities = None

            return {
                'success': True
            }
        except:
            log.error('Failed: %s', traceback.format_exc())

        return {
            'success': False
        }

    def fillBlank(self):
        db = get_db()

        try:
            existing = list(db.all('quality'))
            if len(self.qualities) > len(existing):
                log.error('Filling in new qualities')
                self.fill(reorder = True)
        except:
            log.error('Failed filling quality database with new qualities: %s', traceback.format_exc())

    def fill(self, reorder = False):

        try:
            db = get_db()

            order = 0
            for q in self.qualities:

                existing = None
                try:
                    existing = db.get('quality', q.get('identifier'), with_doc = reorder)
                except RecordNotFound:
                    pass

                if not existing:
                    db.insert({
                        '_t': 'quality',
                        'order': order,
                        'identifier': q.get('identifier'),
                        'size_min': tryInt(q.get('size')[0]),
                        'size_max': tryInt(q.get('size')[1]),
                    })

                    log.info('Creating profile: %s', q.get('label'))
                    db.insert({
                        '_t': 'profile',
                        'order': order + 20,  # Make sure it goes behind other profiles
                        'core': True,
                        'qualities': [q.get('identifier')],
                        'label': toUnicode(q.get('label')),
                        'finish': [True],
                        'wait_for': [0],
                    })
                elif reorder:
                    log.info2('Updating quality order')
                    existing['doc']['order'] = order
                    db.update(existing['doc'])

                order += 1

            return True
        except:
            log.error('Failed: %s', traceback.format_exc())

        return False

    # -------------------------------------------------------------------------
    # Quality guessing — direct extraction (replaces old scoring system)
    # -------------------------------------------------------------------------

    def guess(self, files, extra = None, size = None, use_cache = True):
        if not extra: extra = {}

        # Create hash for cache
        cache_key = str([f.replace('.' + getExt(f), '') if len(getExt(f)) < 4 else f for f in files])
        if use_cache:
            cached = self.getCache(cache_key)
            if cached and len(extra) == 0:
                return cached

        # Use metadata titles as extra check
        all_files = list(files)
        if extra and extra.get('titles'):
            all_files.extend(extra.get('titles'))

        # Collect all words from all files, keeping track for 3D detection
        all_words = []
        threed_words_combined = []

        for cur_file in all_files:
            words = re.split(r'\W+', cur_file.lower())

            # Separate extension from body words
            # Critical: .ts extension must NOT be treated as TeleSync tag
            # Only strip last word if filename has a file extension (dot followed by short suffix)
            has_extension = bool(re.search(r'\.\w{2,4}$', cur_file))
            body_words = words[:-1] if has_extension and len(words) > 1 else words
            all_words.extend(body_words)

            # For 3D detection, strip movie name
            name_year = fireEvent('scanner.name_year', cur_file, file_name = cur_file, single = True)
            if name_year and name_year.get('name'):
                split_name = splitString(name_year.get('name'), ' ')
                threed_words = [x for x in body_words if x not in split_name]
            else:
                threed_words = body_words
            threed_words_combined.extend(threed_words)

        # Phase 1: Detect pre-release (overrides everything)
        pre_release = self._detect_pre_release(all_words)

        # Phase 2: Detect resolution
        resolution = self._detect_resolution(all_words, extra)

        # Phase 3: Detect source
        source = self._detect_source(all_words)

        # Phase 4: Detect codec
        codec = self._detect_codec(all_words)

        # Phase 5: Determine quality_id
        if pre_release:
            quality_id = pre_release
        elif resolution:
            quality_id = resolution
        elif source:
            # Source-based inference when no resolution token present
            if source == 'dvd':
                quality_id = 'sd'
            elif source in ('bluray', 'remux'):
                # BluRay/remux without resolution token — use size heuristic, default 1080p
                quality_id = self._resolution_from_size(size) or '1080p'
            elif source in ('webdl', 'webrip'):
                # WEB content without resolution — use size heuristic, default 720p
                quality_id = self._resolution_from_size(size) or '720p'
            elif source == 'hdtv':
                quality_id = self._resolution_from_size(size) or '720p'
            else:
                quality_id = self._resolution_from_size(size) or 'sd'
        else:
            # No tags at all — try resolution from metadata, then size heuristics
            quality_id = self._resolution_from_size(size)
            if not quality_id:
                # Last resort: check metadata resolution
                if extra:
                    rw = extra.get('resolution_width', 0)
                    rh = extra.get('resolution_height', 0)
                    if rw >= 3200 or rh >= 2000:
                        quality_id = '2160p'
                    elif rw >= 1800 or rh >= 900:
                        quality_id = '1080p'
                    elif rw >= 1100 or rh >= 600:
                        quality_id = '720p'
                    elif rw > 0 or rh > 0:
                        quality_id = 'sd'

        if not quality_id:
            return None

        # Look up the quality definition
        quality_def = self.getQuality(quality_id)
        if not quality_def:
            return None

        # Build result with new fields
        result = quality_def.copy()
        result['is_3d'] = False
        result['source'] = source or 'unknown'
        result['codec'] = codec

        # Detect 3D
        if quality_def.get('allow_3d'):
            for key in self.threed_tags:
                tags = self.threed_tags.get(key, [])
                for tag in tags:
                    if isinstance(tag, tuple):
                        if len(set(threed_words_combined) & set(tag)) == len(tag):
                            result['is_3d'] = True
                            break
                    elif tag in threed_words_combined:
                        result['is_3d'] = True
                        break
                if result['is_3d']:
                    break

        return self.setCache(cache_key, result)

    def _detect_pre_release(self, words):
        """Check for pre-release tags. Returns quality identifier or None."""
        for quality_id, tags in self.pre_release_tags.items():
            for tag in tags:
                if tag in words:
                    return quality_id
        return None

    def _detect_resolution(self, words, extra = None):
        """Detect resolution from words and metadata. Returns quality identifier or None."""
        # Check explicit resolution tokens in words
        for word in words:
            if word in ('2160p', '4k', 'uhd', '2160'):
                return '2160p'
            if word in ('1080p', '1080i', '1080'):
                return '1080p'
            if word in ('720p', '720i', '720'):
                return '720p'
            if word in ('480p', '480i', '576p', '576i'):
                return 'sd'

        # Check metadata resolution
        if extra:
            rw = extra.get('resolution_width', 0)
            rh = extra.get('resolution_height', 0)
            if rw >= 3200 or rh >= 2000:
                return '2160p'
            if rw >= 1800 or rh >= 900:
                return '1080p'
            if rw >= 1100 or rh >= 600:
                return '720p'
            if 0 < rw <= 1100 or 0 < rh < 600:
                return 'sd'

        return None

    def _detect_source(self, words):
        """Detect source from words. Returns source identifier or None.

        Tuple tags are checked across ALL sources first (for specificity),
        then single-word tags in rank order. Bare 'web' is a fallback for webdl.
        """
        word_set = set(words)

        # Pass 1: Check tuple tags across ALL sources (most specific first)
        for source_id, info in sorted(self.source_rankings.items(), key=lambda x: x[1]['rank']):
            for tag in info['tags']:
                if isinstance(tag, tuple):
                    if len(set(tag) & word_set) == len(tag):
                        return source_id

        # Pass 2: Check single-word tags in rank order
        for source_id, info in sorted(self.source_rankings.items(), key=lambda x: x[1]['rank']):
            for tag in info['tags']:
                if isinstance(tag, str) and tag in word_set:
                    return source_id

        # Fallback: bare 'web' → webdl
        if 'web' in word_set:
            return 'webdl'

        return None

    def _detect_codec(self, words):
        """Detect codec from words. Returns codec identifier or None."""
        word_set = set(words)
        for codec_id, tags in self.codec_tags.items():
            for tag in tags:
                if tag in word_set:
                    return codec_id
        return None

    def _resolution_from_size(self, size):
        """Infer resolution from file size in MB. Returns quality identifier or None."""
        if not size:
            return None
        size = tryFloat(size)
        if size <= 0:
            return None

        if size > 15000:
            return '2160p'
        if size > 6000:
            return '1080p'
        if size > 2500:
            return '720p'
        if size > 100:
            return 'sd'

        return None

    # -------------------------------------------------------------------------
    # 3D detection (unchanged from original)
    # -------------------------------------------------------------------------

    def contains3D(self, quality, words, cur_file = ''):
        cur_file = ss(cur_file)

        for key in self.threed_tags:
            tags = self.threed_tags.get(key, [])

            for tag in tags:
                if isinstance(tag, tuple):
                    if len(set(words) & set(tag)) == len(tag):
                        log.debug('Found %s in %s', (tag, cur_file))
                        return 1, key
                elif tag in words:
                    log.debug('Found %s in %s', (tag, cur_file))
                    return 1, key

        return 0, None

    # -------------------------------------------------------------------------
    # Quality comparison (unchanged from original)
    # -------------------------------------------------------------------------

    def isFinish(self, quality, profile, release_age = 0):
        if not isinstance(profile, dict) or not profile.get('qualities'):
            # No profile so anything (scanned) is good enough
            return True

        try:
            index = [i for i, identifier in enumerate(profile['qualities']) if identifier == quality['identifier'] and bool(profile['3d'][i] if profile.get('3d') else False) == bool(quality.get('is_3d', False))][0]

            if index == 0 or (profile['finish'][index] and int(release_age) >= int(profile.get('stop_after', [0])[0])):
                return True

            return False
        except:
            return False

    def isHigher(self, quality, compare_with, profile = None):
        if not isinstance(profile, dict) or not profile.get('qualities'):
            profile = fireEvent('profile.default', single = True)

        # Try to find quality in profile, if not found: a quality we do not want is lower than anything else
        try:
            quality_order = [i for i, identifier in enumerate(profile['qualities']) if identifier == quality['identifier'] and bool(profile['3d'][i] if profile.get('3d') else 0) == bool(quality.get('is_3d', 0))][0]
        except:
            log.debug('Quality %s not found in profile identifiers %s', (quality['identifier'] + (' 3D' if quality.get('is_3d', 0) else ''), \
                [identifier + (' 3D' if (profile['3d'][i] if profile.get('3d') else 0) else '') for i, identifier in enumerate(profile['qualities'])]))
            return 'lower'

        # Try to find compare quality in profile, if not found: anything is higher than a not wanted quality
        try:
            compare_order = [i for i, identifier in enumerate(profile['qualities']) if identifier == compare_with['identifier'] and bool(profile['3d'][i] if profile.get('3d') else 0) == bool(compare_with.get('is_3d', 0))][0]
        except:
            log.debug('Compare quality %s not found in profile identifiers %s', (compare_with['identifier'] + (' 3D' if compare_with.get('is_3d', 0) else ''), \
                [identifier + (' 3D' if (profile['3d'][i] if profile.get('3d') else 0) else '') for i, identifier in enumerate(profile['qualities'])]))
            return 'higher'

        # Note to self: a lower number means higher quality
        if quality_order > compare_order:
            return 'lower'
        elif quality_order == compare_order:
            return 'equal'
        else:
            return 'higher'

    # -------------------------------------------------------------------------
    # Quality migration (old 12-tier → new 9-tier)
    # -------------------------------------------------------------------------

    def migrateQualities(self):
        """Migrate releases and profiles from old quality system to new.
        Runs at app.load priority 100 (after fill at priority 10).
        """
        try:
            db = get_db()

            # Check if migration is needed: look for any release with old quality identifiers
            needs_migration = False
            all_releases = list(db.all('release', with_doc=True))
            for r in all_releases:
                doc = r.get('doc', r)
                if doc.get('quality') in self._old_quality_identifiers:
                    needs_migration = True
                    break

            if not needs_migration:
                # Also check if old quality docs exist that need cleanup
                has_old_quality_docs = False
                for old_id in self._old_quality_identifiers:
                    try:
                        db.get('quality', old_id)
                        has_old_quality_docs = True
                        break
                    except RecordNotFound:
                        pass

                if not has_old_quality_docs:
                    return

                log.info('No releases to migrate but found old quality docs — cleaning up')

            if needs_migration:
                log.info('=== Starting quality system migration ===')
                migrated = 0
                failed = 0

                for r in all_releases:
                    doc = r.get('doc', r)
                    old_quality = doc.get('quality')

                    if old_quality not in self._old_quality_identifiers:
                        continue

                    new_quality = None
                    new_source = 'unknown'

                    if old_quality == 'scr':
                        new_quality = 'screener'
                    elif old_quality == 'dvdr':
                        new_quality = 'sd'
                        new_source = 'dvd'
                    elif old_quality == 'dvdrip':
                        new_quality = 'sd'
                        new_source = 'dvd'
                    elif old_quality == 'bd50':
                        new_quality = '1080p'
                        new_source = 'bluray'
                    elif old_quality == 'brrip':
                        # Need to re-guess from filename/info
                        new_quality, new_source = self._migrateBrrip(doc)

                    if new_quality:
                        doc['_old_quality'] = old_quality
                        doc['quality'] = new_quality
                        doc['source'] = new_source
                        db.update(doc)
                        migrated += 1
                    else:
                        failed += 1
                        log.warning('Failed to migrate release %s (old quality: %s)' % (doc.get('_id', '?'), old_quality))

                log.info('=== Release migration complete: %d migrated, %d failed ===' % (migrated, failed))

            # Migrate profiles
            self._migrateProfiles(db)

            # Clean up old quality docs from DB
            for old_id in self._old_quality_identifiers:
                try:
                    old_doc = db.get('quality', old_id, with_doc=True)['doc']
                    db.delete(old_doc)
                    log.info('Deleted old quality doc: %s' % old_id)
                except RecordNotFound:
                    pass
                except:
                    log.error('Failed deleting old quality doc %s: %s' % (old_id, traceback.format_exc()))

            # Clear caches
            self.cached_qualities = None
            self.cached_order = None

            # Flush DB
            db.compact()

            if needs_migration:
                # Bulk restatus all active movies so done releases now match profiles
                log.info('=== Running bulk restatus on active movies ===')
                try:
                    medias = fireEvent('media.with_status', 'active', single=True)
                    if medias:
                        restatus_count = 0
                        for media in medias:
                            try:
                                new_status = fireEvent('media.restatus', media['_id'], single=True)
                                if new_status == 'done':
                                    restatus_count += 1
                            except:
                                pass
                        log.info('Bulk restatus complete: %d movies changed to done' % restatus_count)
                except:
                    log.error('Bulk restatus failed: %s', traceback.format_exc())

                db.compact()
                log.info('=== Quality system migration finished ===')

        except:
            log.error('Quality migration failed: %s', traceback.format_exc())

    def _migrateBrrip(self, release_doc):
        """Migrate a brrip release by re-guessing from filename/info.
        Returns (new_quality, new_source) tuple.
        """
        # Try to get filename from files
        filename = None
        files = release_doc.get('files', {})
        if files:
            for file_type in ('movie', 'trailer', 'nfo'):
                file_list = files.get(file_type, [])
                if file_list:
                    filename = file_list[0]
                    break

        # Also try release info name
        info_name = None
        info = release_doc.get('info', {})
        if info:
            info_name = info.get('name')

        # Try to parse resolution from filename
        parse_target = filename or info_name
        if parse_target:
            words = re.split(r'\W+', parse_target.lower())
            body_words = words[:-1] if len(words) > 1 else words

            # Check for resolution tokens
            for word in body_words:
                if word in ('2160p', '4k', 'uhd', '2160'):
                    source = self._detect_source(body_words) or 'unknown'
                    return '2160p', source
                if word in ('1080p', '1080i', '1080'):
                    source = self._detect_source(body_words) or 'unknown'
                    return '1080p', source
                if word in ('720p', '720i', '720'):
                    source = self._detect_source(body_words) or 'unknown'
                    return '720p', source
                if word in ('480p', '480i', '576p', '576i'):
                    source = self._detect_source(body_words) or 'unknown'
                    return 'sd', source

            # No resolution token — detect source for context
            source = self._detect_source(body_words) or 'unknown'

            # If we found a source, use size to determine resolution
            size = info.get('size') if info else None
            if size:
                size = tryFloat(size)
                if size > 15000:
                    return '2160p', source
                elif size > 6000:
                    return '1080p', source
                elif size > 2500:
                    return '720p', source
                elif size > 100:
                    return 'sd', source

            # Filename but no resolution and no size — blind default
            return '720p', source

        # No filename at all — try size only
        size = None
        if info:
            size = info.get('size')
        if size:
            size = tryFloat(size)
            if size > 15000:
                return '2160p', 'unknown'
            elif size > 6000:
                return '1080p', 'unknown'
            elif size > 2500:
                return '720p', 'unknown'
            elif size > 100:
                return 'sd', 'unknown'

        # Absolute fallback — no filename, no size
        return '720p', 'unknown'

    def _migrateProfiles(self, db):
        """Migrate profiles: remap old quality identifiers to new ones.
        Deduplicates within each profile. Deletes colliding core profiles.

        Uses two-pass approach: first collect unmigrated core profiles as the
        authoritative set, then process migrated profiles and delete collisions.
        """
        log.info('=== Migrating profiles ===')

        # Mapping from old identifiers to new
        id_map = {
            'brrip': '1080p',
            'dvdrip': 'sd',
            'dvdr': 'sd',
            'bd50': '1080p',
            'scr': 'screener',
        }

        profiles = list(db.all('profile', with_doc=True))

        # Pass 1: Collect all core profiles that DON'T need migration
        # These are the authoritative set (e.g. the new "1080p" and "sd" core profiles)
        core_profiles_by_quality = {}
        needs_migration = []

        for p in profiles:
            doc = p.get('doc', p)
            qualities = doc.get('qualities', [])

            has_old = any(q in id_map for q in qualities)

            if not has_old:
                if doc.get('core'):
                    key = tuple(qualities)
                    if key not in core_profiles_by_quality:
                        core_profiles_by_quality[key] = doc
                    else:
                        # Two unmigrated core profiles with same qualities — delete the newer one
                        log.info('Deleting duplicate unmigrated core profile "%s" (collides with "%s")' % (doc.get('label', '?'), core_profiles_by_quality[key].get('label', '?')))
                        db.delete(doc)
            else:
                needs_migration.append(doc)

        # Pass 2: Process profiles that need migration
        for doc in needs_migration:
            qualities = doc.get('qualities', [])

            # Remap old identifiers
            for i, q in enumerate(qualities):
                if q in id_map:
                    qualities[i] = id_map[q]

            # Deduplicate qualities within profile, keeping most permissive settings
            seen = {}
            new_qualities = []
            new_finish = []
            new_wait_for = []
            new_stop_after = []
            new_3d = []

            for i, q in enumerate(qualities):
                if q in seen:
                    # Merge: most permissive wins
                    idx = seen[q]
                    finish_list = doc.get('finish', [])
                    wait_for_list = doc.get('wait_for', [])
                    stop_after_list = doc.get('stop_after', [])

                    # finish = True if either is True (more permissive)
                    if i < len(finish_list) and finish_list[i]:
                        new_finish[idx] = True
                    # wait_for = min of the two
                    if i < len(wait_for_list):
                        new_wait_for[idx] = min(new_wait_for[idx], tryInt(wait_for_list[i]))
                    # stop_after = min of the two
                    if i < len(stop_after_list):
                        new_stop_after[idx] = min(new_stop_after[idx], tryInt(stop_after_list[i]))
                else:
                    seen[q] = len(new_qualities)
                    new_qualities.append(q)

                    finish_list = doc.get('finish', [])
                    wait_for_list = doc.get('wait_for', [])
                    stop_after_list = doc.get('stop_after', [])
                    threed_list = doc.get('3d', [])

                    new_finish.append(finish_list[i] if i < len(finish_list) else True)
                    new_wait_for.append(tryInt(wait_for_list[i]) if i < len(wait_for_list) else 0)
                    new_stop_after.append(tryInt(stop_after_list[i]) if i < len(stop_after_list) else 0)
                    new_3d.append(threed_list[i] if i < len(threed_list) else False)

            doc['qualities'] = new_qualities
            doc['finish'] = new_finish
            doc['wait_for'] = new_wait_for
            doc['stop_after'] = new_stop_after
            doc['3d'] = new_3d

            # Check if this core profile now collides with an authoritative core profile
            if doc.get('core'):
                key = tuple(new_qualities)
                if key in core_profiles_by_quality:
                    # Collision — delete this migrated one (keep the authoritative one)
                    log.info('Deleting duplicate core profile "%s" (collides with "%s")' % (doc.get('label', '?'), core_profiles_by_quality[key].get('label', '?')))
                    db.delete(doc)
                    continue
                core_profiles_by_quality[key] = doc

            db.update(doc)
            log.info('Migrated profile "%s": %s' % (doc.get('label', '?'), new_qualities))

        log.info('=== Profile migration complete ===')

    # -------------------------------------------------------------------------
    # Tests
    # -------------------------------------------------------------------------

    def testView(self, **kwargs):
        result = self.doTest()
        return {
            'success': result,
        }

    def doTest(self):

        tests = {
            # DVD content → sd
            'Movie Name (1999)-DVD-Rip.avi': {'size': 700, 'quality': 'sd'},
            'Movie.Name.1999.DVDRip-Group': {'size': 750, 'quality': 'sd'},
            'Movie.Name.1999.DVD-Rip-Group': {'size': 700, 'quality': 'sd'},
            'Movie.Name.1999.DVD-R-Group': {'size': 4500, 'quality': 'sd'},
            'Movie.Rising.Name.Girl.2011.NTSC.DVD9-GroupDVD': {'size': 7200, 'quality': 'sd'},
            'Movie Name 2014 HQ DVDRip X264 AC3 (bla)': {'size': 0, 'quality': 'sd'},

            # BluRay 720p
            'Movie Name 1999 720p Bluray.mkv': {'size': 4200, 'quality': '720p'},
            'Movie.Name.Camelie.1999.720p.BluRay.x264-Group': {'size': 5500, 'quality': '720p'},
            'Movie.Name.2014.720p.BluRay.x264-ReleaseGroup': {'size': 10300, 'quality': '720p'},
            'Movie.Name.2014.720.Bluray.x264.DTS-ReleaseGroup': {'size': 9700, 'quality': '720p'},

            # BRRip with resolution → that resolution
            'Movie Name 1999 BR-Rip 720p.avi': {'size': 1000, 'quality': '720p'},
            'Movie Monuments 2013 BrRip 720p': {'size': 1300, 'quality': '720p'},
            'Movie Monuments 2013 BrRip 1080p': {'size': 1800, 'quality': '1080p'},

            # WEB-DL → resolution quality
            'Movie Name 1999 Web DL.avi': {'size': 800, 'quality': 'sd'},
            'Movie Name.2014.720p Web-Dl Aac2.0 h264-ReleaseGroup': {'size': 3800, 'quality': '720p'},

            # WEBRip → resolution quality (not screener anymore)
            'Movie Name 1999 720p Web Rip.avi': {'size': 1200, 'quality': '720p'},
            'Movie.Name.1999.1080p.WEBRip.H264-Group': {'size': 1500, 'quality': '1080p'},
            'Movie.Name.2014.720p.WEBRip.x264.AC3-ReleaseGroup': {'size': 3000, 'quality': '720p'},

            # HDRip → resolution (hdtv source)
            'Movie.Name.2014.1080p.HDrip.x264.aac-ReleaseGroup': {'size': 7000, 'quality': '1080p'},

            # 1080p BluRay
            'Movie.Name.2008.German.DL.AC3.1080p.BluRay.x264-Group': {'size': 8500, 'extra': {'resolution_width': 1920, 'resolution_height': 1080}, 'quality': '1080p'},
            'Movie.Name.2004.GERMAN.AC3D.DL.1080p.BluRay.x264-Group': {'size': 8000, 'quality': '1080p'},

            # BD50 → 1080p with bluray source (no more bd50 quality)
            'Movie.Name.2013.BR-Disk-Group.iso': {'size': 48000, 'quality': '2160p'},
            'Movie.Name.2013.2D+3D.BR-Disk-Group.iso': {'size': 52000, 'quality': '2160p', 'is_3d': True},
            'Movie Name (2013) 2D + 3D': {'size': 49000, 'quality': '2160p', 'is_3d': True},
            'The.Movie.2014.3D.1080p.BluRay.AVC.DTS-HD.MA.5.1-GroupName': {'size': 30000, 'quality': '1080p', 'is_3d': True},

            # Size-only heuristics
            '/home/namehou/Movie Monuments (2012)/Movie Monuments.mkv': {'size': 5500, 'quality': '720p', 'is_3d': False},
            '/home/namehou/Movie Monuments (2012)/Movie Monuments Full-OU.mkv': {'size': 5500, 'quality': '720p', 'is_3d': True},
            '/home/namehou/Movie Monuments (2013)/Movie Monuments.mkv': {'size': 10000, 'quality': '1080p', 'is_3d': False},
            '/home/namehou/Movie Monuments (2013)/Movie Monuments Full-OU.mkv': {'size': 10000, 'quality': '1080p', 'is_3d': True},
            '/volume1/Public/3D/Moviename/Moviename (2009).3D.SBS.ts': {'size': 7500, 'quality': '1080p', 'is_3d': True},
            '/volume1/Public/Moviename/Moviename (2009).ts': {'size': 7500, 'quality': '1080p'},
            '/movies/BluRay HDDVD H.264 MKV 720p EngSub/QuiQui le fou (criterion collection #123, 1915)/QuiQui le fou (1915) 720p x264 BluRay.mkv': {'size': 5500, 'quality': '720p'},
            r'C:\movies\QuiQui le fou (collection #123, 1915)\QuiQui le fou (1915) 720p x264 BluRay.mkv': {'size': 5500, 'quality': '720p'},
            r'C:\movies\QuiQui le fou (collection #123, 1915)\QuiQui le fou (1915) half-sbs 720p x264 BluRay.mkv': {'size': 5500, 'quality': '720p', 'is_3d': True},

            # Pre-releases
            'Moviename 2014 720p HDCAM XviD DualAudio': {'size': 4000, 'quality': 'cam'},
            'Moviename (2014) - 720p CAM x264': {'size': 2250, 'quality': 'cam'},
            'Movie name 2014 New Source 720p HDCAM x264 AC3 xyz': {'size': 750, 'quality': 'cam'},
            'Movie.Name.2014.HDCam.Chinese.Subs-ReleaseGroup': {'size': 15000, 'quality': 'cam'},
            'Movie.Name.2014.1080p.HDCAM.-.ReleaseGroup': {'size': 5300, 'quality': 'cam'},
            'Moviename.2014.720p.R6.WEB-DL.x264.AC3-xyz': {'size': 750, 'quality': 'r5'},
            'Movie.Name.2014.720p.HD.TS.AC3.x264': {'size': 750, 'quality': 'ts'},

            # Screener
            'Movie.Name.2014.720p.HDSCR.4PARTS.MP4.AAC.ReleaseGroup': {'size': 2401, 'quality': 'screener'},

            # Size-only with no tags
            'Movie Name1 (2012).mkv': {'size': 4500, 'quality': '720p'},
            'Movie Name (2013).mkv': {'size': 8500, 'quality': '1080p'},
            'Movie Name (2014).mkv': {'size': 4500, 'quality': '720p', 'extra': {'titles': ['Movie Name 2014 720p Bluray']}},
            'Movie Name (2015).mkv': {'size': 500, 'quality': '1080p', 'extra': {'resolution_width': 1920}},

            # mp4 with size — source detection
            'Movie Name (1997).mp4': {'size': 750, 'quality': 'sd'},
            'Movie Name (2015).mp4': {'size': 6500, 'quality': '1080p'},

            # 2160p / UHD
            'Movie Name 2015 2160p SourceSite WEBRip DD5 1 x264-ReleaseGroup': {'size': 21800, 'quality': '2160p'},
            'Movie Name 2012 2160p WEB-DL FLAC 5 1 x264-ReleaseGroup': {'size': 59650, 'quality': '2160p'},
        }

        correct = 0
        for name in tests:
            test_quality = self.guess(files = [name], extra = tests[name].get('extra', None), size = tests[name].get('size', None), use_cache = False) or {}
            success = test_quality.get('identifier') == tests[name]['quality'] and test_quality.get('is_3d') == tests[name].get('is_3d', False)
            if not success:
                log.error('%s failed check, thinks it\'s "%s" expecting "%s"', (name,
                                                                            test_quality.get('identifier', 'None') + (' 3D' if test_quality.get('is_3d') else ''),
                                                                            tests[name]['quality'] + (' 3D' if tests[name].get('is_3d') else '')
                ))

            correct += success

        if correct == len(tests):
            log.info('Quality test successful')
            return True
        else:
            log.error('Quality test failed: %s out of %s succeeded', (correct, len(tests)))
