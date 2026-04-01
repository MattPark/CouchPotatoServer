#!/usr/bin/env python3
"""
Test the CodernityDB -> TinyDB migration against real data.

Copies the real database to a temp directory, runs the migration,
and validates the results.
"""

import json
import logging
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'libs'))

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
log = logging.getLogger(__name__)

REAL_DB = '/Users/mpark/SynologyDrive/Projects/couchpotato-modernization-data/config/data/database'


def main():
    if not os.path.isdir(REAL_DB):
        log.error('Real database not found at %s', REAL_DB)
        return 1

    tmpdir = tempfile.mkdtemp(prefix='cp_migration_test_')
    data_dir = os.path.join(tmpdir, 'data')
    db_dir = os.path.join(data_dir, 'database')
    os.makedirs(db_dir)

    try:
        log.info('Copying CodernityDB files to temp dir...')
        for fname in ('id_buck', 'id_stor'):
            src = os.path.join(REAL_DB, fname)
            dst = os.path.join(db_dir, fname)
            shutil.copy2(src, dst)
            log.info('  %s: %.1f MB', fname, os.path.getsize(dst) / 1e6)

        # ---- Phase 1: Test the reader directly ----
        log.info('')
        log.info('=' * 60)
        log.info('PHASE 1: Test reading CodernityDB documents')
        log.info('=' * 60)

        from couchpotato.core.migration.migrate import _read_codernity_docs

        t0 = time.time()
        docs = list(_read_codernity_docs(db_dir))
        t1 = time.time()
        log.info('Read %d documents in %.2f seconds', len(docs), t1 - t0)

        by_type = {}
        for d in docs:
            t = d.get('_t', 'MISSING_TYPE')
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            log.info('  %s: %d', t, c)

        missing_id = sum(1 for d in docs if not d.get('_id'))
        missing_t = sum(1 for d in docs if not d.get('_t'))
        log.info('Documents missing _id: %d', missing_id)
        log.info('Documents missing _t: %d', missing_t)

        bytes_count = 0
        for d in docs:
            for k, v in d.items():
                if isinstance(v, bytes):
                    bytes_count += 1
                    if bytes_count <= 5:
                        log.warning('  bytes value: %s.%s = %r', d.get('_id', '?')[:8], k, v[:50])
        log.info('Fields with bytes values: %d (should be 0)', bytes_count)

        expected_counts = {
            'property': 16337, 'release': 11429, 'media': 10350,
            'notification': 51, 'profile': 18, 'quality': 12,
        }
        for t, expected in sorted(expected_counts.items()):
            actual = by_type.get(t, 0)
            status = 'OK' if actual == expected else 'MISMATCH'
            log.info('  %s: expected=%d actual=%d [%s]', t, expected, actual, status)

        # ---- Phase 2: Test full migration ----
        log.info('')
        log.info('=' * 60)
        log.info('PHASE 2: Test full migration')
        log.info('=' * 60)

        from couchpotato.core.migration.migrate import migrate_codernity_to_tinydb

        t0 = time.time()
        result = migrate_codernity_to_tinydb(db_dir, data_dir)
        t1 = time.time()
        log.info('Migration returned: %s (took %.2f seconds)', result, t1 - t0)

        if not result:
            log.error('MIGRATION FAILED!')
            return 1

        legacy_dir = os.path.join(data_dir, 'database_legacy')
        if os.path.isdir(legacy_dir):
            log.info('Legacy dir contains %d files', len(os.listdir(legacy_dir)))
        else:
            log.error('Legacy dir NOT created!')

        remaining = [f for f in os.listdir(db_dir) if f.endswith(('_buck', '_stor'))]
        if remaining:
            log.error('CodernityDB files still in db_dir: %s', remaining)
        else:
            log.info('CodernityDB files successfully moved out of db_dir')

        # ---- Phase 3: Validate TinyDB contents ----
        log.info('')
        log.info('=' * 60)
        log.info('PHASE 3: Validate TinyDB contents')
        log.info('=' * 60)

        db_json = os.path.join(db_dir, 'db.json')
        log.info('db.json size: %.1f MB', os.path.getsize(db_json) / 1e6)

        from couchpotato.core.db import CouchDB, RecordNotFound

        db = CouchDB(db_dir)
        db.open()

        all_docs = db.all('id')
        log.info('Total docs in TinyDB: %d', len(all_docs))

        tiny_by_type = {}
        for d in all_docs:
            t = d.get('_t', 'MISSING_TYPE')
            tiny_by_type[t] = tiny_by_type.get(t, 0) + 1
        for t, c in sorted(tiny_by_type.items()):
            log.info('  %s: %d', t, c)

        if len(all_docs) == len(docs):
            log.info('Document count MATCHES: %d', len(docs))
        else:
            log.error('Document count MISMATCH: read=%d tinydb=%d', len(docs), len(all_docs))

        # ---- Phase 4: Test index queries ----
        log.info('')
        log.info('=' * 60)
        log.info('PHASE 4: Test index queries')
        log.info('=' * 60)

        try:
            prop = db.get('property', 'app_version')
            log.info('property/app_version: found (_id=%s)', prop.get('_id', '?')[:8])
        except RecordNotFound:
            log.error('property/app_version: NOT FOUND')

        sample_media = None
        for d in all_docs:
            if d.get('_t') == 'media' and d.get('identifiers', {}).get('imdb'):
                sample_media = d
                break
        if sample_media:
            imdb = sample_media['identifiers']['imdb']
            key = 'imdb-%s' % imdb
            try:
                found = db.get('media', key)
                log.info('media/%s: found (_id=%s)', key, found.get('_id', '?')[:8])
            except RecordNotFound:
                log.error('media/%s: NOT FOUND', key)

        sample_release = None
        for d in all_docs:
            if d.get('_t') == 'release' and d.get('media_id'):
                sample_release = d
                break
        if sample_release:
            mid = sample_release['media_id']
            releases = db.get_many('release', mid)
            log.info('release/media_id=%s: found %d releases', mid[:8], len(releases))

        profiles = db.all('profile', with_doc=True)
        log.info('all profiles: %d', len(profiles))

        qualities = db.all('quality', with_doc=True)
        log.info('all qualities: %d', len(qualities))

        # ---- Phase 5: Cross-reference integrity ----
        log.info('')
        log.info('=' * 60)
        log.info('PHASE 5: Cross-reference integrity')
        log.info('=' * 60)

        all_ids = {d.get('_id') for d in all_docs}

        broken_release_refs = 0
        release_count = 0
        for d in all_docs:
            if d.get('_t') == 'release':
                release_count += 1
                mid = d.get('media_id')
                if mid and mid not in all_ids:
                    broken_release_refs += 1
                    if broken_release_refs <= 3:
                        log.warning('  Broken ref: release %s -> media %s',
                                    d.get('_id', '?')[:8], mid[:8] if mid else '?')
        log.info('Releases: %d total, %d broken media_id refs', release_count, broken_release_refs)

        broken_profile_refs = 0
        media_count = 0
        profile_ids = {d.get('_id') for d in all_docs if d.get('_t') == 'profile'}
        for d in all_docs:
            if d.get('_t') == 'media':
                media_count += 1
                pid = d.get('profile_id')
                if pid and pid not in profile_ids:
                    broken_profile_refs += 1
                    if broken_profile_refs <= 3:
                        log.warning('  Broken ref: media %s -> profile %s',
                                    d.get('_id', '?')[:8], pid[:8] if pid else '?')
        log.info('Media: %d total, %d broken profile_id refs', media_count, broken_profile_refs)

        broken_cat_refs = 0
        cat_ids = {d.get('_id') for d in all_docs if d.get('_t') == 'category'}
        for d in all_docs:
            if d.get('_t') == 'media':
                cid = d.get('category_id')
                if cid and cid not in cat_ids and cid not in all_ids:
                    broken_cat_refs += 1
        log.info('Media with broken category_id refs: %d', broken_cat_refs)

        db.close()

        # ---- Summary ----
        log.info('')
        log.info('=' * 60)
        log.info('SUMMARY')
        log.info('=' * 60)
        issues = []
        if len(all_docs) != len(docs):
            issues.append('Document count mismatch')
        if bytes_count > 0:
            issues.append('%d bytes values remain' % bytes_count)
        if broken_release_refs > 0:
            issues.append('%d broken release->media refs' % broken_release_refs)
        if broken_profile_refs > 0:
            issues.append('%d broken media->profile refs' % broken_profile_refs)
        if missing_id > 0:
            issues.append('%d docs missing _id' % missing_id)
        if missing_t > 0:
            issues.append('%d docs missing _t' % missing_t)

        if issues:
            log.warning('ISSUES FOUND:')
            for i in issues:
                log.warning('  - %s', i)
            return 1
        else:
            log.info('ALL CHECKS PASSED!')
            return 0

    finally:
        log.info('')
        log.info('Cleaning up temp dir: %s', tmpdir)
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    sys.exit(main())
