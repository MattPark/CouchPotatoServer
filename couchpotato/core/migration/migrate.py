"""
Migrate a CodernityDB database to TinyDB.

Reads all live documents from the raw id_buck / id_stor files using
a custom Python-2 marshal reader, assigns fresh UUIDs, fixes
cross-references, and bulk-inserts into the new TinyDB-backed CouchDB.
"""

import logging
import os
import re
import shutil
import struct
from uuid import uuid4

from couchpotato.core.migration.marshal_reader import loads as marshal_loads

log = logging.getLogger(__name__)

# CodernityDB constants
_START_IND = 500          # property header region in id_buck
_BUCKET_FMT = '<I'        # 4-byte bucket pointer
_BUCKET_SIZE = struct.calcsize(_BUCKET_FMT)


def _read_codernity_docs(db_path):
    """Yield every live document from a CodernityDB database directory.

    Each yielded dict has '_id', '_rev', '_t', and all payload fields.
    All byte strings are already decoded to str by the marshal reader.
    """
    buck_path = os.path.join(db_path, 'id_buck')
    stor_path = os.path.join(db_path, 'id_stor')

    if not os.path.isfile(buck_path) or not os.path.isfile(stor_path):
        raise FileNotFoundError('CodernityDB files not found in %s' % db_path)

    # Read index properties from the first 500 bytes of id_buck
    with open(buck_path, 'rb') as f:
        raw_props = f.read(_START_IND)
    props = marshal_loads(raw_props)

    hash_lim = props.get('hash_lim', 0xfffff)
    entry_fmt = props.get('entry_line_format', '<32s8sIIcI')
    if isinstance(entry_fmt, bytes):
        entry_fmt = entry_fmt.decode('ascii')
    entry_struct = struct.Struct(entry_fmt)
    entry_size = entry_struct.size

    data_start = (hash_lim + 1) * _BUCKET_SIZE + _START_IND + 2

    buck_f = open(buck_path, 'rb')
    stor_f = open(stor_path, 'rb')

    try:
        buck_f.seek(data_start)
        count = 0
        errors = 0

        while True:
            raw = buck_f.read(entry_size)
            if not raw or len(raw) < entry_size:
                break

            doc_id, rev, start, size, status, _next = entry_struct.unpack(raw)

            if status == b'd':
                continue

            doc_id_str = doc_id.rstrip(b'\x00').decode('ascii', errors='replace')
            rev_str = rev.rstrip(b'\x00').decode('ascii', errors='replace')

            if size == 0:
                doc = {}
            else:
                try:
                    stor_f.seek(start)
                    blob = stor_f.read(size)
                    doc = marshal_loads(blob)
                except Exception as e:
                    errors += 1
                    log.warning('Failed to read doc %s: %s', doc_id_str, e)
                    continue

            if not isinstance(doc, dict):
                errors += 1
                continue

            doc['_id'] = doc_id_str
            doc['_rev'] = rev_str
            count += 1
            yield doc

        log.info('Read %d documents (%d errors) from CodernityDB', count, errors)
    finally:
        buck_f.close()
        stor_f.close()


def _remap_ids(docs):
    """Assign new UUIDs and update cross-reference fields.

    Returns (updated_docs, id_map) where id_map is old_id -> new_id.
    """
    id_map = {}

    # Phase 1: assign new IDs
    for doc in docs:
        old_id = doc['_id']
        new_id = uuid4().hex
        id_map[old_id] = new_id
        doc['_id'] = new_id

    # Phase 2: update cross-references
    for doc in docs:
        t = doc.get('_t')
        if t == 'media':
            for field in ('profile_id', 'category_id', 'parent_id'):
                old = doc.get(field)
                if old and old in id_map:
                    doc[field] = id_map[old]
        elif t == 'release':
            old = doc.get('media_id')
            if old and old in id_map:
                doc['media_id'] = id_map[old]

    return docs, id_map


def _normalize_imdb_ids(docs):
    """Fix 8-digit zero-padded IMDB IDs (tt0XXXXXXX -> ttXXXXXXX) in media documents.

    The old CouchPotato used getImdb() with .zfill(8) which produced IDs like
    tt00111161 instead of the native tt0111161. External APIs (TMDB, OMDB) reject
    the padded format. This pass normalizes all IMDB identifiers in-place.

    Returns the count of IDs fixed.
    """
    fixed = 0
    for doc in docs:
        if doc.get('_t') != 'media':
            continue
        identifiers = doc.get('identifiers')
        if not identifiers or not isinstance(identifiers, dict):
            continue
        imdb = identifiers.get('imdb', '')
        if not imdb or not isinstance(imdb, str) or not imdb.startswith('tt'):
            continue
        m = re.match(r'tt0*(\d+)$', imdb)
        if m:
            native = 'tt%s' % m.group(1).zfill(7)
            if native != imdb:
                identifiers['imdb'] = native
                fixed += 1
    return fixed


def migrate_codernity_to_tinydb(old_db_path, new_db_path):
    """Run the full CodernityDB -> TinyDB migration.

    Args:
        old_db_path: Path to the old CodernityDB directory (contains id_buck, id_stor)
        new_db_path: Path where the new TinyDB database directory should be created

    Returns True on success, False on failure.
    """
    from couchpotato.core.db import CouchDB

    # Place legacy folder alongside the old database
    legacy_dir = old_db_path + '_legacy'
    db_json = os.path.join(new_db_path, 'db.json')

    log.info('=' * 50)
    log.info('Starting CodernityDB -> TinyDB migration')
    log.info('Source: %s', old_db_path)
    log.info('Destination: %s', new_db_path)

    # Clean up any partial previous migration
    if os.path.isfile(db_json):
        log.info('Removing partial migration file %s', db_json)
        os.unlink(db_json)

    try:
        # Step 1: Read all documents from CodernityDB
        log.info('Reading documents from CodernityDB...')
        docs = list(_read_codernity_docs(old_db_path))
        log.info('Read %d documents total', len(docs))

        # Log type breakdown
        by_type = {}
        for d in docs:
            t = d.get('_t', 'unknown')
            by_type[t] = by_type.get(t, 0) + 1
        for t, c in sorted(by_type.items()):
            log.info('  %s: %d', t, c)

        # Step 2: Assign new UUIDs and fix cross-references
        log.info('Assigning new UUIDs and updating cross-references...')
        docs, id_map = _remap_ids(docs)
        log.info('Remapped %d IDs', len(id_map))

        # Step 2b: Normalize 8-digit padded IMDB IDs to native 7-digit format
        imdb_fixed = _normalize_imdb_ids(docs)
        if imdb_fixed:
            log.info('Normalized %d padded IMDB IDs (tt0XXXXXXX -> ttXXXXXXX)', imdb_fixed)

        # Step 3: Move old CodernityDB files out of the way
        if os.path.isdir(legacy_dir):
            shutil.rmtree(legacy_dir)
        os.makedirs(legacy_dir)

        for fname in os.listdir(old_db_path):
            # Move all CodernityDB artifacts (_buck, _stor, _compact, etc.)
            if fname.endswith(('_buck', '_stor', '_compact_buck', '_compact_stor')):
                src = os.path.join(old_db_path, fname)
                dst = os.path.join(legacy_dir, fname)
                shutil.move(src, dst)

        # Also move the _indexes dir if present
        idx_dir = os.path.join(old_db_path, '_indexes')
        if os.path.exists(idx_dir):
            shutil.move(idx_dir, os.path.join(legacy_dir, '_indexes'))

        log.info('Moved CodernityDB files to %s', legacy_dir)

        # Step 4: Create TinyDB and bulk-insert
        # Ensure destination directory exists
        if not os.path.isdir(new_db_path):
            os.makedirs(new_db_path)

        log.info('Writing %d documents to TinyDB...', len(docs))
        db = CouchDB(new_db_path)
        db.create()

        # Batch insert for performance
        BATCH = 5000
        for i in range(0, len(docs), BATCH):
            batch = docs[i:i + BATCH]
            db.insert_multiple(batch)
            log.info('  Inserted %d / %d', min(i + BATCH, len(docs)), len(docs))

        db.compact()  # flush to disk
        db.close()

        log.info('Migration complete! TinyDB at %s', db_json)
        log.info('Legacy CodernityDB files preserved at %s', legacy_dir)
        log.info('=' * 50)
        return True

    except Exception:
        log.exception('Migration FAILED — cleaning up partial state')
        # Remove partial TinyDB file
        if os.path.isfile(db_json):
            os.unlink(db_json)
        # Restore CodernityDB files if we moved them
        if os.path.isdir(legacy_dir):
            for fname in os.listdir(legacy_dir):
                src = os.path.join(legacy_dir, fname)
                dst = os.path.join(old_db_path, fname)
                if not os.path.exists(dst):
                    shutil.move(src, dst)
            shutil.rmtree(legacy_dir)
        log.error('CodernityDB files restored. Migration will retry on next start.')
        return False
