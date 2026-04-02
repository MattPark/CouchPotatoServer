"""
TinyDB-based database wrapper for CouchPotato.

Provides a CodernityDB-compatible API over TinyDB, using a single JSON file
with CachingMiddleware for write batching.

Performance strategy: on open(), build three in-memory caches from a single
full scan of the TinyDB file:

  _id_cache   — {_id: tinydb_doc_id}         fast ID lookups
  _type_docs  — {type_str: {_id: doc_dict}}   all docs grouped by _t
  _key_cache  — {spec_name: {key: [_id,...]}} keyed index for specs with keyfn

All subsequent reads hit these caches instead of scanning TinyDB.  Write ops
(insert / update / delete) maintain all three caches incrementally.
"""

import os
import re
import threading
from uuid import uuid4

from tinydb import TinyDB
from tinydb.middlewares import CachingMiddleware
from tinydb.storages import JSONStorage


class RecordNotFound(Exception):
    pass


class RecordDeleted(Exception):
    pass


def _simplify_title(title):
    if not title:
        return ''
    t = title.lower()
    t = re.sub(r'^(the|a|an)\s+', '', t)
    t = re.sub(r'[^a-z0-9 ]', '', t)
    return t.strip()


def _starts_with_char(title):
    s = _simplify_title(title)
    if s and s[0].isalpha():
        return s[0]
    return '#'


def _normalize_imdb(val):
    """Normalize IMDB IDs to native 7-digit format for comparison."""
    if isinstance(val, str) and val.startswith('tt'):
        m = re.match(r'tt0*(\d+)$', val)
        if m:
            return 'tt%s' % m.group(1).zfill(7)
    return val


def _media_id_match(doc, key):
    ids = doc.get('identifiers')
    if not ids or not isinstance(ids, dict):
        return False
    dash = key.find('-')
    if dash < 0:
        return False
    id_type = key[:dash]
    search_val = key[dash + 1:]
    doc_val = str(ids.get(id_type, ''))
    # Normalize IMDB IDs so old 8-digit records match new 7-digit lookups
    if id_type == 'imdb':
        return _normalize_imdb(doc_val) == _normalize_imdb(search_val)
    return doc_val == search_val


def _release_dl_match(doc, key):
    di = doc.get('download_info')
    if not di or not isinstance(di, dict):
        return False
    doc_key = ('%s-%s' % (di.get('downloader', ''), di.get('id', ''))).lower()
    return doc_key == key.lower()


_SPECS = {
    'property':           {'type': 'property',     'match': lambda d, k: d.get('identifier') == k,
                                                    'keyfn': lambda d: d.get('identifier')},
    'media':              {'type': 'media',        'match': _media_id_match},
    'media_status':       {'type': 'media',        'match': lambda d, k: d.get('status') == k,
                                                    'keyfn': lambda d: d.get('status')},
    'media_by_type':      {'type': 'media',        'match': lambda d, k: d.get('type') == k,
                                                    'keyfn': lambda d: d.get('type')},
    'media_search_title': {'type': 'media',        'match': lambda d, k: k.lower() in (d.get('title') or '').lower()},
    'media_title':        {'type': 'media',        'sort': lambda d: _simplify_title(d.get('title', ''))},
    'media_startswith':   {'type': 'media',        'match': lambda d, k: _starts_with_char(d.get('title', '')) == k,
                                                    'keyfn': lambda d: _starts_with_char(d.get('title', '')), 'uniq': True},
    'media_children':     {'type': 'media',        'match': lambda d, k: d.get('parent_id') == k,
                                                    'keyfn': lambda d: d.get('parent_id')},
    'media_tag':          {'type': 'media',        'match': lambda d, k: k in (d.get('tags') or [])},
    'release':            {'type': 'release',      'match': lambda d, k: d.get('media_id') == k,
                                                    'keyfn': lambda d: d.get('media_id')},
    'release_status':     {'type': 'release',      'match': lambda d, k: d.get('status') == k,
                                                    'keyfn': lambda d: d.get('status'),
                                                    'stored': lambda d: {'media_id': d.get('media_id')}},
    'release_identifier': {'type': 'release',      'match': lambda d, k: d.get('identifier') == k,
                                                    'keyfn': lambda d: d.get('identifier'),
                                                    'stored': lambda d: {'media_id': d.get('media_id')}},
    'release_download':   {'type': 'release',      'match': _release_dl_match},
    'profile':            {'type': 'profile',      'sort': lambda d: d.get('order', 99)},
    'quality':            {'type': 'quality',      'match': lambda d, k: d.get('identifier') == k,
                                                    'keyfn': lambda d: d.get('identifier')},
    'category':           {'type': 'category',     'sort': lambda d: d.get('order', -99)},
    'category_media':     {'type': 'media',        'match': lambda d, k: str(d.get('category_id', '')) == str(k),
                                                    'keyfn': lambda d: str(d.get('category_id', ''))},
    'notification':       {'type': 'notification', 'sort': lambda d: d.get('time', 0)},
    'notification_unread':{'type': 'notification', 'filt': lambda d: not d.get('read'),
                                                    'sort': lambda d: d.get('time', 0)},
}


class CouchDB:
    """TinyDB-backed database with CodernityDB-compatible API."""

    def __init__(self, path):
        self.path = path
        self._db = None
        self._lock = threading.RLock()
        self._id_cache = {}          # _id -> tinydb doc_id
        self._type_docs = {}         # _t   -> {_id -> dict}
        self._key_cache = {}         # spec_name -> {key -> set(_id)}
        self._registered_indexes = {}

    @property
    def _db_file(self):
        return os.path.join(self.path, 'db.json')

    def exists(self):
        return os.path.isfile(self._db_file)

    @property
    def opened(self):
        return self._db is not None

    def create(self):
        os.makedirs(self.path, exist_ok=True)
        self._do_open()

    def open(self):
        self._do_open()

    def _do_open(self):
        self._db = TinyDB(self._db_file, storage=CachingMiddleware(JSONStorage))
        self._rebuild_caches()

    def close(self):
        with self._lock:
            if self._db:
                self._db.close()
                self._db = None
                self._id_cache.clear()
                self._type_docs.clear()
                self._key_cache.clear()

    def destroy(self):
        self.close()
        if os.path.isfile(self._db_file):
            os.unlink(self._db_file)

    # ---- cache management ------------------------------------------------

    def _rebuild_caches(self):
        """Single full scan to populate all three caches."""
        id_cache = {}
        type_docs = {}
        key_cache = {name: {} for name, spec in _SPECS.items() if spec.get('keyfn')}

        for raw in self._db.all():
            d = dict(raw)
            cid = d.get('_id')
            if not cid:
                continue
            id_cache[cid] = raw.doc_id

            t = d.get('_t')
            if t:
                bucket = type_docs.get(t)
                if bucket is None:
                    bucket = {}
                    type_docs[t] = bucket
                bucket[cid] = d

            # Populate key caches
            for spec_name, kc in key_cache.items():
                spec = _SPECS[spec_name]
                st = spec.get('type')
                if st and t != st:
                    continue
                kfn = spec['keyfn']
                k = kfn(d)
                if k is not None:
                    ids = kc.get(k)
                    if ids is None:
                        ids = set()
                        kc[k] = ids
                    ids.add(cid)

        self._id_cache = id_cache
        self._type_docs = type_docs
        self._key_cache = key_cache

    def _cache_add(self, doc):
        """Incrementally add a doc to the caches."""
        cid = doc.get('_id')
        t = doc.get('_t')
        if not cid:
            return
        if t:
            bucket = self._type_docs.get(t)
            if bucket is None:
                bucket = {}
                self._type_docs[t] = bucket
            bucket[cid] = dict(doc)

        for spec_name, kc in self._key_cache.items():
            spec = _SPECS[spec_name]
            st = spec.get('type')
            if st and t != st:
                continue
            k = spec['keyfn'](doc)
            if k is not None:
                ids = kc.get(k)
                if ids is None:
                    ids = set()
                    kc[k] = ids
                ids.add(cid)

    def _cache_remove(self, doc):
        """Incrementally remove a doc from the caches."""
        cid = doc.get('_id')
        t = doc.get('_t')
        if not cid:
            return
        if t:
            bucket = self._type_docs.get(t)
            if bucket:
                bucket.pop(cid, None)

        for spec_name, kc in self._key_cache.items():
            spec = _SPECS[spec_name]
            st = spec.get('type')
            if st and t != st:
                continue
            k = spec['keyfn'](doc)
            if k is not None:
                ids = kc.get(k)
                if ids:
                    ids.discard(cid)

    @staticmethod
    def _next_rev(current=None):
        ctr = 1
        if current:
            try:
                ctr = int(current[:4], 16) + 1
            except (ValueError, IndexError):
                pass
        return '%04x%s' % (ctr, uuid4().hex[:4])

    # ---- internal helpers ------------------------------------------------

    def _docs_for_type(self, type_str):
        """Return {_id: doc} dict for a given _t value."""
        return self._type_docs.get(type_str) or {}

    def _ids_for_key(self, spec_name, key):
        """Return set of _ids matching key in a keyed spec, or None if no cache."""
        kc = self._key_cache.get(spec_name)
        if kc is None:
            return None
        return kc.get(key) or set()

    # ---- get (single doc) ------------------------------------------------

    def get(self, index_name, key, with_doc=False, with_storage=True):
        with self._lock:
            if index_name == 'id':
                return self._get_by_id(key)
            return self._get_by_index(index_name, key, with_doc)

    def _get_by_id(self, key):
        tid = self._id_cache.get(key)
        if tid is None:
            raise RecordNotFound('Document not found: %s' % key)
        # Try type_docs first (avoids TinyDB access entirely)
        for bucket in self._type_docs.values():
            d = bucket.get(key)
            if d is not None:
                return dict(d)
        # Fallback to TinyDB
        doc = self._db.get(doc_id=tid)
        if doc is None:
            raise RecordNotFound('Document not found: %s' % key)
        return dict(doc)

    def _get_by_index(self, index_name, key, with_doc):
        spec = _SPECS.get(index_name)
        if not spec:
            raise RecordNotFound('Unknown index: %s' % index_name)
        tf = spec.get('type')
        mf = spec.get('match')
        ef = spec.get('filt')
        sf = spec.get('stored')

        # Use type_docs cache instead of scanning TinyDB
        if tf:
            docs = self._docs_for_type(tf).values()
        else:
            docs = []
            for bucket in self._type_docs.values():
                docs = list(docs) + list(bucket.values())

        for d in docs:
            if ef and not ef(d):
                continue
            if mf and not mf(d, key):
                continue
            r = {'_id': d.get('_id')}
            if sf:
                r.update(sf(d))
            if with_doc:
                r['doc'] = dict(d)
            return r
        raise RecordNotFound('No match: index=%s key=%s' % (index_name, key))

    # ---- get_many --------------------------------------------------------

    def get_many(self, index_name, key, limit=-1, offset=0, with_doc=False):
        with self._lock:
            spec = _SPECS.get(index_name)
            if not spec:
                return []
            tf = spec.get('type')
            mf = spec.get('match')
            ef = spec.get('filt')
            sf = spec.get('stored')
            kfn = spec.get('keyfn')

            # Fast path: if spec has keyfn, use the key cache
            if kfn:
                cached_ids = self._ids_for_key(index_name, key)
                if cached_ids is not None:
                    type_bucket = self._docs_for_type(tf) if tf else None
                    results, skipped = [], 0
                    for cid in cached_ids:
                        d = type_bucket.get(cid) if type_bucket else None
                        if d is None:
                            continue
                        if ef and not ef(d):
                            continue
                        if skipped < offset:
                            skipped += 1
                            continue
                        entry = {'_id': cid}
                        if sf:
                            entry.update(sf(d))
                        if with_doc:
                            entry['doc'] = dict(d)
                        results.append(entry)
                        if 0 < limit <= len(results):
                            break
                    return results

            # Slow path: scan type_docs
            if tf:
                docs = self._docs_for_type(tf).values()
            else:
                docs = []
                for bucket in self._type_docs.values():
                    docs = list(docs) + list(bucket.values())

            results, skipped = [], 0
            for d in docs:
                if ef and not ef(d):
                    continue
                if mf and not mf(d, key):
                    continue
                if skipped < offset:
                    skipped += 1
                    continue
                entry = {'_id': d.get('_id')}
                if sf:
                    entry.update(sf(d))
                if with_doc:
                    entry['doc'] = dict(d)
                results.append(entry)
                if 0 < limit <= len(results):
                    break
            return results

    # ---- all -------------------------------------------------------------

    def all(self, index_name, limit=-1, offset=0, with_doc=False, with_storage=True):
        with self._lock:
            if index_name == 'id':
                docs = []
                for bucket in self._type_docs.values():
                    docs.extend(dict(d) for d in bucket.values())
                if offset > 0:
                    docs = docs[offset:]
                if limit > 0:
                    docs = docs[:limit]
                return docs
            return self._all_index(index_name, limit, offset, with_doc)

    def _all_index(self, index_name, limit, offset, with_doc):
        spec = _SPECS.get(index_name)
        if not spec:
            return []
        tf = spec.get('type')
        ef = spec.get('filt')
        sfn = spec.get('sort')
        kfn = spec.get('keyfn')
        uniq = spec.get('uniq', False)

        # Use type_docs cache
        if tf:
            matches = list(self._docs_for_type(tf).values())
        else:
            matches = []
            for bucket in self._type_docs.values():
                matches.extend(bucket.values())

        if ef:
            matches = [d for d in matches if ef(d)]
        if sfn:
            matches.sort(key=sfn)
        if uniq and kfn:
            seen, tmp = set(), []
            for d in matches:
                k = kfn(d)
                if k not in seen:
                    seen.add(k)
                    tmp.append(d)
            matches = tmp
        if offset > 0:
            matches = matches[offset:]
        if limit > 0:
            matches = matches[:limit]

        results = []
        for d in matches:
            entry = {'_id': d.get('_id')}
            if kfn:
                entry['key'] = kfn(d)
            elif sfn:
                entry['key'] = sfn(d)
            if with_doc:
                entry['doc'] = dict(d)
            results.append(entry)
        return results

    # ---- write ops -------------------------------------------------------

    def insert(self, doc):
        with self._lock:
            if '_id' not in doc:
                doc['_id'] = uuid4().hex
            doc['_rev'] = self._next_rev()
            tid = self._db.insert(doc)
            self._id_cache[doc['_id']] = tid
            self._cache_add(doc)
            return doc

    def update(self, doc):
        with self._lock:
            cid = doc.get('_id')
            if not cid:
                raise RecordNotFound('Cannot update without _id')
            tid = self._id_cache.get(cid)
            if tid is None:
                raise RecordNotFound('Document %s not found' % cid)
            # Remove old version from caches
            old_doc = None
            for bucket in self._type_docs.values():
                old_doc = bucket.get(cid)
                if old_doc:
                    break
            if old_doc:
                self._cache_remove(old_doc)
            doc['_rev'] = self._next_rev(doc.get('_rev'))
            self._db.remove(doc_ids=[tid])
            new_tid = self._db.insert(dict(doc))
            self._id_cache[cid] = new_tid
            self._cache_add(doc)
            return doc

    def delete(self, doc):
        with self._lock:
            cid = doc.get('_id')
            if not cid:
                raise RecordNotFound('Cannot delete without _id')
            tid = self._id_cache.get(cid)
            if tid is None:
                raise RecordNotFound('Document %s not found' % cid)
            # Remove from caches
            old_doc = None
            for bucket in self._type_docs.values():
                old_doc = bucket.get(cid)
                if old_doc:
                    break
            if old_doc:
                self._cache_remove(old_doc)
            self._db.remove(doc_ids=[tid])
            del self._id_cache[cid]

    def insert_multiple(self, docs):
        with self._lock:
            for d in docs:
                if '_id' not in d:
                    d['_id'] = uuid4().hex
                if '_rev' not in d:
                    d['_rev'] = self._next_rev()
            tids = self._db.insert_multiple(docs)
            for d, tid in zip(docs, tids):
                self._id_cache[d['_id']] = tid
                self._cache_add(d)

    # ---- counting --------------------------------------------------------

    def count(self, func, *args, **kwargs):
        r = func(*args, **kwargs)
        return len(r) if hasattr(r, '__len__') else sum(1 for _ in r)

    # ---- index management (no-ops) ---------------------------------------

    @property
    def indexes_names(self):
        return self._registered_indexes

    def add_index(self, inst):
        name = getattr(inst, 'name', None) or getattr(inst, '_name', 'unknown')
        self._registered_indexes[name] = inst

    def reindex(self):
        pass

    def reindex_index(self, name):
        pass

    def destroy_index(self, idx):
        name = idx if isinstance(idx, str) else getattr(idx, 'name', str(idx))
        self._registered_indexes.pop(name, None)

    # ---- misc ------------------------------------------------------------

    def compact(self):
        with self._lock:
            if self._db and hasattr(self._db, 'storage'):
                s = self._db.storage
                if hasattr(s, 'flush'):
                    s.flush()

    def get_db_details(self):
        sz = os.path.getsize(self._db_file) if os.path.isfile(self._db_file) else 0
        return {'size': sz}

    def _delete_id_index(self, _id, _rev, _):
        try:
            self.delete({'_id': _id})
        except RecordNotFound:
            pass
