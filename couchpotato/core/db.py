"""
TinyDB-based database wrapper for CouchPotato.

Provides a CodernityDB-compatible API over TinyDB, using a single JSON file
with CachingMiddleware for write batching.
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


def _media_id_match(doc, key):
    ids = doc.get('identifiers')
    if not ids or not isinstance(ids, dict):
        return False
    dash = key.find('-')
    if dash < 0:
        return False
    return str(ids.get(key[:dash], '')) == key[dash + 1:]


def _release_dl_match(doc, key):
    di = doc.get('download_info')
    if not di or not isinstance(di, dict):
        return False
    doc_key = ('%s-%s' % (di.get('downloader', ''), di.get('id', ''))).lower()
    return doc_key == key.lower()


_SPECS = {
    'property':           {'type': 'property',     'match': lambda d, k: d.get('identifier') == k},
    'media':              {'type': 'media',        'match': _media_id_match},
    'media_status':       {'type': 'media',        'match': lambda d, k: d.get('status') == k},
    'media_by_type':      {'type': 'media',        'match': lambda d, k: d.get('type') == k},
    'media_search_title': {'type': 'media',        'match': lambda d, k: k.lower() in (d.get('title') or '').lower()},
    'media_title':        {'type': 'media',        'sort': lambda d: _simplify_title(d.get('title', ''))},
    'media_startswith':   {'type': 'media',        'match': lambda d, k: _starts_with_char(d.get('title', '')) == k,
                                                    'keyfn': lambda d: _starts_with_char(d.get('title', '')), 'uniq': True},
    'media_children':     {'type': 'media',        'match': lambda d, k: d.get('parent_id') == k},
    'media_tag':          {'type': 'media',        'match': lambda d, k: k in (d.get('tags') or [])},
    'release':            {'type': 'release',      'match': lambda d, k: d.get('media_id') == k,
                                                    'keyfn': lambda d: d.get('media_id')},
    'release_status':     {'type': 'release',      'match': lambda d, k: d.get('status') == k,
                                                    'stored': lambda d: {'media_id': d.get('media_id')}},
    'release_identifier': {'type': 'release',      'match': lambda d, k: d.get('identifier') == k,
                                                    'stored': lambda d: {'media_id': d.get('media_id')}},
    'release_download':   {'type': 'release',      'match': _release_dl_match},
    'profile':            {'type': 'profile',      'sort': lambda d: d.get('order', 99)},
    'quality':            {'type': 'quality',      'match': lambda d, k: d.get('identifier') == k},
    'category':           {'type': 'category',     'sort': lambda d: d.get('order', -99)},
    'category_media':     {'type': 'media',        'match': lambda d, k: str(d.get('category_id', '')) == str(k)},
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
        self._id_cache = {}
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
        self._rebuild_id_cache()

    def close(self):
        with self._lock:
            if self._db:
                self._db.close()
                self._db = None
                self._id_cache.clear()

    def destroy(self):
        self.close()
        if os.path.isfile(self._db_file):
            os.unlink(self._db_file)

    def _rebuild_id_cache(self):
        self._id_cache = {d.get('_id'): d.doc_id for d in self._db.all() if d.get('_id')}

    @staticmethod
    def _next_rev(current=None):
        ctr = 1
        if current:
            try:
                ctr = int(current[:4], 16) + 1
            except (ValueError, IndexError):
                pass
        return '%04x%s' % (ctr, uuid4().hex[:4])

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
        doc = self._db.get(doc_id=tid)
        if doc is None:
            self._rebuild_id_cache()
            tid = self._id_cache.get(key)
            if tid is None:
                raise RecordNotFound('Document not found: %s' % key)
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
        for raw in self._db.all():
            d = dict(raw)
            if tf and d.get('_t') != tf:
                continue
            if ef and not ef(d):
                continue
            if mf and not mf(d, key):
                continue
            r = {'_id': d.get('_id')}
            if sf:
                r.update(sf(d))
            if with_doc:
                r['doc'] = d
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
            results, skipped = [], 0
            for raw in self._db.all():
                d = dict(raw)
                if tf and d.get('_t') != tf:
                    continue
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
                    entry['doc'] = d
                results.append(entry)
                if 0 < limit <= len(results):
                    break
            return results

    # ---- all -------------------------------------------------------------

    def all(self, index_name, limit=-1, offset=0, with_doc=False, with_storage=True):
        with self._lock:
            if index_name == 'id':
                docs = [dict(d) for d in self._db.all()]
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

        matches = []
        for raw in self._db.all():
            d = dict(raw)
            if tf and d.get('_t') != tf:
                continue
            if ef and not ef(d):
                continue
            matches.append(d)

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
                entry['doc'] = d
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
            return doc

    def update(self, doc):
        with self._lock:
            cid = doc.get('_id')
            if not cid:
                raise RecordNotFound('Cannot update without _id')
            tid = self._id_cache.get(cid)
            if tid is None:
                raise RecordNotFound('Document %s not found' % cid)
            doc['_rev'] = self._next_rev(doc.get('_rev'))
            self._db.remove(doc_ids=[tid])
            new_tid = self._db.insert(dict(doc))
            self._id_cache[cid] = new_tid
            return doc

    def delete(self, doc):
        with self._lock:
            cid = doc.get('_id')
            if not cid:
                raise RecordNotFound('Cannot delete without _id')
            tid = self._id_cache.get(cid)
            if tid is None:
                raise RecordNotFound('Document %s not found' % cid)
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
