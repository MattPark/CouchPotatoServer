import atexit
import json
import os
import time
import traceback

from couchpotato import CPLog
from couchpotato.api import addApiView
from couchpotato.core.db import RecordNotFound
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import sp


log = CPLog(__name__)


class Database(object):

    indexes = None
    db = None

    def __init__(self):

        self.indexes = {}

        addApiView('database.list_documents', self.listDocuments)
        addApiView('database.reindex', self.reindex)
        addApiView('database.compact', self.compact)
        addApiView('database.document.update', self.updateDocument)
        addApiView('database.document.delete', self.deleteDocument)

        addEvent('database.setup_index', self.setupIndex)
        addEvent('app.after_shutdown', self.close)

        # Safety net: flush DB on interpreter exit even if graceful shutdown
        # is interrupted (e.g. s6-overlay sends SIGKILL before shutdown completes)
        atexit.register(self._atexit_flush)

    def getDB(self):
        if not self.db:
            from couchpotato import get_db
            self.db = get_db()
        return self.db

    def close(self, **kwargs):
        self.getDB().close()

    def _atexit_flush(self):
        """Last-resort flush: called by atexit if the process is exiting
        without a clean app.after_shutdown (e.g. SIGKILL grace period expired
        while plugins were still winding down)."""
        try:
            db = self.getDB()
            if db and db._db:
                storage = db._db.storage
                if hasattr(storage, 'flush'):
                    storage.flush()
        except Exception:
            pass  # Best-effort; don't raise during interpreter shutdown

    def setupIndex(self, index_name, klass):
        """Register an index name.  TinyDB does not use separate index
        files so this is just bookkeeping for compatibility."""
        self.indexes[index_name] = klass

    def deleteDocument(self, **kwargs):
        db = self.getDB()
        try:
            document_id = kwargs.get('_request').get_argument('id')
            document = db.get('id', document_id)
            db.delete(document)
            return {'success': True}
        except:
            return {'success': False, 'error': traceback.format_exc()}

    def updateDocument(self, **kwargs):
        db = self.getDB()
        try:
            document = json.loads(kwargs.get('_request').get_argument('document'))
            d = db.update(document)
            document.update(d)
            return {'success': True, 'document': document}
        except:
            return {'success': False, 'error': traceback.format_exc()}

    def listDocuments(self, **kwargs):
        db = self.getDB()
        results = {'unknown': []}
        for document in db.all('id'):
            key = document.get('_t', 'unknown')
            if kwargs.get('show') and key != kwargs.get('show'):
                continue
            if not results.get(key):
                results[key] = []
            results[key].append(document)
        return results

    def reindex(self, **kwargs):
        # TinyDB does not use separate indexes; this is a no-op.
        return {'success': True}

    def compact(self, **kwargs):
        try:
            db = self.getDB()
            db.compact()
            return {'success': True}
        except:
            log.error('Failed compact: %s', traceback.format_exc())
            return {'success': False}
