import asyncio
import json
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote as _urllib_unquote

from couchpotato.core.helpers.request import getParams
from couchpotato.core.logger import CPLog
from tornado.web import RequestHandler


log = CPLog(__name__)


api = {}
api_locks = {}
api_nonblock = {}

api_docs = {}
api_docs_missing = []

# Shared thread pool for API handlers
_api_executor = ThreadPoolExecutor(max_workers=8)


# NonBlock API handler
class NonBlockHandler(RequestHandler):

    stopper = None

    def get(self, route, *args, **kwargs):
        route = route.strip('/')
        start, stop = api_nonblock[route]
        self.stopper = stop

        start(self.sendData, last_id = self.get_argument('last_id', None))

    def sendData(self, response):
        try:
            if not self._finished:
                self.finish(response)
        except:
            log.debug('Failed doing nonblock request, probably already closed: %s', (traceback.format_exc()))
            try:
                if not self._finished:
                    self.finish({'success': False, 'error': 'Failed returning results'})
            except: pass

        self.removeStopper()

    def removeStopper(self):
        if self.stopper:
            self.stopper(self.sendData)

        self.stopper = None


def addNonBlockApiView(route, func_tuple, docs = None, **kwargs):
    api_nonblock[route] = func_tuple

    if docs:
        api_docs[route[4:] if route[0:4] == 'api.' else route] = docs
    else:
        api_docs_missing.append(route)


# Blocking API handler — Tornado 6.x compatible
# Uses async get() with run_in_executor to run handlers off the IOLoop thread,
# then sends the response back on the IOLoop thread via await.
class ApiHandler(RequestHandler):
    route = None

    async def get(self, route, *args, **kwargs):
        self.route = route = route.strip('/')

        # Exact match first, then try prefix match for routes like "file.cache/filename.jpg"
        handler = api.get(route)
        extra_path = None
        if not handler and '/' in route:
            prefix = route.rsplit('/', 1)[0]
            handler = api.get(prefix)
            if handler:
                extra_path = route[len(prefix) + 1:]
                route = prefix

        if not handler:
            # Empty route = base API URL hit (e.g. NZBGet connectivity check)
            if not route:
                self.write({'success': True})
                self.finish()
            else:
                self.write('API call doesn\'t seem to exist')
                self.finish()
            return

        # Create lock if it doesn't exist
        if route in api_locks and not api_locks.get(route):
            api_locks[route] = threading.Lock()

        lock = api_locks[route]

        try:
            kwargs = {}
            for x in self.request.arguments:
                kwargs[x] = _urllib_unquote(self.get_argument(x))

            # Split array arguments
            kwargs = getParams(kwargs)
            kwargs['_request'] = self

            # Pass sub-path as 'filename' for static-style routes (e.g. file.cache/image.jpg)
            if extra_path is not None:
                kwargs['filename'] = extra_path

            # Remove t random string
            try: del kwargs['t']
            except: pass

            # Run the handler in a thread pool so it doesn't block the IOLoop.
            # The lock is acquired/released inside the thread to serialize
            # concurrent requests to the same route.
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                _api_executor,
                self._run_locked, route, kwargs, lock
            )

            self.sendData(result, route)

        except:
            log.error('Failed doing api request "%s": %s', (route, traceback.format_exc()))
            try:
                if not self._finished:
                    self.write({'success': False, 'error': 'Failed returning results'})
                    self.finish()
            except:
                log.error('Failed write error "%s": %s', (route, traceback.format_exc()))

    post = get

    def _run_locked(self, route, kwargs, lock):
        """Run the API handler with the per-route lock held. Runs in executor thread."""
        import time as _time
        t0 = _time.time()
        log.debug('API request start: %s', route)
        lock.acquire()
        try:
            result = api[route](**kwargs)
            log.debug('API request done: %s (%.2fs)', (route, _time.time() - t0))
            return result
        except:
            log.error('Failed doing api request "%s": %s', (route, traceback.format_exc()))
            return {'success': False, 'error': 'Failed returning results'}
        finally:
            lock.release()

    def sendData(self, result, route):
        if self._finished:
            return

        try:
            # Check JSONP callback
            jsonp_callback = self.get_argument('callback_func', default = None)

            if jsonp_callback:
                self.set_header('Content-Type', 'text/javascript')
                self.finish(str(jsonp_callback) + '(' + json.dumps(result) + ')')
            elif isinstance(result, tuple) and result[0] == 'redirect':
                self.redirect(result[1])
            elif isinstance(result, tuple) and result[0] == 'file':
                # Binary file serving: ('file', content_type, bytes_data)
                _, content_type, data = result
                self.set_header('Content-Type', content_type)
                self.set_header('Cache-Control', 'public, max-age=604800')
                self.finish(data)
            else:
                self.finish(result)
        except UnicodeDecodeError:
            log.error('Failed proper encode: %s', traceback.format_exc())
        except:
            log.debug('Failed doing request, probably already closed: %s', (traceback.format_exc()))
            try:
                if not self._finished:
                    self.finish({'success': False, 'error': 'Failed returning results'})
            except: pass


def addApiView(route, func, static = False, docs = None, **kwargs):

    if static: func(route)
    else:
        api[route] = func
        api_locks[route] = threading.Lock()

    if docs:
        api_docs[route[4:] if route[0:4] == 'api.' else route] = docs
    else:
        api_docs_missing.append(route)
