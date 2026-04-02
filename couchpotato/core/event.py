import sys
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from couchpotato.core.helpers.variable import mergeDicts, natsortKey
from couchpotato.core.logger import CPLog

log = CPLog(__name__)
events = {}

# Shared pool for concurrent handler execution
_pool = ThreadPoolExecutor(max_workers=10)


def runHandler(name, handler, *args, **kwargs):
    try:
        return handler(*args, **kwargs)
    except Exception:
        from couchpotato.environment import Env
        log.error('Error in event "%s", that wasn\'t caught: %s%s',
                  (name, traceback.format_exc(),
                   Env.all() if not Env.get('dev') else ''))


def addEvent(name, handler, priority=100):
    if name not in events:
        events[name] = []

    def createHandle(*args, **kwargs):
        h = None
        try:
            has_parent = hasattr(handler, 'im_self')
            parent = None
            if has_parent:
                parent = handler.__self__
                if hasattr(parent, 'beforeCall'):
                    parent.beforeCall(handler)
            try:
                h = runHandler(name, handler, *args, **kwargs)
            finally:
                if parent and has_parent:
                    if hasattr(parent, 'afterCall'):
                        parent.afterCall(handler)
        except Exception:
            log.error('Failed creating handler %s %s: %s',
                      (name, handler, traceback.format_exc()))
        return h

    events[name].append({
        'handler': createHandle,
        'priority': priority,
    })


def removeEvent(name, handler):
    """Remove a previously registered handler by reference."""
    if name not in events:
        return
    events[name] = [e for e in events[name]
                    if getattr(e['handler'], '__wrapped__', None) is not handler]


def fireEvent(name, *args, **kwargs):
    if name not in events:
        return

    try:
        options = {
            'is_after_event': False,
            'on_complete': False,
            'single': False,
            'merge': False,
            'in_order': False,
        }

        for x in list(options.keys()):
            if x in kwargs:
                options[x] = kwargs.pop(x)

        handlers = sorted(events[name], key=lambda e: e['priority'])

        if options['in_order'] or options['single'] or len(handlers) == 1:
            # Serial execution
            results = _run_serial(name, handlers, options['single'], *args, **kwargs)
        else:
            # Concurrent execution via thread pool
            results = _run_concurrent(name, handlers, *args, **kwargs)

        # Process results
        if options['single'] and not options['merge']:
            final = None
            for success, value in results:
                if success and value is not None:
                    final = value
                    break
        else:
            final = [value for success, value in results if success and value]

            if options['merge'] and final:
                if isinstance(final[0], dict):
                    final.reverse()
                    merged = {}
                    for d in final:
                        merged = mergeDicts(merged, d, prepend_list=True)
                    final = merged
                elif isinstance(final[0], list):
                    merged = []
                    for lst in final:
                        if lst not in merged:
                            merged += lst
                    final = merged

        modified = fireEvent('result.modify.%s' % name, final, single=True)
        if modified:
            log.debug('Return modified results for %s', name)
            final = modified

        if not options['is_after_event']:
            fireEvent('%s.after' % name, is_after_event=True)

        if options['on_complete']:
            options['on_complete']()

        return final
    except Exception:
        log.error('%s: %s', (name, traceback.format_exc()))


def _run_serial(name, handlers, return_on_first, *args, **kwargs):
    """Run handlers sequentially, optionally stopping at first non-None result."""
    results = []
    for entry in handlers:
        try:
            r = entry['handler'](*args, **kwargs)
            results.append((True, r))
            if return_on_first and r is not None:
                break
        except Exception:
            log.error('Failed running event handler for %s: %s',
                      (name, traceback.format_exc()))
            results.append((False, sys.exc_info()))
    return results


def _run_concurrent(name, handlers, *args, **kwargs):
    """Run handlers concurrently via thread pool, return results in priority order."""
    futures = {}
    try:
        for i, entry in enumerate(handlers):
            f = _pool.submit(entry['handler'], *args, **kwargs)
            futures[f] = i
    except RuntimeError:
        # Pool has been shut down (app is exiting) — fall back to serial
        log.debug('Thread pool shut down, running %s handlers serially', name)
        return _run_serial(name, handlers, False, *args, **kwargs)

    results = [None] * len(handlers)
    for f in as_completed(futures):
        idx = futures[f]
        try:
            r = f.result()
            results[idx] = (True, r)
        except Exception:
            log.error('Failed running event handler for %s: %s',
                      (name, traceback.format_exc()))
            results[idx] = (False, sys.exc_info())

    return [r for r in results if r is not None]


def fireEventAsync(*args, **kwargs):
    try:
        t = threading.Thread(target=fireEvent, args=args, kwargs=kwargs)
        t.daemon = True
        t.start()
        return True
    except Exception as e:
        log.error('%s: %s', (args[0], e))


def errorHandler(error):
    etype, value, tb = error
    log.error(''.join(traceback.format_exception(etype, value, tb)))


def getEvent(name):
    return events.get(name, [])
