"""Emby / Jellyfin integration — library refresh + admin notifications.

Supports both Emby and Jellyfin via a server_type dropdown:
- Emby: URL prefix /emby/, auth via X-Emby-Token header
- Jellyfin: No URL prefix, auth via Authorization: MediaBrowser Token="..." header

Library refresh options:
- Full scan: POST /Library/Refresh
- Path-specific scan: POST /Library/Media/Updated (when destination_dir is available)
"""

import json

import requests as req_lib

from couchpotato.core.helpers.variable import cleanHost
from couchpotato.core.logger import CPLog
from couchpotato.core.notifications.base import Notification

log = CPLog(__name__)

autoload = 'Emby'

# Timeout for all HTTP requests (seconds)
REQUEST_TIMEOUT = 10


class Emby(Notification):

    listen_to = ['renamer.after', 'movie.snatched']
    http_time_between_calls = 0

    def _baseUrl(self):
        """Build base URL for the media server."""
        host = cleanHost(self.conf('host') or 'localhost:8096')
        server_type = self.conf('server_type') or 'emby'
        if server_type == 'emby':
            return '%semby/' % host
        else:
            # Jellyfin — no /emby/ prefix
            return host

    def _authHeaders(self):
        """Build authentication headers based on server type."""
        apikey = self.conf('apikey') or ''
        server_type = self.conf('server_type') or 'emby'

        headers = {'Content-Type': 'application/json'}

        if server_type == 'emby':
            headers['X-Emby-Token'] = apikey
        else:
            # Jellyfin uses MediaBrowser auth header
            headers['Authorization'] = 'MediaBrowser Token="%s"' % apikey

        return headers

    def _request(self, path, method='POST', data=None):
        """Make an authenticated request to the media server.

        Args:
            path: API path (appended to base URL)
            method: HTTP method
            data: dict to JSON-encode as request body

        Returns:
            (success: bool, status_code: int or None)
        """
        url = '%s%s' % (self._baseUrl(), path.lstrip('/'))
        headers = self._authHeaders()
        server_type = (self.conf('server_type') or 'emby').capitalize()

        try:
            log.debug('%s: %s %s' % (server_type, method, url))
            resp = req_lib.request(
                method,
                url,
                headers=headers,
                data=json.dumps(data) if data else None,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            log.debug('%s: %s -> %d' % (server_type, path, resp.status_code))
            return True, resp.status_code
        except req_lib.exceptions.ConnectionError:
            log.error('%s: cannot connect to server at %s — is it running?' % (server_type, self._baseUrl()))
            return False, None
        except req_lib.exceptions.Timeout:
            log.error('%s: request timed out for %s' % (server_type, url))
            return False, None
        except req_lib.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 401 or status == 403:
                log.error('%s: authentication failed (%d) — check your API key' % (server_type, status))
            else:
                log.error('%s: HTTP %d for %s' % (server_type, status, path))
            return False, status

    def _refreshLibrary(self, destination_dir=None):
        """Refresh the media library.

        If destination_dir is provided and force_full_scan is off, uses path-specific scan.
        Otherwise does a full library refresh.
        """
        server_type = (self.conf('server_type') or 'emby').capitalize()

        if destination_dir and not self.conf('force_full_scan'):
            # Path-specific scan
            data = {
                'Updates': [{
                    'Path': destination_dir,
                    'UpdateType': 'Created',
                }]
            }
            success, status = self._request('Library/Media/Updated', data=data)
            if success:
                log.info('%s: path-specific library scan triggered for: %s' % (server_type, destination_dir))
            return success
        else:
            # Full library refresh
            success, status = self._request('Library/Refresh')
            if success:
                log.info('%s: full library refresh triggered' % server_type)
            return success

    def _sendAdminNotification(self, message):
        """Send a notification to the server admin dashboard (Emby only)."""
        server_type = self.conf('server_type') or 'emby'
        if server_type != 'emby':
            log.debug('Jellyfin: admin notifications not supported — skipping popup')
            return True  # Not a failure, just not supported

        data = {
            'Name': 'CouchPotato',
            'Description': message,
        }
        success, status = self._request('Notifications/Admin', data=data)
        if success:
            log.info('Emby: admin notification sent')
        return success

    def notify(self, message='', data=None, listener=None):
        if not data:
            data = {}

        server_type = (self.conf('server_type') or 'emby').capitalize()

        # Refresh library
        destination_dir = data.get('destination_dir')
        refresh_ok = self._refreshLibrary(destination_dir=destination_dir)

        # Send admin notification (Emby only, best-effort)
        self._sendAdminNotification(message)

        return refresh_ok

    def test(self, **kwargs):
        """Test server connectivity."""
        server_type = (self.conf('server_type') or 'emby').capitalize()
        apikey = self.conf('apikey')
        if not apikey:
            return {'success': False, 'message': 'No API key configured'}

        log.info('%s: running connectivity test' % server_type)

        # Try a full library refresh as connectivity test
        success = self._refreshLibrary()
        if success:
            return {'success': True, 'message': '%s library refresh triggered' % server_type}
        else:
            return {'success': False, 'message': 'Failed to connect to %s — check host and API key' % server_type}


config = [{
    'name': 'emby',
    'groups': [
        {
            'tab': 'notifications',
            'list': 'notification_providers',
            'name': 'emby',
            'label': 'Emby / Jellyfin',
            'description': 'Refresh library on <a href="https://emby.media" target="_blank">Emby</a> '
                           'or <a href="https://jellyfin.org" target="_blank">Jellyfin</a> when movies are processed.',
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                },
                {
                    'name': 'server_type',
                    'label': 'Server Type',
                    'default': 'emby',
                    'type': 'dropdown',
                    'values': [('Emby', 'emby'), ('Jellyfin', 'jellyfin')],
                    'description': 'Select your media server software. This controls the API URL format and auth header style.',
                },
                {
                    'name': 'host',
                    'default': 'localhost:8096',
                    'description': 'Server <code>host:port</code>. Default port is 8096 for Emby, '
                                   '8096 for Jellyfin. Example: <code>192.168.1.50:8096</code>',
                },
                {
                    'name': 'apikey',
                    'label': 'API Key',
                    'default': '',
                    'type': 'password',
                    'description': '<strong>Emby:</strong> Dashboard &gt; Advanced &gt; API Keys &gt; New API Key.<br>'
                                   '<strong>Jellyfin:</strong> Dashboard &gt; API Keys &gt; Add.',
                },
                {
                    'name': 'force_full_scan',
                    'label': 'Full Library Scan',
                    'default': 0,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Always do a full library scan instead of scanning only the new movie folder.',
                },
                {
                    'name': 'on_snatch',
                    'default': 0,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Also refresh library when a movie is snatched.',
                },
            ],
        }
    ],
}]
