"""Kodi (formerly XBMC) integration — notification popups + library scan.

Uses Kodi's JSON-RPC API (v6+, stable since XBMC v12 Frodo, Jan 2013).
All legacy xbmcHttp and JSON-RPC v2/v4 detection code has been removed —
no Kodi version in the last 13 years uses those APIs.

API docs: https://kodi.wiki/view/JSON-RPC_API
"""

import base64
import json
import socket

import requests as req_lib

from couchpotato.core.helpers.variable import splitString
from couchpotato.core.logger import CPLog
from couchpotato.core.notifications.base import Notification

log = CPLog(__name__)

autoload = 'Kodi'

# Timeout for all HTTP requests to Kodi (seconds)
REQUEST_TIMEOUT = 10


class Kodi(Notification):

    listen_to = ['renamer.after', 'movie.snatched']
    http_time_between_calls = 0

    def notify(self, message='', data=None, listener=None):
        if not data:
            data = {}

        hosts = splitString(self.conf('host'))
        if not hosts:
            log.warning('Kodi: no hosts configured')
            return False

        total_success = True

        for host in hosts:
            host = host.strip()
            if not host:
                continue

            # 1) Send notification popup
            notif_ok = self._sendNotification(host, message)
            if not notif_ok:
                total_success = False

            # 2) Trigger library scan (if we have a destination directory)
            if data.get('destination_dir') and (not self.conf('only_first') or hosts.index(host) == 0):
                scan_params = {}
                if not self.conf('force_full_scan') and (self.conf('remote_dir_scan') or self._isLocalHost(host)):
                    scan_params = {'directory': data['destination_dir']}

                scan_ok = self._scanLibrary(host, scan_params)
                if not scan_ok:
                    total_success = False

        return total_success

    def _isLocalHost(self, host):
        """Check if host appears to be localhost."""
        hostname = host.split(':')[0]
        try:
            return socket.getfqdn('localhost') == socket.getfqdn(hostname)
        except Exception:
            return False

    def _buildAuth(self):
        """Build Basic auth header if credentials are configured."""
        username = self.conf('username')
        password = self.conf('password')
        if not password:
            return None
        user = username or 'kodi'
        # base64.b64encode returns bytes, decode to str for the header
        creds = base64.b64encode(('%s:%s' % (user, password)).encode('utf-8')).decode('utf-8')
        return 'Basic %s' % creds

    def _jsonrpc(self, host, method, params=None):
        """Send a single JSON-RPC request to a Kodi host.

        Args:
            host: "hostname:port" string
            method: JSON-RPC method name (e.g. "GUI.ShowNotification")
            params: dict of method parameters

        Returns:
            dict with JSON-RPC response, or None on error
        """
        if params is None:
            params = {}

        url = 'http://%s/jsonrpc' % host
        payload = {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': method,
        }

        headers = {'Content-Type': 'application/json'}
        auth_header = self._buildAuth()
        if auth_header:
            headers['Authorization'] = auth_header

        try:
            log.debug('Kodi: %s -> %s' % (host, method))
            resp = req_lib.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json()
            log.debug('Kodi: %s <- %s' % (host, result.get('result', result.get('error', 'unknown'))))
            return result
        except req_lib.exceptions.ConnectionError:
            log.info('Kodi: cannot connect to %s — is it running?' % host)
            return None
        except req_lib.exceptions.Timeout:
            log.info('Kodi: request timed out for %s' % host)
            return None
        except req_lib.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 'unknown'
            if status == 401:
                log.error('Kodi: authentication failed for %s — check username/password' % host)
            else:
                log.error('Kodi: HTTP %s from %s' % (status, host))
            return None
        except ValueError:
            log.error('Kodi: invalid JSON response from %s' % host)
            return None

    def _sendNotification(self, host, message):
        """Send a GUI notification popup to a Kodi host."""
        result = self._jsonrpc(host, 'GUI.ShowNotification', {
            'title': self.default_title,
            'message': message,
            'image': self.getNotificationImage('small'),
        })
        if result and result.get('result') == 'OK':
            log.info('Kodi: notification sent to %s' % host)
            return True
        elif result and result.get('error'):
            err = result['error']
            log.error('Kodi: notification error on %s: %s (code %s)' % (host, err.get('message', '?'), err.get('code', '?')))
            return False
        else:
            # result is None (connection error already logged)
            return False

    def _scanLibrary(self, host, params=None):
        """Trigger a video library scan on a Kodi host."""
        if params is None:
            params = {}

        scan_type = 'path-specific' if params.get('directory') else 'full'
        result = self._jsonrpc(host, 'VideoLibrary.Scan', params)
        if result and result.get('result') == 'OK':
            log.info('Kodi: %s library scan triggered on %s' % (scan_type, host))
            return True
        elif result and result.get('error'):
            err = result['error']
            log.error('Kodi: library scan error on %s: %s (code %s)' % (host, err.get('message', '?'), err.get('code', '?')))
            return False
        else:
            return False

    def test(self, **kwargs):
        """Test Kodi connectivity."""
        hosts = splitString(self.conf('host'))
        if not hosts:
            return {'success': False, 'message': 'No Kodi hosts configured'}

        log.info('Kodi: running connectivity test')
        success = self.notify(
            message=self.test_message,
            data={},
            listener='test',
        )
        return {'success': success}


config = [{
    'name': 'xbmc',
    'groups': [
        {
            'tab': 'notifications',
            'list': 'notification_providers',
            'name': 'xbmc',
            'label': 'Kodi',
            'description': 'Send notifications and trigger library scans on <a href="https://kodi.tv" target="_blank">Kodi</a> (v12+). '
                           'Enable HTTP control in Kodi: <em>Settings &gt; Services &gt; Control &gt; Allow remote control via HTTP</em>.',
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                },
                {
                    'name': 'host',
                    'default': 'localhost:8080',
                    'description': 'Kodi <code>host:port</code> (default web port is 8080). '
                                   'For multiple instances, separate with commas: '
                                   '<code>192.168.1.10:8080, 192.168.1.11:8080</code>',
                },
                {
                    'name': 'username',
                    'default': 'kodi',
                    'description': 'Web server username (set in Kodi under Services &gt; Control).',
                },
                {
                    'name': 'password',
                    'default': '',
                    'type': 'password',
                    'description': 'Web server password.',
                },
                {
                    'name': 'only_first',
                    'default': 0,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Only scan library on the first host (useful when multiple Kodi instances share a database).',
                },
                {
                    'name': 'remote_dir_scan',
                    'label': 'Remote Folder Scan',
                    'default': 0,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Send the movie folder path to remote Kodi for a path-specific scan instead of a full scan.',
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
                    'description': 'Also send notification when a movie is snatched.',
                },
            ],
        }
    ],
}]
