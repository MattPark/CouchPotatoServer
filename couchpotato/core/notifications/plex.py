"""Plex Media Server integration — library refresh + PIN-based OAuth auth.

This replaces the old 4-file plex/ directory. Modern Plex clients do not support
notification popups (Player.NotifyMessage only worked with discontinued Plex Home
Theater), so this provider only does library refresh.

Auth: Uses PIN-based OAuth flow (no username/password needed).
 - Frontend calls plex.start_auth -> gets auth URL to open in browser
 - Frontend polls plex.check_auth -> gets token when user approves
 - Token stored in config, used for all subsequent requests
"""

import uuid

import requests as req_lib
import xml.etree.ElementTree as etree

from couchpotato.api import addApiView
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.variable import cleanHost
from couchpotato.core.logger import CPLog
from couchpotato.core.notifications.base import Notification
from couchpotato.environment import Env

log = CPLog(__name__)

autoload = 'Plex'

PLEX_TV_PIN_URL = 'https://plex.tv/api/v2/pins'
PLEX_TV_AUTH_URL = 'https://app.plex.tv/auth#?clientID=%s&code=%s&context%%5Bdevice%%5D%%5Bproduct%%5D=CouchPotato'

# Timeout for all HTTP requests to Plex (seconds)
REQUEST_TIMEOUT = 10


class Plex(Notification):

    # Only care about library refresh events, not snatches
    listen_to = [
        'media.available',
        'renamer.after',
    ]

    http_time_between_calls = 0

    def __init__(self):
        super().__init__()

        # Instance-aware name for API views and config section
        self._section = self.getName().lower()

        # Generate a persistent client identifier immediately so PIN auth
        # works right away — even for instances created after app.load.
        self._ensureClientId()

        # Register PIN auth API endpoints (instance-aware names)
        addApiView('%s.start_auth' % self._section, self.startAuth)
        addApiView('%s.check_auth' % self._section, self.checkAuth)

        # Library refresh on renamer completion
        addEvent('renamer.after', self.addToLibrary)

    def _ensureClientId(self):
        """Generate and persist a UUID for X-Plex-Client-Identifier if not set."""
        if not self.conf('client_id'):
            client_id = str(uuid.uuid4())
            settings = Env.get('settings')
            settings.set(self._section, 'client_id', client_id)
            settings.save()
            log.info('Plex: generated new client identifier')

    def _getVersion(self):
        """Get CouchPotato version safely.

        Env.get() does NOT support default values — the second param is as_unicode,
        not a fallback. Use fireEvent('app.version') which is how the rest of the
        codebase does it.
        """
        try:
            version = fireEvent('app.version', single=True)
            return str(version) if version else '4.0'
        except Exception:
            return '4.0'

    def _plexHeaders(self):
        """Standard headers required by plex.tv API."""
        return {
            'X-Plex-Client-Identifier': self.conf('client_id') or 'couchpotato-unknown',
            'X-Plex-Product': 'CouchPotato',
            'X-Plex-Version': self._getVersion(),
            'Accept': 'application/json',
        }

    def _serverUrl(self):
        """Build base URL for the local Plex Media Server."""
        host = self.conf('media_server') or 'localhost'
        port = self.conf('media_server_port') or '32400'
        use_ssl = self.conf('use_https')

        h = cleanHost(host, True, ssl=bool(use_ssl))
        h = h.rstrip('/')
        # Add port if not already present
        if ':' not in h.split('//')[-1]:
            h += ':%s' % port
        return h

    def _serverRequest(self, path, method='GET', data_type='json'):
        """Make an authenticated request to the local Plex Media Server.

        Returns parsed JSON/XML or None on error.
        """
        token = self.conf('auth_token')
        if not token:
            log.warning('Plex: no auth token — link your Plex account first')
            return None

        url = '%s/%s' % (self._serverUrl(), path.lstrip('/'))
        params = {'X-Plex-Token': token}
        headers = self._plexHeaders()
        if data_type == 'xml':
            del headers['Accept']

        try:
            log.debug('Plex: %s %s' % (method, url.split('?')[0]))
            resp = req_lib.request(method, url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except req_lib.exceptions.ConnectionError:
            log.error('Plex: cannot connect to server at %s — is it running?' % self._serverUrl())
            return None
        except req_lib.exceptions.Timeout:
            log.error('Plex: request timed out for %s' % url.split('?')[0])
            return None
        except req_lib.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 'unknown'
            if status == 401:
                log.error('Plex: authentication failed (401) — re-link your Plex account')
            else:
                log.error('Plex: HTTP error %s for %s' % (status, url.split('?')[0]))
            return None

        if data_type == 'xml':
            try:
                return etree.fromstring(resp.content)
            except etree.ParseError as e:
                log.error('Plex: failed to parse XML response: %s' % e)
                return None
        elif data_type == 'text':
            return resp.text
        else:
            try:
                return resp.json()
            except ValueError:
                return resp.text

    # -----------------------------------------------------------------------
    # PIN-based OAuth authentication
    # -----------------------------------------------------------------------

    def startAuth(self, **kwargs):
        """API endpoint: request a new PIN from plex.tv.

        Returns: {success, auth_url, pin_id}
        """
        # Regenerate client_id if it was wiped (e.g. user X-deleted the
        # provider then re-enabled it — removeInstance clears all config).
        self._ensureClientId()
        client_id = self.conf('client_id')
        if not client_id:
            log.error('Plex: failed to generate client_id')
            return {'success': False, 'message': 'Failed to generate client ID'}

        headers = self._plexHeaders()
        headers['Content-Type'] = 'application/x-www-form-urlencoded'

        try:
            resp = req_lib.post(
                PLEX_TV_PIN_URL,
                data={'strong': 'true', 'X-Plex-Product': 'CouchPotato', 'X-Plex-Client-Identifier': client_id},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            pin_data = resp.json()
        except req_lib.exceptions.RequestException as e:
            log.error('Plex: failed to request PIN from plex.tv: %s' % e)
            return {'success': False, 'message': 'Failed to contact plex.tv: %s' % e}

        pin_id = pin_data.get('id')
        code = pin_data.get('code')
        if not pin_id or not code:
            log.error('Plex: unexpected PIN response: %s' % pin_data)
            return {'success': False, 'message': 'Unexpected response from plex.tv'}

        auth_url = PLEX_TV_AUTH_URL % (client_id, code)
        log.info('Plex: PIN auth started (pin_id=%s), waiting for user approval' % pin_id)

        return {
            'success': True,
            'auth_url': auth_url,
            'pin_id': pin_id,
        }

    def checkAuth(self, **kwargs):
        """API endpoint: check if PIN has been approved.

        Params: pin_id (from startAuth)
        Returns: {success, authenticated, token (if authenticated)}
        """
        pin_id = kwargs.get('pin_id')
        if not pin_id:
            return {'success': False, 'message': 'pin_id is required'}

        client_id = self.conf('client_id')
        headers = self._plexHeaders()

        try:
            resp = req_lib.get(
                '%s/%s' % (PLEX_TV_PIN_URL, pin_id),
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            pin_data = resp.json()
        except req_lib.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 'unknown'
            if status == 404:
                log.warning('Plex: PIN %s expired or invalid' % pin_id)
                return {'success': True, 'authenticated': False, 'expired': True}
            log.error('Plex: failed to check PIN: %s' % e)
            return {'success': False, 'message': 'Failed to check PIN: %s' % e}
        except req_lib.exceptions.RequestException as e:
            log.error('Plex: failed to check PIN: %s' % e)
            return {'success': False, 'message': 'Failed to contact plex.tv: %s' % e}

        auth_token = pin_data.get('authToken')
        if auth_token:
            # Save token to config
            settings = Env.get('settings')
            settings.set(self._section, 'auth_token', auth_token)
            settings.save()
            log.info('Plex: successfully authenticated with plex.tv')
            return {'success': True, 'authenticated': True}
        else:
            return {'success': True, 'authenticated': False, 'expired': False}

    # -----------------------------------------------------------------------
    # Library refresh
    # -----------------------------------------------------------------------

    def addToLibrary(self, message=None, group=None):
        """Called after renamer finishes — refresh Plex library."""
        if self.isDisabled():
            return
        if not group:
            group = {}
        return self.refreshLibrary()

    def refreshLibrary(self):
        """Refresh all movie-type library sections."""
        sections_data = self._serverRequest('library/sections')
        if sections_data is None:
            return False

        # Handle both JSON and XML responses
        sections = []
        if isinstance(sections_data, dict):
            # JSON response
            container = sections_data.get('MediaContainer', {})
            sections = container.get('Directory', [])
            if isinstance(sections, dict):
                sections = [sections]
        elif hasattr(sections_data, 'findall'):
            # XML response
            sections = sections_data.findall('Directory')

        if not sections:
            log.warning('Plex: no library sections found — is the server configured?')
            return False

        movie_sections = []
        for section in sections:
            if isinstance(section, dict):
                sec_type = section.get('type', '')
                sec_key = section.get('key', '')
                sec_title = section.get('title', '')
            else:
                sec_type = section.get('type', '')
                sec_key = section.get('key', '')
                sec_title = section.get('title', '')

            if sec_type == 'movie':
                movie_sections.append((sec_key, sec_title))

        if not movie_sections:
            log.warning('Plex: no movie-type library sections found')
            return False

        success = True
        for key, title in movie_sections:
            result = self._serverRequest('library/sections/%s/refresh' % key, data_type='text')
            if result is not None:
                log.info('Plex: refreshed library section "%s"' % title)
            else:
                log.error('Plex: failed to refresh library section "%s"' % title)
                success = False

        return success

    # -----------------------------------------------------------------------
    # Notification interface (base class contract)
    # -----------------------------------------------------------------------

    def notify(self, message='', data=None, listener=None):
        """Plex doesn't send notification popups — only refreshes library.

        This is called by the base class for non-renamer events.
        For renamer events, addToLibrary handles it directly.
        """
        if not data:
            data = {}
        # For test and other listeners, just verify connectivity
        return self.refreshLibrary()

    def test(self, **kwargs):
        """Test Plex connectivity and library refresh."""
        log.info('Plex: running connectivity test')

        token = self.conf('auth_token')
        if not token:
            return {'success': False, 'message': 'No auth token — link your Plex account first'}

        result = self.refreshLibrary()
        if result:
            return {'success': True, 'message': 'Library refresh triggered successfully'}
        else:
            return {'success': False, 'message': 'Failed to refresh library — check server address and token'}


config = [{
    'name': 'plex',
    'groups': [
        {
            'tab': 'notifications',
            'list': 'notification_providers',
            'name': 'plex',
            'label': 'Plex',
            'description': 'Refresh <a href="https://plex.tv" target="_blank">Plex</a> library when movies are processed.',
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                },
                {
                    'name': 'media_server',
                    'label': 'Server Host',
                    'default': 'localhost',
                    'description': 'Your <strong>local</strong> Plex Media Server IP or hostname '
                                   '(e.g. <code>192.168.1.40</code>). This is NOT plex.tv.',
                },
                {
                    'name': 'media_server_port',
                    'label': 'Port',
                    'default': '32400',
                    'type': 'int',
                    'description': 'Default Plex port is 32400. Only change if you customized it.',
                },
                {
                    'name': 'use_https',
                    'label': 'Use HTTPS',
                    'default': 0,
                    'type': 'bool',
                    'description': 'Connect to Plex over HTTPS. Only enable if your server requires it.',
                },
                {
                    'name': 'auth_token',
                    'label': 'Auth Token',
                    'default': '',
                    'description': 'Set automatically when you link your Plex account below, or '
                                   'paste a token manually from '
                                   '<a href="https://support.plex.tv/articles/204059436/" target="_blank">Plex docs</a>.',
                    'type': 'plex_auth',
                },
                {
                    'name': 'client_id',
                    'default': '',
                    'hidden': True,
                },
                {
                    'name': 'on_snatch',
                    'default': 0,
                    'type': 'bool',
                    'advanced': True,
                    'description': 'Also refresh library when a movie is snatched (not just when processed).',
                },
            ],
        }
    ],
}]
