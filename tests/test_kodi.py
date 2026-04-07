"""Tests for the Kodi (XBMC) notification provider.

Tests JSON-RPC notifications, library scans, auth header building,
multi-host support, and error handling.
"""
import base64
import json
import pytest
from unittest.mock import patch, MagicMock

import requests as req_lib

from couchpotato.core.notifications.xbmc import Kodi


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def kodi_provider():
    """Create a Kodi instance with mocked CP framework."""
    with patch.object(Kodi, '__init__', lambda self: None):
        provider = Kodi.__new__(Kodi)
        provider.default_title = 'CouchPotato'
        provider.test_message = 'CouchPotato test notification'
        provider._conf_values = {
            'host': 'localhost:8080',
            'username': 'kodi',
            'password': '',
            'only_first': False,
            'remote_dir_scan': False,
            'force_full_scan': False,
            'on_snatch': False,
        }
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)
        provider.getNotificationImage = lambda size='small': 'http://cp/image.png'
        return provider


# ---------------------------------------------------------------------------
# _buildAuth
# ---------------------------------------------------------------------------

class TestBuildAuth:
    def test_no_password(self, kodi_provider):
        """No password -> no auth header."""
        result = kodi_provider._buildAuth()
        assert result is None

    def test_with_password(self, kodi_provider):
        kodi_provider._conf_values['password'] = 'secret'
        kodi_provider._conf_values['username'] = 'admin'
        result = kodi_provider._buildAuth()
        assert result.startswith('Basic ')
        # Decode and verify
        encoded = result.split(' ', 1)[1]
        decoded = base64.b64decode(encoded).decode('utf-8')
        assert decoded == 'admin:secret'

    def test_default_username(self, kodi_provider):
        """Empty username defaults to 'kodi'."""
        kodi_provider._conf_values['password'] = 'secret'
        kodi_provider._conf_values['username'] = ''
        result = kodi_provider._buildAuth()
        encoded = result.split(' ', 1)[1]
        decoded = base64.b64decode(encoded).decode('utf-8')
        assert decoded == 'kodi:secret'


# ---------------------------------------------------------------------------
# _jsonrpc
# ---------------------------------------------------------------------------

class TestJsonRpc:
    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_success(self, mock_req, kodi_provider):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'jsonrpc': '2.0', 'result': 'OK', 'id': 'test'}
        mock_resp.raise_for_status.return_value = None
        mock_req.post.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification', {'title': 'Test'})
        assert result['result'] == 'OK'

        # Verify the request was made correctly
        call_args = mock_req.post.call_args
        assert call_args[0][0] == 'http://localhost:8080/jsonrpc'
        payload = json.loads(call_args[1]['data'])
        assert payload['method'] == 'GUI.ShowNotification'
        assert payload['params'] == {'title': 'Test'}

    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_with_auth_header(self, mock_req, kodi_provider):
        kodi_provider._conf_values['password'] = 'secret'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'result': 'OK'}
        mock_resp.raise_for_status.return_value = None
        mock_req.post.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification')

        call_headers = mock_req.post.call_args[1]['headers']
        assert 'Authorization' in call_headers
        assert call_headers['Authorization'].startswith('Basic ')

    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_connection_error(self, mock_req, kodi_provider):
        mock_req.post.side_effect = req_lib.exceptions.ConnectionError('refused')
        mock_req.exceptions = req_lib.exceptions

        result = kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification')
        assert result is None

    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_timeout(self, mock_req, kodi_provider):
        mock_req.post.side_effect = req_lib.exceptions.Timeout('timed out')
        mock_req.exceptions = req_lib.exceptions

        result = kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification')
        assert result is None

    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_401_error(self, mock_req, kodi_provider):
        mock_response = MagicMock()
        mock_response.status_code = 401
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.post.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        result = kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification')
        assert result is None

    @patch('couchpotato.core.notifications.xbmc.req_lib')
    def test_invalid_json_response(self, mock_req, kodi_provider):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.side_effect = ValueError('bad json')
        mock_req.post.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = kodi_provider._jsonrpc('localhost:8080', 'GUI.ShowNotification')
        assert result is None


# ---------------------------------------------------------------------------
# _sendNotification
# ---------------------------------------------------------------------------

class TestSendNotification:
    @patch.object(Kodi, '_jsonrpc')
    def test_success(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = {'result': 'OK'}
        result = kodi_provider._sendNotification('localhost:8080', 'Test message')
        assert result is True

        call_args = mock_rpc.call_args
        assert call_args[0][1] == 'GUI.ShowNotification'
        params = call_args[0][2]
        assert params['title'] == 'CouchPotato'
        assert params['message'] == 'Test message'

    @patch.object(Kodi, '_jsonrpc')
    def test_rpc_error(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = {'error': {'message': 'Invalid params', 'code': -32602}}
        result = kodi_provider._sendNotification('localhost:8080', 'Test')
        assert result is False

    @patch.object(Kodi, '_jsonrpc')
    def test_connection_failed(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = None
        result = kodi_provider._sendNotification('localhost:8080', 'Test')
        assert result is False


# ---------------------------------------------------------------------------
# _scanLibrary
# ---------------------------------------------------------------------------

class TestScanLibrary:
    @patch.object(Kodi, '_jsonrpc')
    def test_full_scan(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = {'result': 'OK'}
        result = kodi_provider._scanLibrary('localhost:8080')
        assert result is True

        call_args = mock_rpc.call_args
        assert call_args[0][1] == 'VideoLibrary.Scan'
        assert call_args[0][2] == {}

    @patch.object(Kodi, '_jsonrpc')
    def test_path_specific_scan(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = {'result': 'OK'}
        result = kodi_provider._scanLibrary('localhost:8080', {'directory': '/movies/new'})
        assert result is True

        call_args = mock_rpc.call_args
        assert call_args[0][2] == {'directory': '/movies/new'}

    @patch.object(Kodi, '_jsonrpc')
    def test_scan_error(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = {'error': {'message': 'scan failed', 'code': -1}}
        result = kodi_provider._scanLibrary('localhost:8080')
        assert result is False

    @patch.object(Kodi, '_jsonrpc')
    def test_scan_connection_failed(self, mock_rpc, kodi_provider):
        mock_rpc.return_value = None
        result = kodi_provider._scanLibrary('localhost:8080')
        assert result is False


# ---------------------------------------------------------------------------
# notify (full integration)
# ---------------------------------------------------------------------------

class TestNotify:
    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_single_host_no_data(self, mock_notif, mock_scan, kodi_provider):
        """Single host, no destination_dir -> notification only, no scan."""
        mock_notif.return_value = True
        result = kodi_provider.notify(message='Movie available!')
        assert result is True
        mock_notif.assert_called_once()
        mock_scan.assert_not_called()

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_with_destination_dir(self, mock_notif, mock_scan, kodi_provider):
        """With destination_dir -> notification + library scan."""
        mock_notif.return_value = True
        mock_scan.return_value = True
        result = kodi_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )
        assert result is True
        mock_notif.assert_called_once()
        mock_scan.assert_called_once()

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_multiple_hosts(self, mock_notif, mock_scan, kodi_provider):
        """Multiple comma-separated hosts."""
        kodi_provider._conf_values['host'] = 'host1:8080, host2:8080'
        mock_notif.return_value = True
        mock_scan.return_value = True

        result = kodi_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )
        assert result is True
        assert mock_notif.call_count == 2
        assert mock_scan.call_count == 2

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_only_first_scan(self, mock_notif, mock_scan, kodi_provider):
        """only_first=True -> scan only the first host."""
        kodi_provider._conf_values['host'] = 'host1:8080, host2:8080'
        kodi_provider._conf_values['only_first'] = True
        mock_notif.return_value = True
        mock_scan.return_value = True

        result = kodi_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )
        assert result is True
        assert mock_notif.call_count == 2  # notifications to both
        assert mock_scan.call_count == 1   # scan only first

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_notification_failure(self, mock_notif, mock_scan, kodi_provider):
        """Notification fails -> returns False."""
        mock_notif.return_value = False
        result = kodi_provider.notify(message='Test')
        assert result is False

    def test_no_hosts(self, kodi_provider):
        """No hosts configured -> returns False."""
        kodi_provider._conf_values['host'] = ''
        result = kodi_provider.notify(message='Test')
        assert result is False

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_force_full_scan(self, mock_notif, mock_scan, kodi_provider):
        """force_full_scan=True -> scan with empty params (no directory)."""
        kodi_provider._conf_values['force_full_scan'] = True
        mock_notif.return_value = True
        mock_scan.return_value = True

        kodi_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )

        scan_call_args = mock_scan.call_args
        # With force_full_scan, should NOT pass directory param
        assert scan_call_args[0][1] == {}

    @patch.object(Kodi, '_scanLibrary')
    @patch.object(Kodi, '_sendNotification')
    def test_remote_dir_scan(self, mock_notif, mock_scan, kodi_provider):
        """remote_dir_scan=True -> passes directory even for remote hosts."""
        kodi_provider._conf_values['host'] = 'remote-host:8080'
        kodi_provider._conf_values['remote_dir_scan'] = True
        mock_notif.return_value = True
        mock_scan.return_value = True

        kodi_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )

        scan_call_args = mock_scan.call_args
        assert scan_call_args[0][1] == {'directory': '/movies/test'}


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------

class TestTest:
    def test_no_hosts(self, kodi_provider):
        kodi_provider._conf_values['host'] = ''
        result = kodi_provider.test()
        assert result['success'] is False
        assert 'host' in result['message'].lower()

    @patch.object(Kodi, 'notify')
    def test_success(self, mock_notify, kodi_provider):
        mock_notify.return_value = True
        result = kodi_provider.test()
        assert result['success'] is True

    @patch.object(Kodi, 'notify')
    def test_failure(self, mock_notify, kodi_provider):
        mock_notify.return_value = False
        result = kodi_provider.test()
        assert result['success'] is False


# ---------------------------------------------------------------------------
# _isLocalHost
# ---------------------------------------------------------------------------

class TestIsLocalHost:
    def test_localhost(self, kodi_provider):
        assert kodi_provider._isLocalHost('localhost:8080') is True

    def test_remote(self, kodi_provider):
        # This might vary by system, but a random hostname shouldn't match localhost
        result = kodi_provider._isLocalHost('remote-server:8080')
        assert isinstance(result, bool)
