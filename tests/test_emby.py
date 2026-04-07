"""Tests for the Emby / Jellyfin notification provider.

Tests library refresh (full and path-specific), admin notifications,
auth headers for both server types, and error handling.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

import requests as req_lib

from couchpotato.core.notifications.emby import Emby


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def emby_provider():
    """Create an Emby instance configured as Emby server."""
    with patch.object(Emby, '__init__', lambda self: None):
        provider = Emby.__new__(Emby)
        provider.default_title = 'CouchPotato'
        provider.test_message = 'CouchPotato test notification'
        provider._conf_values = {
            'server_type': 'emby',
            'host': 'localhost:8096',
            'apikey': 'test-api-key-123',
            'force_full_scan': False,
            'on_snatch': False,
        }
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)
        return provider


@pytest.fixture
def jellyfin_provider():
    """Create an Emby instance configured as Jellyfin server."""
    with patch.object(Emby, '__init__', lambda self: None):
        provider = Emby.__new__(Emby)
        provider.default_title = 'CouchPotato'
        provider.test_message = 'CouchPotato test notification'
        provider._conf_values = {
            'server_type': 'jellyfin',
            'host': 'localhost:8096',
            'apikey': 'jf-api-key-456',
            'force_full_scan': False,
            'on_snatch': False,
        }
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)
        return provider


# ---------------------------------------------------------------------------
# _baseUrl
# ---------------------------------------------------------------------------

class TestBaseUrl:
    def test_emby_prefix(self, emby_provider):
        url = emby_provider._baseUrl()
        assert url.endswith('emby/')
        assert 'localhost' in url or '8096' in url

    def test_jellyfin_no_prefix(self, jellyfin_provider):
        url = jellyfin_provider._baseUrl()
        assert 'emby/' not in url


# ---------------------------------------------------------------------------
# _authHeaders
# ---------------------------------------------------------------------------

class TestAuthHeaders:
    def test_emby_headers(self, emby_provider):
        headers = emby_provider._authHeaders()
        assert headers['X-Emby-Token'] == 'test-api-key-123'
        assert 'Authorization' not in headers
        assert headers['Content-Type'] == 'application/json'

    def test_jellyfin_headers(self, jellyfin_provider):
        headers = jellyfin_provider._authHeaders()
        assert 'X-Emby-Token' not in headers
        assert 'MediaBrowser Token="jf-api-key-456"' in headers['Authorization']
        assert headers['Content-Type'] == 'application/json'


# ---------------------------------------------------------------------------
# _request
# ---------------------------------------------------------------------------

class TestRequest:
    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_success(self, mock_req, emby_provider):
        mock_resp = MagicMock()
        mock_resp.status_code = 204
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is True
        assert status == 204

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_connection_error(self, mock_req, emby_provider):
        mock_req.request.side_effect = req_lib.exceptions.ConnectionError('refused')
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is False
        assert status is None

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_timeout(self, mock_req, emby_provider):
        mock_req.request.side_effect = req_lib.exceptions.Timeout('timed out')
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is False
        assert status is None

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_401_error(self, mock_req, emby_provider):
        mock_response = MagicMock()
        mock_response.status_code = 401
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.request.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is False
        assert status == 401

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_403_error(self, mock_req, emby_provider):
        mock_response = MagicMock()
        mock_response.status_code = 403
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.request.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is False
        assert status == 403

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_500_error(self, mock_req, emby_provider):
        mock_response = MagicMock()
        mock_response.status_code = 500
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.request.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        success, status = emby_provider._request('Library/Refresh')
        assert success is False
        assert status == 500

    @patch('couchpotato.core.notifications.emby.req_lib')
    def test_post_with_data(self, mock_req, emby_provider):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        data = {'Updates': [{'Path': '/movies', 'UpdateType': 'Created'}]}
        emby_provider._request('Library/Media/Updated', data=data)

        call_args = mock_req.request.call_args
        sent_data = json.loads(call_args[1]['data'])
        assert sent_data == data


# ---------------------------------------------------------------------------
# _refreshLibrary
# ---------------------------------------------------------------------------

class TestRefreshLibrary:
    @patch.object(Emby, '_request')
    def test_full_refresh(self, mock_request, emby_provider):
        mock_request.return_value = (True, 204)
        result = emby_provider._refreshLibrary()
        assert result is True
        mock_request.assert_called_once_with('Library/Refresh')

    @patch.object(Emby, '_request')
    def test_path_specific_refresh(self, mock_request, emby_provider):
        mock_request.return_value = (True, 204)
        result = emby_provider._refreshLibrary(destination_dir='/movies/NewMovie')
        assert result is True

        call_args = mock_request.call_args
        assert call_args[0][0] == 'Library/Media/Updated'
        sent_data = call_args[1]['data']
        assert sent_data['Updates'][0]['Path'] == '/movies/NewMovie'
        assert sent_data['Updates'][0]['UpdateType'] == 'Created'

    @patch.object(Emby, '_request')
    def test_force_full_scan_overrides_path(self, mock_request, emby_provider):
        """force_full_scan=True -> always full refresh even with destination_dir."""
        emby_provider._conf_values['force_full_scan'] = True
        mock_request.return_value = (True, 204)

        result = emby_provider._refreshLibrary(destination_dir='/movies/NewMovie')
        assert result is True
        mock_request.assert_called_once_with('Library/Refresh')

    @patch.object(Emby, '_request')
    def test_refresh_failure(self, mock_request, emby_provider):
        mock_request.return_value = (False, None)
        result = emby_provider._refreshLibrary()
        assert result is False


# ---------------------------------------------------------------------------
# _sendAdminNotification
# ---------------------------------------------------------------------------

class TestAdminNotification:
    @patch.object(Emby, '_request')
    def test_emby_sends_notification(self, mock_request, emby_provider):
        mock_request.return_value = (True, 200)
        result = emby_provider._sendAdminNotification('Test message')
        assert result is True

        call_args = mock_request.call_args
        assert call_args[0][0] == 'Notifications/Admin'
        sent_data = call_args[1]['data']
        assert sent_data['Name'] == 'CouchPotato'
        assert sent_data['Description'] == 'Test message'

    @patch.object(Emby, '_request')
    def test_jellyfin_skips_notification(self, mock_request, jellyfin_provider):
        """Jellyfin doesn't support admin notifications -> skip, return True."""
        result = jellyfin_provider._sendAdminNotification('Test message')
        assert result is True
        mock_request.assert_not_called()


# ---------------------------------------------------------------------------
# notify
# ---------------------------------------------------------------------------

class TestNotify:
    @patch.object(Emby, '_sendAdminNotification')
    @patch.object(Emby, '_refreshLibrary')
    def test_notify_emby(self, mock_refresh, mock_admin, emby_provider):
        mock_refresh.return_value = True
        mock_admin.return_value = True

        result = emby_provider.notify(message='Movie available!', data={})
        assert result is True
        mock_refresh.assert_called_once_with(destination_dir=None)
        mock_admin.assert_called_once_with('Movie available!')

    @patch.object(Emby, '_sendAdminNotification')
    @patch.object(Emby, '_refreshLibrary')
    def test_notify_with_destination(self, mock_refresh, mock_admin, emby_provider):
        mock_refresh.return_value = True
        mock_admin.return_value = True

        result = emby_provider.notify(
            message='Movie available!',
            data={'destination_dir': '/movies/test'},
        )
        assert result is True
        mock_refresh.assert_called_once_with(destination_dir='/movies/test')

    @patch.object(Emby, '_sendAdminNotification')
    @patch.object(Emby, '_refreshLibrary')
    def test_notify_refresh_failure(self, mock_refresh, mock_admin, emby_provider):
        """Library refresh failure -> return False, even if admin notif succeeds."""
        mock_refresh.return_value = False
        mock_admin.return_value = True

        result = emby_provider.notify(message='Test', data={})
        assert result is False

    @patch.object(Emby, '_sendAdminNotification')
    @patch.object(Emby, '_refreshLibrary')
    def test_notify_jellyfin(self, mock_refresh, mock_admin, jellyfin_provider):
        mock_refresh.return_value = True

        result = jellyfin_provider.notify(message='Movie available!', data={})
        assert result is True
        mock_refresh.assert_called_once()


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------

class TestTest:
    def test_no_api_key(self, emby_provider):
        emby_provider._conf_values['apikey'] = ''
        result = emby_provider.test()
        assert result['success'] is False
        assert 'API key' in result['message']

    @patch.object(Emby, '_refreshLibrary')
    def test_success_emby(self, mock_refresh, emby_provider):
        mock_refresh.return_value = True
        result = emby_provider.test()
        assert result['success'] is True
        assert 'Emby' in result['message']

    @patch.object(Emby, '_refreshLibrary')
    def test_success_jellyfin(self, mock_refresh, jellyfin_provider):
        mock_refresh.return_value = True
        result = jellyfin_provider.test()
        assert result['success'] is True
        assert 'Jellyfin' in result['message']

    @patch.object(Emby, '_refreshLibrary')
    def test_failure(self, mock_refresh, emby_provider):
        mock_refresh.return_value = False
        result = emby_provider.test()
        assert result['success'] is False
