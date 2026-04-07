"""Tests for the Plex notification provider.

Tests PIN-based OAuth flow, library refresh, server connectivity,
error handling, and client ID persistence.
"""
import pytest
from unittest.mock import patch, MagicMock

import requests as req_lib

from couchpotato.core.notifications.plex import Plex


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def plex_provider():
    """Create a Plex instance with mocked CP framework."""
    with patch.object(Plex, '__init__', lambda self: None):
        provider = Plex.__new__(Plex)
        provider.default_title = 'CouchPotato'
        provider.test_message = 'CouchPotato test notification'
        # Mock conf() values
        provider._conf_values = {
            'media_server': 'localhost',
            'media_server_port': '32400',
            'auth_token': 'test-token-abc',
            'client_id': 'test-client-id-uuid',
            'on_snatch': False,
        }
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)

        # Mock isDisabled
        provider.isDisabled = lambda: False

        return provider


# ---------------------------------------------------------------------------
# _serverUrl
# ---------------------------------------------------------------------------

class TestServerUrl:
    def test_default(self, plex_provider):
        url = plex_provider._serverUrl()
        assert 'localhost' in url
        assert '32400' in url

    def test_custom_host_port(self, plex_provider):
        plex_provider._conf_values['media_server'] = '192.168.1.10'
        plex_provider._conf_values['media_server_port'] = '32401'
        url = plex_provider._serverUrl()
        assert '192.168.1.10' in url
        assert '32401' in url

    def test_host_with_port_included(self, plex_provider):
        """If host already contains a port, don't add another."""
        plex_provider._conf_values['media_server'] = 'http://myserver:9999'
        url = plex_provider._serverUrl()
        assert '9999' in url
        # Should not have doubled ports
        assert url.count(':') == 2  # http:// + host:port


# ---------------------------------------------------------------------------
# _plexHeaders
# ---------------------------------------------------------------------------

class TestPlexHeaders:
    @patch('couchpotato.core.notifications.plex.Env')
    def test_contains_client_id(self, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Client-Identifier'] == 'test-client-id-uuid'
        assert headers['X-Plex-Product'] == 'CouchPotato'
        assert headers['X-Plex-Version'] == '4.3.0'

    @patch('couchpotato.core.notifications.plex.Env')
    def test_fallback_client_id(self, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        plex_provider._conf_values['client_id'] = ''
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Client-Identifier'] == 'couchpotato-unknown'


# ---------------------------------------------------------------------------
# _serverRequest
# ---------------------------------------------------------------------------

class TestServerRequest:
    def test_no_token_returns_none(self, plex_provider):
        plex_provider._conf_values['auth_token'] = ''
        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_json_response(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'MediaContainer': {'Directory': []}}
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result == {'MediaContainer': {'Directory': []}}

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_xml_response(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        xml_content = b'<MediaContainer><Directory type="movie" key="1" title="Movies"/></MediaContainer>'
        mock_resp = MagicMock()
        mock_resp.content = xml_content
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections', data_type='xml')
        assert result is not None
        assert result.tag == 'MediaContainer'

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_connection_error(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_req.request.side_effect = req_lib.exceptions.ConnectionError('refused')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_timeout_error(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_req.request.side_effect = req_lib.exceptions.Timeout('timed out')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_401_error(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_response = MagicMock()
        mock_response.status_code = 401
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.request.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_text_response(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.text = 'OK'
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections/1/refresh', data_type='text')
        assert result == 'OK'


# ---------------------------------------------------------------------------
# startAuth (PIN auth)
# ---------------------------------------------------------------------------

class TestStartAuth:
    def test_no_client_id(self, plex_provider):
        plex_provider._conf_values['client_id'] = ''
        result = plex_provider.startAuth()
        assert result['success'] is False
        assert 'client ID' in result['message']

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_success(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'id': 12345, 'code': 'ABCDE'}
        mock_resp.raise_for_status.return_value = None
        mock_req.post.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.startAuth()
        assert result['success'] is True
        assert result['pin_id'] == 12345
        assert 'auth_url' in result
        assert 'ABCDE' in result['auth_url']
        assert 'test-client-id-uuid' in result['auth_url']

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_pin_request_failure(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_req.post.side_effect = req_lib.exceptions.RequestException('network error')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.startAuth()
        assert result['success'] is False

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_unexpected_response(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'unexpected': 'data'}
        mock_resp.raise_for_status.return_value = None
        mock_req.post.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.startAuth()
        assert result['success'] is False


# ---------------------------------------------------------------------------
# checkAuth
# ---------------------------------------------------------------------------

class TestCheckAuth:
    def test_missing_pin_id(self, plex_provider):
        result = plex_provider.checkAuth()
        assert result['success'] is False

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_approved(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'authToken': 'my-new-token'}
        mock_resp.raise_for_status.return_value = None
        mock_req.get.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        mock_settings = MagicMock()
        mock_env.get.side_effect = lambda key, *args: mock_settings if key == 'settings' else '4.3.0'

        result = plex_provider.checkAuth(pin_id='12345')
        assert result['success'] is True
        assert result['authenticated'] is True

        # Token should be saved
        mock_settings.set.assert_called_once_with('plex', 'auth_token', 'my-new-token')
        mock_settings.save.assert_called_once()

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_pending(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'authToken': None}
        mock_resp.raise_for_status.return_value = None
        mock_req.get.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.checkAuth(pin_id='12345')
        assert result['success'] is True
        assert result['authenticated'] is False
        assert result['expired'] is False

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_expired_pin(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.get.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.checkAuth(pin_id='99999')
        assert result['success'] is True
        assert result['authenticated'] is False
        assert result['expired'] is True

    @patch('couchpotato.core.notifications.plex.Env')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_network_error(self, mock_req, mock_env, plex_provider):
        mock_env.get.return_value = '4.3.0'
        mock_req.get.side_effect = req_lib.exceptions.ConnectionError('refused')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.checkAuth(pin_id='12345')
        assert result['success'] is False


# ---------------------------------------------------------------------------
# refreshLibrary
# ---------------------------------------------------------------------------

class TestRefreshLibrary:
    @patch.object(Plex, '_serverRequest')
    def test_no_sections_data(self, mock_request, plex_provider):
        mock_request.return_value = None
        result = plex_provider.refreshLibrary()
        assert result is False

    @patch.object(Plex, '_serverRequest')
    def test_json_movie_sections(self, mock_request, plex_provider):
        """JSON response with movie sections triggers refresh."""
        mock_request.side_effect = [
            # First call: get sections
            {'MediaContainer': {'Directory': [
                {'type': 'movie', 'key': '1', 'title': 'Movies'},
                {'type': 'show', 'key': '2', 'title': 'TV Shows'},
                {'type': 'movie', 'key': '3', 'title': 'Kids Movies'},
            ]}},
            # Second call: refresh section 1
            'OK',
            # Third call: refresh section 3
            'OK',
        ]

        result = plex_provider.refreshLibrary()
        assert result is True
        assert mock_request.call_count == 3
        # Verify correct sections refreshed
        mock_request.assert_any_call('library/sections/1/refresh', data_type='text')
        mock_request.assert_any_call('library/sections/3/refresh', data_type='text')

    @patch.object(Plex, '_serverRequest')
    def test_no_movie_sections(self, mock_request, plex_provider):
        """Server has sections but none are movie type."""
        mock_request.return_value = {'MediaContainer': {'Directory': [
            {'type': 'show', 'key': '1', 'title': 'TV Shows'},
        ]}}

        result = plex_provider.refreshLibrary()
        assert result is False

    @patch.object(Plex, '_serverRequest')
    def test_empty_sections(self, mock_request, plex_provider):
        """Server returns empty section list."""
        mock_request.return_value = {'MediaContainer': {'Directory': []}}

        result = plex_provider.refreshLibrary()
        assert result is False

    @patch.object(Plex, '_serverRequest')
    def test_single_section_dict(self, mock_request, plex_provider):
        """When there's one section, Plex may return dict instead of list."""
        mock_request.side_effect = [
            {'MediaContainer': {'Directory': {'type': 'movie', 'key': '1', 'title': 'Movies'}}},
            'OK',
        ]

        result = plex_provider.refreshLibrary()
        assert result is True

    @patch.object(Plex, '_serverRequest')
    def test_refresh_failure_partial(self, mock_request, plex_provider):
        """One section succeeds, another fails -> returns False."""
        mock_request.side_effect = [
            {'MediaContainer': {'Directory': [
                {'type': 'movie', 'key': '1', 'title': 'Movies'},
                {'type': 'movie', 'key': '2', 'title': 'More Movies'},
            ]}},
            'OK',   # section 1 OK
            None,   # section 2 failed
        ]

        result = plex_provider.refreshLibrary()
        assert result is False


# ---------------------------------------------------------------------------
# notify and test
# ---------------------------------------------------------------------------

class TestNotifyAndTest:
    @patch.object(Plex, 'refreshLibrary')
    def test_notify_calls_refresh(self, mock_refresh, plex_provider):
        mock_refresh.return_value = True
        result = plex_provider.notify(message='test', data={})
        assert result is True
        mock_refresh.assert_called_once()

    def test_test_no_token(self, plex_provider):
        plex_provider._conf_values['auth_token'] = ''
        result = plex_provider.test()
        assert result['success'] is False
        assert 'token' in result['message'].lower()

    @patch.object(Plex, 'refreshLibrary')
    def test_test_success(self, mock_refresh, plex_provider):
        mock_refresh.return_value = True
        result = plex_provider.test()
        assert result['success'] is True

    @patch.object(Plex, 'refreshLibrary')
    def test_test_failure(self, mock_refresh, plex_provider):
        mock_refresh.return_value = False
        result = plex_provider.test()
        assert result['success'] is False


# ---------------------------------------------------------------------------
# addToLibrary
# ---------------------------------------------------------------------------

class TestAddToLibrary:
    @patch.object(Plex, 'refreshLibrary')
    def test_calls_refresh(self, mock_refresh, plex_provider):
        mock_refresh.return_value = True
        result = plex_provider.addToLibrary(message='test', group={'destination_dir': '/movies/test'})
        assert result is True

    def test_disabled_skips(self, plex_provider):
        plex_provider.isDisabled = lambda: True
        result = plex_provider.addToLibrary(message='test')
        assert result is None


# ---------------------------------------------------------------------------
# _ensureClientId
# ---------------------------------------------------------------------------

class TestEnsureClientId:
    @patch('couchpotato.core.notifications.plex.Env')
    def test_generates_uuid_when_empty(self, mock_env, plex_provider):
        plex_provider._conf_values['client_id'] = ''
        mock_settings = MagicMock()
        mock_env.get.return_value = mock_settings

        plex_provider._ensureClientId()

        mock_settings.set.assert_called_once()
        call_args = mock_settings.set.call_args
        assert call_args[0][0] == 'plex'
        assert call_args[0][1] == 'client_id'
        # Should be a valid UUID-like string
        assert len(call_args[0][2]) == 36  # UUID format: 8-4-4-4-12

    @patch('couchpotato.core.notifications.plex.Env')
    def test_noop_when_already_set(self, mock_env, plex_provider):
        # client_id already set in fixture
        mock_settings = MagicMock()
        mock_env.get.return_value = mock_settings

        plex_provider._ensureClientId()

        mock_settings.set.assert_not_called()
