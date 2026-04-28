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
            'use_https': 0,
        }
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)

        # Set the section name (normally set in __init__)
        provider._section = 'plex'

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
        assert url.startswith('http://')

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

    def test_use_https(self, plex_provider):
        """use_https option should produce https:// URL."""
        plex_provider._conf_values['use_https'] = 1
        url = plex_provider._serverUrl()
        assert url.startswith('https://')

    def test_no_https_by_default(self, plex_provider):
        """Default should be http://."""
        plex_provider._conf_values['use_https'] = 0
        url = plex_provider._serverUrl()
        assert url.startswith('http://')

    def test_host_with_explicit_https_prefix(self, plex_provider):
        """If user puts https:// in host field, cleanHost preserves it."""
        plex_provider._conf_values['media_server'] = 'https://secure.plex.local'
        url = plex_provider._serverUrl()
        assert 'https://' in url


# ---------------------------------------------------------------------------
# _plexHeaders
# ---------------------------------------------------------------------------

class TestPlexHeaders:
    @patch('couchpotato.core.notifications.plex.fireEvent')
    def test_contains_client_id(self, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Client-Identifier'] == 'test-client-id-uuid'
        assert headers['X-Plex-Product'] == 'CouchPotato'
        assert headers['X-Plex-Version'] == '4.3.0'

    @patch('couchpotato.core.notifications.plex.fireEvent')
    def test_fallback_client_id(self, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        plex_provider._conf_values['client_id'] = ''
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Client-Identifier'] == 'couchpotato-unknown'

    @patch('couchpotato.core.notifications.plex.fireEvent')
    def test_version_fallback_on_none(self, mock_fire, plex_provider):
        """If fireEvent returns None, version should fall back to '4.0'."""
        mock_fire.return_value = None
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Version'] == '4.0'

    @patch('couchpotato.core.notifications.plex.fireEvent')
    def test_version_fallback_on_exception(self, mock_fire, plex_provider):
        """If fireEvent raises, version should fall back to '4.0'."""
        mock_fire.side_effect = Exception('no event handler')
        headers = plex_provider._plexHeaders()
        assert headers['X-Plex-Version'] == '4.0'


# ---------------------------------------------------------------------------
# _serverRequest
# ---------------------------------------------------------------------------

class TestServerRequest:
    def test_no_token_returns_none(self, plex_provider):
        plex_provider._conf_values['auth_token'] = ''
        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_json_response(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'MediaContainer': {'Directory': []}}
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result == {'MediaContainer': {'Directory': []}}

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_xml_response(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        xml_content = b'<MediaContainer><Directory type="movie" key="1" title="Movies"/></MediaContainer>'
        mock_resp = MagicMock()
        mock_resp.content = xml_content
        mock_resp.raise_for_status.return_value = None
        mock_req.request.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections', data_type='xml')
        assert result is not None
        assert result.tag == 'MediaContainer'

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_connection_error(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_req.request.side_effect = req_lib.exceptions.ConnectionError('refused')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_timeout_error(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_req.request.side_effect = req_lib.exceptions.Timeout('timed out')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_401_error(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_response = MagicMock()
        mock_response.status_code = 401
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.request.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider._serverRequest('/library/sections')
        assert result is None

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_text_response(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
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
    def test_regenerates_client_id_when_empty(self, plex_provider):
        """If client_id was wiped (e.g. X-delete then re-enable), startAuth
        regenerates it on the fly instead of erroring."""
        plex_provider._conf_values['client_id'] = ''

        # Mock _ensureClientId to simulate successful generation
        def mock_ensure():
            plex_provider._conf_values['client_id'] = 'newly-generated-uuid'
        plex_provider._ensureClientId = mock_ensure

        with patch('couchpotato.core.notifications.plex.fireEvent') as mock_fire, \
             patch('couchpotato.core.notifications.plex.req_lib') as mock_req:
            mock_fire.return_value = '4.3.0'
            mock_resp = MagicMock()
            mock_resp.json.return_value = {'id': 99, 'code': 'XYZ'}
            mock_resp.raise_for_status.return_value = None
            mock_req.post.return_value = mock_resp
            mock_req.exceptions = req_lib.exceptions

            result = plex_provider.startAuth()
            assert result['success'] is True
            assert result['pin_id'] == 99

    def test_error_if_ensure_fails(self, plex_provider):
        """If _ensureClientId can't generate an ID (broken settings), error."""
        plex_provider._conf_values['client_id'] = ''
        plex_provider._ensureClientId = lambda: None  # no-op, doesn't fix it

        result = plex_provider.startAuth()
        assert result['success'] is False
        assert 'client id' in result['message'].lower()

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_success(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
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

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_pin_request_failure(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_req.post.side_effect = req_lib.exceptions.RequestException('network error')
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.startAuth()
        assert result['success'] is False

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_unexpected_response(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
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
    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_approved(self, mock_req, mock_fire, mock_env, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'authToken': 'my-new-token'}
        mock_resp.raise_for_status.return_value = None
        mock_req.get.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        mock_settings = MagicMock()
        mock_env.get.return_value = mock_settings

        result = plex_provider.checkAuth(pin_id='12345')
        assert result['success'] is True
        assert result['authenticated'] is True

        # Token should be saved
        mock_settings.set.assert_called_once_with('plex', 'auth_token', 'my-new-token')
        mock_settings.save.assert_called_once()

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_pending(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_resp = MagicMock()
        mock_resp.json.return_value = {'authToken': None}
        mock_resp.raise_for_status.return_value = None
        mock_req.get.return_value = mock_resp
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.checkAuth(pin_id='12345')
        assert result['success'] is True
        assert result['authenticated'] is False
        assert result['expired'] is False

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_expired_pin(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
        mock_response = MagicMock()
        mock_response.status_code = 404
        http_err = req_lib.exceptions.HTTPError(response=mock_response)
        mock_req.get.side_effect = http_err
        mock_req.exceptions = req_lib.exceptions

        result = plex_provider.checkAuth(pin_id='99999')
        assert result['success'] is True
        assert result['authenticated'] is False
        assert result['expired'] is True

    @patch('couchpotato.core.notifications.plex.fireEvent')
    @patch('couchpotato.core.notifications.plex.req_lib')
    def test_network_error(self, mock_req, mock_fire, plex_provider):
        mock_fire.return_value = '4.3.0'
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


# ---------------------------------------------------------------------------
# getMachineIdentifier
# ---------------------------------------------------------------------------

class TestGetMachineIdentifier:

    @patch.object(Plex, '_serverRequest')
    def test_returns_identifier(self, mock_req, plex_provider):
        mock_req.return_value = {
            'MediaContainer': {
                'machineIdentifier': 'abc123def',
                'version': '1.40.0',
            }
        }
        assert plex_provider.getMachineIdentifier() == 'abc123def'
        mock_req.assert_called_once_with('identity')

    @patch.object(Plex, '_serverRequest')
    def test_returns_none_on_failure(self, mock_req, plex_provider):
        mock_req.return_value = None
        assert plex_provider.getMachineIdentifier() is None

    @patch.object(Plex, '_serverRequest')
    def test_handles_flat_response(self, mock_req, plex_provider):
        """Some Plex versions return machineIdentifier at top level."""
        mock_req.return_value = {'machineIdentifier': 'flat123'}
        assert plex_provider.getMachineIdentifier() == 'flat123'


# ---------------------------------------------------------------------------
# getFileToRatingKeyMap
# ---------------------------------------------------------------------------

class TestGetFileToRatingKeyMap:

    @patch.object(Plex, '_serverRequest')
    def test_builds_map_from_library(self, mock_req, plex_provider):
        """Builds file path -> ratingKey map from Plex library."""
        mock_req.side_effect = [
            # sections response
            {'MediaContainer': {'Directory': [
                {'type': 'movie', 'key': '2', 'title': 'Movies'},
            ]}},
            # all movies response
            {'MediaContainer': {'Metadata': [
                {
                    'ratingKey': '100',
                    'title': "The 'Burbs",
                    'year': 1989,
                    'Guid': [{'id': 'imdb://tt0096734'}, {'id': 'tmdb://11974'}],
                    'Media': [{'Part': [
                        {'file': '/home/plex/media/Movies/Burbs (1989)/Burbs (1989) 1080p.mkv'}
                    ]}],
                },
                {
                    'ratingKey': '200',
                    'title': 'King Kong',
                    'year': 2005,
                    'Guid': [{'id': 'imdb://tt0360717'}],
                    'Media': [{'Part': [
                        {'file': '/home/plex/media/Movies/King Kong (2005)/King Kong (2005) 1080p.mkv'}
                    ]}],
                },
            ]}},
        ]

        result = plex_provider.getFileToRatingKeyMap()
        assert len(result) == 2
        burbs = result['/home/plex/media/Movies/Burbs (1989)/Burbs (1989) 1080p.mkv']
        assert burbs['ratingKey'] == '100'
        assert burbs['imdb_id'] == 'tt0096734'
        assert burbs['title'] == "The 'Burbs"
        assert burbs['year'] == 1989
        kong = result['/home/plex/media/Movies/King Kong (2005)/King Kong (2005) 1080p.mkv']
        assert kong['ratingKey'] == '200'

    @patch.object(Plex, '_serverRequest')
    def test_empty_when_disabled(self, mock_req, plex_provider):
        plex_provider.isDisabled = lambda: True
        assert plex_provider.getFileToRatingKeyMap() == {}
        mock_req.assert_not_called()

    @patch.object(Plex, '_serverRequest')
    def test_empty_when_no_token(self, mock_req, plex_provider):
        plex_provider._conf_values['auth_token'] = ''
        assert plex_provider.getFileToRatingKeyMap() == {}

    @patch.object(Plex, '_serverRequest')
    def test_empty_when_sections_fail(self, mock_req, plex_provider):
        mock_req.return_value = None
        assert plex_provider.getFileToRatingKeyMap() == {}

    @patch.object(Plex, '_serverRequest')
    def test_no_movie_sections(self, mock_req, plex_provider):
        mock_req.return_value = {'MediaContainer': {'Directory': [
            {'type': 'show', 'key': '1', 'title': 'TV Shows'},
        ]}}
        assert plex_provider.getFileToRatingKeyMap() == {}

    @patch.object(Plex, '_serverRequest')
    def test_movie_without_guid(self, mock_req, plex_provider):
        """Movie with no Guid array still maps by file path, imdb_id is None."""
        mock_req.side_effect = [
            {'MediaContainer': {'Directory': [
                {'type': 'movie', 'key': '2'},
            ]}},
            {'MediaContainer': {'Metadata': [
                {
                    'ratingKey': '300',
                    'Media': [{'Part': [{'file': '/movies/Test (2020)/Test.mkv'}]}],
                },
            ]}},
        ]
        result = plex_provider.getFileToRatingKeyMap()
        assert result['/movies/Test (2020)/Test.mkv']['imdb_id'] is None
        assert result['/movies/Test (2020)/Test.mkv']['ratingKey'] == '300'

    @patch.object(Plex, '_serverRequest')
    def test_multi_part_movie(self, mock_req, plex_provider):
        """Multi-part movie maps each file separately."""
        mock_req.side_effect = [
            {'MediaContainer': {'Directory': [{'type': 'movie', 'key': '2'}]}},
            {'MediaContainer': {'Metadata': [
                {
                    'ratingKey': '400',
                    'Guid': [{'id': 'imdb://tt0071381'}],
                    'Media': [
                        {'Part': [{'file': '/movies/Movie/Movie CD1.mkv'}]},
                        {'Part': [{'file': '/movies/Movie/Movie CD2.mkv'}]},
                    ],
                },
            ]}},
        ]
        result = plex_provider.getFileToRatingKeyMap()
        assert len(result) == 2
        assert result['/movies/Movie/Movie CD1.mkv']['ratingKey'] == '400'
        assert result['/movies/Movie/Movie CD2.mkv']['ratingKey'] == '400'
