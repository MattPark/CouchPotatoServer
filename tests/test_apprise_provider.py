"""Tests for the Apprise notification provider.

Tests cover:
- mask_url() helper
- _parse_urls_config() JSON parsing
- _get_schemas() schema list building
- Apprise.schemasView() API endpoint
- Apprise.testUrlView() API endpoint
- Apprise.notify() with JSON config format, enabled/disabled filtering,
  IMDB link appending, and error handling.
"""
import json
import pytest
from unittest.mock import patch, MagicMock

from couchpotato.core.notifications.apprise_notify import (
    Apprise as AppriseNotifier,
    mask_url,
    _parse_urls_config,
    _get_schemas,
)


# ---------------------------------------------------------------------------
# mask_url helper
# ---------------------------------------------------------------------------

class TestMaskUrl:
    def test_empty(self):
        assert mask_url('') == '(empty)'
        assert mask_url(None) == '(empty)'

    def test_no_scheme(self):
        assert mask_url('just-a-token') == 'just***'

    def test_short_no_scheme(self):
        assert mask_url('abc') == '***'
        assert mask_url('abcd') == '***'

    def test_short_rest(self):
        assert mask_url('pover://abc') == 'pover://***'

    def test_normal(self):
        assert mask_url('pover://userkey@apitoken') == 'pover://userke***'

    def test_http_url(self):
        assert mask_url('https://hooks.slack.com/services/xxx') == 'https://hooks.***'

    def test_int_input(self):
        # Should convert to string via str()
        assert mask_url(12345678) == '1234***'


# ---------------------------------------------------------------------------
# _parse_urls_config
# ---------------------------------------------------------------------------

class TestParseUrlsConfig:
    def test_empty_string(self):
        assert _parse_urls_config('') == []

    def test_none(self):
        assert _parse_urls_config(None) == []

    def test_whitespace(self):
        assert _parse_urls_config('   ') == []

    def test_valid_json_array(self):
        raw = json.dumps([
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True},
            {'url': 'slack://a/b/c', 'schema': 'slack', 'enabled': False},
        ])
        result = _parse_urls_config(raw)
        assert len(result) == 2
        assert result[0]['url'] == 'pover://user@token'
        assert result[1]['enabled'] is False

    def test_empty_array(self):
        assert _parse_urls_config('[]') == []

    def test_invalid_json(self):
        assert _parse_urls_config('not json') == []

    def test_json_object_not_array(self):
        # JSON object instead of array -> returns empty
        assert _parse_urls_config('{"url": "test"}') == []

    def test_json_string_not_array(self):
        assert _parse_urls_config('"just a string"') == []


# ---------------------------------------------------------------------------
# _get_schemas
# ---------------------------------------------------------------------------

class TestGetSchemas:
    def test_returns_list(self):
        """_get_schemas() returns a non-empty list when Apprise is installed."""
        # Clear cache to force rebuild
        import couchpotato.core.notifications.apprise_notify as mod
        mod._schemas_cache = None

        schemas = _get_schemas()
        assert isinstance(schemas, list)
        assert len(schemas) > 50  # Apprise has 100+ plugins

    def test_schema_structure(self):
        import couchpotato.core.notifications.apprise_notify as mod
        mod._schemas_cache = None

        schemas = _get_schemas()
        for s in schemas:
            assert 'service_name' in s
            assert 'schemas' in s
            assert isinstance(s['schemas'], list)
            assert len(s['schemas']) > 0
            assert 'template' in s
            assert 'service_url' in s
            assert isinstance(s['service_name'], str)  # Not LazyTranslation

    def test_sorted_by_name(self):
        import couchpotato.core.notifications.apprise_notify as mod
        mod._schemas_cache = None

        schemas = _get_schemas()
        names = [s['service_name'].lower() for s in schemas]
        assert names == sorted(names)

    def test_caching(self):
        """Second call returns same object (cached)."""
        import couchpotato.core.notifications.apprise_notify as mod
        mod._schemas_cache = None

        first = _get_schemas()
        second = _get_schemas()
        assert first is second


# ---------------------------------------------------------------------------
# AppriseNotifier fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def apprise_provider():
    """Create an Apprise instance with mocked CP framework."""
    with patch.object(AppriseNotifier, '__init__', lambda self: None):
        provider = AppriseNotifier.__new__(AppriseNotifier)
        provider.default_title = 'CouchPotato'
        provider._conf_values = {'urls': ''}
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)
        return provider


# ---------------------------------------------------------------------------
# schemasView
# ---------------------------------------------------------------------------

class TestSchemasView:
    def test_success(self, apprise_provider):
        result = apprise_provider.schemasView()
        assert result['success'] is True
        assert isinstance(result['schemas'], list)
        assert len(result['schemas']) > 50

    def test_contains_pushover(self, apprise_provider):
        result = apprise_provider.schemasView()
        names = [s['service_name'] for s in result['schemas']]
        assert 'Pushover' in names

    def test_contains_discord(self, apprise_provider):
        result = apprise_provider.schemasView()
        names = [s['service_name'] for s in result['schemas']]
        assert 'Discord' in names


# ---------------------------------------------------------------------------
# testUrlView
# ---------------------------------------------------------------------------

class TestTestUrlView:
    def test_empty_url(self, apprise_provider):
        result = apprise_provider.testUrlView(url='')
        assert result['success'] is False
        assert 'No URL' in result['message']

    def test_missing_url(self, apprise_provider):
        result = apprise_provider.testUrlView()
        assert result['success'] is False

    def test_invalid_url(self, apprise_provider):
        result = apprise_provider.testUrlView(url='notreal://foo')
        assert result['success'] is False
        assert 'Invalid' in result['message'] or 'unsupported' in result['message']

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_valid_url_send_success(self, mock_apprise_cls, apprise_provider):
        # Mock instantiate to return a plugin instance
        mock_instance = MagicMock()
        mock_instance.service_name = 'JSON'
        mock_apprise_cls.instantiate.return_value = mock_instance

        # Mock the Apprise() instance for sending
        mock_ap = MagicMock()
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.testUrlView(url='json://localhost')
        assert result['success'] is True
        assert result['service_name'] == 'JSON'

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_valid_url_send_failure(self, mock_apprise_cls, apprise_provider):
        mock_instance = MagicMock()
        mock_instance.service_name = 'Pushover'
        mock_apprise_cls.instantiate.return_value = mock_instance

        mock_ap = MagicMock()
        mock_ap.notify.return_value = False
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.testUrlView(url='pover://user@token')
        assert result['success'] is False
        assert 'failed' in result['message'].lower()


# ---------------------------------------------------------------------------
# notify() — JSON config format
# ---------------------------------------------------------------------------

class TestAppriseNotify:

    def test_no_urls_configured(self, apprise_provider):
        """Empty config -> return False."""
        apprise_provider._conf_values['urls'] = ''
        result = apprise_provider.notify(message='test')
        assert result is False

    def test_empty_json_array(self, apprise_provider):
        """Empty JSON array -> return False."""
        apprise_provider._conf_values['urls'] = '[]'
        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_success(self, mock_apprise_cls, apprise_provider):
        """Enabled URLs send successfully."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Movie available!')
        assert result is True
        assert mock_ap.add.call_count == 2
        mock_ap.notify.assert_called_once()

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_disabled_urls_filtered(self, mock_apprise_cls, apprise_provider):
        """Disabled URLs are not sent to."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
            {'url': 'slack://a/b/c', 'schema': 'slack', 'enabled': False},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        # Only 2 enabled URLs should be added (slack is disabled)
        assert mock_ap.add.call_count == 2

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_all_disabled_returns_false(self, mock_apprise_cls, apprise_provider):
        """All URLs disabled -> return False."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': False},
        ])

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_all_fail(self, mock_apprise_cls, apprise_provider):
        """Delivery failure returns False."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = False
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_invalid_url_in_config(self, mock_apprise_cls, apprise_provider):
        """Invalid URL rejected by apprise.add() -> no valid URLs -> False."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'not-valid', 'schema': 'unknown', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = False
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_with_imdb(self, mock_apprise_cls, apprise_provider):
        """IMDB link appended when identifier present in data."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        data = {'identifier': 'tt1234567', 'info': {'titles': ['Test Movie']}}
        apprise_provider.notify(message='Available', data=data)

        call_kwargs = mock_ap.notify.call_args
        body = call_kwargs.kwargs.get('body') or call_kwargs[1].get('body', '')
        assert 'tt1234567' in body
        assert 'imdb.com' in body

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_without_imdb(self, mock_apprise_cls, apprise_provider):
        """No crash when data has no identifier."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Test', data={})
        assert result is True

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_exception_handling(self, mock_apprise_cls, apprise_provider):
        """Exception during notify() is caught gracefully."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.side_effect = Exception('connection error')
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_empty_url_strings_skipped(self, mock_apprise_cls, apprise_provider):
        """Entries with empty url strings are skipped."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': '', 'schema': 'json', 'enabled': True},
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
            {'url': '  ', 'schema': 'pover', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        # Only 1 non-empty URL should be added
        assert mock_ap.add.call_count == 1

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_notify_mixed_valid_invalid(self, mock_apprise_cls, apprise_provider):
        """Mix of valid and invalid URLs — only valid ones count."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
            {'url': 'invalid-url', 'schema': 'unknown', 'enabled': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.side_effect = [True, False, True]
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        assert mock_ap.add.call_count == 3

    def test_notify_malformed_json_in_config(self, apprise_provider):
        """Malformed JSON in config -> no URLs -> False."""
        apprise_provider._conf_values['urls'] = 'this is not json [{'
        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_enabled_defaults_true(self, mock_apprise_cls, apprise_provider):
        """Entries without 'enabled' key default to enabled."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json'},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        assert mock_ap.add.call_count == 1

    def test_non_dict_entries_skipped(self, apprise_provider):
        """Non-dict entries in the JSON array are skipped."""
        apprise_provider._conf_values['urls'] = json.dumps([
            'just-a-string',
            42,
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
        ])
        # The string/int entries should be skipped, leaving only 1 valid entry
        # but we need to mock AppriseLib for the actual send
        with patch('couchpotato.core.notifications.apprise_notify.AppriseLib') as mock_cls:
            mock_ap = MagicMock()
            mock_ap.add.return_value = True
            mock_ap.notify.return_value = True
            mock_cls.return_value = mock_ap

            result = apprise_provider.notify(message='test')
            assert result is True
            assert mock_ap.add.call_count == 1


# ---------------------------------------------------------------------------
# notify() — per-URL on_snatch filtering
# ---------------------------------------------------------------------------

class TestAppriseOnSnatch:

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_snatch_listener_skips_on_snatch_false(self, mock_apprise_cls, apprise_provider):
        """When listener is movie.snatched, entries with on_snatch=false are skipped."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True, 'on_snatch': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True, 'on_snatch': False},
            {'url': 'slack://a/b/c', 'schema': 'slack', 'enabled': True, 'on_snatch': True},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Snatched!', listener='movie.snatched')
        assert result is True
        # Only 2 URLs (json + slack) should be added; pover has on_snatch=False
        assert mock_ap.add.call_count == 2

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_non_snatch_listener_ignores_on_snatch_false(self, mock_apprise_cls, apprise_provider):
        """For non-snatch listeners, on_snatch=false entries are NOT skipped."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True, 'on_snatch': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True, 'on_snatch': False},
            {'url': 'slack://a/b/c', 'schema': 'slack', 'enabled': True, 'on_snatch': False},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Available!', listener='media.available')
        assert result is True
        # All 3 URLs should be added — on_snatch is irrelevant for non-snatch listeners
        assert mock_ap.add.call_count == 3

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_snatch_all_on_snatch_false_returns_false(self, mock_apprise_cls, apprise_provider):
        """If all entries have on_snatch=false and listener is snatch, no URLs -> False."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True, 'on_snatch': False},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True, 'on_snatch': False},
        ])

        result = apprise_provider.notify(message='Snatched!', listener='movie.snatched')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_on_snatch_defaults_to_true(self, mock_apprise_cls, apprise_provider):
        """Entries without an on_snatch key default to on_snatch=true."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': True, 'on_snatch': False},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Snatched!', listener='movie.snatched')
        assert result is True
        # json entry has no on_snatch key -> defaults to True, pover has on_snatch=False -> skipped
        assert mock_ap.add.call_count == 1

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_on_snatch_combined_with_enabled(self, mock_apprise_cls, apprise_provider):
        """Both enabled and on_snatch filters apply for snatch listener."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True, 'on_snatch': True},
            {'url': 'pover://user@token', 'schema': 'pover', 'enabled': False, 'on_snatch': True},
            {'url': 'slack://a/b/c', 'schema': 'slack', 'enabled': True, 'on_snatch': False},
            {'url': 'tgram://bot@chat', 'schema': 'tgram', 'enabled': False, 'on_snatch': False},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='Snatched!', listener='movie.snatched')
        assert result is True
        # Only json: enabled=True + on_snatch=True passes both filters
        assert mock_ap.add.call_count == 1

    @patch('couchpotato.core.notifications.apprise_notify.AppriseLib')
    def test_none_listener_ignores_on_snatch(self, mock_apprise_cls, apprise_provider):
        """When listener is None (e.g., direct notify call), on_snatch is irrelevant."""
        apprise_provider._conf_values['urls'] = json.dumps([
            {'url': 'json://localhost', 'schema': 'json', 'enabled': True, 'on_snatch': False},
        ])

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_cls.return_value = mock_ap

        result = apprise_provider.notify(message='test', listener=None)
        assert result is True
        assert mock_ap.add.call_count == 1


# ---------------------------------------------------------------------------
# createNotifyHandler override
# ---------------------------------------------------------------------------

class TestCreateNotifyHandler:

    def test_handler_always_calls_notify_for_snatch(self, apprise_provider):
        """The Apprise createNotifyHandler does NOT gate on global on_snatch;
        it always delegates to _notify, letting notify() handle per-URL filtering."""
        apprise_provider._notify = MagicMock(return_value=True)

        handler = apprise_provider.createNotifyHandler('movie.snatched')
        handler(message='Snatched!', group={'identifier': 'tt1234567'})

        apprise_provider._notify.assert_called_once_with(
            message='Snatched!',
            data={'identifier': 'tt1234567'},
            listener='movie.snatched',
        )

    def test_handler_passes_data_over_group(self, apprise_provider):
        """When both data and group are provided, data takes precedence."""
        apprise_provider._notify = MagicMock(return_value=True)

        handler = apprise_provider.createNotifyHandler('media.available')
        handler(message='Available!', group={'g': 1}, data={'d': 2})

        apprise_provider._notify.assert_called_once_with(
            message='Available!',
            data={'d': 2},
            listener='media.available',
        )

    def test_handler_defaults_group_to_empty_dict(self, apprise_provider):
        """When group is None/empty, an empty dict is used."""
        apprise_provider._notify = MagicMock(return_value=True)

        handler = apprise_provider.createNotifyHandler('media.available')
        handler(message='Test')

        apprise_provider._notify.assert_called_once_with(
            message='Test',
            data={},
            listener='media.available',
        )
