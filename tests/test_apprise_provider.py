"""Tests for the Apprise notification provider.

Tests the notify() method with mocked apprise library, URL validation,
IMDB link appending, and error handling.
"""
import pytest
from unittest.mock import patch, MagicMock

from couchpotato.core.notifications.apprise_notify import (
    AppriseNotification,
    mask_token,
    mask_url,
)


# ---------------------------------------------------------------------------
# mask_token / mask_url helpers
# ---------------------------------------------------------------------------

class TestMaskToken:
    def test_empty(self):
        assert mask_token('') == '(empty)'
        assert mask_token(None) == '(empty)'

    def test_short(self):
        assert mask_token('abc') == '***'
        assert mask_token('abcd') == '***'

    def test_normal(self):
        assert mask_token('abcdef12345') == 'abcd***'

    def test_int_input(self):
        assert mask_token(12345678) == '1234***'


class TestMaskUrl:
    def test_empty(self):
        assert mask_url('') == '(empty)'
        assert mask_url(None) == '(empty)'

    def test_no_scheme(self):
        assert mask_url('just-a-token') == 'just***'

    def test_short_rest(self):
        assert mask_url('pover://abc') == 'pover://***'

    def test_normal(self):
        assert mask_url('pover://userkey@apitoken') == 'pover://userke***'

    def test_http_url(self):
        assert mask_url('https://hooks.slack.com/services/xxx') == 'https://hooks.***'


# ---------------------------------------------------------------------------
# AppriseNotification.notify() — requires mocking the CP framework
# ---------------------------------------------------------------------------

@pytest.fixture
def apprise_provider():
    """Create an AppriseNotification instance with mocked CP framework."""
    with patch.object(AppriseNotification, '__init__', lambda self: None):
        provider = AppriseNotification.__new__(AppriseNotification)
        provider.default_title = 'CouchPotato'
        # Mock conf() to return test values
        provider._conf_values = {'urls': '', 'on_snatch': False}
        provider.conf = lambda key, default='': provider._conf_values.get(key, default)
        return provider


class TestAppriseNotify:

    def test_no_urls_configured(self, apprise_provider):
        """No URLs -> return False, don't crash."""
        apprise_provider._conf_values['urls'] = ''
        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_success(self, mock_apprise_mod, apprise_provider):
        """All URLs valid and send succeeds."""
        apprise_provider._conf_values['urls'] = 'json://localhost, pover://user@token'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='Movie available!')
        assert result is True
        assert mock_ap.add.call_count == 2
        mock_ap.notify.assert_called_once()

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_all_fail(self, mock_apprise_mod, apprise_provider):
        """All URLs fail to send."""
        apprise_provider._conf_values['urls'] = 'json://localhost'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = False
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_invalid_url(self, mock_apprise_mod, apprise_provider):
        """Invalid URL is rejected by apprise.add()."""
        apprise_provider._conf_values['urls'] = 'not-a-valid-url'

        mock_ap = MagicMock()
        mock_ap.add.return_value = False
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_with_imdb(self, mock_apprise_mod, apprise_provider):
        """IMDB link appended when identifier present in data."""
        apprise_provider._conf_values['urls'] = 'json://localhost'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_mod.Apprise.return_value = mock_ap

        data = {'identifier': 'tt1234567', 'info': {'titles': ['Test Movie']}}
        apprise_provider.notify(message='Available', data=data)

        call_kwargs = mock_ap.notify.call_args
        body = call_kwargs.kwargs.get('body') or call_kwargs[1].get('body', '')
        assert 'tt1234567' in body
        assert 'imdb.com' in body

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_without_imdb(self, mock_apprise_mod, apprise_provider):
        """No crash when data has no identifier."""
        apprise_provider._conf_values['urls'] = 'json://localhost'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='Test', data={})
        assert result is True

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_exception_handling(self, mock_apprise_mod, apprise_provider):
        """Exception during notify() is caught gracefully."""
        apprise_provider._conf_values['urls'] = 'json://localhost'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.side_effect = Exception('connection error')
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is False

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_partial_valid_urls(self, mock_apprise_mod, apprise_provider):
        """Mix of valid and invalid URLs — only valid ones count."""
        apprise_provider._conf_values['urls'] = 'json://localhost, invalid-url, pover://user@token'

        mock_ap = MagicMock()
        # First and third URLs valid, second invalid
        mock_ap.add.side_effect = [True, False, True]
        mock_ap.notify.return_value = True
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        assert mock_ap.add.call_count == 3

    @patch('couchpotato.core.notifications.apprise_notify.apprise')
    def test_notify_empty_url_strings_skipped(self, mock_apprise_mod, apprise_provider):
        """Empty strings in URL list are skipped."""
        apprise_provider._conf_values['urls'] = 'json://localhost, , ,pover://user@token'

        mock_ap = MagicMock()
        mock_ap.add.return_value = True
        mock_ap.notify.return_value = True
        mock_apprise_mod.Apprise.return_value = mock_ap

        result = apprise_provider.notify(message='test')
        assert result is True
        # Only 2 non-empty URLs should be added
        assert mock_ap.add.call_count == 2
