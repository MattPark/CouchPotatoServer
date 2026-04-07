"""Tests for the notification multi-instance manager.

Tests creation, removal, and restoration of duplicate notification
provider instances (e.g., plex_2, emby_3).
"""
import configparser
import copy
import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from couchpotato.core.notifications import (
    NotificationInstanceManager,
    _provider_registry,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_settings(sections=None, values=None):
    """Create a mock Settings object."""
    mock = MagicMock()
    mock.sections.return_value = sections or []
    mock.options = {}
    mock.getOptions.return_value = {}
    mock.getValues.return_value = values or {}

    parser = configparser.RawConfigParser()
    for s in (sections or []):
        parser.add_section(s)
    mock.parser.return_value = parser
    mock.save = MagicMock()

    return mock


def _make_mock_plex_config():
    """Create a mock plex provider config template."""
    return [{
        'name': 'plex',
        'groups': [{
            'tab': 'notifications',
            'list': 'notification_providers',
            'name': 'plex',
            'label': 'Plex',
            'description': 'Refresh Plex library',
            'options': [
                {'name': 'enabled', 'default': 0, 'type': 'enabler'},
                {'name': 'media_server', 'default': 'localhost'},
                {'name': 'media_server_port', 'default': '32400', 'type': 'int'},
                {'name': 'auth_token', 'default': '', 'type': 'plex_auth'},
                {'name': 'client_id', 'default': '', 'hidden': True},
            ],
        }],
    }]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def manager():
    """Create a NotificationInstanceManager with mocked CP framework."""
    with patch.object(NotificationInstanceManager, '__init__', lambda self: None):
        mgr = NotificationInstanceManager.__new__(NotificationInstanceManager)
        mgr._instances = {}
        return mgr


@pytest.fixture
def mock_settings():
    return _make_mock_settings(sections=['core', 'plex', 'emby'])


@pytest.fixture
def plex_registry_entry():
    """A provider registry entry for plex."""
    mock_module = MagicMock()
    mock_module.autoload = 'Plex'
    mock_module.config = _make_mock_plex_config()

    # Create a minimal mock Plex class
    mock_cls = MagicMock()
    mock_module.Plex = mock_cls

    return {
        'module': mock_module,
        'class_name': 'Plex',
        'config': mock_module.config,
        'label': 'Plex',
    }


# ---------------------------------------------------------------------------
# _nextSectionName
# ---------------------------------------------------------------------------

class TestNextSectionName:

    @patch('couchpotato.core.notifications.Env')
    def test_first_duplicate(self, mock_env, manager):
        mock_settings = _make_mock_settings(sections=['plex'])
        mock_env.get.return_value = mock_settings
        result = manager._nextSectionName('plex')
        assert result == 'plex_2'

    @patch('couchpotato.core.notifications.Env')
    def test_third_duplicate(self, mock_env, manager):
        mock_settings = _make_mock_settings(sections=['plex', 'plex_2'])
        mock_env.get.return_value = mock_settings
        result = manager._nextSectionName('plex')
        assert result == 'plex_3'

    @patch('couchpotato.core.notifications.Env')
    def test_gap_in_numbering(self, mock_env, manager):
        mock_settings = _make_mock_settings(sections=['plex', 'plex_2', 'plex_4'])
        mock_env.get.return_value = mock_settings
        # Should fill the gap
        result = manager._nextSectionName('plex')
        assert result == 'plex_3'


# ---------------------------------------------------------------------------
# _cloneConfigForSection
# ---------------------------------------------------------------------------

class TestCloneConfig:

    def test_updates_section_name(self, manager):
        base = _make_mock_plex_config()
        result = manager._cloneConfigForSection(base, 'plex_2', 2)
        assert result[0]['name'] == 'plex_2'
        assert result[0]['groups'][0]['name'] == 'plex_2'

    def test_updates_label(self, manager):
        base = _make_mock_plex_config()
        result = manager._cloneConfigForSection(base, 'plex_2', 2)
        assert result[0]['groups'][0]['label'] == 'Plex #2'

    def test_does_not_mutate_original(self, manager):
        base = _make_mock_plex_config()
        original_name = base[0]['name']
        manager._cloneConfigForSection(base, 'plex_2', 2)
        assert base[0]['name'] == original_name

    def test_third_instance_label(self, manager):
        base = _make_mock_plex_config()
        result = manager._cloneConfigForSection(base, 'plex_3', 3)
        assert result[0]['groups'][0]['label'] == 'Plex #3'


# ---------------------------------------------------------------------------
# addInstanceView
# ---------------------------------------------------------------------------

class TestAddInstanceView:

    @patch('couchpotato.core.notifications.Env')
    @patch('couchpotato.core.notifications.fireEvent')
    def test_missing_provider_type(self, mock_fire, mock_env, manager):
        result = manager.addInstanceView()
        assert result['success'] is False
        assert 'required' in result['message']

    @patch('couchpotato.core.notifications.Env')
    @patch('couchpotato.core.notifications.fireEvent')
    def test_unknown_provider(self, mock_fire, mock_env, manager):
        manager._getProviderModules = lambda: {}
        result = manager.addInstanceView(provider_type='nonexistent')
        assert result['success'] is False
        assert 'Unknown' in result['message']

    @patch('couchpotato.core.notifications.Env')
    @patch('couchpotato.core.notifications.fireEvent')
    def test_successful_creation(self, mock_fire, mock_env, manager, plex_registry_entry):
        mock_settings = _make_mock_settings(sections=['plex'])
        mock_env.get.return_value = mock_settings

        manager._getProviderModules = lambda: {'plex': plex_registry_entry}

        # Mock _createInstance to avoid actual class instantiation
        manager._createInstance = MagicMock(return_value=MagicMock())

        result = manager.addInstanceView(provider_type='plex')
        assert result['success'] is True
        assert result['section_name'] == 'plex_2'
        manager._createInstance.assert_called_once_with('plex', 'plex_2')


# ---------------------------------------------------------------------------
# removeInstanceView
# ---------------------------------------------------------------------------

class TestRemoveInstanceView:

    def test_missing_section_name(self, manager):
        result = manager.removeInstanceView()
        assert result['success'] is False
        assert 'required' in result['message']

    def test_cannot_remove_base_provider(self, manager):
        result = manager.removeInstanceView(section_name='plex')
        assert result['success'] is False
        assert 'Cannot remove base' in result['message']

    @patch('couchpotato.core.notifications.Env')
    def test_successful_removal(self, mock_env, manager):
        mock_settings = _make_mock_settings(sections=['plex', 'plex_2'])
        mock_env.get.return_value = mock_settings
        manager._instances['plex_2'] = {'instance': MagicMock(), 'provider_type': 'plex'}

        result = manager.removeInstanceView(section_name='plex_2')
        assert result['success'] is True
        assert 'plex_2' not in manager._instances
        mock_settings.save.assert_called_once()

    @patch('couchpotato.core.notifications.Env')
    def test_removal_missing_section(self, mock_env, manager):
        """Removing a section that doesn't exist in config should still succeed."""
        mock_settings = _make_mock_settings(sections=['plex'])
        mock_env.get.return_value = mock_settings

        result = manager.removeInstanceView(section_name='plex_2')
        assert result['success'] is True


# ---------------------------------------------------------------------------
# listInstancesView
# ---------------------------------------------------------------------------

class TestListInstancesView:

    def test_empty(self, manager):
        result = manager.listInstancesView()
        assert result['success'] is True
        assert result['instances'] == []

    def test_with_instances(self, manager):
        manager._instances = {
            'plex_2': {'instance': MagicMock(), 'provider_type': 'plex'},
            'emby_2': {'instance': MagicMock(), 'provider_type': 'emby'},
        }
        result = manager.listInstancesView()
        assert result['success'] is True
        assert len(result['instances']) == 2
        types = {i['provider_type'] for i in result['instances']}
        assert types == {'plex', 'emby'}


# ---------------------------------------------------------------------------
# _restoreDuplicateInstances
# ---------------------------------------------------------------------------

class TestRestoreDuplicateInstances:

    @patch('couchpotato.core.notifications.Env')
    def test_restores_matching_sections(self, mock_env, manager, plex_registry_entry):
        mock_settings = _make_mock_settings(sections=['core', 'plex', 'plex_2', 'emby'])
        mock_env.get.return_value = mock_settings

        manager._getProviderModules = lambda: {'plex': plex_registry_entry}
        manager._createInstance = MagicMock(return_value=MagicMock())

        manager._restoreDuplicateInstances()

        manager._createInstance.assert_called_once_with('plex', 'plex_2')

    @patch('couchpotato.core.notifications.Env')
    def test_skips_non_matching_sections(self, mock_env, manager, plex_registry_entry):
        mock_settings = _make_mock_settings(sections=['core', 'plex', 'core_2'])
        mock_env.get.return_value = mock_settings

        manager._getProviderModules = lambda: {'plex': plex_registry_entry}
        manager._createInstance = MagicMock()

        manager._restoreDuplicateInstances()

        # core_2 shouldn't match because 'core' isn't in the provider registry
        manager._createInstance.assert_not_called()

    @patch('couchpotato.core.notifications.Env')
    def test_skips_already_created(self, mock_env, manager, plex_registry_entry):
        mock_settings = _make_mock_settings(sections=['plex', 'plex_2'])
        mock_env.get.return_value = mock_settings

        manager._getProviderModules = lambda: {'plex': plex_registry_entry}
        manager._instances['plex_2'] = {'instance': MagicMock(), 'provider_type': 'plex'}
        manager._createInstance = MagicMock()

        manager._restoreDuplicateInstances()

        # Should not recreate an already-existing instance
        manager._createInstance.assert_not_called()


# ---------------------------------------------------------------------------
# isEnabled
# ---------------------------------------------------------------------------

class TestManagerEnabled:

    def test_always_enabled(self, manager):
        assert manager.isEnabled() is True
