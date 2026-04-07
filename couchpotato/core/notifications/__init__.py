"""Notification system — provider list + multi-instance manager.

The NotificationInstanceManager allows users to add multiple independent
instances of the same notification provider (e.g., two Plex servers each
with their own host/port/token).

Each instance gets a unique config section name (e.g., plex_2, plex_3)
and a unique class name so events and API views don't collide.
"""

import copy
import re

from couchpotato.api import addApiView
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin
from couchpotato.environment import Env

log = CPLog(__name__)

autoload = 'NotificationInstanceManager'

# Registry: maps config section name -> (module_path, class_name, config_template)
# Populated during provider loading; the manager uses this to create new instances.
_provider_registry = {}


class NotificationInstanceManager(Plugin):
    """Manages creation and deletion of duplicate notification provider instances."""

    _instances = {}  # section_name -> provider instance

    def __init__(self):
        # Don't call super().__init__() — we're not a notification provider,
        # just a manager plugin. But Plugin.__new__ already called registerPlugin().

        addApiView('notification.add_instance', self.addInstanceView)
        addApiView('notification.remove_instance', self.removeInstanceView)
        addApiView('notification.list_instances', self.listInstancesView)

        # After all plugins are loaded, scan for existing duplicate sections
        addEvent('app.load', self._restoreDuplicateInstances, priority=90)

    def isEnabled(self):
        """Manager is always enabled."""
        return True

    def _getProviderModules(self):
        """Return dict of provider_type -> (module, class, config_template).

        Lazily discovers notification provider modules.
        """
        if _provider_registry:
            return _provider_registry

        # Import notification provider modules
        from importhelper import import_module
        import os

        notifications_dir = os.path.dirname(__file__)
        for filename in sorted(os.listdir(notifications_dir)):
            if not filename.endswith('.py'):
                continue
            if filename in ('__init__.py', 'base.py'):
                continue

            module_name = 'couchpotato.core.notifications.%s' % filename[:-3]
            try:
                mod = import_module(module_name)
            except Exception:
                continue

            if not hasattr(mod, 'autoload') or not hasattr(mod, 'config'):
                continue

            class_name = mod.autoload
            if not hasattr(mod, class_name):
                continue

            # Extract the config section name (first config entry's 'name')
            if mod.config and len(mod.config) > 0:
                section_name = mod.config[0].get('name', '')
                if section_name:
                    _provider_registry[section_name] = {
                        'module': mod,
                        'class_name': class_name,
                        'config': mod.config,
                        'label': '',
                    }
                    # Extract label from first group
                    groups = mod.config[0].get('groups', [])
                    if groups:
                        _provider_registry[section_name]['label'] = groups[0].get('label', section_name.capitalize())

        return _provider_registry

    def _nextSectionName(self, base_name):
        """Find the next available section name like base_name_2, base_name_3, ..."""
        settings = Env.get('settings')
        existing = settings.sections()
        n = 2
        while True:
            candidate = '%s_%d' % (base_name, n)
            if candidate not in existing:
                return candidate
            n += 1

    def _cloneConfigForSection(self, base_config, new_section_name, instance_num):
        """Deep-copy a provider's config template and update it for the new section."""
        new_config = copy.deepcopy(base_config)
        for section in new_config:
            section['name'] = new_section_name
            for group in section.get('groups', []):
                group['name'] = new_section_name
                # Update the label to show instance number
                if group.get('label'):
                    group['label'] = '%s #%d' % (group['label'], instance_num)
        return new_config

    def _createInstance(self, provider_type, section_name):
        """Create a new provider instance with the given section name.

        Args:
            provider_type: base config name (e.g., 'plex', 'xbmc', 'emby')
            section_name: the config.ini section name (e.g., 'plex_2')

        Returns:
            The new provider instance, or None on error.
        """
        registry = self._getProviderModules()
        if provider_type not in registry:
            log.error('Unknown notification provider type: %s' % provider_type)
            return None

        entry = registry[provider_type]
        mod = entry['module']
        cls_name = entry['class_name']
        cls = getattr(mod, cls_name)

        # Extract instance number from section name
        match = re.match(r'^.+_(\d+)$', section_name)
        instance_num = int(match.group(1)) if match else 1

        # Clone config template for the new section
        new_config = self._cloneConfigForSection(entry['config'], section_name, instance_num)

        # Register options metadata and defaults with the settings system
        for section in new_config:
            fireEvent('settings.options', section['name'], section)
            options = {}
            for group in section.get('groups', []):
                for option in group.get('options', []):
                    options[option['name']] = option
            fireEvent('settings.register', section_name=section['name'], options=options, save=True)

        # Instantiate the provider class
        # setName BEFORE __init__ runs — but Plugin.__new__ runs registerPlugin() first,
        # and then __init__ reads getName(). We need to set _class_name before __init__.
        # Use a trick: create the object, then re-init? No — __new__ already does too much.
        #
        # Better approach: temporarily patch the class, or set _class_name on the instance
        # after creation. The __init__ uses getName() to register events and API views.
        # So we need to set _class_name BEFORE __init__ fires.
        #
        # Approach: subclass dynamically with _class_name set as class attribute.
        try:
            # Create a dynamic subclass that has the right _class_name
            dynamic_cls = type(
                '%s_%s' % (cls_name, section_name),
                (cls,),
                {'_class_name': section_name}
            )
            instance = dynamic_cls()
            self._instances[section_name] = {
                'instance': instance,
                'provider_type': provider_type,
            }
            log.info('Created notification instance: %s (type: %s)' % (section_name, provider_type))
            return instance
        except Exception as e:
            log.error('Failed to create notification instance %s: %s' % (section_name, e))
            import traceback
            log.debug(traceback.format_exc())
            return None

    def _restoreDuplicateInstances(self):
        """On startup, find {provider}_{N} sections in config.ini and create instances."""
        registry = self._getProviderModules()
        settings = Env.get('settings')
        existing_sections = settings.sections()

        for section in existing_sections:
            # Match pattern: provider_name + _N (where N is a number)
            match = re.match(r'^(.+?)_(\d+)$', section)
            if not match:
                continue

            base_name = match.group(1)
            if base_name not in registry:
                continue

            # This is a duplicate instance section — create the instance
            if section not in self._instances:
                log.info('Restoring notification instance: %s (type: %s)' % (section, base_name))
                self._createInstance(base_name, section)

    def addInstanceView(self, **kwargs):
        """API endpoint: create a new duplicate notification provider instance.

        Params:
            provider_type: base provider name (e.g., 'plex', 'xbmc', 'emby')

        Returns:
            {success, section_name, label, options, values}
        """
        provider_type = kwargs.get('provider_type')
        if not provider_type:
            return {'success': False, 'message': 'provider_type is required'}

        registry = self._getProviderModules()
        if provider_type not in registry:
            return {'success': False, 'message': 'Unknown provider type: %s' % provider_type}

        section_name = self._nextSectionName(provider_type)
        instance = self._createInstance(provider_type, section_name)
        if not instance:
            return {'success': False, 'message': 'Failed to create instance'}

        # Return the new section's options and values so the frontend can render it
        settings = Env.get('settings')
        options = settings.getOptions().get(section_name, {})
        values = {}
        try:
            values = settings.getValues().get(section_name, {})
        except Exception:
            pass

        return {
            'success': True,
            'section_name': section_name,
            'options': options,
            'values': values,
        }

    def removeInstanceView(self, **kwargs):
        """API endpoint: remove a notification provider instance.

        Works for both base providers (e.g., 'plex') and duplicate instances
        (e.g., 'plex_2').  Base providers have their config reset to defaults
        with enabled=0 (the section is kept because the loader recreates it on
        restart).  Duplicates are fully deleted from config.ini.

        Params:
            section_name: the config.ini section name to remove

        Returns:
            {success}
        """
        section_name = kwargs.get('section_name')
        if not section_name:
            return {'success': False, 'message': 'section_name is required'}

        settings = Env.get('settings')
        parser = settings.parser()
        is_duplicate = bool(re.match(r'^(.+?)_(\d+)$', section_name))

        if is_duplicate:
            # Duplicate instance: delete section entirely
            if parser.has_section(section_name):
                parser.remove_section(section_name)
                settings.save()

            # Remove options metadata so the settings API no longer returns it
            if section_name in settings.options:
                del settings.options[section_name]

            # Clean up the runtime instance (events/API views stay registered
            # but isEnabled() returns False since the config section is gone)
            if section_name in self._instances:
                del self._instances[section_name]
        else:
            # Base provider: clear all user-configured values, set enabled=0.
            # The section is kept alive so the loader doesn't crash on restart;
            # registerDefaults will repopulate defaults on next boot.
            if parser.has_section(section_name):
                for option in list(parser.options(section_name)):
                    parser.remove_option(section_name, option)
                parser.set(section_name, 'enabled', '0')
                settings.save()

        log.info('Removed notification instance: %s' % section_name)

        return {'success': True}

    def listInstancesView(self, **kwargs):
        """API endpoint: list all duplicate notification provider instances.

        Returns:
            {success, instances: [{section_name, provider_type, label}]}
        """
        instances = []
        for section_name, info in self._instances.items():
            instances.append({
                'section_name': section_name,
                'provider_type': info['provider_type'],
            })
        return {'success': True, 'instances': instances}


config = [{
    'name': 'notification_providers',
    'groups': [
        {
            'label': 'Notifications',
            'description': 'Notify when movies are done or snatched',
            'type': 'list',
            'name': 'notification_providers',
            'tab': 'notifications',
            'options': [],
        },
    ],
}]
