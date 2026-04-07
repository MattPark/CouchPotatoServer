import json

from apprise import Apprise as AppriseLib
from apprise.manager_plugins import NotificationManager

from couchpotato.api import addApiView
from couchpotato.core.helpers.variable import getTitle, getIdentifier
from couchpotato.core.logger import CPLog
from couchpotato.core.notifications.base import Notification

log = CPLog(__name__)

autoload = 'Apprise'

# Cache for schema list — populated once on first request
_schemas_cache = None


def mask_url(url):
    """Mask sensitive parts of an apprise URL for safe logging.

    Keeps the scheme and first few chars of the host/token, masks the rest.
    e.g. 'pover://userkey@apitoken' -> 'pover://userke***'
    """
    if not url:
        return '(empty)'
    url = str(url)
    scheme_end = url.find('://')
    if scheme_end == -1:
        if len(url) <= 4:
            return '***'
        return url[:4] + '***'
    scheme = url[:scheme_end]
    rest = url[scheme_end + 3:]
    if len(rest) <= 6:
        return '%s://***' % scheme
    return '%s://%s***' % (scheme, rest[:6])


def _get_schemas():
    """Build and cache the list of available Apprise notification schemas."""
    global _schemas_cache
    if _schemas_cache is not None:
        return _schemas_cache

    n_mgr = NotificationManager()
    result = []

    for plugin_cls in n_mgr.plugins():
        if not plugin_cls.enabled:
            continue

        # Collect all schemas for this plugin
        schemas = []
        if plugin_cls.secure_protocol:
            if isinstance(plugin_cls.secure_protocol, (list, tuple)):
                schemas.extend(plugin_cls.secure_protocol)
            else:
                schemas.append(plugin_cls.secure_protocol)
        if plugin_cls.protocol:
            if isinstance(plugin_cls.protocol, (list, tuple)):
                schemas.extend(plugin_cls.protocol)
            else:
                schemas.append(plugin_cls.protocol)

        if not schemas:
            continue

        # Get the first URL template for placeholder hint
        template = ''
        if plugin_cls.templates:
            template = plugin_cls.templates[0]
            # Replace {schema} with the primary schema
            template = template.replace('{schema}', schemas[0])

        result.append({
            'service_name': str(getattr(plugin_cls, 'service_name', schemas[0])),
            'schemas': schemas,
            'template': template,
            'service_url': getattr(plugin_cls, 'service_url', ''),
            'setup_url': getattr(plugin_cls, 'setup_url', ''),
        })

    # Sort by service name
    result.sort(key=lambda x: x['service_name'].lower())
    _schemas_cache = result
    return result


def _parse_urls_config(raw_value):
    """Parse the urls config value as a JSON array.

    Returns a list of dicts: [{"url": "...", "schema": "...", "enabled": true}, ...]
    Returns empty list if value is empty or invalid.
    """
    if not raw_value:
        return []

    raw_value = raw_value.strip()
    if not raw_value:
        return []

    try:
        entries = json.loads(raw_value)
        if isinstance(entries, list):
            return entries
    except (json.JSONDecodeError, TypeError):
        pass

    return []


class Apprise(Notification):

    def __init__(self):
        super().__init__()
        addApiView('apprise.schemas', self.schemasView)
        addApiView('apprise.test_url', self.testUrlView)

    def schemasView(self, **kwargs):
        """API endpoint: return all available Apprise notification service schemas."""
        try:
            schemas = _get_schemas()
            return {'success': True, 'schemas': schemas}
        except Exception as e:
            log.error('Failed to load Apprise schemas: %s' % e)
            return {'success': False, 'message': str(e)}

    def testUrlView(self, **kwargs):
        """API endpoint: test a single Apprise notification URL."""
        url = kwargs.get('url', '').strip()
        if not url:
            return {'success': False, 'message': 'No URL provided'}

        log.info('Apprise: testing URL %s' % mask_url(url))

        # Validate the URL first
        instance = AppriseLib.instantiate(url)
        if not instance:
            log.warning('Apprise: invalid URL for test: %s' % mask_url(url))
            return {'success': False, 'message': 'Invalid or unsupported Apprise URL'}

        service_name = getattr(instance, 'service_name', 'Unknown')

        # Send test notification
        ap = AppriseLib()
        ap.add(url)
        try:
            result = ap.notify(
                title='CouchPotato Test',
                body='This is a test notification from CouchPotato.',
            )
        except Exception as e:
            log.error('Apprise: test failed for %s: %s' % (mask_url(url), e))
            return {'success': False, 'message': 'Error: %s' % str(e), 'service_name': service_name}

        if result:
            log.info('Apprise: test notification sent successfully to %s' % service_name)
            return {'success': True, 'service_name': service_name}
        else:
            log.warning('Apprise: test notification failed for %s' % service_name)
            return {'success': False, 'message': 'Notification delivery failed', 'service_name': service_name}

    def notify(self, message='', data=None, listener=None):
        if not data:
            data = {}

        raw_urls = self.conf('urls', default='')
        entries = _parse_urls_config(raw_urls)

        if not entries:
            log.warning('Apprise: no notification URLs configured — skipping')
            return False

        # Collect only enabled URLs
        active_urls = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if not entry.get('enabled', True):
                continue
            url = entry.get('url', '').strip()
            if url:
                active_urls.append(url)

        if not active_urls:
            log.warning('Apprise: no enabled notification URLs — skipping')
            return False

        # Append IMDB link if we have an identifier
        body = message
        identifier = getIdentifier(data)
        if identifier:
            title = getTitle(data)
            imdb_url = 'https://www.imdb.com/title/%s/' % identifier
            if title:
                body = '%s\n%s - %s' % (message, title, imdb_url)
            else:
                body = '%s\n%s' % (message, imdb_url)

        # Create Apprise instance and add all enabled URLs
        ap = AppriseLib()
        valid_count = 0
        for url in active_urls:
            result = ap.add(url)
            if not result:
                log.warning('Apprise: invalid or unsupported URL: %s' % mask_url(url))
            else:
                valid_count += 1

        if valid_count == 0:
            log.error('Apprise: no valid notification URLs after parsing — check your configuration')
            return False

        log.debug('Apprise: sending to %d service(s), listener=%s' % (valid_count, listener))

        try:
            success = ap.notify(
                title=self.default_title,
                body=body,
            )
        except Exception as e:
            log.error('Apprise: unexpected error during notify: %s' % e)
            return False

        if success:
            log.info('Apprise: notification sent successfully to %d service(s)' % valid_count)
        else:
            log.warning('Apprise: one or more services failed — check service URLs and credentials')

        return success


config = [{
    'name': 'apprise',
    'groups': [
        {
            'tab': 'notifications',
            'list': 'notification_providers',
            'name': 'apprise',
            'label': 'Apprise',
            'multi_instance': False,
            'description': '<a href="https://github.com/caronc/apprise/wiki" target="_blank">Apprise</a> '
                           'supports 100+ notification services (Pushover, Telegram, Discord, Slack, Email, and many more).',
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                },
                {
                    'name': 'urls',
                    'label': 'Notification Services',
                    'default': '',
                    'type': 'apprise_urls',
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
