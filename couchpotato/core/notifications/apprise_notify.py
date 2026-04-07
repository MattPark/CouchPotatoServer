import apprise

from couchpotato.core.event import addEvent
from couchpotato.core.helpers.variable import getTitle, getIdentifier, splitString
from couchpotato.core.logger import CPLog
from couchpotato.core.notifications.base import Notification
from couchpotato.environment import Env

log = CPLog(__name__)

autoload = 'AppriseNotification'


def mask_token(token):
    """Mask a token/key for safe logging: show first 4 chars + ***"""
    if not token:
        return '(empty)'
    token = str(token)
    if len(token) <= 4:
        return '***'
    return token[:4] + '***'


def mask_url(url):
    """Mask sensitive parts of an apprise URL for safe logging.

    Keeps the scheme and first few chars of the host/token, masks the rest.
    e.g. 'pover://userkey@apitoken' -> 'pover://user***'
    """
    if not url:
        return '(empty)'
    url = str(url)
    scheme_end = url.find('://')
    if scheme_end == -1:
        return mask_token(url)
    scheme = url[:scheme_end]
    rest = url[scheme_end + 3:]
    if len(rest) <= 6:
        return '%s://***' % scheme
    return '%s://%s***' % (scheme, rest[:6])


# ---------------------------------------------------------------------------
# Migration: convert old provider configs to Apprise URLs
# ---------------------------------------------------------------------------

def _get(parser, section, option, fallback=''):
    """Safely read a value from configparser."""
    try:
        if parser.has_option(section, option):
            return parser.get(section, option).strip()
    except Exception:
        pass
    return fallback


def _is_enabled(parser, section):
    """Check if a config section has enabled = 1/True."""
    val = _get(parser, section, 'enabled', '0')
    return val.lower() in ('1', 'true', 'yes', 'on')


def migrate_pushover(parser):
    """Pushover -> pover://user_key@api_token"""
    section = 'pushover'
    if not _is_enabled(parser, section):
        return None
    user_key = _get(parser, section, 'user_key')
    api_token = _get(parser, section, 'api_token', 'YkxHMYDZp285L265L3IwH3LmzkTaCy')
    if not user_key:
        log.warning('Migration: pushover enabled but no user_key configured — skipping')
        return None
    url = 'pover://%s@%s' % (user_key, api_token)
    priority = _get(parser, section, 'priority', '0')
    if priority and priority != '0':
        url += '?priority=%s' % priority
    sound = _get(parser, section, 'sound')
    if sound:
        url += ('&' if '?' in url else '?') + 'sound=%s' % sound
    return url


def migrate_pushbullet(parser):
    """Pushbullet -> pbul://api_key or pbul://api_key/device or pbul://api_key/#channel"""
    section = 'pushbullet'
    if not _is_enabled(parser, section):
        return None
    api_key = _get(parser, section, 'api_key')
    if not api_key:
        log.warning('Migration: pushbullet enabled but no api_key configured — skipping')
        return None
    url = 'pbul://%s' % api_key
    devices = _get(parser, section, 'devices')
    channels = _get(parser, section, 'channels')
    if devices:
        url += '/%s' % devices.split(',')[0].strip()
    if channels:
        url += '/#%s' % channels.split(',')[0].strip()
    return url


def migrate_telegram(parser):
    """Telegram -> tgram://bot_token/chat_id"""
    section = 'telegrambot'
    if not _is_enabled(parser, section):
        return None
    bot_token = _get(parser, section, 'bot_token')
    chat_id = _get(parser, section, 'receiver_user_id')
    if not bot_token:
        log.warning('Migration: telegram enabled but no bot_token configured — skipping')
        return None
    if not chat_id:
        log.warning('Migration: telegram enabled but no receiver_user_id configured — skipping')
        return None
    return 'tgram://%s/%s/' % (bot_token, chat_id)


def migrate_discord(parser):
    """Discord -> raw webhook URL (Apprise accepts Discord webhooks directly)"""
    section = 'discord'
    if not _is_enabled(parser, section):
        return None
    webhook_url = _get(parser, section, 'webhook_url')
    if not webhook_url:
        log.warning('Migration: discord enabled but no webhook_url configured — skipping')
        return None
    return webhook_url


def migrate_slack(parser):
    """Slack -> slack://token_a/token_b/token_c/#channel (incoming webhook style)"""
    section = 'slack'
    if not _is_enabled(parser, section):
        return None
    token = _get(parser, section, 'token')
    if not token:
        log.warning('Migration: slack enabled but no token configured — skipping')
        return None
    channels = _get(parser, section, 'channels')
    bot_name = _get(parser, section, 'bot_name', 'CouchPotato')
    # Slack token might be a webhook URL or a bot token
    # If it looks like a webhook URL, pass it directly (Apprise handles it)
    if token.startswith('http'):
        return token
    # Otherwise construct apprise URL
    url = 'slack://%s@%s' % (bot_name, token)
    if channels:
        for ch in channels.split(','):
            ch = ch.strip()
            if ch:
                url += '/%s%s' % ('' if ch.startswith('#') else '#', ch)
    return url


def migrate_email(parser):
    """Email -> mailtos://user:pass@domain?smtp=server&to=addr"""
    section = 'email'
    if not _is_enabled(parser, section):
        return None
    smtp_server = _get(parser, section, 'smtp_server')
    from_addr = _get(parser, section, 'from')
    to_addr = _get(parser, section, 'to')
    if not smtp_server or not to_addr:
        log.warning('Migration: email enabled but missing smtp_server or to address — skipping')
        return None
    smtp_user = _get(parser, section, 'smtp_user')
    smtp_pass = _get(parser, section, 'smtp_pass')
    smtp_port = _get(parser, section, 'smtp_port', '25')
    use_ssl = _get(parser, section, 'ssl', '0').lower() in ('1', 'true', 'yes')
    use_starttls = _get(parser, section, 'starttls', '0').lower() in ('1', 'true', 'yes')

    # Determine scheme and mode
    if use_ssl:
        scheme = 'mailtos'
        mode = 'ssl'
    elif use_starttls:
        scheme = 'mailtos'
        mode = 'starttls'
    else:
        scheme = 'mailto'
        mode = None

    # Build URL
    if smtp_user and smtp_pass:
        # URL-encode special chars in password
        import urllib.parse
        url = '%s://%s:%s@%s' % (scheme, urllib.parse.quote(smtp_user, safe=''),
                                  urllib.parse.quote(smtp_pass, safe=''), smtp_server)
    elif smtp_user:
        url = '%s://%s@%s' % (scheme, smtp_user, smtp_server)
    else:
        url = '%s://%s' % (scheme, smtp_server)

    params = []
    if smtp_port and smtp_port != '25':
        params.append('port=%s' % smtp_port)
    if from_addr:
        params.append('from=%s' % from_addr)
    params.append('to=%s' % to_addr)
    if mode:
        params.append('mode=%s' % mode)
    if params:
        url += '?' + '&'.join(params)
    return url


def migrate_webhook(parser):
    """Webhook -> form://url (form POST)"""
    section = 'webhook'
    if not _is_enabled(parser, section):
        return None
    url = _get(parser, section, 'url')
    if not url:
        log.warning('Migration: webhook enabled but no url configured — skipping')
        return None
    # Strip protocol and wrap with form://
    clean = url
    if clean.startswith('https://'):
        clean = clean[8:]
        return 'forms://%s' % clean
    elif clean.startswith('http://'):
        clean = clean[7:]
        return 'form://%s' % clean
    return 'form://%s' % clean


def migrate_homey(parser):
    """Homey -> json://url (JSON POST)"""
    section = 'homey'
    if not _is_enabled(parser, section):
        return None
    url = _get(parser, section, 'url')
    if not url:
        log.warning('Migration: homey enabled but no url configured — skipping')
        return None
    clean = url
    if clean.startswith('https://'):
        clean = clean[8:]
        return 'jsons://%s' % clean
    elif clean.startswith('http://'):
        clean = clean[7:]
        return 'json://%s' % clean
    return 'json://%s' % clean


def migrate_join(parser):
    """Join -> join://apikey/device1/device2"""
    section = 'join'
    if not _is_enabled(parser, section):
        return None
    apikey = _get(parser, section, 'apikey')
    if not apikey:
        log.warning('Migration: join enabled but no apikey configured — skipping')
        return None
    devices = _get(parser, section, 'devices')
    url = 'join://%s' % apikey
    if devices:
        for dev in devices.split(','):
            dev = dev.strip()
            if dev:
                url += '/%s' % dev
    else:
        url += '/group.all'
    return url


# Map of section name -> migration function
MIGRATORS = {
    'pushover': migrate_pushover,
    'pushbullet': migrate_pushbullet,
    'telegrambot': migrate_telegram,
    'discord': migrate_discord,
    'slack': migrate_slack,
    'email': migrate_email,
    'webhook': migrate_webhook,
    'homey': migrate_homey,
    'join': migrate_join,
}


def migrate_old_providers(parser):
    """Migrate enabled old notification providers to Apprise URLs.

    Args:
        parser: configparser.RawConfigParser instance

    Returns:
        list of apprise URLs that were migrated, or empty list if nothing to do
    """
    # Check if migration already happened (apprise section has migrated marker)
    if parser.has_option('apprise', '_migrated'):
        log.debug('Migration: apprise migration already completed — skipping')
        return []

    migrated_urls = []
    migrated_names = []
    skipped_names = []

    for name, func in MIGRATORS.items():
        try:
            url = func(parser)
            if url:
                migrated_urls.append(url)
                migrated_names.append(name)
                log.info('Migration: %s -> %s' % (name, mask_url(url)))
                # Disable old provider
                if parser.has_section(name):
                    parser.set(name, 'enabled', '0')
            elif _is_enabled(parser, name):
                skipped_names.append(name)
        except Exception as e:
            log.error('Migration: failed to migrate %s: %s' % (name, e))
            skipped_names.append(name)

    if not migrated_urls:
        log.debug('Migration: no enabled old notification providers found — nothing to migrate')
        return []

    # Write migrated URLs to apprise section
    if not parser.has_section('apprise'):
        parser.add_section('apprise')
    parser.set('apprise', 'enabled', '1')
    # Merge with any existing URLs
    existing = _get(parser, 'apprise', 'urls')
    existing_urls = [u.strip() for u in existing.split(',') if u.strip()] if existing else []
    all_urls = existing_urls + migrated_urls
    parser.set('apprise', 'urls', ', '.join(all_urls))
    # Set marker to prevent re-migration
    parser.set('apprise', '_migrated', '1')

    log.info('Migration: migrated %d provider(s) to Apprise: %s' % (len(migrated_names), ', '.join(migrated_names)))
    if skipped_names:
        log.warning('Migration: skipped %d provider(s) (missing config): %s' % (len(skipped_names), ', '.join(skipped_names)))

    return migrated_urls


# ---------------------------------------------------------------------------
# Apprise notification provider
# ---------------------------------------------------------------------------

class AppriseNotification(Notification):

    def __init__(self):
        super().__init__()
        addEvent('app.load', self.migrateFromOldProviders, priority=10)

    def migrateFromOldProviders(self):
        """Run migration on startup — convert old provider configs to Apprise URLs."""
        try:
            settings = Env.get('settings')
            parser = settings.parser()
            urls = migrate_old_providers(parser)
            if urls:
                settings.save()
                log.info('Migration: saved %d new Apprise URL(s) to config' % len(urls))
        except Exception as e:
            log.error('Migration: unexpected error: %s' % e)

    def notify(self, message='', data=None, listener=None):
        if not data:
            data = {}

        urls = splitString(self.conf('urls', default=''))
        if not urls:
            log.warning('Apprise: no notification URLs configured — skipping')
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

        # Create Apprise instance and add all URLs
        ap = apprise.Apprise()
        valid_count = 0
        for url in urls:
            url = url.strip()
            if not url:
                continue
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
            # Apprise returns False if ANY service failed
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
            'description': '<a href="https://github.com/caronc/apprise/wiki" target="_blank">Apprise</a> '
                           'supports 100+ notification services (Pushover, Telegram, Discord, Slack, Email, and many more). '
                           'Use <a href="https://appriseit.com/tools/url-builder/" target="_blank">the URL builder</a> '
                           'to construct your service URLs.',
            'options': [
                {
                    'name': 'enabled',
                    'default': 0,
                    'type': 'enabler',
                },
                {
                    'name': 'urls',
                    'label': 'Service URLs',
                    'default': '',
                    'description': 'Comma-separated Apprise notification URLs. '
                                   'See <a href="https://github.com/caronc/apprise/wiki" target="_blank">Apprise wiki</a> for URL formats. '
                                   'Example: pover://user@token, tgram://bottoken/chatid',
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
