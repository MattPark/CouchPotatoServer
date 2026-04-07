"""Tests for notification migration — converting old provider configs to Apprise URLs.

Tests each provider's migration function with valid configs, missing fields,
disabled providers, and edge cases.
"""
import configparser

from couchpotato.core.notifications.apprise_notify import (
    migrate_old_providers,
    migrate_pushover,
    migrate_pushbullet,
    migrate_telegram,
    migrate_discord,
    migrate_slack,
    migrate_email,
    migrate_webhook,
    migrate_homey,
    migrate_join,
    _is_enabled,
)


def make_parser(sections=None):
    """Create a configparser with the given sections dict.

    Args:
        sections: dict of {section_name: {option: value, ...}}
    """
    p = configparser.RawConfigParser()
    if sections:
        for name, options in sections.items():
            p.add_section(name)
            for k, v in options.items():
                p.set(name, k, str(v))
    return p


# ---------------------------------------------------------------------------
# _is_enabled
# ---------------------------------------------------------------------------

class TestIsEnabled:
    def test_enabled_1(self):
        p = make_parser({'foo': {'enabled': '1'}})
        assert _is_enabled(p, 'foo') is True

    def test_enabled_true(self):
        p = make_parser({'foo': {'enabled': 'True'}})
        assert _is_enabled(p, 'foo') is True

    def test_disabled_0(self):
        p = make_parser({'foo': {'enabled': '0'}})
        assert _is_enabled(p, 'foo') is False

    def test_missing_section(self):
        p = make_parser({})
        assert _is_enabled(p, 'foo') is False


# ---------------------------------------------------------------------------
# Individual provider migrations
# ---------------------------------------------------------------------------

class TestMigratePushover:
    def test_basic(self):
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN'}})
        url = migrate_pushover(p)
        assert url == 'pover://UKEY@ATOKEN'

    def test_with_priority_and_sound(self):
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN', 'priority': '1', 'sound': 'cashregister'}})
        url = migrate_pushover(p)
        assert 'priority=1' in url
        assert 'sound=cashregister' in url

    def test_default_priority_omitted(self):
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN', 'priority': '0'}})
        url = migrate_pushover(p)
        assert 'priority' not in url

    def test_disabled(self):
        p = make_parser({'pushover': {'enabled': '0', 'user_key': 'UKEY'}})
        assert migrate_pushover(p) is None

    def test_missing_user_key(self):
        p = make_parser({'pushover': {'enabled': '1', 'api_token': 'ATOKEN'}})
        assert migrate_pushover(p) is None

    def test_default_api_token(self):
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY'}})
        url = migrate_pushover(p)
        assert 'YkxHMYDZp285L265L3IwH3LmzkTaCy' in url


class TestMigratePushbullet:
    def test_basic(self):
        p = make_parser({'pushbullet': {'enabled': '1', 'api_key': 'APIKEY'}})
        url = migrate_pushbullet(p)
        assert url == 'pbul://APIKEY'

    def test_with_device(self):
        p = make_parser({'pushbullet': {'enabled': '1', 'api_key': 'APIKEY', 'devices': 'myphone'}})
        url = migrate_pushbullet(p)
        assert url == 'pbul://APIKEY/myphone'

    def test_with_channel(self):
        p = make_parser({'pushbullet': {'enabled': '1', 'api_key': 'APIKEY', 'channels': 'movies'}})
        url = migrate_pushbullet(p)
        assert url == 'pbul://APIKEY/#movies'

    def test_disabled(self):
        p = make_parser({'pushbullet': {'enabled': '0', 'api_key': 'APIKEY'}})
        assert migrate_pushbullet(p) is None

    def test_missing_api_key(self):
        p = make_parser({'pushbullet': {'enabled': '1'}})
        assert migrate_pushbullet(p) is None


class TestMigrateTelegram:
    def test_basic(self):
        p = make_parser({'telegrambot': {'enabled': '1', 'bot_token': 'BOT123', 'receiver_user_id': '456'}})
        url = migrate_telegram(p)
        assert url == 'tgram://BOT123/456/'

    def test_disabled(self):
        p = make_parser({'telegrambot': {'enabled': '0', 'bot_token': 'BOT123', 'receiver_user_id': '456'}})
        assert migrate_telegram(p) is None

    def test_missing_bot_token(self):
        p = make_parser({'telegrambot': {'enabled': '1', 'receiver_user_id': '456'}})
        assert migrate_telegram(p) is None

    def test_missing_user_id(self):
        p = make_parser({'telegrambot': {'enabled': '1', 'bot_token': 'BOT123'}})
        assert migrate_telegram(p) is None


class TestMigrateDiscord:
    def test_basic(self):
        p = make_parser({'discord': {'enabled': '1', 'webhook_url': 'https://discord.com/api/webhooks/123/abc'}})
        url = migrate_discord(p)
        assert url == 'https://discord.com/api/webhooks/123/abc'

    def test_disabled(self):
        p = make_parser({'discord': {'enabled': '0', 'webhook_url': 'https://discord.com/api/webhooks/123/abc'}})
        assert migrate_discord(p) is None

    def test_missing_url(self):
        p = make_parser({'discord': {'enabled': '1'}})
        assert migrate_discord(p) is None


class TestMigrateSlack:
    def test_webhook_url(self):
        """Slack webhook URL passed directly."""
        p = make_parser({'slack': {'enabled': '1', 'token': 'https://hooks.slack.com/services/xxx'}})
        url = migrate_slack(p)
        assert url == 'https://hooks.slack.com/services/xxx'

    def test_bot_token(self):
        p = make_parser({'slack': {'enabled': '1', 'token': 'xoxb-123-456', 'channels': '#general'}})
        url = migrate_slack(p)
        assert 'slack://' in url
        assert '#general' in url

    def test_disabled(self):
        p = make_parser({'slack': {'enabled': '0', 'token': 'xoxb-123'}})
        assert migrate_slack(p) is None

    def test_missing_token(self):
        p = make_parser({'slack': {'enabled': '1'}})
        assert migrate_slack(p) is None


class TestMigrateEmail:
    def test_basic_smtp(self):
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.example.com',
                                   'to': 'user@example.com', 'from': 'cp@example.com'}})
        url = migrate_email(p)
        assert url.startswith('mailto://')
        assert 'smtp.example.com' in url
        assert 'to=user@example.com' in url

    def test_ssl(self):
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.gmail.com',
                                   'to': 'user@gmail.com', 'ssl': '1',
                                   'smtp_user': 'user@gmail.com', 'smtp_pass': 'pass123'}})
        url = migrate_email(p)
        assert url.startswith('mailtos://')
        assert 'mode=ssl' in url

    def test_starttls(self):
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.example.com',
                                   'to': 'user@example.com', 'starttls': '1'}})
        url = migrate_email(p)
        assert 'mode=starttls' in url

    def test_custom_port(self):
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.example.com',
                                   'to': 'user@example.com', 'smtp_port': '587'}})
        url = migrate_email(p)
        assert 'port=587' in url

    def test_default_port_omitted(self):
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.example.com',
                                   'to': 'user@example.com', 'smtp_port': '25'}})
        url = migrate_email(p)
        assert 'port=' not in url

    def test_disabled(self):
        p = make_parser({'email': {'enabled': '0', 'smtp_server': 'smtp.example.com', 'to': 'user@example.com'}})
        assert migrate_email(p) is None

    def test_missing_smtp_server(self):
        p = make_parser({'email': {'enabled': '1', 'to': 'user@example.com'}})
        assert migrate_email(p) is None

    def test_password_special_chars(self):
        """Password with special characters is URL-encoded."""
        p = make_parser({'email': {'enabled': '1', 'smtp_server': 'smtp.example.com',
                                   'to': 'user@example.com', 'smtp_user': 'user', 'smtp_pass': 'p@ss w0rd!'}})
        url = migrate_email(p)
        assert 'p%40ss' in url  # @ encoded
        assert 'w0rd%21' in url  # ! encoded


class TestMigrateWebhook:
    def test_http(self):
        p = make_parser({'webhook': {'enabled': '1', 'url': 'http://example.com/hook'}})
        url = migrate_webhook(p)
        assert url == 'form://example.com/hook'

    def test_https(self):
        p = make_parser({'webhook': {'enabled': '1', 'url': 'https://example.com/hook'}})
        url = migrate_webhook(p)
        assert url == 'forms://example.com/hook'

    def test_disabled(self):
        p = make_parser({'webhook': {'enabled': '0', 'url': 'http://example.com/hook'}})
        assert migrate_webhook(p) is None

    def test_missing_url(self):
        p = make_parser({'webhook': {'enabled': '1'}})
        assert migrate_webhook(p) is None


class TestMigrateHomey:
    def test_http(self):
        p = make_parser({'homey': {'enabled': '1', 'url': 'http://webhooks.athom.com/abc'}})
        url = migrate_homey(p)
        assert url == 'json://webhooks.athom.com/abc'

    def test_https(self):
        p = make_parser({'homey': {'enabled': '1', 'url': 'https://webhooks.athom.com/abc'}})
        url = migrate_homey(p)
        assert url == 'jsons://webhooks.athom.com/abc'

    def test_disabled(self):
        p = make_parser({'homey': {'enabled': '0', 'url': 'http://webhooks.athom.com/abc'}})
        assert migrate_homey(p) is None

    def test_missing_url(self):
        p = make_parser({'homey': {'enabled': '1'}})
        assert migrate_homey(p) is None


class TestMigrateJoin:
    def test_basic(self):
        p = make_parser({'join': {'enabled': '1', 'apikey': 'KEY123', 'devices': 'phone1,phone2'}})
        url = migrate_join(p)
        assert url == 'join://KEY123/phone1/phone2'

    def test_no_devices(self):
        p = make_parser({'join': {'enabled': '1', 'apikey': 'KEY123'}})
        url = migrate_join(p)
        assert url == 'join://KEY123/group.all'

    def test_disabled(self):
        p = make_parser({'join': {'enabled': '0', 'apikey': 'KEY123'}})
        assert migrate_join(p) is None

    def test_missing_apikey(self):
        p = make_parser({'join': {'enabled': '1'}})
        assert migrate_join(p) is None


# ---------------------------------------------------------------------------
# Full migration: migrate_old_providers()
# ---------------------------------------------------------------------------

class TestMigrateOldProviders:
    def test_no_enabled_providers(self):
        """No providers enabled -> empty list, no apprise section created."""
        p = make_parser({'pushover': {'enabled': '0'}})
        result = migrate_old_providers(p)
        assert result == []

    def test_single_provider(self):
        """One enabled provider -> one URL migrated."""
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN'}})
        result = migrate_old_providers(p)
        assert len(result) == 1
        assert 'pover://UKEY@ATOKEN' in result[0]
        # Apprise section created and enabled
        assert p.get('apprise', 'enabled') == '1'
        assert 'pover://' in p.get('apprise', 'urls')
        # Old provider disabled
        assert p.get('pushover', 'enabled') == '0'

    def test_multiple_providers(self):
        """Multiple enabled providers -> multiple URLs migrated."""
        p = make_parser({
            'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN'},
            'discord': {'enabled': '1', 'webhook_url': 'https://discord.com/api/webhooks/123/abc'},
            'telegrambot': {'enabled': '0', 'bot_token': 'BOT', 'receiver_user_id': '789'},
        })
        result = migrate_old_providers(p)
        assert len(result) == 2  # Only pushover and discord, telegram disabled

    def test_idempotent(self):
        """Running migration twice doesn't duplicate URLs."""
        p = make_parser({'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN'}})
        result1 = migrate_old_providers(p)
        assert len(result1) == 1
        # Migration marker set
        assert p.get('apprise', '_migrated') == '1'
        # Run again
        result2 = migrate_old_providers(p)
        assert result2 == []

    def test_missing_fields_skipped(self):
        """Provider enabled but missing required fields -> skipped with warning."""
        p = make_parser({
            'pushover': {'enabled': '1'},  # No user_key
            'discord': {'enabled': '1', 'webhook_url': 'https://discord.com/api/webhooks/123/abc'},
        })
        result = migrate_old_providers(p)
        assert len(result) == 1  # Only discord migrated

    def test_merges_with_existing_urls(self):
        """If apprise section already has URLs, new ones are appended."""
        p = make_parser({
            'apprise': {'urls': 'json://existing'},
            'pushover': {'enabled': '1', 'user_key': 'UKEY', 'api_token': 'ATOKEN'},
        })
        result = migrate_old_providers(p)
        urls = p.get('apprise', 'urls')
        assert 'json://existing' in urls
        assert 'pover://UKEY@ATOKEN' in urls
