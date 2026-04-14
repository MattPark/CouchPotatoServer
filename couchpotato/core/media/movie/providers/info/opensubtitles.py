"""OpenSubtitles.com provider for hash-based movie identification.

Used by the audit system's Tier 2 identification to quickly identify
video files via their OpenSubtitles moviehash (64-bit, reads only 128KB).

Search queries are unlimited; only subtitle downloads count against the
daily quota.

TODO: Add subtitle download functionality for matched movies.
"""

import time

from couchpotato.core.event import addEvent
from couchpotato.core.logger import CPLog
from couchpotato.core.media.movie.providers.base import MovieProvider


log = CPLog(__name__)

autoload = 'OpenSubtitlesCom'

# Built-in application-level API key (registered at opensubtitles.com/consumers).
# This identifies the CouchPotato application — it is NOT a user secret.
# Searches (hash lookups) are unlimited and free; only subtitle downloads
# consume quota (which requires user login via username/password below).
OS_APP_API_KEY = '6myLcgfFag887jWtWC40vKhs92LocQUA'


class OpenSubtitlesCom(MovieProvider):

    http_time_between_calls = 0

    # In-memory daily call counter: {'YYYYMMDD': count}
    _daily_calls = {}
    _daily_hash_hits = {}

    def __init__(self):
        addEvent('metadata.stats', self.getStats)
        addEvent('opensubtitles.api_key', self.getApiKey)

    # --- API key -------------------------------------------------------------

    def getApiKey(self):
        """Return the API key to use for OpenSubtitles requests.

        Always returns the built-in application key.  There is no user-
        configurable override — this is an app-level credential, not a
        user secret.
        """
        return OS_APP_API_KEY

    # --- call tracking -------------------------------------------------------

    def _todayKey(self):
        return time.strftime('%Y%m%d')

    def _incrementDaily(self, counter_dict=None):
        if counter_dict is None:
            counter_dict = self._daily_calls
        key = self._todayKey()
        counter_dict[key] = counter_dict.get(key, 0) + 1
        for k in list(counter_dict):
            if k != key:
                del counter_dict[k]

    def _getDailyCount(self, counter_dict=None):
        if counter_dict is None:
            counter_dict = self._daily_calls
        return counter_dict.get(self._todayKey(), 0)

    def incrementSearchCall(self):
        """Increment the daily search call counter (called from audit.py)."""
        self._incrementDaily(self._daily_calls)

    def incrementHashHit(self):
        """Increment the daily hash hit counter (called from audit.py)."""
        self._incrementDaily(self._daily_hash_hits)

    def getStats(self):
        return {
            'opensubtitles': {
                'searches_today': self._getDailyCount(self._daily_calls),
                'hash_hits_today': self._getDailyCount(self._daily_hash_hits),
                'key_type': 'Built-in',
                # TODO: Add download quota tracking when subtitle
                #       download is implemented.  The REST API returns
                #       remaining downloads in the /download response.
            }
        }

    def isDisabled(self):
        return False


config = [{
    'name': 'opensubtitles',
    'groups': [
        {
            'tab': 'metadata',
            'name': 'opensubtitles',
            'label': 'OpenSubtitles',
            'description': 'Hash-based movie identification for library audit. '
                           'Searches are unlimited; only subtitle downloads use quota.',
            'options': [
                # TODO: Add username/password fields here when subtitle
                #       download functionality is implemented.  The user
                #       would enter their opensubtitles.com credentials
                #       to enable downloading (5/day anonymous, 10/day
                #       logged-in, 20+/day VIP).
            ],
        },
    ],
}]
