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


class OpenSubtitlesCom(MovieProvider):

    http_time_between_calls = 0

    # In-memory daily call counter: {'YYYYMMDD': count}
    _daily_calls = {}
    _daily_hash_hits = {}

    def __init__(self):
        addEvent('metadata.stats', self.getStats)

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
        api_key = self.conf('opensubtitles_api_key') or ''
        return {
            'opensubtitles': {
                'searches_today': self._getDailyCount(self._daily_calls),
                'hash_hits_today': self._getDailyCount(self._daily_hash_hits),
                'has_api_key': api_key != '',
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
                {
                    'name': 'opensubtitles_api_key',
                    'default': '',
                    'type': 'password',
                    'label': 'API Key',
                    'description': 'Get a free key at '
                                   '<a href="https://www.opensubtitles.com/en/consumers" '
                                   'target="_blank">opensubtitles.com/consumers</a>',
                },
            ],
        },
    ],
}]
