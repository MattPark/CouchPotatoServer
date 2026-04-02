import json
import re
import time
import traceback

from couchpotato import Env
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import tryUrlencode
from couchpotato.core.helpers.variable import tryInt, tryFloat, splitString, nativeImdbId
from couchpotato.core.logger import CPLog
from couchpotato.core.media.movie.providers.base import MovieProvider


log = CPLog(__name__)

autoload = 'OMDBAPI'

# Cache durations
CACHE_SUCCESS = 30 * 24 * 3600   # 30 days for successful lookups
CACHE_FAILURE = 1 * 3600         # 1 hour for errors / empty results

# Daily API budgets by tier (with headroom)
BUDGET_FREE = 900       # Free tier: 1,000/day, keep 100 headroom
BUDGET_PATRON = 95000   # Patron tier: 100,000/day, keep 5,000 headroom

# Actual hard caps enforced by OMDB servers
HARD_CAP_FREE = 1000
HARD_CAP_PATRON = 100000

# Default built-in API key
DEFAULT_API_KEY = 'bbc0e412'


class OMDBAPI(MovieProvider):

    urls = {
        'search': 'https://www.omdbapi.com/?apikey=%s&type=movie&%s',
        'info': 'https://www.omdbapi.com/?apikey=%s&type=movie&i=%s',
    }

    http_time_between_calls = 0

    # In-memory daily counters: {'YYYYMMDD': count}
    _daily_calls = {}
    _daily_cache_hits = {}
    _rate_limited_today = False  # True once OMDB returns "Request limit reached"
    def __init__(self):
        addEvent('info.search', self.search)
        addEvent('movie.search', self.search)
        addEvent('movie.info', self.getInfo)
        addEvent('metadata.stats', self.getStats)

    # --- daily call budget ---------------------------------------------------

    def _todayKey(self):
        return time.strftime('%Y%m%d')

    def _getDailyBudget(self):
        tier = self.conf('key_tier') or 'free'
        return BUDGET_PATRON if tier == 'patron' else BUDGET_FREE

    def _getHardCap(self):
        tier = self.conf('key_tier') or 'free'
        return HARD_CAP_PATRON if tier == 'patron' else HARD_CAP_FREE

    def _getDailyCount(self):
        return self._daily_calls.get(self._todayKey(), 0)

    def _getDailyCacheHits(self):
        return self._daily_cache_hits.get(self._todayKey(), 0)

    def _incrementDaily(self):
        key = self._todayKey()
        count = self._daily_calls.get(key, 0) + 1
        self._daily_calls[key] = count
        # Prune old days (keep only today)
        for k in list(self._daily_calls):
            if k != key:
                del self._daily_calls[k]
        budget = self._getDailyBudget()
        if count == budget:
            log.warning('OMDB daily budget of %d reached — skipping further calls today' % budget)
        return count

    def _incrementCacheHit(self):
        key = self._todayKey()
        self._daily_cache_hits[key] = self._daily_cache_hits.get(key, 0) + 1
        for k in list(self._daily_cache_hits):
            if k != key:
                del self._daily_cache_hits[k]

    def _overBudget(self):
        return self._rate_limited_today or self._getDailyCount() >= self._getDailyBudget()

    def _checkRateLimited(self, data):
        """Parse raw OMDB response; if rate-limited, snap counter to hard cap and return True."""
        if not data:
            return False
        try:
            if isinstance(data, bytes):
                data = data.decode('utf-8')
            if isinstance(data, str):
                parsed = json.loads(data)
            else:
                parsed = data
            error_msg = parsed.get('Error', '')
            if 'request limit reached' in error_msg.lower():
                hard_cap = self._getHardCap()
                key = self._todayKey()
                self._daily_calls[key] = max(self._daily_calls.get(key, 0), hard_cap)
                self._rate_limited_today = True
                log.warning('OMDB rate limit reached — API returned: %s' % error_msg)
                return True
        except (ValueError, AttributeError):
            pass
        return False

    # --- stats ---------------------------------------------------------------

    def getStats(self):
        budget = self._getDailyBudget()
        hard_cap = self._getHardCap()
        calls = self._getDailyCount()
        api_key = self.getApiKey()
        return {
            'omdb': {
                'calls_today': calls,
                'budget': budget,
                'hard_cap': hard_cap,
                'budget_remaining': max(0, hard_cap - calls) if self._rate_limited_today else max(0, budget - calls),
                'cache_hits_today': self._getDailyCacheHits(),
                'key_tier': self.conf('key_tier') or 'free',
                'has_custom_key': api_key != DEFAULT_API_KEY and api_key != '',
                'rate_limited': self._rate_limited_today,
            }
        }

    # --- API methods ---------------------------------------------------------

    def search(self, q, limit = 12):
        if self.isDisabled():
            return []

        name_year = fireEvent('scanner.name_year', q, single = True)

        if not name_year or (name_year and not name_year.get('name')):
            name_year = {
                'name': q
            }

        cache_key = 'omdbapi.cache.%s' % q

        # Check cache first (before budget check)
        cached = self.getCache(cache_key)
        if cached:
            self._incrementCacheHit()
            result = self.parseMovie(cached)
            if result.get('titles') and len(result.get('titles')) > 0:
                log.info('Found: %s', result['titles'][0] + ' (' + str(result.get('year')) + ')')
                return [result]
            return []

        # Budget gate — only checked when cache misses
        if self._overBudget():
            return []

        url = self.urls['search'] % (self.getApiKey(), tryUrlencode({'t': name_year.get('name'), 'y': name_year.get('year', '')}))
        data = None
        try:
            data = self.urlopen(url, timeout = 3, headers = {'User-Agent': Env.getIdentifier()})
            self._incrementDaily()
        except:
            log.info('OMDB search request failed for: %s', q)
            self._incrementDaily()
            self.setCache(cache_key, '', timeout = CACHE_FAILURE)
            return []

        if data:
            if self._checkRateLimited(data):
                return []
            result = self.parseMovie(data)
            if result.get('titles') and len(result.get('titles')) > 0:
                self.setCache(cache_key, data, timeout = CACHE_SUCCESS)
                log.info('Found: %s', result['titles'][0] + ' (' + str(result.get('year')) + ')')
                return [result]

        # Empty / error result — cache briefly to avoid re-hitting
        self.setCache(cache_key, data or '', timeout = CACHE_FAILURE)
        return []

    def getInfo(self, identifier = None, **kwargs):
        if self.isDisabled() or not identifier:
            return {}

        # Normalize padded IMDB IDs — OMDB rejects 8-digit padded IDs
        identifier = nativeImdbId(identifier)

        cache_key = 'omdbapi.cache.%s' % identifier

        # Check cache first (before budget check)
        cached = self.getCache(cache_key)
        if cached:
            self._incrementCacheHit()
            result = self.parseMovie(cached)
            if result.get('titles') and len(result.get('titles')) > 0:
                log.info('Found: %s', result['titles'][0] + ' (' + str(result['year']) + ')')
                return result
            return {}

        # Budget gate
        if self._overBudget():
            return {}

        url = self.urls['info'] % (self.getApiKey(), identifier)
        data = None
        try:
            data = self.urlopen(url, timeout = 3, headers = {'User-Agent': Env.getIdentifier()})
            self._incrementDaily()
        except:
            log.info('OMDB info request failed for: %s', identifier)
            self._incrementDaily()
            self.setCache(cache_key, '', timeout = CACHE_FAILURE)
            return {}

        if data:
            if self._checkRateLimited(data):
                return {}
            result = self.parseMovie(data)
            if result.get('titles') and len(result.get('titles')) > 0:
                self.setCache(cache_key, data, timeout = CACHE_SUCCESS)
                log.info('Found: %s', result['titles'][0] + ' (' + str(result['year']) + ')')
                return result

        # Empty / error result
        self.setCache(cache_key, data or '', timeout = CACHE_FAILURE)
        return {}

    def parseMovie(self, movie):

        movie_data = {}
        try:

            try:
                if isinstance(movie, bytes):
                    movie = movie.decode('utf-8')
                if isinstance(movie, str):
                    movie = json.loads(movie)
            except ValueError:
                log.info('No proper json to decode')
                return movie_data

            if movie.get('Response') == 'Parse Error' or movie.get('Response') == 'False':
                return movie_data

            if movie.get('Type').lower() != 'movie':
                return movie_data

            tmp_movie = movie.copy()
            for key in tmp_movie:
                tmp_movie_elem = tmp_movie.get(key)
                if not isinstance(tmp_movie_elem, str) or tmp_movie_elem.lower() == 'n/a':
                    del movie[key]

            year = tryInt(movie.get('Year', ''))

            movie_data = {
                'type': 'movie',
                'via_imdb': True,
                'titles': [movie.get('Title')] if movie.get('Title') else [],
                'original_title': movie.get('Title'),
                'images': {
                    'poster': [movie.get('Poster', '')] if movie.get('Poster') and len(movie.get('Poster', '')) > 4 else [],
                },
                'rating': {
                    'imdb': (tryFloat(movie.get('imdbRating', 0)), tryInt(movie.get('imdbVotes', '').replace(',', ''))),
                    #'rotten': (tryFloat(movie.get('tomatoRating', 0)), tryInt(movie.get('tomatoReviews', '').replace(',', ''))),
                },
                'imdb': str(movie.get('imdbID', '')),
                'mpaa': str(movie.get('Rated', '')),
                'runtime': self.runtimeToMinutes(movie.get('Runtime', '')),
                'released': movie.get('Released'),
                'year': year if isinstance(year, int) else None,
                'plot': movie.get('Plot'),
                'genres': splitString(movie.get('Genre', '')),
                'directors': splitString(movie.get('Director', '')),
                'writers': splitString(movie.get('Writer', '')),
                'actors': splitString(movie.get('Actors', '')),
            }
            movie_data = dict((k, v) for k, v in movie_data.items() if v)
        except:
            log.error('Failed parsing IMDB API json: %s', traceback.format_exc())

        return movie_data

    def isDisabled(self):
        if self.getApiKey() == '':
            log.error('No API key provided.')
            return True
        return False

    def getApiKey(self):
        apikey = self.conf('api_key')
        return apikey

    def runtimeToMinutes(self, runtime_str):
        runtime = 0

        regex = r'(\d*.?\d+).(h|hr|hrs|mins|min)+'
        matches = re.findall(regex, runtime_str)
        for match in matches:
            nr, size = match
            runtime += tryInt(nr) * (60 if 'h' == str(size)[0] else 1)

        return runtime


config = [{
    'name': 'omdbapi',
    'groups': [
        {
            'tab': 'metadata',
            'name': 'omdbapi',
            'label': 'OMDB',
            'description': 'Open Movie Database — provides IMDB ratings, plot summaries, and cast info.',
            'options': [
                {
                    'name': 'api_key',
                    'default': DEFAULT_API_KEY,
                    'label': 'API Key',
                    'description': 'Get a free key at <a href="https://www.omdbapi.com/apikey.aspx" target="_blank">omdbapi.com</a>',
                },
                {
                    'name': 'key_tier',
                    'default': 'free',
                    'type': 'dropdown',
                    'label': 'Key Tier',
                    'description': 'Free keys: 1,000 calls/day. Patron keys: 100,000 calls/day and access to the Poster API.',
                    'values': [('Free (1,000/day)', 'free'), ('Patron (100,000/day)', 'patron')],
                },
            ],
        },
    ],
}]
