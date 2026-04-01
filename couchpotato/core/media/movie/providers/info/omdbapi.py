import json
import re
import time
import traceback

from couchpotato import Env
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import tryUrlencode
from couchpotato.core.helpers.variable import tryInt, tryFloat, splitString
from couchpotato.core.logger import CPLog
from couchpotato.core.media.movie.providers.base import MovieProvider


log = CPLog(__name__)

autoload = 'OMDBAPI'

# Cache durations
CACHE_SUCCESS = 30 * 24 * 3600   # 30 days for successful lookups
CACHE_FAILURE = 1 * 3600         # 1 hour for errors / empty results

# Daily API budget (free tier = 1000/day, keep 100 headroom)
DAILY_BUDGET = 900


class OMDBAPI(MovieProvider):

    urls = {
        'search': 'https://www.omdbapi.com/?apikey=%s&type=movie&%s',
        'info': 'https://www.omdbapi.com/?apikey=%s&type=movie&i=%s',
    }

    http_time_between_calls = 0

    # In-memory daily call counter: {'YYYYMMDD': count}
    _daily_calls = {}

    def __init__(self):
        addEvent('info.search', self.search)
        addEvent('movie.search', self.search)
        addEvent('movie.info', self.getInfo)

    # --- daily call budget ---------------------------------------------------

    def _todayKey(self):
        return time.strftime('%Y%m%d')

    def _getDailyCount(self):
        return self._daily_calls.get(self._todayKey(), 0)

    def _incrementDaily(self):
        key = self._todayKey()
        count = self._daily_calls.get(key, 0) + 1
        self._daily_calls[key] = count
        # Prune old days (keep only today)
        for k in list(self._daily_calls):
            if k != key:
                del self._daily_calls[k]
        if count == DAILY_BUDGET:
            log.warning('OMDB daily budget of %d reached — skipping further calls today' % DAILY_BUDGET)
        return count

    def _overBudget(self):
        count = self._getDailyCount()
        if count >= DAILY_BUDGET:
            return True
        return False

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

        cache_key = 'omdbapi.cache.%s' % identifier

        # Check cache first (before budget check)
        cached = self.getCache(cache_key)
        if cached:
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
            'tab': 'providers',
            'name': 'tmdb',
            'label': 'OMDB API',
            'hidden': True,
            'description': 'Used for all calls to TheMovieDB.',
            'options': [
                {
                    'name': 'api_key',
                    'default': 'bbc0e412',  # Don't be a dick and use this somewhere else
                    'label': 'Api Key',
                },
            ],
        },
    ],
}]
