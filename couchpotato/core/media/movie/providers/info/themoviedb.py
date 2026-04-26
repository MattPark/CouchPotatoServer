import random
import time
import traceback
import itertools
from base64 import b64decode as bd

from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import toUnicode, ss, tryUrlencode
from couchpotato.core.helpers.variable import tryInt, tryFloat, splitString, nativeImdbId
from couchpotato.core.logger import CPLog
from couchpotato.core.media.movie.providers.base import MovieProvider
from couchpotato.environment import Env

log = CPLog(__name__)


# Use the shared nativeImdbId from variable.py
_native_imdb_id = nativeImdbId

autoload = 'TheMovieDb'


class TheMovieDb(MovieProvider):

    http_time_between_calls = .35

    configuration = {
        'images': {
            'secure_base_url': 'https://image.tmdb.org/t/p/',
        },
    }

    ak = ['ZTIyNGZlNGYzZmVjNWY3YjU1NzA2NDFmN2NkM2RmM2E=', 'ZjZiZDY4N2ZmYTYzY2QyODJiNmZmMmM2ODc3ZjI2Njk=']

    languages = [ 'en' ]
    default_language = 'en'

    # In-memory daily call counter: {'YYYYMMDD': count}
    _daily_calls = {}

    def __init__(self):
        addEvent('info.search', self.search, priority = 3)
        addEvent('movie.search', self.search, priority = 3)
        addEvent('movie.info', self.getInfo, priority = 3)
        addEvent('movie.info_by_tmdb', self.getInfo)
        addEvent('app.load', self.config)
        addEvent('movie.is_movie', self.isMovie)
        addEvent('movie.suggest', self.getSuggestions)
        addEvent('metadata.stats', self.getStats)

    # --- call tracking -------------------------------------------------------

    def _todayKey(self):
        return time.strftime('%Y%m%d')

    def _incrementDaily(self):
        key = self._todayKey()
        self._daily_calls[key] = self._daily_calls.get(key, 0) + 1
        for k in list(self._daily_calls):
            if k != key:
                del self._daily_calls[k]

    def _getDailyCount(self):
        return self._daily_calls.get(self._todayKey(), 0)

    def getStats(self):
        key = self.conf('api_key')
        return {
            'tmdb': {
                'calls_today': self._getDailyCount(),
                'has_custom_key': key != '' and key is not None,
            }
        }

    def config(self):

        # Reset invalid key
        if self.conf('api_key') == '9b939aee0aaafc12a65bf448e4af9543':
            self.conf('api_key', '')

        languages = self.getLanguages()

        # languages should never be empty, the first language is the default language used for all the description details
        self.default_language = languages[0]

        # en is always downloaded and it is the fallback
        if 'en' in languages:
            languages.remove('en')

        # default language has a special management
        if self.default_language in languages:
            languages.remove(self.default_language)

        self.languages = languages

        configuration = self.request('configuration')
        if configuration:
            self.configuration = configuration

    def getSuggestions(self, movies = None, ignore = None, **kwargs):
        """Get movie suggestions based on the user's library using TMDB recommendations."""

        if not movies:
            log.debug('getSuggestions: no movies in library, returning empty')
            return []
        if not ignore:
            ignore = []

        if self.isDisabled():
            return []

        log.debug('getSuggestions: %d library movies, sampling up to 5' % len(movies))

        # Sample a handful of library movies to get recommendations from
        sample_size = min(5, len(movies))
        sampled = random.sample(movies, sample_size)

        seen_tmdb_ids = set()
        suggestions = []

        for imdb_id in sampled:
            if self.shuttingDown():
                break

            if not imdb_id:
                continue

            # Look up TMDB ID from IMDB ID
            try:
                native_id = _native_imdb_id(imdb_id)
                find_data = self.request('find/%s' % native_id, {
                    'external_source': 'imdb_id',
                })
                if not find_data or not find_data.get('movie_results'):
                    log.debug('getSuggestions: no TMDB match for %s' % imdb_id)
                    continue
                tmdb_id = find_data['movie_results'][0]['id']
            except Exception:
                log.debug('getSuggestions: failed TMDB lookup for %s: %s' % (imdb_id, traceback.format_exc()))
                continue

            # Get recommendations for this movie
            try:
                recs = self.request('movie/%s/recommendations' % tmdb_id, {
                    'language': 'en',
                }, return_key = 'results')
                if not recs:
                    log.debug('getSuggestions: no recommendations for TMDB %s' % tmdb_id)
                    continue
                log.debug('getSuggestions: got %d recommendations for TMDB %s' % (len(recs), tmdb_id))
            except Exception:
                continue

            for rec in recs[:10]:
                rec_id = rec.get('id')
                if rec_id in seen_tmdb_ids:
                    continue
                seen_tmdb_ids.add(rec_id)

                # Parse into full movie data (includes IMDB ID lookup)
                try:
                    parsed = self.parseMovie({'id': rec_id}, extended = False)
                    if not parsed:
                        continue
                    # Skip if already in library or ignored
                    rec_imdb = parsed.get('imdb')
                    if rec_imdb and (rec_imdb in movies or rec_imdb in ignore):
                        continue
                    suggestions.append(parsed)
                except Exception:
                    continue

                if len(suggestions) >= 20:
                    break

            if len(suggestions) >= 20:
                break

        log.debug('getSuggestions: returning %d suggestions' % len(suggestions))
        return suggestions

    def isMovie(self, identifier = None, adding = False, **kwargs):
        """Check if an IMDB identifier is a movie (not a TV show).
        Uses TMDB's /find endpoint to look up by external ID."""

        if not identifier:
            return True

        if self.isDisabled():
            return True

        try:
            data = self.request('find/%s' % _native_imdb_id(identifier), {
                'external_source': 'imdb_id',
            })
            if data:
                if data.get('movie_results'):
                    return True
                if data.get('tv_results') or data.get('tv_episode_results') or data.get('tv_season_results'):
                    return False
            # Default to True (assume movie) if we can't determine
            return True
        except Exception:
            log.error('Failed checking if %s is a movie: %s' % (identifier, traceback.format_exc()))
            return True

    def search(self, q, limit = 3):
        """ Find movie by name """

        if self.isDisabled():
            return False

        log.debug('Searching for movie: %s', q)

        raw = None
        try:
            name_year = fireEvent('scanner.name_year', q, single = True)
            raw = self.request('search/movie', {
                'query': name_year.get('name', q),
                'year': name_year.get('year'),
                'search_type': 'ngram' if limit > 1 else 'phrase'
            }, return_key = 'results')
        except Exception:
            log.error('Failed searching TMDB for "%s": %s', (q, traceback.format_exc()))

        results = []
        if raw:
            try:
                nr = 0

                for movie in raw:
                    parsed_movie = self.parseMovie(movie, extended = False)
                    if parsed_movie:
                        results.append(parsed_movie)

                    nr += 1
                    if nr == limit:
                        break

                log.info('Found: %s', [result['titles'][0] + ' (' + str(result.get('year', 0)) + ')' for result in results])

                return results
            except SyntaxError as e:
                log.error('Failed to parse XML response: %s', e)
                return False

        return results

    def getInfo(self, identifier = None, extended = True, **kwargs):

        if not identifier:
            return {}

        # TMDB's /movie/ endpoint accepts IMDB IDs but only in native format (tt0111161),
        # not the 8-digit padded format (tt00111161) used internally
        is_imdb = isinstance(identifier, str) and identifier.startswith('tt')
        if is_imdb:
            identifier = _native_imdb_id(identifier)

        result = self.parseMovie({
            'id': identifier
        }, extended = extended)

        # Fallback: TMDB's /movie/tt... endpoint returns 404 for some movies even
        # though they exist.  Use /find to resolve the numeric TMDB ID and retry.
        if not result and is_imdb:
            try:
                find_data = self.request('find/%s' % identifier, {
                    'external_source': 'imdb_id',
                })
                if find_data and find_data.get('movie_results'):
                    tmdb_id = find_data['movie_results'][0]['id']
                    log.info('TMDB /movie/%s failed, resolved via /find to TMDB id %d' % (identifier, tmdb_id))
                    result = self.parseMovie({
                        'id': tmdb_id
                    }, extended = extended)
            except Exception:
                log.debug('TMDB /find fallback failed for %s: %s' % (identifier, traceback.format_exc()))

        return result or {}

    def parseMovie(self, movie, extended = True):

        # Do request, append other items
        movie = self.request('movie/%s' % movie.get('id'), {
            'append_to_response': 'alternative_titles,videos' + (',images,casts' if extended else ''),
            'language': 'en'
        })
        if not movie:
            return

        movie_default = movie if self.default_language == 'en' else self.request('movie/%s' % movie.get('id'), {
            'append_to_response': 'alternative_titles' + (',images,casts' if extended else ''),
			'language': self.default_language
        })

        movie_default = movie_default or movie

        movie_others = [ self.request('movie/%s' % movie.get('id'), {
            'append_to_response': 'alternative_titles' + (',images,casts' if extended else ''),
			'language': language
        }) for language in self.languages] if self.languages else []

        # Images
        poster = self.getImage(movie, type = 'poster', size = 'w154')
        poster_original = self.getImage(movie, type = 'poster', size = 'original')
        backdrop_original = self.getImage(movie, type = 'backdrop', size = 'original')
        extra_thumbs = self.getMultImages(movie, type = 'backdrops', size = 'original') if extended else []

        images = {
            'poster': [poster] if poster else [],
            #'backdrop': [backdrop] if backdrop else [],
            'poster_original': [poster_original] if poster_original else [],
            'backdrop_original': [backdrop_original] if backdrop_original else [],
            'actors': {},
            'extra_thumbs': extra_thumbs
        }

        # Genres
        try:
            genres = [genre.get('name') for genre in movie.get('genres', [])]
        except Exception:
            genres = []

        # 1900 is the same as None
        year = str(movie.get('release_date') or '')[:4]
        if not movie.get('release_date') or year == '1900' or year.lower() == 'none':
            year = None

        # Gather actors data
        actors = {}
        if extended:

            # Full data
            cast = movie.get('casts', {}).get('cast', [])

            for cast_item in cast:
                try:
                    actors[toUnicode(cast_item.get('name'))] = toUnicode(cast_item.get('character'))
                    images['actors'][toUnicode(cast_item.get('name'))] = self.getImage(cast_item, type = 'profile', size = 'original')
                except Exception:
                    log.debug('Error getting cast info for %s: %s', (cast_item, traceback.format_exc()))

        # Extract YouTube trailer key from TMDB videos response
        trailer_key = None
        try:
            videos = movie.get('videos', {}).get('results', [])
            # Prefer official trailers, then teasers, then any YouTube video
            for preferred_type in ['Trailer', 'Teaser']:
                for v in videos:
                    if v.get('site') == 'YouTube' and v.get('type') == preferred_type:
                        trailer_key = v.get('key')
                        break
                if trailer_key:
                    break
            if not trailer_key:
                for v in videos:
                    if v.get('site') == 'YouTube':
                        trailer_key = v.get('key')
                        break
        except Exception:
            pass

        movie_data = {
            'type': 'movie',
            'via_tmdb': True,
            'tmdb_id': movie.get('id'),
            'titles': [toUnicode(movie_default.get('title') or movie.get('title'))],
            'original_title': movie.get('original_title'),
            'original_language': movie.get('original_language'),
            'spoken_languages': [sl.get('iso_639_1') for sl in movie.get('spoken_languages', []) if sl.get('iso_639_1')],
            'production_countries': [pc.get('iso_3166_1') for pc in movie.get('production_countries', []) if pc.get('iso_3166_1')],
            'images': images,
            'imdb': movie.get('imdb_id'),
            'runtime': movie.get('runtime'),
            'released': str(movie.get('release_date')),
            'year': tryInt(year, None),
            'plot': movie_default.get('overview') or movie.get('overview'),
            'rating': tryFloat(movie.get('vote_average', 0)),
            'votes': tryInt(movie.get('vote_count', 0)),
            'genres': genres,
            'collection': getattr(movie.get('belongs_to_collection'), 'name', None),
            'actor_roles': actors,
            'trailer_key': trailer_key
        }

        movie_data = dict((k, v) for k, v in movie_data.items() if v)

        # Add alternative names
        movies = [ movie ] + movie_others if movie == movie_default else [ movie, movie_default ] + movie_others
        movie_titles = [ self.getTitles(movie) for movie in movies ]

        all_titles = sorted(list(itertools.chain.from_iterable(movie_titles)))

        alternate_titles = movie_data['titles']

        for title in all_titles:
            if title and title not in alternate_titles and title.lower() != 'none' and title is not None:
                alternate_titles.append(title)

        movie_data['titles'] = alternate_titles

        return movie_data

    def getImage(self, movie, type = 'poster', size = 'poster'):

        image_url = ''
        try:
            path = movie.get('%s_path' % type)
            if path:
                image_url = '%s%s%s' % (self.configuration['images']['secure_base_url'], size, path)
        except Exception:
            log.debug('Failed getting %s.%s for "%s"', (type, size, ss(str(movie))))

        return image_url

    def getMultImages(self, movie, type = 'backdrops', size = 'original'):

        image_urls = []
        try:
            for image in movie.get('images', {}).get(type, [])[1:5]:
                image_urls.append(self.getImage(image, 'file', size))
        except Exception:
            log.debug('Failed getting %s.%s for "%s"', (type, size, ss(str(movie))))

        return image_urls

    def request(self, call = '', params = {}, return_key = None):

        params = dict((k, v) for k, v in params.items() if v)
        params = tryUrlencode(params)

        try:
            url = 'https://api.themoviedb.org/3/%s?api_key=%s%s' % (call, self.getApiKey(), '&%s' % params if params else '')
            data = self.getJsonData(url, show_error = False)
            self._incrementDaily()
        except Exception:
            log.debug('Movie not found: %s, %s', (call, params))
            self._incrementDaily()
            data = None

        if data and return_key and return_key in data:
            data = data.get(return_key)

        return data

    def isDisabled(self):
        if self.getApiKey() == '':
            log.error('No API key provided.')
            return True
        return False

    def getApiKey(self):
        key = self.conf('api_key')
        return bd(random.choice(self.ak)).decode('utf-8') if key == '' else key

    def getLanguages(self):
        languages = splitString(Env.setting('languages', section = 'core'))
        if len(languages):
            return languages

        return [ 'en' ]

    def getTitles(self, movie):
        # add the title to the list
        title = toUnicode(movie.get('title'))

        titles = [title] if title else []

        # add the original_title to the list
        alternate_title = toUnicode(movie.get('original_title'))

        if alternate_title and alternate_title not in titles:
            titles.append(alternate_title)

        # Add alternative titles
        alternate_titles = movie.get('alternative_titles', {}).get('titles', [])

        for alt in alternate_titles:
            alt_name = toUnicode(alt.get('title'))
            if alt_name and alt_name not in titles and alt_name.lower() != 'none' and alt_name is not None:
                titles.append(alt_name)

        return titles


config = [{
    'name': 'themoviedb',
    'groups': [
        {
            'tab': 'metadata',
            'name': 'themoviedb',
            'label': 'TheMovieDB',
            'description': 'Primary source for movie info, posters, and search results.',
            'options': [
                {
                    'name': 'api_key',
                    'default': '',
                    'label': 'API Key',
                    'description': 'Optional. Leave empty to use built-in keys. Get your own at <a href="https://www.themoviedb.org/settings/api" target="_blank">themoviedb.org</a>',
                },
            ],
        },
    ],
}]
