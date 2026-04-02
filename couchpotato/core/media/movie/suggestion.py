import time
import traceback
from couchpotato.api import addApiView
from couchpotato.core.event import fireEvent, addEvent
from couchpotato.core.helpers.variable import splitString, removeDuplicate, getIdentifier, getTitle
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin
from couchpotato.environment import Env

log = CPLog(__name__)

autoload = 'Suggestion'


class Suggestion(Plugin):

    def __init__(self):

        addApiView('suggestion.view', self.suggestView)
        addApiView('suggestion.ignore', self.ignoreView)

        def test():
            time.sleep(1)
            self.suggestView()

        addEvent('app.load', test)

    def suggestView(self, limit = 6, **kwargs):
        if self.isDisabled():
            return {
                'success': True,
                'movies': []
            }

        movies = splitString(kwargs.get('movies', ''))
        ignored = splitString(kwargs.get('ignored', ''))
        seen = splitString(kwargs.get('seen', ''))

        cached_suggestion = self.getCache('suggestion_cached')
        if cached_suggestion:
            log.info('Returning %s cached suggestions' % len(cached_suggestion))
            suggestions = cached_suggestion
        else:

            if not movies or len(movies) == 0:
                active_movies = fireEvent('media.with_status', ['active', 'done'], types = 'movie', single = True)
                movies = [getIdentifier(x) for x in active_movies]
                log.info('suggestView: found %s active/done movies in library' % len(movies))
                # Log sample of identifiers for debugging
                non_none = [m for m in movies if m]
                log.info('suggestView: %s have identifiers, sample: %s' % (len(non_none), non_none[:5]))

            if not ignored or len(ignored) == 0:
                ignored = splitString(Env.prop('suggest_ignore', default = ''))
            if not seen or len(seen) == 0:
                movies.extend(splitString(Env.prop('suggest_seen', default = '')))

            try:
                suggestions = fireEvent('movie.suggest', movies = movies, ignore = ignored, single = True)
                log.info('suggestView: movie.suggest returned %s suggestions' % (len(suggestions) if suggestions else 0))
            except:
                log.error('suggestView: movie.suggest failed: %s' % traceback.format_exc())
                suggestions = None
            self.setCache('suggestion_cached', suggestions, timeout = 86400)  # Cache for 1 day

        if not suggestions:
            suggestions = []

        medias = []
        for suggestion in suggestions[:int(limit)]:

            # Cache poster
            posters = suggestion.get('images', {}).get('poster', [])
            poster = [x for x in posters if 'tmdb' in x]
            posters = poster if len(poster) > 0 else posters

            cached_poster = fireEvent('file.download', url = posters[0], single = True) if len(posters) > 0 else False
            files = {'image_poster': [cached_poster] } if cached_poster else {}

            # Normalize rating to dict format for JS frontend compatibility
            rating = suggestion.get('rating')
            if rating is not None and not isinstance(rating, dict):
                suggestion['rating'] = {'imdb': (rating, suggestion.pop('votes', 0))}

            medias.append({
                'status': 'suggested',
                'title': getTitle(suggestion),
                'type': 'movie',
                'info': suggestion,
                'files': files,
                'identifiers': {
                    'imdb': suggestion.get('imdb')
                }
            })

        return {
            'success': True,
            'movies': medias
        }

    def ignoreView(self, imdb = None, limit = 6, remove_only = False, mark_seen = False, **kwargs):

        ignored = splitString(Env.prop('suggest_ignore', default = ''))
        seen = splitString(Env.prop('suggest_seen', default = ''))

        new_suggestions = []
        if imdb:
            if mark_seen:
                seen.append(imdb)
                Env.prop('suggest_seen', ','.join(set(seen)))
            elif not remove_only:
                ignored.append(imdb)
                Env.prop('suggest_ignore', ','.join(set(ignored)))

            new_suggestions = self.updateSuggestionCache(ignore_imdb = imdb, limit = limit, ignored = ignored, seen = seen)

        if len(new_suggestions) <= limit:
            return {
                'result': False
            }

        # Only return new (last) item
        media = {
            'status': 'suggested',
            'title': getTitle(new_suggestions[limit]),
            'type': 'movie',
            'info': new_suggestions[limit],
            'identifiers': {
                'imdb': new_suggestions[limit].get('imdb')
            }
        }

        return {
            'result': True,
            'movie': media
        }

    def updateSuggestionCache(self, ignore_imdb = None, limit = 6, ignored = None, seen = None):

        # Combine with previous suggestion_cache
        cached_suggestion = self.getCache('suggestion_cached') or []
        new_suggestions = []
        ignored = [] if not ignored else ignored
        seen = [] if not seen else seen

        if ignore_imdb:
            suggested_imdbs = []
            for cs in cached_suggestion:
                if cs.get('imdb') != ignore_imdb and cs.get('imdb') not in suggested_imdbs:
                    suggested_imdbs.append(cs.get('imdb'))
                    new_suggestions.append(cs)

        # Get new results and add them
        if len(new_suggestions) - 1 < limit:
            active_movies = fireEvent('media.with_status', ['active', 'done'], single = True)
            movies = [getIdentifier(x) for x in active_movies]
            movies.extend(seen)

            ignored.extend([x.get('imdb') for x in cached_suggestion])
            suggestions = fireEvent('movie.suggest', movies = movies, ignore = removeDuplicate(ignored), single = True)

            if suggestions:
                new_suggestions.extend(suggestions)

        self.setCache('suggestion_cached', new_suggestions, timeout = 86400)

        return new_suggestions

config = [{
    'name': 'suggestion',
    'groups': [
        {
            'label': 'Suggestions',
            'description': 'Displays suggestions on the home page',
            'name': 'suggestions',
            'tab': 'display',
            'options': [
                {
                    'name': 'enabled',
                    'default': True,
                    'type': 'enabler',
                },
            ],
        },
    ],
}]
