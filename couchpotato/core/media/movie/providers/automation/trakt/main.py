import json
import time
import traceback

from couchpotato import Env, fireEvent
from couchpotato.api import addApiView
from couchpotato.core.event import addEvent
from couchpotato.core.logger import CPLog
from couchpotato.core.media._base.providers.base import Provider
from couchpotato.core.media.movie.providers.automation.base import Automation


log = CPLog(__name__)


class TraktBase(Provider):

    client_id = '8a54ed7b5e1b56d874642770ad2e8b73e2d09d6e993c3a92b1e89690bb1c9014'
    api_url = 'https://api-v2launch.trakt.tv/'

    def call(self, method_url, post_data = None):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer %s' % self.conf('automation_oauth_token'),
            'trakt-api-version': 2,
            'trakt-api-key': self.client_id,
        }

        if post_data:
            post_data = json.dumps(post_data)

        data = self.getJsonData(self.api_url + method_url, data = post_data or {}, headers = headers)
        return data if data else []


class Trakt(Automation, TraktBase):

    urls = {
        'watchlist': 'sync/watchlist/movies?extended=full',
        # TODO: Implement direct Trakt OAuth flow. The old flow proxied through
        # api.couchpota.to which is permanently dead. Existing tokens in
        # config.ini will continue to work until they expire.
    }

    def __init__(self):
        super(Trakt, self).__init__()

        addApiView('automation.trakt.auth_url', self.getAuthorizationUrl)
        addApiView('automation.trakt.credentials', self.getCredentials)

        fireEvent('schedule.interval', 'updater.check', self.refreshToken, hours = 24)
        addEvent('app.load', self.refreshToken)

    def refreshToken(self):

        token = self.conf('automation_oauth_token')
        refresh_token = self.conf('automation_oauth_refresh')
        if token and refresh_token:

            prop_name = 'last_trakt_refresh'
            last_refresh = int(Env.prop(prop_name, default = 0))

            if last_refresh < time.time()-4838400:  # refresh every 8 weeks
                log.warning('Trakt token refresh is unavailable (api.couchpota.to proxy is dead). '
                            'If your token expires, you will need to re-configure Trakt manually.')

        elif token and not refresh_token:
            log.error('Refresh token is missing, please re-register Trakt for autorefresh of the token in the future')

    def getIMDBids(self):
        movies = []
        for movie in self.getWatchlist():
            m = movie.get('movie')
            m['original_title'] = m['title']
            log.debug("Movie: %s", m)
            if self.isMinimalMovie(m):
                log.info("Trakt automation: %s satisfies requirements, added", m.get('title'))
                movies.append(m.get('ids').get('imdb'))
                continue

        return movies

    def getWatchlist(self):
        return self.call(self.urls['watchlist'])

    def getAuthorizationUrl(self, host = None, **kwargs):
        # TODO: Implement direct Trakt OAuth flow
        log.error('Trakt authorization is unavailable (api.couchpota.to proxy is dead)')
        return {
            'success': False,
            'error': 'Trakt OAuth proxy (api.couchpota.to) is no longer available. '
                     'Direct OAuth support is planned for a future update.',
        }

    def getCredentials(self, **kwargs):
        try:
            oauth_token = kwargs.get('oauth')
            refresh_token = kwargs.get('refresh')

            log.debug('oauth_token is: %s', oauth_token)
            self.conf('automation_oauth_token', value = oauth_token)
            self.conf('automation_oauth_refresh', value = refresh_token)

            Env.prop('last_trakt_refresh', value = int(time.time()))
        except:
            log.error('Failed setting trakt token: %s', traceback.format_exc())

        return 'redirect', Env.get('web_base') + 'settings/automation/'
