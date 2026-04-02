import time
import traceback

from couchpotato import tryInt
from couchpotato.core.event import addEvent
from couchpotato.core.logger import CPLog
from couchpotato.core.media.movie.providers.base import MovieProvider
from requests import HTTPError


log = CPLog(__name__)

autoload = 'FanartTV'

# Default built-in API key
DEFAULT_API_KEY = 'b28b14e9be662e027cfbc7c3dd600405'


class FanartTV(MovieProvider):

    urls = {
        'api': 'http://webservice.fanart.tv/v3/movies/%s?api_key=%s'
    }

    MAX_EXTRAFANART = 20
    http_time_between_calls = 0

    # In-memory daily call counter: {'YYYYMMDD': count}
    _daily_calls = {}

    def __init__(self):
        addEvent('movie.info', self.getArt, priority = 1)
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
        api_key = self.getApiKey()
        return {
            'fanarttv': {
                'calls_today': self._getDailyCount(),
                'has_custom_key': api_key != DEFAULT_API_KEY and api_key != '',
            }
        }

    def getApiKey(self):
        key = self.conf('api_key')
        return key if key else DEFAULT_API_KEY

    # --- API methods ---------------------------------------------------------

    def getArt(self, identifier = None, extended = True, **kwargs):

        if not identifier or not extended:
            return {}

        images = {}

        try:
            url = self.urls['api'] % (identifier, self.getApiKey())
            fanart_data = self.getJsonData(url, show_error = False)
            self._incrementDaily()

            if fanart_data:
                log.debug('Found images for %s', fanart_data.get('name'))
                images = self._parseMovie(fanart_data)
        except HTTPError as e:
            self._incrementDaily()
            log.debug('Failed getting extra art for %s: %s',
                      (identifier, e))
        except:
            self._incrementDaily()
            log.error('Failed getting extra art for %s: %s',
                      (identifier, traceback.format_exc()))
            return {}

        return {
            'images': images
        }

    def _parseMovie(self, movie):
        images = {
            'landscape': self._getMultImages(movie.get('moviethumb', []), 1),
            'logo': [],
            'disc_art': self._getMultImages(self._trimDiscs(movie.get('moviedisc', [])), 1),
            'clear_art': self._getMultImages(movie.get('hdmovieart', []), 1),
            'banner': self._getMultImages(movie.get('moviebanner', []), 1),
            'extra_fanart': [],
        }

        if len(images['clear_art']) == 0:
            images['clear_art'] = self._getMultImages(movie.get('movieart', []), 1)

        images['logo'] = self._getMultImages(movie.get('hdmovielogo', []), 1)
        if len(images['logo']) == 0:
            images['logo'] = self._getMultImages(movie.get('movielogo', []), 1)

        fanarts = self._getMultImages(movie.get('moviebackground', []), self.MAX_EXTRAFANART + 1)

        if fanarts:
            images['backdrop_original'] = [fanarts[0]]
            images['extra_fanart'] = fanarts[1:]

        return images

    def _trimDiscs(self, disc_images):
        """
        Return a subset of discImages. Only bluray disc images will be returned.
        """

        trimmed = []
        for disc in disc_images:
            if disc.get('disc_type') == 'bluray':
                trimmed.append(disc)

        if len(trimmed) == 0:
            return disc_images

        return trimmed

    def _getImage(self, images):
        image_url = None
        highscore = -1
        for image in images:
            if tryInt(image.get('likes')) > highscore:
                highscore = tryInt(image.get('likes'))
                image_url = image.get('url') or image.get('href')

        return image_url

    def _getMultImages(self, images, n):
        """
        Chooses the best n images and returns them as a list.
        If n<0, all images will be returned.
        """
        image_urls = []
        pool = []
        for image in images:
            if image.get('lang') == 'en':
                pool.append(image)
        orig_pool_size = len(pool)

        while len(pool) > 0 and (n < 0 or orig_pool_size - len(pool) < n):
            best = None
            highscore = -1
            for image in pool:
                if tryInt(image.get('likes')) > highscore:
                    highscore = tryInt(image.get('likes'))
                    best = image
            url = best.get('url') or best.get('href')
            if url:
                image_urls.append(url)
            pool.remove(best)

        return image_urls

    def isDisabled(self):
        if self.getApiKey() == '':
            log.error('No API key provided.')
            return True
        return False


config = [{
    'name': 'fanarttv',
    'groups': [
        {
            'tab': 'metadata',
            'name': 'fanarttv',
            'label': 'Fanart.tv',
            'description': 'Provides extra artwork — logos, disc art, banners, and fanart backgrounds.',
            'options': [
                {
                    'name': 'api_key',
                    'default': DEFAULT_API_KEY,
                    'label': 'API Key',
                    'description': 'A built-in key is provided. Get your own at <a href="https://fanart.tv/get-an-api-key/" target="_blank">fanart.tv</a>',
                },
            ],
        },
    ],
}]
