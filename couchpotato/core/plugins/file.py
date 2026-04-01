import mimetypes
import os.path
import traceback

from couchpotato import get_db
from couchpotato.api import addApiView
from couchpotato.core.event import addEvent, fireEvent
from couchpotato.core.helpers.encoding import toUnicode, ss, sp
from couchpotato.core.helpers.variable import md5, getExt, isSubFolder
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin
from couchpotato.environment import Env


log = CPLog(__name__)

autoload = 'FileManager'


class FileManager(Plugin):

    def __init__(self):
        addEvent('file.download', self.download)

        addApiView('file.cache', self.serveCacheFile, docs = {
            'desc': 'Return a file from the cp_data/cache directory',
            'params': {
                'filename': {'desc': 'path/filename of the wanted file'}
            },
            'return': {'type': 'file'}
        })

        fireEvent('schedule.interval', 'file.cleanup', self.cleanup, hours = 24)

        addEvent('app.test', self.doSubfolderTest)

    def cleanup(self):

        # Wait a bit after starting before cleanup
        log.debug('Cleaning up unused files')

        try:
            db = get_db()
            cache_dir = Env.get('cache_dir')
            medias = db.all('media', with_doc = True)

            # Collect basenames of referenced files (paths may reference old Docker paths)
            referenced_basenames = set()
            for media in medias:
                file_dict = media['doc'].get('files', {})
                for x in file_dict.keys():
                    for path in file_dict[x]:
                        referenced_basenames.add(os.path.basename(path))

            for f in os.listdir(cache_dir):
                if os.path.splitext(f)[1] in ['.png', '.jpg', '.jpeg']:
                    if f not in referenced_basenames:
                        file_path = os.path.join(cache_dir, f)
                        os.remove(file_path)
        except:
            log.error('Failed removing unused file: %s', traceback.format_exc())

    def serveCacheFile(self, filename=None, _request=None, **kwargs):
        cache_dir = sp(Env.get('cache_dir'))

        if not filename:
            return ''

        # Sanitize: only allow the basename (no path traversal)
        filename = os.path.basename(filename)
        file_path = os.path.join(cache_dir, filename)

        if not os.path.isfile(file_path):
            # Image not cached yet — try to download it on the fly
            # by looking up which media references this filename
            file_path = self._tryDownloadMissing(filename, cache_dir)
            if not file_path or not os.path.isfile(file_path):
                return ''

        # Return a special tuple that ApiHandler.sendData recognizes for binary file serving
        content_type, _ = mimetypes.guess_type(file_path)
        if not content_type:
            content_type = 'application/octet-stream'

        try:
            with open(file_path, 'rb') as f:
                return ('file', content_type, f.read())
        except:
            log.error('Failed serving cache file %s: %s', (filename, traceback.format_exc()))
            return ''

    def _tryDownloadMissing(self, filename, cache_dir):
        """Try to find the poster URL for a missing cached file and download it."""
        try:
            db = get_db()
            medias = db.all('media', with_doc=True)
            for media in medias:
                file_dict = media['doc'].get('files', {})
                for file_type, paths in file_dict.items():
                    for path in paths:
                        if os.path.basename(path) == filename:
                            # Found the media that references this file
                            # Get the URL from info.images
                            info = media['doc'].get('info', {})
                            images = info.get('images', {})
                            poster_urls = images.get('poster', [])
                            if poster_urls:
                                dest = os.path.join(cache_dir, filename)
                                result = fireEvent('file.download', url=poster_urls[0], dest=dest, single=True)
                                if result:
                                    return result
            return None
        except:
            log.debug('Failed trying to download missing cache file %s: %s', (filename, traceback.format_exc()))
            return None

    def download(self, url = '', dest = None, overwrite = False, urlopen_kwargs = None):
        if not urlopen_kwargs: urlopen_kwargs = {}

        # Return response object to stream download
        urlopen_kwargs['stream'] = True

        if not dest:  # to Cache
            dest = os.path.join(Env.get('cache_dir'), ss('%s.%s' % (md5(url), getExt(url))))

        dest = sp(dest)

        if not overwrite and os.path.isfile(dest):
            return dest

        try:
            filedata = self.urlopen(url, **urlopen_kwargs)
        except:
            log.error('Failed downloading file %s: %s', (url, traceback.format_exc()))
            return False

        self.createFile(dest, filedata, binary = True)
        return dest

    def doSubfolderTest(self):

        tests = {
            ('/test/subfolder', '/test/sub'): False,
            ('/test/sub/folder', '/test/sub'): True,
            ('/test/sub/folder', '/test/sub2'): False,
            ('/sub/fold', '/test/sub/fold'): False,
            ('/sub/fold', '/test/sub/folder'): False,
            ('/opt/couchpotato', '/var/opt/couchpotato'): False,
            ('/var/opt', '/var/opt/couchpotato'): False,
            ('/CapItaLs/Are/OK', '/CapItaLs/Are/OK'): True,
            ('/CapItaLs/Are/OK', '/CapItaLs/Are/OK2'): False,
            ('/capitals/are/not/OK', '/capitals/are/NOT'): False,
            ('\\\\Mounted\\Volume\\Test', '\\\\Mounted\\Volume'): True,
            ('C:\\\\test\\path', 'C:\\\\test2'): False
        }

        failed = 0
        for x in tests:
            if isSubFolder(x[0], x[1]) is not tests[x]:
                log.error('Failed subfolder test %s %s', x)
                failed += 1

        if failed > 0:
            log.error('Subfolder test failed %s tests', failed)
        else:
            log.info('Subfolder test succeeded')

        return failed == 0
