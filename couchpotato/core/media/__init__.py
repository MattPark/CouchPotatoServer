import os
import traceback

from couchpotato import CPLog, md5
from couchpotato.core.event import addEvent, fireEvent, fireEventAsync
from couchpotato.core.helpers.encoding import toUnicode
from couchpotato.core.helpers.variable import getExt
from couchpotato.core.plugins.base import Plugin


log = CPLog(__name__)


class MediaBase(Plugin):

    _type = None

    def initType(self):
        addEvent('media.types', self.getType)

    def getType(self):
        return self._type

    def createOnComplete(self, media_id):

        def onComplete():
            try:
                media = fireEvent('media.get', media_id, single = True)
                if media:
                    event_name = '%s.searcher.single' % media.get('type')
                    fireEventAsync(event_name, media, on_complete = self.createNotifyFront(media_id), manual = True)
            except:
                log.error('Failed creating onComplete: %s', traceback.format_exc())

        return onComplete

    def createNotifyFront(self, media_id):

        def notifyFront():
            try:
                media = fireEvent('media.get', media_id, single = True)
                if media:
                    event_name = '%s.update' % media.get('type')
                    fireEvent('notify.frontend', type = event_name, data = media)
            except:
                log.error('Failed creating onComplete: %s', traceback.format_exc())

        return notifyFront

    def getDefaultTitle(self, info, default_title = None):

        # Set default title
        default_title = default_title if default_title else toUnicode(info.get('title'))
        titles = info.get('titles', [])
        counter = 0
        def_title = None
        for title in titles:
            if (len(default_title) == 0 and counter == 0) or len(titles) == 1 or title.lower() == toUnicode(default_title.lower()) or (toUnicode(default_title) == '' and toUnicode(titles[0]) == title):
                def_title = toUnicode(title)
                break
            counter += 1

        if not def_title and titles and len(titles) > 0:
            def_title = toUnicode(titles[0])

        return def_title or 'UNKNOWN'

    def getPoster(self, media, image_urls):
        if 'files' not in media:
            media['files'] = {}

        existing_files = media['files']

        image_type = 'poster'
        file_type = 'image_%s' % image_type

        # Make existing unique
        unique_files = list(set(existing_files.get(file_type, [])))

        # Remove files that can't be found
        for ef in unique_files:
            if not os.path.isfile(ef):
                unique_files.remove(ef)

        # Replace new files list
        existing_files[file_type] = unique_files
        if len(existing_files) == 0:
            del existing_files[file_type]

        images = image_urls.get(image_type, [])

        # Prefer reliable CDNs (FanartTV, TMDB) over OMDB's Amazon URLs which go stale.
        # The original code here was destructive — it dropped non-matching URLs
        # via a broken slice assignment (images[:-1] = initially_try).
        # The merge order already puts FanartTV first (priority 1) and TMDB second
        # (priority 3), so just push stale-prone Amazon/OMDB URLs to the end.
        reliable = [x for x in images if 'fanart' in x or 'tmdb' in x]
        rest = [x for x in images if x not in reliable]
        images = reliable + rest

        # Loop over type
        for image in images:
            if not isinstance(image, str):
                continue

            # Check if it has top image
            filename = '%s.%s' % (md5(image), getExt(image))
            existing = existing_files.get(file_type, [])
            has_latest = False
            for x in existing:
                if filename in x:
                    has_latest = True

            if not has_latest or file_type not in existing_files or len(existing_files.get(file_type, [])) == 0:
                file_path = fireEvent('file.download', url = image, single = True)
                if file_path:
                    existing_files[file_type] = [toUnicode(file_path)]
                    break
            else:
                break
