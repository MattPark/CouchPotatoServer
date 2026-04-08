from couchpotato.core.event import addEvent
from couchpotato.core.helpers.variable import splitString
from couchpotato.core.logger import CPLog
from couchpotato.core.plugins.base import Plugin


log = CPLog(__name__)

autoload = 'Subtitle'


class Subtitle(Plugin):

    def __init__(self):
        addEvent('renamer.before', self.searchSingle)

    def searchSingle(self, group):
        if self.isDisabled(): return

        log.info('Subtitle downloading is no longer supported. '
                 'Use external tools like Bazarr for subtitle management.')
        return True

    def getLanguages(self):
        return splitString(self.conf('languages'))


config = [{
    'name': 'subtitle',
    'groups': [
        {
            'tab': 'renamer',
            'name': 'subtitle',
            'label': 'Download subtitles',
            'description': 'after rename',
            'options': [
                {
                    'name': 'enabled',
                    'label': 'Search and download subtitles',
                    'default': False,
                    'type': 'enabler',
                },
                {
                    'name': 'languages',
                    'description': ('Comma separated, 2 letter country code.', 'Example: en, nl. See the codes at <a href="http://en.wikipedia.org/wiki/List_of_ISO_639-1_codes" target="_blank">on Wikipedia</a>'),
                },
                {
                    'advanced': True,
                    'name': 'force',
                    'label': 'Force',
                    'description': ('Force download all languages (including embedded).', 'This will also <strong>overwrite</strong> all existing subtitles.'),
                    'default': False,
                    'type': 'bool',
                },
            ],
        },
    ],
}]
