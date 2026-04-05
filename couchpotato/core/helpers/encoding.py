from string import ascii_letters, digits
from urllib.parse import quote_plus, urlencode
import os
import re
import traceback
import unicodedata

from couchpotato.core.logger import CPLog


log = CPLog(__name__)


def toSafeString(original):
    valid_chars = "-_.() %s%s" % (ascii_letters, digits)
    cleaned_filename = unicodedata.normalize('NFKD', toUnicode(original)).encode('ASCII', 'ignore').decode('ASCII')
    valid_string = ''.join(c for c in cleaned_filename if c in valid_chars)
    return ' '.join(valid_string.split())


def simplifyString(original):
    string = original.lower()

    # Replace problematic Unicode before ASCII flattening:
    # Vulgar fractions: insert space before so '1½' doesn't become '112'
    _vulgar_fractions = {
        '\u00bc': ' 1/4', '\u00bd': ' 1/2', '\u00be': ' 3/4',
        '\u2153': ' 1/3', '\u2154': ' 2/3', '\u2155': ' 1/5',
        '\u2156': ' 2/5', '\u2157': ' 3/5', '\u2158': ' 4/5',
        '\u2159': ' 1/6', '\u215a': ' 5/6', '\u215b': ' 1/8',
        '\u215c': ' 3/8', '\u215d': ' 5/8', '\u215e': ' 7/8',
    }
    for frac, repl in _vulgar_fractions.items():
        string = string.replace(frac, repl)
    # Middle dot (U+00B7, as in WALL·E): replace with space so it becomes two tokens
    # rather than being silently dropped by ASCII encoding
    string = string.replace('\u00b7', ' ')

    string = stripAccents(string)
    string = toSafeString(' '.join(re.split(r'\W+', string)))
    split = re.split(r'\W+|_', string.lower())
    return toUnicode(' '.join(split))


def toUnicode(original, *args):
    try:
        if isinstance(original, str):
            return original
        elif isinstance(original, bytes):
            try:
                return original.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    from couchpotato.environment import Env
                    return original.decode(Env.get("encoding"))
                except:
                    try:
                        from chardet import detect
                        detected = detect(original)
                        if detected.get('confidence', 0) > 0.8:
                            return original.decode(detected.get('encoding'))
                    except:
                        pass
                    return original.decode('utf-8', 'replace')
        else:
            return str(original)
    except:
        log.error('Unable to decode value "%s..." : %s ', (repr(original)[:20], traceback.format_exc()))
        return 'ERROR DECODING STRING'


def ss(original, *args):
    """Convert to native string. In Python 3, native strings are unicode,
    so this is equivalent to toUnicode."""
    return toUnicode(original, *args)


def sp(path, *args):
    """Standardise path encoding, normalise case, and strip trailing separators."""
    if not path or len(path) == 0:
        return path

    # Ensure path is a string
    path = toUnicode(path)

    # convert windows path (from remote box) to *nix path
    if os.path.sep == '/' and '\\' in path:
        path = '/' + path.replace(':', '').replace('\\', '/')

    path = os.path.normpath(path)

    # Remove any trailing path separators
    if path != os.path.sep:
        path = path.rstrip(os.path.sep)

    # Add a trailing separator in case it is a root folder on windows (crashes guessit)
    if len(path) == 2 and path[1] == ':':
        path = path + os.path.sep

    # Replace *NIX ambiguous '//' at the beginning of a path with '/' (crashes guessit)
    path = re.sub('^//', '/', path)

    return path


def ek(original, *args):
    """Encoding kludge — in Python 3 this just ensures we have a str."""
    if isinstance(original, bytes):
        try:
            from couchpotato.environment import Env
            return original.decode(Env.get('encoding'), 'ignore')
        except:
            return original.decode('utf-8', 'ignore')
    return str(original) if not isinstance(original, str) else original


def isInt(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


def stripAccents(s):
    return ''.join((c for c in unicodedata.normalize('NFD', toUnicode(s)) if unicodedata.category(c) != 'Mn'))


def tryUrlencode(s):
    if isinstance(s, dict):
        # urlencode handles value encoding itself — do NOT pre-encode values
        # with quote_plus or commas in append_to_response etc. get double-encoded
        return urlencode({k: toUnicode(v) for k, v in s.items()})
    else:
        return quote_plus(toUnicode(s))
