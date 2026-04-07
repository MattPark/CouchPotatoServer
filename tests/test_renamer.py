"""Unit tests for Renamer.replaceDoubles and Renamer.doReplace."""
from unittest.mock import MagicMock

from couchpotato.core.plugins.renamer import Renamer


def _make_renamer(separator=' ', replace_doubles=True, foldersep=' '):
    """Create a Renamer instance without calling __init__, with a mocked conf()."""
    renamer = Renamer.__new__(Renamer)
    renamer.conf = MagicMock(side_effect=lambda key, **kw: {
        'separator': separator,
        'replace_doubles': replace_doubles,
        'foldersep': foldersep,
    }.get(key, ''))
    return renamer


# ---------------------------------------------------------------------------
# replaceDoubles
# ---------------------------------------------------------------------------

def test_replace_doubles_collapses_double_spaces():
    r = _make_renamer()
    assert r.replaceDoubles('hello  world') == 'hello world'


def test_replace_doubles_collapses_double_dots():
    r = _make_renamer()
    assert r.replaceDoubles('hello..world') == 'hello.world'


def test_replace_doubles_collapses_double_dashes():
    r = _make_renamer()
    assert r.replaceDoubles('hello--world') == 'hello-world'


def test_replace_doubles_strips_trailing_separators():
    r = _make_renamer()
    # Trailing comma, dash, underscore, slash, backslash, space should be stripped
    assert r.replaceDoubles('hello,') == 'hello'
    assert r.replaceDoubles('hello-') == 'hello'
    assert r.replaceDoubles('hello ') == 'hello'


def test_replace_doubles_preserves_curly_braces():
    """Curly-brace tokens like {edition-Director's Cut} must survive."""
    r = _make_renamer()
    result = r.replaceDoubles("Movie {edition-Director's Cut}")
    assert "{edition-Director's Cut}" in result


# ---------------------------------------------------------------------------
# doReplace
# ---------------------------------------------------------------------------

def test_do_replace_basic_token():
    r = _make_renamer()
    result = r.doReplace('<thename> (<year>).<ext>', {
        'thename': 'Inception',
        'namethe': 'Inception',
        'year': '2010',
        'ext': 'mkv',
    })
    assert result == 'Inception (2010).mkv'


def test_do_replace_empty_token_no_trace():
    """When a token value is empty, the placeholder and any resulting double
    spaces should be cleaned up (replace_doubles=True)."""
    r = _make_renamer()
    result = r.doReplace('<thename> <3d> (<year>).<ext>', {
        'thename': 'Inception',
        'namethe': 'Inception',
        'year': '2010',
        'ext': 'mkv',
        '3d': '',
    })
    # No double spaces should remain
    assert '  ' not in result
    assert result == 'Inception (2010).mkv'


def test_do_replace_multiple_tokens():
    r = _make_renamer()
    result = r.doReplace('<thename> (<year>) - <quality>.<ext>', {
        'thename': 'The Matrix',
        'namethe': 'Matrix, The',
        'year': '1999',
        'quality': '1080p',
        'ext': 'mkv',
    })
    assert result == 'The Matrix (1999) - 1080p.mkv'


def test_do_replace_edition_plex_survives():
    """Plex edition token with curly braces must not be mangled."""
    r = _make_renamer()
    result = r.doReplace('<thename> (<year>) <edition_plex>.<ext>', {
        'thename': 'Blade Runner',
        'namethe': 'Blade Runner',
        'year': '1982',
        'ext': 'mkv',
        'edition_plex': "{edition-Director's Cut}",
    })
    assert "{edition-Director's Cut}" in result
    assert result.startswith('Blade Runner (1982)')


def test_do_replace_imdb_id_plex_survives():
    """Plex IMDB token with curly braces must not be mangled."""
    r = _make_renamer()
    result = r.doReplace('<thename> (<year>) <imdb_id_plex>.<ext>', {
        'thename': 'Inception',
        'namethe': 'Inception',
        'year': '2010',
        'ext': 'mkv',
        'imdb_id_plex': '{imdb-tt1375666}',
    })
    assert '{imdb-tt1375666}' in result
