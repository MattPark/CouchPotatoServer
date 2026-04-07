"""Unit tests for module-level helper functions in couchpotato.core.db."""


from couchpotato.core.db import (
    _simplify_title,
    _normalize_imdb,
    _starts_with_char,
    _media_id_match,
    _release_dl_match,
)


# ---------------------------------------------------------------------------
# _simplify_title
# ---------------------------------------------------------------------------

def test_simplify_title_basic():
    """Strips leading articles, lowercases, and removes non-alnum chars."""
    assert _simplify_title("The Dark Knight") == "dark knight"


def test_simplify_title_empty():
    assert _simplify_title("") == ""
    assert _simplify_title(None) == ""


def test_simplify_title_special_chars():
    """Non-alphanumeric characters (except spaces) are removed."""
    assert _simplify_title("Spider-Man: Homecoming!") == "spiderman homecoming"


# ---------------------------------------------------------------------------
# _normalize_imdb
# ---------------------------------------------------------------------------

def test_normalize_imdb_with_tt_prefix():
    """Should zero-pad to 7 digits after 'tt'."""
    assert _normalize_imdb("tt0133093") == "tt0133093"
    assert _normalize_imdb("tt133093") == "tt0133093"


def test_normalize_imdb_short_id():
    """Very short numeric part should be zero-padded."""
    assert _normalize_imdb("tt12345") == "tt0012345"


def test_normalize_imdb_non_imdb():
    """Non-IMDB values (no 'tt' prefix) are returned unchanged."""
    assert _normalize_imdb("12345") == "12345"
    assert _normalize_imdb(None) is None
    assert _normalize_imdb(42) == 42


# ---------------------------------------------------------------------------
# _starts_with_char
# ---------------------------------------------------------------------------

def test_starts_with_char_alpha():
    assert _starts_with_char("Inception") == "i"


def test_starts_with_char_numeric_and_special():
    """Titles starting with a digit or only special chars return '#'."""
    assert _starts_with_char("2001: A Space Odyssey") == "#"
    assert _starts_with_char("!!!") == "#"
    assert _starts_with_char("") == "#"


# ---------------------------------------------------------------------------
# _media_id_match
# ---------------------------------------------------------------------------

def test_media_id_match_imdb():
    doc = {"identifiers": {"imdb": "tt0133093"}}
    assert _media_id_match(doc, "imdb-tt133093") is True
    assert _media_id_match(doc, "imdb-tt9999999") is False


def test_media_id_match_no_identifiers():
    assert _media_id_match({}, "imdb-tt0133093") is False
    assert _media_id_match({"identifiers": "bad"}, "imdb-tt0133093") is False


def test_media_id_match_no_dash_in_key():
    doc = {"identifiers": {"imdb": "tt0133093"}}
    assert _media_id_match(doc, "nodashhere") is False


# ---------------------------------------------------------------------------
# _release_dl_match
# ---------------------------------------------------------------------------

def test_release_dl_match():
    doc = {"download_info": {"downloader": "nzbget", "id": "ABC123"}}
    assert _release_dl_match(doc, "nzbget-abc123") is True
    assert _release_dl_match(doc, "sabnzbd-abc123") is False


def test_release_dl_match_missing_info():
    assert _release_dl_match({}, "nzbget-abc123") is False
    assert _release_dl_match({"download_info": "bad"}, "nzbget-abc123") is False
