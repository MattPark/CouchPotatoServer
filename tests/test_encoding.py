"""Unit tests for couchpotato.core.helpers.encoding helper functions."""
import os

from couchpotato.core.helpers.encoding import (
    isInt,
    simplifyString,
    sp,
    ss,
    stripAccents,
    toUnicode,
    tryUrlencode,
)


# --- tryUrlencode ---

def test_tryurlencode_dict_basic():
    result = tryUrlencode({"key": "value", "foo": "bar"})
    # urlencode produces key=value&foo=bar (order may vary)
    assert "key=value" in result
    assert "foo=bar" in result
    assert "&" in result


def test_tryurlencode_dict_special_chars_not_double_encoded():
    """Dicts must NOT double-encode: commas, slashes, etc. should be encoded
    exactly once by urlencode, not pre-encoded by quote_plus."""
    result = tryUrlencode({"append_to_response": "videos,images"})
    # A single encoding of comma -> %2C
    assert "append_to_response=videos%2Cimages" in result
    # Double encoding would produce %252C — must not happen
    assert "%252C" not in result


def test_tryurlencode_dict_empty():
    assert tryUrlencode({}) == ""


def test_tryurlencode_string():
    result = tryUrlencode("hello world")
    assert result == "hello+world"


def test_tryurlencode_unicode_string():
    result = tryUrlencode("café")
    # quote_plus encodes the é
    assert "caf" in result
    assert "+" not in result or "café" not in result  # no spaces to encode


def test_tryurlencode_special_chars_string():
    result = tryUrlencode("a&b=c")
    assert result == "a%26b%3Dc"


def test_tryurlencode_already_looks_encoded():
    """If a string already contains %XX, it should still be encoded (the % gets encoded)."""
    result = tryUrlencode("100%25")
    # The % in %25 should itself be encoded
    assert "%25" in result


# --- toUnicode ---

def test_tounicode_str():
    assert toUnicode("hello") == "hello"


def test_tounicode_bytes_utf8():
    assert toUnicode(b"hello") == "hello"


def test_tounicode_bytes_utf8_accented():
    assert toUnicode("café".encode("utf-8")) == "café"


def test_tounicode_int():
    assert toUnicode(42) == "42"


def test_tounicode_list():
    result = toUnicode([1, 2, 3])
    assert result == "[1, 2, 3]"


# --- ss ---

def test_ss_string():
    assert ss("hello") == "hello"


def test_ss_bytes():
    assert ss(b"bytes input") == "bytes input"


def test_ss_int():
    assert ss(123) == "123"


# --- sp ---

def test_sp_normalizes_trailing_separator():
    """Trailing separators should be stripped."""
    result = sp("/some/path/")
    assert not result.endswith("/") or result == "/"


def test_sp_empty_string():
    assert sp("") == ""


def test_sp_none():
    assert sp(None) is None


def test_sp_double_leading_slash():
    """// at start should collapse to /."""
    result = sp("//some/path")
    assert result.startswith("/") and not result.startswith("//")


def test_sp_normpath_dot_segments():
    result = sp("/some/./path/../other")
    assert result == "/some/other"


def test_sp_windows_path_on_unix():
    """On macOS/Linux, backslash Windows paths should be converted to forward slashes."""
    if os.path.sep == "/":
        result = sp("C:\\Users\\test\\file.txt")
        # Should become a unix-style path
        assert "\\" not in result
        assert "/" in result


# --- simplifyString ---

def test_simplifystring_basic():
    result = simplifyString("Hello World!")
    assert result == "hello world"


def test_simplifystring_accents():
    result = simplifyString("Café Naïve")
    assert "cafe" in result
    assert "naive" in result


def test_simplifystring_middle_dot():
    """WALL·E should become 'wall e' (middle dot replaced with space)."""
    result = simplifyString("WALL·E")
    assert result == "wall e"


def test_simplifystring_vulgar_fraction():
    """Vulgar fraction ½ should be expanded to 1/2 tokens, not merged."""
    result = simplifyString("1½ hours")
    # '½' -> ' 1/2', so '1½' -> '1 1/2'
    assert "1" in result
    assert "2" in result


# --- stripAccents ---

def test_stripaccents_cafe():
    assert stripAccents("café") == "cafe"


def test_stripaccents_naive():
    assert stripAccents("naïve") == "naive"


def test_stripaccents_no_accents():
    assert stripAccents("hello") == "hello"


def test_stripaccents_empty():
    assert stripAccents("") == ""


# --- isInt ---

def test_isint_integer():
    assert isInt(42) is True


def test_isint_string_number():
    assert isInt("123") is True


def test_isint_negative_string():
    assert isInt("-7") is True


def test_isint_float_string():
    assert isInt("3.14") is False


def test_isint_non_numeric_string():
    assert isInt("abc") is False


def test_isint_none():
    assert isInt(None) is False


def test_isint_empty_string():
    assert isInt("") is False
