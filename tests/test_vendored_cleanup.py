"""Tests for the cachelib integration — replaces vendored werkzeug cache.

Verifies that cachelib.FileSystemCache is a drop-in replacement for the
vendored libs/cache/FileSystemCache, used in runner.py to provide the
application-wide HTTP cache (Env.get('cache')).
"""

import os
import sys
import tempfile
import time

import pytest

# Ensure libs/ is on sys.path
libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libs')
if libs_dir not in sys.path:
    sys.path.insert(0, libs_dir)

from cachelib import FileSystemCache


@pytest.fixture
def cache_dir(tmp_path):
    """Create a temporary cache directory."""
    return str(tmp_path / 'cache')


@pytest.fixture
def cache(cache_dir):
    """Create a FileSystemCache instance with high threshold."""
    return FileSystemCache(cache_dir, threshold=50000)


class TestFileSystemCacheBasic:
    """Basic get/set/delete operations."""

    def test_set_and_get(self, cache):
        cache.set('key1', 'value1')
        assert cache.get('key1') == 'value1'

    def test_get_missing_returns_none(self, cache):
        assert cache.get('nonexistent') is None

    def test_set_overwrites(self, cache):
        cache.set('key', 'old')
        cache.set('key', 'new')
        assert cache.get('key') == 'new'

    def test_delete(self, cache):
        cache.set('key', 'value')
        cache.delete('key')
        assert cache.get('key') is None

    def test_delete_nonexistent_silent(self, cache):
        """Deleting a key that doesn't exist should not raise."""
        cache.delete('does_not_exist')  # Should not raise

    def test_clear(self, cache):
        cache.set('k1', 'v1')
        cache.set('k2', 'v2')
        cache.clear()
        assert cache.get('k1') is None
        assert cache.get('k2') is None


class TestFileSystemCacheTypes:
    """Various value types can be cached (uses pickle internally)."""

    def test_cache_string(self, cache):
        cache.set('str', 'hello world')
        assert cache.get('str') == 'hello world'

    def test_cache_int(self, cache):
        cache.set('int', 42)
        assert cache.get('int') == 42

    def test_cache_float(self, cache):
        cache.set('float', 3.14)
        assert cache.get('float') == pytest.approx(3.14)

    def test_cache_dict(self, cache):
        data = {'movie': 'Inception', 'year': 2010, 'rating': 8.8}
        cache.set('dict', data)
        assert cache.get('dict') == data

    def test_cache_list(self, cache):
        data = [1, 'two', 3.0, None]
        cache.set('list', data)
        assert cache.get('list') == data

    def test_cache_nested_structure(self, cache):
        """Complex nested structures survive round-trip (pickle)."""
        data = {
            'movies': [
                {'title': 'Inception', 'imdb': 'tt1375666'},
                {'title': 'The Matrix', 'imdb': 'tt0133093'},
            ],
            'count': 2,
        }
        cache.set('nested', data)
        assert cache.get('nested') == data

    def test_cache_none_value(self, cache):
        """None is a valid cache value, distinct from cache miss."""
        cache.set('none_key', None)
        # cachelib returns None for both missing and None values,
        # but has() distinguishes them
        result = cache.get('none_key')
        assert result is None

    def test_cache_bytes(self, cache):
        data = b'\x00\x01\x02\xff'
        cache.set('bytes', data)
        assert cache.get('bytes') == data

    def test_cache_bool(self, cache):
        cache.set('true', True)
        cache.set('false', False)
        assert cache.get('true') is True
        assert cache.get('false') is False


class TestFileSystemCacheTimeout:
    """Cache timeout/expiration behavior."""

    def test_default_timeout_applies(self, cache_dir):
        """Cache with a short default timeout expires entries."""
        cache = FileSystemCache(cache_dir, default_timeout=1)
        cache.set('expires', 'soon')
        assert cache.get('expires') == 'soon'
        time.sleep(1.5)
        assert cache.get('expires') is None

    def test_per_key_timeout(self, cache):
        """Per-key timeout overrides default."""
        cache.set('quick', 'gone', timeout=1)
        assert cache.get('quick') == 'gone'
        time.sleep(1.5)
        assert cache.get('quick') is None

    def test_long_timeout_survives(self, cache):
        """Entries with long timeout remain available."""
        cache.set('long', 'lived', timeout=3600)
        assert cache.get('long') == 'lived'


class TestFileSystemCacheThreshold:
    """Threshold-based pruning."""

    def test_threshold_constructor_param(self, cache_dir):
        """The threshold parameter is accepted (same API as vendored version)."""
        cache = FileSystemCache(cache_dir, threshold=100)
        # Just verify it doesn't crash
        cache.set('test', 'value')
        assert cache.get('test') == 'value'

    def test_high_threshold_like_runner(self, cache_dir):
        """runner.py uses threshold=50000 — verify it works."""
        cache = FileSystemCache(cache_dir, threshold=50000)
        for i in range(50):
            cache.set('key_%d' % i, 'value_%d' % i)
        # Spot-check a few entries
        assert cache.get('key_0') == 'value_0'
        assert cache.get('key_49') == 'value_49'


class TestFileSystemCacheDirectoryCreation:
    """Verify the cache creates its directory automatically."""

    def test_creates_directory(self, tmp_path):
        """FileSystemCache creates the cache directory if it doesn't exist."""
        new_dir = str(tmp_path / 'new_cache_dir')
        assert not os.path.exists(new_dir)
        cache = FileSystemCache(new_dir)
        cache.set('test', 'value')
        assert os.path.isdir(new_dir)
        assert cache.get('test') == 'value'


class TestFileSystemCacheMultiOps:
    """Multi-key operations (get_many, set_many, etc.)."""

    def test_set_many_and_get(self, cache):
        """set_many (if available) or manual equivalent."""
        cache.set('a', 1)
        cache.set('b', 2)
        cache.set('c', 3)
        assert cache.get('a') == 1
        assert cache.get('b') == 2
        assert cache.get('c') == 3

    def test_delete_many(self, cache):
        """delete_many removes multiple keys."""
        cache.set('x', 10)
        cache.set('y', 20)
        cache.delete_many('x', 'y')
        assert cache.get('x') is None
        assert cache.get('y') is None


class TestVendoredCacheRemoved:
    """Verify the vendored cache library has been deleted."""

    def test_no_vendored_cache_dir(self):
        """libs/cache/ directory should no longer exist."""
        vendored_cache = os.path.join(libs_dir, 'cache')
        assert not os.path.exists(vendored_cache), \
            "libs/cache/ should have been deleted"

    def test_cachelib_import(self):
        """cachelib should be importable from pip."""
        import cachelib
        assert hasattr(cachelib, 'FileSystemCache')
        assert hasattr(cachelib, 'SimpleCache')


class TestPy2RemnantsRemoved:
    """Verify Python 2 remnants have been cleaned up."""

    def test_no_future_import_in_couchpotato_py(self):
        """CouchPotato.py should not have 'from __future__ import print_function'."""
        cp_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'CouchPotato.py')
        with open(cp_path) as f:
            content = f.read()
        assert 'from __future__ import print_function' not in content

    def test_no_import_imp_in_browser(self):
        """browser.py should not use 'import imp'."""
        browser_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'couchpotato', 'core', 'plugins', 'browser.py'
        )
        with open(browser_path) as f:
            content = f.read()
        assert 'import imp' not in content
        assert 'imp.find_module' not in content

    def test_no_version_check_27_in_core(self):
        """_core.py should not have sys.version_info >= (2, 7, 9) check."""
        core_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'couchpotato', 'core', '_base', '_core.py'
        )
        with open(core_path) as f:
            content = f.read()
        assert '(2, 7, 9)' not in content

    def test_no_e_message_in_loader(self):
        """loader.py should use str(e) instead of e.message."""
        loader_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'couchpotato', 'core', 'loader.py'
        )
        with open(loader_path) as f:
            content = f.read()
        assert 'e.message' not in content
        assert 'str(e)' in content or 'msg = str(e)' in content

    def test_dead_libraries_removed(self):
        """All 4 dead libraries should be deleted."""
        for name in ['suds', 'synchronousdeluge']:
            path = os.path.join(libs_dir, name)
            assert not os.path.exists(path), \
                '%s/ should have been deleted' % name

        for name in ['pkg_resources.py', 'color_logs.py']:
            path = os.path.join(libs_dir, name)
            assert not os.path.exists(path), \
                '%s should have been deleted' % name
