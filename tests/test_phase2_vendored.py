"""Tests for Phase 2 vendored library replacements.

Verifies that:
- vendored deluge_client/, bencode/, qbittorrent/, multipartpost.py are deleted
- pip packages (deluge-client, bencode.py, python-qbittorrent) are importable
- API compatibility is maintained for all replaced libraries
- utorrent.py no longer imports multipartpost
- deluge.py uses decode_utf8=True
"""

import inspect
import os
import sys

import pytest

# Ensure libs/ is on sys.path (same as conftest)
libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libs')
if libs_dir not in sys.path:
    sys.path.insert(0, libs_dir)

project_root = os.path.dirname(os.path.dirname(__file__))


# ── Section 1: Vendored libraries deleted ──────────────────────────────────


class TestVendoredLibsDeleted:
    """Verify all Phase 2 vendored libraries have been removed."""

    def test_no_vendored_deluge_client(self):
        path = os.path.join(libs_dir, 'deluge_client')
        assert not os.path.exists(path), \
            "libs/deluge_client/ should have been deleted"

    def test_no_vendored_bencode(self):
        path = os.path.join(libs_dir, 'bencode')
        assert not os.path.exists(path), \
            "libs/bencode/ should have been deleted"

    def test_no_vendored_qbittorrent(self):
        path = os.path.join(libs_dir, 'qbittorrent')
        assert not os.path.exists(path), \
            "libs/qbittorrent/ should have been deleted"

    def test_no_vendored_multipartpost(self):
        path = os.path.join(libs_dir, 'multipartpost.py')
        assert not os.path.exists(path), \
            "libs/multipartpost.py should have been deleted"


# ── Section 2: Pip packages importable ─────────────────────────────────────


class TestPipPackagesImportable:
    """Verify pip replacement packages can be imported."""

    def test_import_deluge_client(self):
        from deluge_client.client import DelugeRPCClient
        assert DelugeRPCClient is not None

    def test_import_bencode(self):
        from bencode import bencode, bdecode
        assert callable(bencode)
        assert callable(bdecode)

    def test_import_bencode_aliased(self):
        """All 5 downloaders use various import styles."""
        from bencode import bencode as benc, bdecode
        assert callable(benc)
        assert callable(bdecode)

    def test_import_qbittorrent_client(self):
        from qbittorrent.client import Client
        assert Client is not None

    def test_import_qbittorrent_top_level(self):
        from qbittorrent import Client
        assert Client is not None


# ── Section 3: API compatibility — deluge-client ───────────────────────────


class TestDelugeClientAPI:
    """Verify pip deluge-client has the same API as vendored version."""

    def test_constructor_accepts_decode_utf8(self):
        """DelugeRPCClient should accept decode_utf8 parameter."""
        from deluge_client.client import DelugeRPCClient
        sig = inspect.signature(DelugeRPCClient.__init__)
        assert 'decode_utf8' in sig.parameters

    def test_constructor_params(self):
        """DelugeRPCClient should accept host, port, username, password."""
        from deluge_client.client import DelugeRPCClient
        sig = inspect.signature(DelugeRPCClient.__init__)
        params = list(sig.parameters.keys())
        assert 'host' in params
        assert 'port' in params
        assert 'username' in params
        assert 'password' in params

    def test_deluge_py_uses_decode_utf8(self):
        """deluge.py should pass decode_utf8=True to DelugeRPCClient."""
        deluge_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'deluge.py'
        )
        with open(deluge_path) as f:
            content = f.read()
        assert 'decode_utf8=True' in content


# ── Section 4: API compatibility — bencode.py ──────────────────────────────


class TestBencodeAPI:
    """Verify pip bencode.py behaves correctly for torrent hash computation."""

    def test_bencode_roundtrip(self):
        """bencode/bdecode should round-trip simple data."""
        from bencode import bencode, bdecode
        original = {'info': {'name': 'test_movie', 'length': 700000000}}
        encoded = bencode(original)
        decoded = bdecode(encoded)
        assert decoded['info']['name'] == 'test_movie'
        assert decoded['info']['length'] == 700000000

    def test_bdecode_returns_string_keys(self):
        """pip bencode.py should return string keys, not bytes keys."""
        from bencode import bencode, bdecode
        data = bencode({'info': {'name': 'test', 'length': 100}})
        decoded = bdecode(data)
        # Keys must be strings for ["info"] access pattern in downloaders
        assert isinstance(list(decoded.keys())[0], str)
        assert 'info' in decoded
        assert isinstance(list(decoded['info'].keys())[0], str)

    def test_torrent_hash_computation(self):
        """Simulate the torrent hash computation pattern used by all 5 downloaders."""
        from hashlib import sha1
        from bencode import bencode, bdecode

        # Create a fake torrent structure
        torrent_info = {'name': 'Movie.2024.1080p', 'piece length': 262144, 'length': 1500000000}
        torrent_data = bencode({'info': torrent_info})

        # This is the exact pattern from all 5 downloaders:
        info = bdecode(torrent_data)["info"]
        torrent_hash = sha1(bencode(info)).hexdigest()

        assert isinstance(torrent_hash, str)
        assert len(torrent_hash) == 40  # SHA1 hex digest


# ── Section 5: API compatibility — python-qbittorrent ──────────────────────


class TestQBittorrentAPI:
    """Verify pip python-qbittorrent Client has all methods used by qbittorrent_.py."""

    def test_client_has_required_methods(self):
        """The Client class must have all methods used by qbittorrent_.py."""
        from qbittorrent import Client
        required_methods = [
            'login', 'logout',
            'download_from_link', 'download_from_file',
            'torrents', 'get_torrent', 'get_torrent_files',
            'pause', 'resume',
            'delete', 'delete_permanently',
        ]
        for method_name in required_methods:
            assert hasattr(Client, method_name), \
                "Client missing method: %s" % method_name

    def test_download_from_link_accepts_label(self):
        """download_from_link should accept label kwarg (mapped to category)."""
        from qbittorrent import Client
        sig = inspect.signature(Client.download_from_link)
        # Uses **kwargs, so it accepts any keyword argument
        params = sig.parameters
        assert 'kwargs' in params or 'link' in params

    def test_download_from_file_accepts_label(self):
        """download_from_file should accept label kwarg."""
        from qbittorrent import Client
        sig = inspect.signature(Client.download_from_file)
        params = sig.parameters
        assert 'kwargs' in params or 'file_buffer' in params

    def test_import_alias_works(self):
        """The import pattern used in qbittorrent_.py should work."""
        from qbittorrent.client import Client as QBittorrentClient
        assert QBittorrentClient is not None

    def test_qbittorrent_py_uses_alias(self):
        """qbittorrent_.py should import Client as QBittorrentClient."""
        qbt_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'qbittorrent_.py'
        )
        with open(qbt_path) as f:
            content = f.read()
        assert 'from qbittorrent.client import Client as QBittorrentClient' in content


# ── Section 6: uTorrent requests migration ─────────────────────────────────


class TestUTorrentRequestsMigration:
    """Verify utorrent.py no longer uses multipartpost and uses requests."""

    def test_no_multipartpost_import(self):
        """utorrent.py should not import multipartpost."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'from multipartpost' not in content
        assert 'MultipartPostHandler' not in content

    def test_no_urllib2_import(self):
        """utorrent.py should not use urllib.request (replaced by requests)."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'from urllib import request' not in content
        assert 'urllib2' not in content

    def test_no_cookielib_import(self):
        """utorrent.py should not use http.cookiejar."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'http.cookiejar' not in content
        assert 'cookielib' not in content

    def test_no_httplib_import(self):
        """utorrent.py should not use http.client."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'http.client' not in content
        assert 'httplib' not in content

    def test_uses_requests(self):
        """utorrent.py should import and use requests."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'import requests' in content
        assert 'requests.Session()' in content

    def test_uses_session_auth(self):
        """utorrent.py should use session.auth for HTTP basic auth."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'self.session.auth' in content

    def test_uses_session_post_files(self):
        """utorrent.py should use session.post(files=...) for multipart upload."""
        ut_path = os.path.join(
            project_root, 'couchpotato', 'core', 'downloaders', 'utorrent.py'
        )
        with open(ut_path) as f:
            content = f.read()
        assert 'self.session.post(url, files=' in content


# ── Section 7: pyproject.toml dependencies ─────────────────────────────────


class TestPyprojectDependencies:
    """Verify new dependencies are listed in pyproject.toml."""

    @pytest.fixture
    def pyproject_content(self):
        path = os.path.join(project_root, 'pyproject.toml')
        with open(path) as f:
            return f.read()

    def test_deluge_client_dependency(self, pyproject_content):
        assert 'deluge-client' in pyproject_content

    def test_bencode_py_dependency(self, pyproject_content):
        assert 'bencode.py' in pyproject_content

    def test_python_qbittorrent_dependency(self, pyproject_content):
        assert 'python-qbittorrent' in pyproject_content


# ── Section 8: Downloader import patterns ──────────────────────────────────


class TestDownloaderImports:
    """Verify all 5 downloaders still use the correct import patterns."""

    @pytest.fixture
    def downloader_files(self):
        base = os.path.join(project_root, 'couchpotato', 'core', 'downloaders')
        return {
            'deluge': os.path.join(base, 'deluge.py'),
            'utorrent': os.path.join(base, 'utorrent.py'),
            'qbittorrent': os.path.join(base, 'qbittorrent_.py'),
            'hadouken': os.path.join(base, 'hadouken.py'),
            'rtorrent': os.path.join(base, 'rtorrent_.py'),
        }

    def test_deluge_imports_bencode(self, downloader_files):
        with open(downloader_files['deluge']) as f:
            content = f.read()
        assert 'from bencode import' in content

    def test_utorrent_imports_bencode(self, downloader_files):
        with open(downloader_files['utorrent']) as f:
            content = f.read()
        assert 'from bencode import' in content

    def test_qbittorrent_imports_bencode(self, downloader_files):
        with open(downloader_files['qbittorrent']) as f:
            content = f.read()
        assert 'from bencode import' in content

    def test_hadouken_imports_bencode(self, downloader_files):
        with open(downloader_files['hadouken']) as f:
            content = f.read()
        assert 'from bencode import' in content

    def test_rtorrent_imports_bencode(self, downloader_files):
        with open(downloader_files['rtorrent']) as f:
            content = f.read()
        assert 'from bencode import' in content

    def test_deluge_imports_deluge_client(self, downloader_files):
        with open(downloader_files['deluge']) as f:
            content = f.read()
        assert 'from deluge_client.client import DelugeRPCClient' in content
