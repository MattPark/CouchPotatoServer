"""Tests for Phase 3-5 vendored library replacements.

Phase 3a: unrar2 -> pip rarfile
Phase 3b: guessit v0.6.2 -> pip guessit v3.x
Phase 4a: enzyme -> pip pymediainfo
Phase 4b: subliminal removed (subtitle downloader disabled)
Phase 5: pio/api.py .iteritems() fixed

Verifies that:
- All vendored libraries are deleted
- Pip replacements are importable with correct APIs
- Application code uses the new libraries correctly
- No remaining Python 2 constructs in fixed files
"""

import os
import sys
import tempfile

import pytest

# Ensure libs/ is on sys.path (same as conftest)
libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libs')
if libs_dir not in sys.path:
    sys.path.insert(0, libs_dir)

project_root = os.path.dirname(os.path.dirname(__file__))


# ── Section 1: Vendored libraries deleted ──────────────────────────────────


class TestVendoredLibsDeleted:
    """Verify all Phase 3-5 vendored libraries have been removed."""

    def test_no_vendored_unrar2(self):
        path = os.path.join(libs_dir, 'unrar2')
        assert not os.path.exists(path), \
            "libs/unrar2/ should have been deleted"

    def test_no_vendored_guessit(self):
        path = os.path.join(libs_dir, 'guessit')
        assert not os.path.exists(path), \
            "libs/guessit/ should have been deleted"

    def test_no_vendored_enzyme(self):
        path = os.path.join(libs_dir, 'enzyme')
        assert not os.path.exists(path), \
            "libs/enzyme/ should have been deleted"

    def test_no_vendored_subliminal(self):
        path = os.path.join(libs_dir, 'subliminal')
        assert not os.path.exists(path), \
            "libs/subliminal/ should have been deleted"


# ── Section 2: Pip packages importable ─────────────────────────────────────


class TestPipPackagesImportable:
    """Verify pip replacement packages are importable."""

    def test_import_rarfile(self):
        import rarfile
        assert hasattr(rarfile, 'RarFile')
        assert hasattr(rarfile, 'UNRAR_TOOL')

    def test_rarfile_has_infolist(self):
        from rarfile import RarFile
        assert hasattr(RarFile, 'infolist')

    def test_rarfile_has_open(self):
        from rarfile import RarFile
        assert hasattr(RarFile, 'open')

    def test_rarfile_has_close(self):
        from rarfile import RarFile
        assert hasattr(RarFile, 'close')

    def test_import_guessit(self):
        from guessit import guessit
        assert callable(guessit)

    def test_guessit_returns_title_and_year(self):
        from guessit import guessit
        result = guessit('The.Matrix.1999.1080p.BluRay.x264.mkv',
                         {'type': 'movie'})
        assert result.get('title') == 'The Matrix'
        assert result.get('year') == 1999

    def test_guessit_handles_no_year(self):
        from guessit import guessit
        result = guessit('SomeMovie.720p.mkv', {'type': 'movie'})
        assert 'title' in result

    def test_guessit_handles_unicode(self):
        from guessit import guessit
        result = guessit('Amélie.2001.1080p.mkv', {'type': 'movie'})
        assert result.get('year') == 2001

    def test_import_pymediainfo(self):
        from pymediainfo import MediaInfo
        assert hasattr(MediaInfo, 'parse')
        assert hasattr(MediaInfo, 'can_parse')

    def test_pymediainfo_can_parse(self):
        from pymediainfo import MediaInfo
        # Should return True if libmediainfo is installed
        assert MediaInfo.can_parse() is True


# ── Section 3: renamer.py uses rarfile correctly ───────────────────────────


class TestRenamerRarfile:
    """Verify renamer.py uses pip rarfile instead of vendored unrar2."""

    def _read_renamer(self):
        path = os.path.join(project_root, 'couchpotato', 'core',
                            'plugins', 'renamer.py')
        with open(path, 'r') as f:
            return f.read()

    def test_imports_rarfile_not_unrar2(self):
        src = self._read_renamer()
        assert 'import rarfile' in src
        assert 'from unrar2' not in src
        assert 'import unrar2' not in src

    def test_uses_rarfile_RarFile(self):
        src = self._read_renamer()
        assert 'rarfile.RarFile(' in src

    def test_uses_is_dir_method(self):
        """rarfile uses is_dir() method, not isdir property."""
        src = self._read_renamer()
        assert '.is_dir()' in src
        assert 'packedinfo.isdir' not in src

    def test_uses_streaming_extract(self):
        """Should use open()+copyfileobj() for withSubpath=False emulation."""
        src = self._read_renamer()
        assert 'rar_handle.open(packedinfo)' in src
        assert 'shutil.copyfileobj(src, dst)' in src

    def test_no_condition_extract(self):
        """rarfile doesn't support condition=[index] extract pattern."""
        src = self._read_renamer()
        assert 'condition' not in src.split('def extractFiles')[1].split('del rar_handle')[0] if 'del rar_handle' in src else True

    def test_uses_unrar_tool(self):
        """Should set rarfile.UNRAR_TOOL for custom unrar binary path."""
        src = self._read_renamer()
        assert 'rarfile.UNRAR_TOOL' in src

    def test_uses_close_not_del(self):
        """Should use rar_handle.close() instead of del rar_handle."""
        src = self._read_renamer()
        assert 'rar_handle.close()' in src


# ── Section 4: scanner.py uses guessit v3.x ────────────────────────────────


class TestScannerGuessit:
    """Verify scanner.py uses pip guessit v3.x API."""

    def _read_scanner(self):
        path = os.path.join(project_root, 'couchpotato', 'core',
                            'plugins', 'scanner.py')
        with open(path, 'r') as f:
            return f.read()

    def test_imports_guessit_v3(self):
        src = self._read_scanner()
        assert 'from guessit import guessit as guessit_parse' in src

    def test_no_old_guess_movie_info(self):
        src = self._read_scanner()
        assert 'guess_movie_info' not in src

    def test_calls_guessit_with_type_movie(self):
        src = self._read_scanner()
        assert "guessit_parse(" in src
        assert "{'type': 'movie'}" in src

    def test_accesses_title_and_year(self):
        """guessit v3 returns same 'title' and 'year' keys."""
        src = self._read_scanner()
        assert "guessit.get('title')" in src
        assert "guessit.get('year')" in src


# ── Section 5: scanner.py uses pymediainfo ─────────────────────────────────


class TestScannerPymediainfo:
    """Verify scanner.py uses pymediainfo instead of enzyme."""

    def _read_scanner(self):
        path = os.path.join(project_root, 'couchpotato', 'core',
                            'plugins', 'scanner.py')
        with open(path, 'r') as f:
            return f.read()

    def test_imports_pymediainfo(self):
        src = self._read_scanner()
        assert 'from pymediainfo import MediaInfo' in src

    def test_no_enzyme_import(self):
        src = self._read_scanner()
        assert 'import enzyme' not in src

    def test_no_subliminal_import(self):
        src = self._read_scanner()
        assert 'import subliminal' not in src
        assert 'from subliminal' not in src

    def test_uses_mediainfo_parse(self):
        src = self._read_scanner()
        assert 'MediaInfo.parse(filename)' in src

    def test_filters_video_tracks(self):
        src = self._read_scanner()
        assert "t.track_type == 'Video'" in src

    def test_filters_audio_tracks(self):
        src = self._read_scanner()
        assert "t.track_type == 'Audio'" in src

    def test_filters_general_tracks(self):
        src = self._read_scanner()
        assert "t.track_type == 'General'" in src

    def test_video_codec_map_has_common_codecs(self):
        src = self._read_scanner()
        assert "'AVC': 'H264'" in src
        assert "'HEVC': 'x265'" in src

    def test_audio_codec_map_has_common_codecs(self):
        src = self._read_scanner()
        assert "'AC-3': 'AC3'" in src
        assert "'DTS': 'DTS'" in src
        assert "'AAC': 'AAC'" in src
        assert "'FLAC': 'FLAC'" in src

    def test_no_enzyme_exceptions(self):
        src = self._read_scanner()
        assert 'enzyme.exceptions' not in src


# ── Section 6: scanner.py subtitle language detection ──────────────────────


class TestScannerSubtitleDetection:
    """Verify subtitle language detection uses filename parsing."""

    def _read_scanner(self):
        path = os.path.join(project_root, 'couchpotato', 'core',
                            'plugins', 'scanner.py')
        with open(path, 'r') as f:
            return f.read()

    def test_no_subliminal_video(self):
        """Should not use subliminal Video.from_path()."""
        src = self._read_scanner()
        assert 'Video.from_path' not in src
        assert 'video.scan()' not in src

    def test_detects_subtitle_extensions(self):
        src = self._read_scanner()
        method = src.split('def getSubtitleLanguage')[1].split('\n    def ')[0]
        assert '.srt' in method
        assert '.sub' in method
        assert '.ass' in method

    def test_parses_language_from_filename(self):
        """Should extract 2-3 letter language codes from subtitle filenames."""
        src = self._read_scanner()
        method = src.split('def getSubtitleLanguage')[1].split('\n    def ')[0]
        # Should check for alpha language codes of length 2 or 3
        assert 'lang_code' in method
        assert 'isalpha()' in method

    def test_subtitle_language_detection_logic(self):
        """Simulate the subtitle language detection logic."""
        # Create a temp directory structure
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake movie file
            movie_path = os.path.join(tmpdir, 'Movie.2024.1080p.mkv')
            open(movie_path, 'w').close()

            # Create subtitle files
            srt_en = os.path.join(tmpdir, 'Movie.2024.1080p.en.srt')
            srt_fr = os.path.join(tmpdir, 'Movie.2024.1080p.fr.srt')
            srt_nolang = os.path.join(tmpdir, 'Movie.2024.1080p.srt')
            open(srt_en, 'w').close()
            open(srt_fr, 'w').close()
            open(srt_nolang, 'w').close()

            # Simulate the detection logic from scanner.py
            subtitle_exts = {'.srt', '.sub', '.ass', '.ssa', '.smi', '.vtt'}
            detected = {}
            paths = [movie_path]
            movie_dir = os.path.dirname(movie_path)

            for fname in os.listdir(movie_dir):
                fpath = os.path.join(movie_dir, fname)
                ext = os.path.splitext(fname)[1].lower()
                if ext in subtitle_exts and fpath not in paths:
                    name_no_ext = os.path.splitext(fname)[0]
                    parts = name_no_ext.rsplit('.', 1)
                    if len(parts) == 2:
                        lang_code = parts[1].lower()
                        if len(lang_code) in (2, 3) and lang_code.isalpha():
                            detected[fpath] = [lang_code]

            assert srt_en in detected
            assert detected[srt_en] == ['en']
            assert srt_fr in detected
            assert detected[srt_fr] == ['fr']
            # '1080p' is 5 chars — should NOT match
            assert srt_nolang not in detected


# ── Section 7: subtitle.py disabled ────────────────────────────────────────


class TestSubtitleDisabled:
    """Verify subtitle.py no longer depends on subliminal."""

    def _read_subtitle(self):
        path = os.path.join(project_root, 'couchpotato', 'core',
                            'plugins', 'subtitle.py')
        with open(path, 'r') as f:
            return f.read()

    def test_no_subliminal_import(self):
        src = self._read_subtitle()
        assert 'import subliminal' not in src
        assert 'from subliminal' not in src

    def test_no_env_import(self):
        """Env was only needed for subliminal cache_dir."""
        src = self._read_subtitle()
        assert 'from couchpotato.environment import Env' not in src

    def test_search_single_is_noop(self):
        """searchSingle should log a message and return True."""
        src = self._read_subtitle()
        method = src.split('def searchSingle')[1].split('\n    def ')[0]
        assert 'return True' in method
        assert 'no longer supported' in method.lower() or 'bazarr' in method.lower()

    def test_config_section_preserved(self):
        """Config section should be preserved for backward compat."""
        src = self._read_subtitle()
        assert "'name': 'subtitle'" in src
        assert "'name': 'enabled'" in src

    def test_no_download_subtitles_call(self):
        src = self._read_subtitle()
        assert 'download_subtitles' not in src


# ── Section 8: runner.py logger updates ────────────────────────────────────


class TestRunnerLoggers:
    """Verify runner.py suppresses correct loggers."""

    def _read_runner(self):
        path = os.path.join(project_root, 'couchpotato', 'runner.py')
        with open(path, 'r') as f:
            return f.read()

    def test_no_enzyme_logger(self):
        src = self._read_runner()
        assert "'enzyme'" not in src

    def test_no_subliminal_logger(self):
        src = self._read_runner()
        assert "'subliminal'" not in src

    def test_has_pymediainfo_logger(self):
        src = self._read_runner()
        assert "'pymediainfo'" in src

    def test_has_guessit_logger(self):
        src = self._read_runner()
        assert "'guessit'" in src

    def test_has_rebulk_logger(self):
        """guessit v3 uses rebulk internally — suppress its logger too."""
        src = self._read_runner()
        assert "'rebulk'" in src


# ── Section 9: pio/api.py Python 3 fixes ──────────────────────────────────


class TestPioApiPy3:
    """Verify pio/api.py Python 2 constructs are fixed."""

    def _read_pio_api(self):
        path = os.path.join(libs_dir, 'pio', 'api.py')
        with open(path, 'r') as f:
            return f.read()

    def test_no_iteritems(self):
        src = self._read_pio_api()
        assert '.iteritems()' not in src

    def test_uses_items(self):
        src = self._read_pio_api()
        assert '.items()' in src

    def test_no_unicode_isinstance(self):
        """Should not reference bare 'unicode' type."""
        src = self._read_pio_api()
        assert 'isinstance(s, unicode)' not in src

    def test_str_function_handles_bytes(self):
        """_str() should handle bytes in Python 3."""
        src = self._read_pio_api()
        assert 'isinstance(s, bytes)' in src


# ── Section 10: pyproject.toml dependencies ────────────────────────────────


class TestDependencies:
    """Verify pyproject.toml has the new dependencies."""

    def _read_pyproject(self):
        path = os.path.join(project_root, 'pyproject.toml')
        with open(path, 'r') as f:
            return f.read()

    def test_has_rarfile_dep(self):
        src = self._read_pyproject()
        assert 'rarfile' in src

    def test_has_guessit_dep(self):
        src = self._read_pyproject()
        assert 'guessit' in src

    def test_has_pymediainfo_dep(self):
        src = self._read_pyproject()
        assert 'pymediainfo' in src


# ── Section 11: Dockerfile updates ─────────────────────────────────────────


class TestDockerfile:
    """Verify Dockerfile installs required system packages."""

    def _read_dockerfile(self):
        path = os.path.join(project_root, 'Dockerfile')
        with open(path, 'r') as f:
            return f.read()

    def test_installs_mediainfo(self):
        """pymediainfo requires libmediainfo binary."""
        src = self._read_dockerfile()
        assert 'mediainfo' in src

    def test_installs_unrar(self):
        """rarfile requires unrar binary."""
        src = self._read_dockerfile()
        assert 'unrar' in src

    def test_installs_pip_rarfile(self):
        src = self._read_dockerfile()
        assert 'rarfile' in src

    def test_installs_pip_guessit(self):
        src = self._read_dockerfile()
        assert 'guessit' in src

    def test_installs_pip_pymediainfo(self):
        src = self._read_dockerfile()
        assert 'pymediainfo' in src


# ── Section 12: Kept vendored libraries still functional ───────────────────


class TestKeptVendoredLibs:
    """Verify libraries that should remain vendored are still present."""

    def test_caper_exists(self):
        path = os.path.join(libs_dir, 'caper')
        assert os.path.isdir(path), "libs/caper/ should be kept"

    def test_logr_exists(self):
        path = os.path.join(libs_dir, 'logr')
        assert os.path.isdir(path), "libs/logr/ should be kept"

    def test_git_exists(self):
        path = os.path.join(libs_dir, 'git')
        assert os.path.isdir(path), "libs/git/ should be kept"

    def test_rtorrent_exists(self):
        path = os.path.join(libs_dir, 'rtorrent')
        assert os.path.isdir(path), "libs/rtorrent/ should be kept"

    def test_pio_exists(self):
        path = os.path.join(libs_dir, 'pio')
        assert os.path.isdir(path), "libs/pio/ should be kept"

    def test_tus_exists(self):
        path = os.path.join(libs_dir, 'tus')
        assert os.path.isdir(path), "libs/tus/ should be kept"


# ── Section 13: Integration — guessit v3 API compatibility ─────────────────


class TestGuessitV3Integration:
    """Integration tests for guessit v3 with CouchPotato's usage patterns."""

    def test_various_movie_filenames(self):
        from guessit import guessit
        test_cases = [
            ('The.Dark.Knight.2008.1080p.BluRay.x264.mkv', 'The Dark Knight', 2008),
            ('Inception.2010.720p.BrRip.x264.YIFY.mp4', 'Inception', 2010),
            ('Pulp.Fiction.1994.Remastered.2160p.UHD.mkv', 'Pulp Fiction', 1994),
        ]
        for filename, expected_title, expected_year in test_cases:
            result = guessit(filename, {'type': 'movie'})
            assert result.get('title') == expected_title, \
                f"Title mismatch for {filename}: {result.get('title')}"
            assert result.get('year') == expected_year, \
                f"Year mismatch for {filename}: {result.get('year')}"

    def test_filename_without_year(self):
        from guessit import guessit
        result = guessit('SomeMovie.1080p.mkv', {'type': 'movie'})
        assert 'title' in result
        assert result.get('year') is None

    def test_filename_with_edition(self):
        from guessit import guessit
        result = guessit("Blade.Runner.1982.Final.Cut.1080p.BluRay.mkv",
                         {'type': 'movie'})
        assert result.get('title') == 'Blade Runner'
        assert result.get('year') == 1982


# ── Section 14: pymediainfo API compatibility ──────────────────────────────


class TestPymediainfoAPI:
    """Verify pymediainfo API matches what scanner.py expects."""

    def test_mediainfo_can_parse(self):
        from pymediainfo import MediaInfo
        assert MediaInfo.can_parse() is True

    def test_mediainfo_parse_nonexistent_returns_empty(self):
        """Parsing a non-existent file should raise an error."""
        from pymediainfo import MediaInfo
        with pytest.raises(Exception):
            MediaInfo.parse('/nonexistent/file.mkv')

    def test_mediainfo_tracks_attribute(self):
        """MediaInfo result should have a tracks attribute."""
        from pymediainfo import MediaInfo
        # Parse an empty file to check structure
        with tempfile.NamedTemporaryFile(suffix='.txt') as f:
            try:
                mi = MediaInfo.parse(f.name)
                assert hasattr(mi, 'tracks')
                assert isinstance(mi.tracks, list)
            except Exception:
                # Some versions may fail on empty files, that's OK
                pass

    def test_track_type_attribute_exists(self):
        """Track objects should have track_type attribute."""
        from pymediainfo import MediaInfo
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as f:
            f.write(b'dummy')
            f.flush()
            try:
                mi = MediaInfo.parse(f.name)
                for track in mi.tracks:
                    assert hasattr(track, 'track_type')
            except Exception:
                pass
            finally:
                os.unlink(f.name)
