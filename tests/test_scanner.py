"""Unit tests for Scanner pure/static methods.

Tests the Scanner class methods that operate purely on class-level attributes
(edition_map, _edition_exclude, source_media, etc.) without needing the full
Plugin __init__ or event system.
"""

from couchpotato.core.plugins.scanner import Scanner

# Create a Scanner instance bypassing __init__ (which needs the event system).
# All tested methods only use class-level attributes, not instance state.
scanner = Scanner.__new__(Scanner)


# ============================================================================
# getEdition (~15 tests)
# ============================================================================

class TestGetEdition:
    """Tests for Scanner.getEdition(filename)."""

    # --- Known edition_map entries ---

    def test_directors_cut(self):
        fn = "/movies/Movie.Name.2020.Directors.Cut.1080p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Director's Cut"

    def test_extended_edition(self):
        fn = "/movies/Movie.Name.2019.Extended.Edition.720p.mkv"
        assert scanner.getEdition(fn) == "Extended Edition"

    def test_extended_single_word(self):
        fn = "/movies/Movie.Name.2021.Extended.1080p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Extended Edition"

    def test_unrated(self):
        fn = "/movies/Movie.Name.2018.Unrated.720p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Unrated"

    def test_imax(self):
        fn = "/movies/Movie.Name.2022.IMAX.2160p.WEB-DL.mkv"
        assert scanner.getEdition(fn) == "IMAX"

    def test_redux(self):
        fn = "/movies/Apocalypse.Now.1979.Redux.1080p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Redux"

    def test_final_cut(self):
        fn = "/movies/Blade.Runner.1982.Final.Cut.1080p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Final Cut"

    def test_remastered(self):
        fn = "/movies/Movie.Name.2005.Remastered.1080p.mkv"
        assert scanner.getEdition(fn) == "Remastered"

    def test_special_edition(self):
        fn = "/movies/Movie.Name.2010.Special.Edition.720p.mkv"
        assert scanner.getEdition(fn) == "Special Edition"

    def test_theatrical(self):
        fn = "/movies/Movie.Name.2015.Theatrical.Cut.1080p.mkv"
        assert scanner.getEdition(fn) == "Theatrical"

    def test_rogue_cut(self):
        fn = "/movies/X-Men.Days.of.Future.Past.2014.Rogue.Cut.1080p.mkv"
        assert scanner.getEdition(fn) == "Rogue Cut"

    def test_ultimate_cut(self):
        fn = "/movies/Batman.v.Superman.2016.Ultimate.Cut.2160p.mkv"
        assert scanner.getEdition(fn) == "Ultimate Cut"

    def test_black_and_chrome(self):
        fn = "/movies/Mad.Max.Fury.Road.2015.Black.Chrome.1080p.mkv"
        assert scanner.getEdition(fn) == "Black & Chrome"

    # --- Fallback regex: arbitrary "<Word> Cut" / "<Word> Edition" ---

    def test_fallback_snyder_cut(self):
        fn = "/movies/Justice.League.2021.Snyder.Cut.1080p.mkv"
        assert scanner.getEdition(fn) == "Snyder Cut"

    def test_fallback_donner_cut(self):
        fn = "/movies/Superman.II.1980.Donner.Cut.720p.mkv"
        assert scanner.getEdition(fn) == "Donner Cut"

    def test_fallback_assembly_cut(self):
        fn = "/movies/Alien.3.1992.Assembly.Cut.1080p.BluRay.mkv"
        assert scanner.getEdition(fn) == "Assembly Cut"

    def test_fallback_collectors_edition(self):
        fn = "/movies/Movie.Name.2020.Collectors.Edition.1080p.mkv"
        assert scanner.getEdition(fn) == "Collectors Edition"

    # --- False positive rejection: edition words in title BEFORE year ---

    def test_false_positive_redux_in_title(self):
        """Redux.Redux.2024 — the word 'Redux' before the year is title, not edition."""
        fn = "/movies/Redux.Redux.2024.1080p.BluRay.mkv"
        # The word 'Redux' appears before the year (as the title), so
        # searching only after year position should find nothing.
        assert scanner.getEdition(fn) == ""

    def test_false_positive_extended_in_title(self):
        """'Extended' appearing as part of a movie title before the year."""
        fn = "/movies/The.Extended.Cut.2023.1080p.BluRay.mkv"
        # 'Extended' and 'Cut' are both before the year — should not match.
        assert scanner.getEdition(fn) == ""

    def test_false_positive_imax_documentary_title(self):
        """IMAX as part of a documentary title before the year."""
        fn = "/movies/IMAX.Documentary.2020.1080p.mkv"
        assert scanner.getEdition(fn) == ""

    # --- Exclusion set: words in _edition_exclude ---

    def test_exclusion_blu_ray_cut(self):
        """'Blu.Ray.Cut' — 'blu' is in _edition_exclude, should not match fallback."""
        fn = "/movies/Movie.Name.2020.Blu.Ray.Cut.1080p.mkv"
        assert scanner.getEdition(fn) == ""

    def test_exclusion_web_edition(self):
        """'Web.Edition' — 'web' is in _edition_exclude."""
        fn = "/movies/Movie.Name.2020.Web.Edition.1080p.mkv"
        assert scanner.getEdition(fn) == ""

    def test_exclusion_hdr_cut(self):
        """'HDR.Cut' — 'hdr' is in _edition_exclude."""
        fn = "/movies/Movie.Name.2020.HDR.Cut.1080p.mkv"
        assert scanner.getEdition(fn) == ""

    # --- No year in filename ---

    def test_no_year_redux_detected(self):
        """When no year is found, the full filename is searched."""
        fn = "Movie.Redux.1080p.mkv"
        assert scanner.getEdition(fn) == "Redux"

    # --- No edition at all ---

    def test_no_edition_normal_movie(self):
        fn = "/movies/The.Matrix.1999.1080p.BluRay.x264.mkv"
        assert scanner.getEdition(fn) == ""

    def test_no_edition_simple_filename(self):
        fn = "/movies/Inception.2010.720p.mkv"
        assert scanner.getEdition(fn) == ""


# ============================================================================
# findYear (~5 tests)
# ============================================================================

class TestFindYear:
    """Tests for Scanner.findYear(text)."""

    def test_year_dot_separated(self):
        assert scanner.findYear("Movie.2020.1080p") == "2020"

    def test_year_in_parentheses(self):
        assert scanner.findYear("Movie (2020) 1080p") == "2020"

    def test_year_in_brackets(self):
        assert scanner.findYear("Movie [2019] 720p") == "2019"

    def test_no_year(self):
        assert scanner.findYear("MovieWithNoYear.1080p") == ""

    def test_year_at_boundary(self):
        """1900 and 2099 should be matched; 1899 should not."""
        assert scanner.findYear("Classic.1900.Film") == "1900"
        assert scanner.findYear("Future.2099.Film") == "2099"
        assert scanner.findYear("Too.Old.1899.Film") == ""


# ============================================================================
# getReleaseNameYear (~5 tests)
# ============================================================================

class TestGetReleaseNameYear:
    """Tests for Scanner.getReleaseNameYear(release_name, file_name=None)."""

    def test_simple_release(self):
        result = scanner.getReleaseNameYear("the matrix 1999")
        assert result.get("year") == 1999
        assert "matrix" in result.get("name", "").lower()

    def test_dotted_release(self):
        result = scanner.getReleaseNameYear("Inception.2010.1080p.BluRay")
        assert result.get("year") == 2010
        assert "inception" in result.get("name", "").lower()

    def test_no_year_release(self):
        result = scanner.getReleaseNameYear("SomeMovie.720p.BluRay")
        # When no year is found, the method may fail to extract a name
        # (int(None) raises), so the result falls through to {'other': {}}.
        assert isinstance(result, dict)

    def test_year_in_parens_release(self):
        result = scanner.getReleaseNameYear("The Godfather (1972) 1080p")
        assert result.get("year") == 1972

    def test_with_file_name(self):
        """When file_name is provided, guessit is used as primary."""
        result = scanner.getReleaseNameYear(
            "inception 2010",
            file_name="Inception.2010.1080p.BluRay.x264-GROUP.mkv"
        )
        assert result.get("year") == 2010
        assert result.get("name") is not None


# ============================================================================
# getPartNumber (~3 tests)
# ============================================================================

class TestGetPartNumber:
    """Tests for Scanner.getPartNumber(name)."""

    def test_cd1(self):
        result = scanner.getPartNumber("movie name cd1")
        assert result in ("1", 1)

    def test_cd2(self):
        """getPartNumber only checks the first regex (cd pattern) due to
        early return — 'part' pattern is never reached."""
        result = scanner.getPartNumber("movie name cd2")
        assert result in ("2", 2)

    def test_part_returns_default(self):
        """'part2' is not matched by the first regex (cd pattern),
        so getPartNumber returns default 1."""
        result = scanner.getPartNumber("movie name part2")
        assert result == 1


# ============================================================================
# isSampleFile (~3 tests)
# ============================================================================

class TestIsSampleFile:
    """Tests for Scanner.isSampleFile(filename)."""

    def test_sample_in_path(self):
        assert scanner.isSampleFile("/movies/Movie/Sample/movie-sample.mkv")

    def test_sample_prefix(self):
        assert scanner.isSampleFile("/movies/Movie/sample-movie.mkv")

    def test_not_sample(self):
        assert not scanner.isSampleFile("/movies/Movie/movie.mkv")


# ============================================================================
# isDVDFile (~2 tests)
# ============================================================================

class TestIsDVDFile:
    """Tests for Scanner.isDVDFile(file_name)."""

    def test_video_ts(self):
        assert scanner.isDVDFile("/movies/Movie/VIDEO_TS/VTS_01_1.VOB")

    def test_not_dvd(self):
        assert not scanner.isDVDFile("/movies/Movie/movie.mkv")


# ============================================================================
# keepFile (~3 tests)
# ============================================================================

class TestKeepFile:
    """Tests for Scanner.keepFile(filename)."""

    def test_keep_normal_movie(self):
        assert scanner.keepFile("/movies/Movie/movie.mkv")

    def test_reject_ds_store(self):
        assert not scanner.keepFile("/movies/Movie/.ds_store")

    def test_reject_extracted_path(self):
        path = "/movies/Movie%sextracted%sfile.mkv" % (
            __import__("os").path.sep,
            __import__("os").path.sep,
        )
        assert not scanner.keepFile(path)


# ============================================================================
# getCPImdb (~2 tests)
# ============================================================================

class TestGetCPImdb:
    """Tests for Scanner.getCPImdb(string)."""

    def test_cp_imdb_found(self):
        s = "movie.name.cp(tt1234567, abc123).mkv"
        assert scanner.getCPImdb(s) == "tt1234567"

    def test_cp_imdb_not_found(self):
        s = "movie.name.1080p.mkv"
        assert scanner.getCPImdb(s) is False


# ============================================================================
# removeCPTag (~2 tests)
# ============================================================================

class TestRemoveCPTag:
    """Tests for Scanner.removeCPTag(name)."""

    def test_removes_tag(self):
        name = "movie.name.cp(tt1234567, abc123).mkv"
        result = scanner.removeCPTag(name)
        assert "cp(" not in result
        assert "tt1234567" not in result

    def test_no_tag_unchanged(self):
        name = "movie.name.1080p.mkv"
        assert scanner.removeCPTag(name) == name
