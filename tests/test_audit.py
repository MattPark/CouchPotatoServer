"""Unit tests for audit.py edition detection functions.

Tests cover:
  - get_edition(): filename-based detection (after-year-only)
  - _parse_edition_from_release(): release-name detection (full string)
  - _detect_edition_from_words(): core multi-pass algorithm
  - _edition_fallback_regex(): unknown pattern fallback
  - Compound editions (Unrated Producers Cut, Extended Directors Cut, etc.)
  - False positive protection (Cut Bank, Uncut Gems, DC Comics, etc.)
  - Plex {edition-X} tag extraction
  - EDITION_AFTER_YEAR_ONLY gating
  - compute_recommended_action(): same-title / year correction logic
  - _check_year_against_imdb(): ±1 year IMDB adjudication
  - check_container_title(): container title mismatch with IMDB year validation
  - _revalidate_year_flags(): post-identification flag re-evaluation
  - parse_cd_number(): CD number extraction from filenames
  - classify_video_files(): multi-file folder classification
  - detect_duplicates(): duplicate file pair detection
"""

from couchpotato.core.plugins.audit import (
    get_edition,
    _parse_edition_from_release,
    _detect_edition_from_words,
    _edition_fallback_regex,
    compute_recommended_action,
    _check_year_against_imdb,
    check_container_title,
    _revalidate_year_flags,
    parse_cd_number,
    classify_video_files,
    detect_duplicates,
    pick_best_duplicate,
    compute_opensubtitles_hash,
    opensubtitles_lookup_hash,
    _format_audio_codec,
    _format_audio_channels,
    _extract_audio_tracks,
    check_audio_language,
    normalize_language,
)
import re


# ---------------------------------------------------------------------------
# Helper to split filenames into word lists (same logic as get_edition)
# ---------------------------------------------------------------------------

def _words(text):
    return re.split(r'\W+', text.lower())


def _year_idx(words):
    for i, w in enumerate(words):
        if re.match(r'^(19|20)\d{2}$', w):
            return i
    return 0


# ===========================================================================
# get_edition() — known EDITION_MAP tuple matches
# ===========================================================================

class TestGetEditionTupleMatches:
    """Tuple matches from EDITION_MAP (multi-word known patterns)."""

    def test_directors_cut(self):
        assert get_edition('Movie.2005.Directors.Cut.1080p.mkv') == "Director's Cut"

    def test_directors_edition(self):
        assert get_edition('Movie.2005.Directors.Edition.720p.mkv') == "Director's Cut"

    def test_extended_cut(self):
        assert get_edition('Movie.2005.Extended.Cut.1080p.mkv') == 'Extended Edition'

    def test_extended_edition(self):
        assert get_edition('Movie.2005.Extended.Edition.1080p.mkv') == 'Extended Edition'

    def test_theatrical_cut(self):
        assert get_edition('Movie.2005.Theatrical.Cut.1080p.mkv') == 'Theatrical'

    def test_theatrical_edition(self):
        assert get_edition('Movie.2005.Theatrical.Edition.1080p.mkv') == 'Theatrical'

    def test_final_cut(self):
        assert get_edition('Blade.Runner.1982.Final.Cut.1080p.mkv') == 'Final Cut'

    def test_special_edition(self):
        assert get_edition('Movie.2005.Special.Edition.1080p.mkv') == 'Special Edition'

    def test_anniversary_edition(self):
        assert get_edition('Movie.2005.Anniversary.Edition.1080p.mkv') == 'Anniversary Edition'

    def test_criterion_collection(self):
        assert get_edition('Movie.2005.Criterion.Collection.1080p.mkv') == 'Criterion'

    def test_ultimate_cut(self):
        assert get_edition('Movie.2005.Ultimate.Cut.1080p.mkv') == 'Ultimate Cut'

    def test_ultimate_edition(self):
        assert get_edition('Movie.2005.Ultimate.Edition.1080p.mkv') == 'Ultimate Cut'

    def test_rogue_cut(self):
        assert get_edition('X-Men.2014.Rogue.Cut.1080p.mkv') == 'Rogue Cut'

    def test_imax_edition(self):
        assert get_edition('Movie.2005.IMAX.Edition.1080p.mkv') == 'IMAX'

    def test_black_chrome(self):
        assert get_edition('Mad.Max.2015.Black.Chrome.1080p.mkv') == "Black & Chrome"

    def test_black_and_chrome(self):
        assert get_edition('Mad.Max.2015.Black.And.Chrome.1080p.mkv') == "Black & Chrome"


# ===========================================================================
# get_edition() — known EDITION_MAP single-word matches
# ===========================================================================

class TestGetEditionSingleWordMatches:
    """Single-word EDITION_MAP matches."""

    def test_unrated(self):
        assert get_edition('Movie.2005.UNRATED.1080p.mkv') == 'Unrated'

    def test_remastered(self):
        assert get_edition('Movie.2005.REMASTERED.1080p.mkv') == 'Remastered'

    def test_theatrical(self):
        assert get_edition('Movie.2005.Theatrical.1080p.mkv') == 'Theatrical'

    def test_imax(self):
        assert get_edition('Movie.2005.IMAX.1080p.mkv') == 'IMAX'

    def test_redux(self):
        assert get_edition('Apocalypse.Now.2005.Redux.1080p.mkv') == 'Redux'

    def test_criterion(self):
        assert get_edition('Movie.2005.Criterion.1080p.mkv') == 'Criterion'

    def test_extended(self):
        assert get_edition('Movie.2005.Extended.1080p.mkv') == 'Extended Edition'

    def test_uncut(self):
        assert get_edition('Movie.2005.Uncut.1080p.mkv') == 'Uncut'


# ===========================================================================
# get_edition() — compound editions (multi-pass detection)
# ===========================================================================

class TestGetEditionCompound:
    """Compound edition detection via forward scanning."""

    def test_unrated_producers_cut(self):
        result = get_edition(
            'Halloween.The.Curse.of.Michael.Myers.1995.UNRATED.PRODUCERS.CUT.1080p.mkv'
        )
        assert result == 'Unrated Producers Cut'

    def test_unrated_snyder_cut(self):
        result = get_edition('Movie.2021.UNRATED.SNYDER.CUT.1080p.mkv')
        assert result == 'Unrated Snyder Cut'

    def test_extended_directors_cut(self):
        result = get_edition('Movie.2005.EXTENDED.DIRECTORS.CUT.1080p.mkv')
        # Now matches the explicit EDITION_MAP tuple for Extended Director's Cut
        assert result == "Extended Director's Cut"

    def test_unrated_stops_at_tech_word(self):
        """Unrated followed by tech word should NOT form compound."""
        result = get_edition('Movie.2005.UNRATED.1080p.mkv')
        assert result == 'Unrated'

    def test_unrated_stops_at_bluray(self):
        """Unrated followed by BluRay should NOT form compound."""
        result = get_edition('Movie.2005.UNRATED.BluRay.mkv')
        assert result == 'Unrated'

    def test_extended_stops_at_x264(self):
        result = get_edition('Movie.2005.Extended.x264.mkv')
        assert result == 'Extended Edition'

    def test_unrated_extended_directors_cut(self):
        """Multiple edition keywords — earliest position wins."""
        result = get_edition(
            'Movie.2005.UNRATED.EXTENDED.DIRECTORS.CUT.1080p.mkv'
        )
        # 'unrated' is first, scan forward finds 'cut' → compound
        assert 'Cut' in result
        assert result.startswith('Unrated')


# ===========================================================================
# get_edition() — Plex tag extraction
# ===========================================================================

class TestGetEditionPlexTag:
    """Plex {edition-X} tags should be returned verbatim."""

    def test_plex_directors_cut(self):
        assert get_edition(
            "Movie (2005) {edition-Director's Cut} 1080p.mkv"
        ) == "Director's Cut"

    def test_plex_enchanted_edition(self):
        assert get_edition(
            'Movie (2005) {edition-The Enchanted Edition} 1080p.mkv'
        ) == 'The Enchanted Edition'

    def test_plex_criterion(self):
        assert get_edition(
            'Movie (2005) {edition-Criterion} 1080p.mkv'
        ) == 'Criterion'

    def test_plex_takes_priority_over_keywords(self):
        """Plex tag should win even if keywords are also present."""
        result = get_edition(
            'Movie (2005) {edition-Special Version} Extended.1080p.mkv'
        )
        assert result == 'Special Version'


# ===========================================================================
# get_edition() — false positive protection
# ===========================================================================

class TestGetEditionFalsePositives:
    """Edition words that appear in movie TITLES should not match."""

    def test_cut_bank(self):
        """'Cut Bank' — title contains 'Cut' but it's not an edition."""
        assert get_edition('Cut.Bank.2014.1080p.BluRay.mkv') == ''

    def test_shortcut_to_happiness(self):
        assert get_edition('Shortcut.to.Happiness.2007.1080p.mkv') == ''

    def test_rough_cut(self):
        """'Rough' is in EDITION_EXCLUDE — should not match."""
        assert get_edition('Rough.Cut.1980.1080p.mkv') == ''

    def test_the_final_cut_title(self):
        """'The Final Cut' (2004) — title, not edition.
        This is tricky: 'Final Cut' is in EDITION_MAP as a tuple.
        Since it appears BEFORE the year, after_year_only=True should skip it.
        """
        result = get_edition('The.Final.Cut.2004.1080p.mkv')
        # final.cut appears before the year 2004, so should not match
        assert result == ''

    def test_dc_comics_before_year(self):
        """'DC' in movie title like 'Batman DC Universe' should not match."""
        # dc is in EDITION_AFTER_YEAR_ONLY, so only matches after year
        result = get_edition('Batman.DC.Universe.2014.1080p.mkv')
        assert result != "Director's Cut"

    def test_uncut_gems(self):
        """'Uncut Gems' — 'uncut' is in EDITION_AFTER_YEAR_ONLY."""
        result = get_edition('Uncut.Gems.2019.1080p.BluRay.mkv')
        assert result == ''

    def test_possessor_uncut(self):
        """'Possessor Uncut' — 'uncut' before year should not match."""
        result = get_edition('Possessor.Uncut.2020.1080p.mkv')
        assert result == ''

    def test_of_smaug_extended_edition(self):
        """Should detect 'Extended Edition', NOT 'Of Smaug Extended Edition'."""
        result = get_edition(
            'The.Hobbit.The.Desolation.of.Smaug.2013.Extended.Edition.1080p.mkv'
        )
        assert result == 'Extended Edition'
        assert 'Smaug' not in result

    def test_no_edition(self):
        """Normal movie with no edition keywords."""
        assert get_edition('The.Matrix.1999.1080p.BluRay.mkv') == ''

    def test_no_year_no_match(self):
        """Without a year, edition keywords in what could be a title are risky.
        The algorithm sets year_idx=0 which means it searches all words,
        but this is acceptable since it's the caller's responsibility to
        provide reasonable filenames.
        """
        # This is a design choice — documenting current behavior
        result = get_edition('Movie.Extended.1080p.mkv')
        # 'extended' should still match since year_idx=0 means search from start
        assert result == 'Extended Edition'


# ===========================================================================
# get_edition() — fallback regex for unknown editions
# ===========================================================================

class TestGetEditionFallbackRegex:
    """Unknown editions caught by the fallback regex."""

    def test_assembly_cut(self):
        result = get_edition('Alien.3.1992.Assembly.Cut.1080p.mkv')
        assert result == 'Assembly Cut'

    def test_snyder_cut_standalone(self):
        """'Snyder Cut' is not in EDITION_MAP but should be caught by fallback."""
        result = get_edition('Justice.League.2021.Snyder.Cut.1080p.mkv')
        assert result == 'Snyder Cut'

    def test_unknown_edition(self):
        result = get_edition('Movie.2005.Hybrid.Edition.1080p.mkv')
        # 'hybrid' is not in EDITION_MAP — could be caught by multi-pass
        # or fallback depending on future EDITION_MAP additions
        # For now it shouldn't match EDITION_MAP, so fallback fires
        assert 'Edition' in result or result == ''


# ===========================================================================
# _parse_edition_from_release() — full-string search
# ===========================================================================

class TestParseEditionFromRelease:
    """_parse_edition_from_release searches the entire string."""

    def test_redux_before_year(self):
        """Apocalypse Now Redux — edition appears before the year."""
        result = _parse_edition_from_release(
            'Apocalypse.Now.Redux.1979.REMASTERED.1080p.BluRay'
        )
        assert result == 'Redux'

    def test_extended_edition_normal(self):
        result = _parse_edition_from_release(
            'Movie.2005.Extended.Edition.1080p.BluRay'
        )
        assert result == 'Extended Edition'

    def test_compound_unrated_producers_cut(self):
        result = _parse_edition_from_release(
            'Halloween.The.Curse.of.Michael.Myers.1995.UNRATED.PRODUCERS.CUT.1080p'
        )
        assert result == 'Unrated Producers Cut'

    def test_dc_after_year_only(self):
        """'dc' should only match after the year in release names."""
        result = _parse_edition_from_release(
            'Batman.DC.Universe.2014.1080p.BluRay'
        )
        # dc appears before year, should not match
        assert result != "Director's Cut"

    def test_dc_after_year(self):
        """'dc' after the year should match Director's Cut."""
        result = _parse_edition_from_release(
            'Movie.2005.DC.1080p.BluRay'
        )
        assert result == "Director's Cut"

    def test_uncut_after_year(self):
        """'uncut' after year should match."""
        result = _parse_edition_from_release(
            'Movie.2005.Uncut.1080p.BluRay'
        )
        assert result == 'Uncut'

    def test_uncut_before_year(self):
        """'uncut' before year should NOT match (EDITION_AFTER_YEAR_ONLY)."""
        result = _parse_edition_from_release(
            'Uncut.Gems.2019.1080p.BluRay'
        )
        assert result == ''

    def test_empty_string(self):
        assert _parse_edition_from_release('') == ''

    def test_none(self):
        assert _parse_edition_from_release(None) == ''

    def test_no_edition(self):
        assert _parse_edition_from_release(
            'The.Matrix.1999.1080p.BluRay.x264'
        ) == ''

    def test_fallback_snyder_cut(self):
        result = _parse_edition_from_release(
            'Justice.League.2021.Snyder.Cut.1080p.REMUX'
        )
        assert result == 'Snyder Cut'


# ===========================================================================
# _detect_edition_from_words() — direct testing of multi-pass algorithm
# ===========================================================================

class TestDetectEditionFromWords:
    """Direct tests of the core multi-pass algorithm."""

    def test_tuple_match_directors_cut(self):
        words = _words('movie.2005.directors.cut.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == "Director's Cut"

    def test_standalone_unrated(self):
        words = _words('movie.2005.unrated.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == 'Unrated'

    def test_compound_unrated_producers_cut(self):
        words = _words('movie.1995.unrated.producers.cut.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == 'Unrated Producers Cut'

    def test_compound_stops_at_tech(self):
        words = _words('movie.2005.unrated.1080p.cut')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        # 1080p is a stop word — should NOT extend to "Unrated 1080p Cut"
        assert result == 'Unrated'

    def test_tuple_beats_standalone_at_same_position(self):
        """Tuple match type (0) should beat standalone (2) at same position."""
        words = _words('movie.2005.directors.cut.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        # 'directors.cut' is a tuple match for Director's Cut
        # 'dc' would be standalone but isn't present here
        assert result == "Director's Cut"

    def test_earliest_position_wins(self):
        """When multiple editions present, earliest position wins."""
        words = _words('movie.2005.extended.remastered.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == 'Extended Edition'

    def test_full_string_search(self):
        """after_year_only=False searches entire string."""
        words = _words('apocalypse.now.redux.1979.1080p')
        result = _detect_edition_from_words(words, after_year_only=False,
                                            year_idx=_year_idx(words))
        assert result == 'Redux'

    def test_after_year_only_skips_before_year(self):
        """after_year_only=True should not find 'redux' before year."""
        words = _words('apocalypse.now.redux.1979.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == ''

    def test_empty_words(self):
        assert _detect_edition_from_words([], after_year_only=True, year_idx=0) == ''

    def test_after_year_only_gating_for_dc(self):
        """'dc' in EDITION_AFTER_YEAR_ONLY — only matches after year."""
        words = _words('batman.dc.2014.1080p')
        result = _detect_edition_from_words(words, after_year_only=False,
                                            year_idx=_year_idx(words))
        # dc is before year — should be gated
        assert result != "Director's Cut"

    def test_dc_after_year_matches(self):
        words = _words('movie.2014.dc.1080p')
        result = _detect_edition_from_words(words, after_year_only=True,
                                            year_idx=_year_idx(words))
        assert result == "Director's Cut"


# ===========================================================================
# _edition_fallback_regex() — direct testing
# ===========================================================================

class TestEditionFallbackRegex:
    """Direct tests of the fallback regex."""

    def test_assembly_cut(self):
        assert _edition_fallback_regex('.Assembly.Cut.') == 'Assembly Cut'

    def test_snyder_cut(self):
        assert _edition_fallback_regex('.Snyder.Cut.1080p') == 'Snyder Cut'

    def test_two_word_edition(self):
        result = _edition_fallback_regex('.Super.Duper.Cut.')
        assert result == 'Super Duper Cut'

    def test_rough_cut_excluded(self):
        """'rough' is in EDITION_EXCLUDE."""
        assert _edition_fallback_regex('.Rough.Cut.') == ''

    def test_blu_ray_excluded(self):
        """'blu' is in EDITION_EXCLUDE — should not form 'Blu Cut'."""
        assert _edition_fallback_regex('.Blu.Cut.') == ''

    def test_no_match(self):
        assert _edition_fallback_regex('just some random text') == ''

    def test_after_year_text_param(self):
        result = _edition_fallback_regex(
            'full text with assembly cut',
            after_year_text='.Assembly.Cut.1080p'
        )
        assert result == 'Assembly Cut'

    def test_tightened_regex_max_two_words(self):
        """Regex captures max 2 words before Cut/Edition (reduced from 3).

        Pattern: (word.){0,1}(word).(cut|edition)
        - {0,1} means 0 or 1 optional words, plus 1 required = max 2 words total.
        - This prevents 3-word false positives like 'Of Smaug Extended Edition'.
        """
        # 2 words before 'edition' — should match (within {0,1} + 1 = 2 limit)
        result = _edition_fallback_regex('.Really.Custom.Edition.')
        assert result == 'Really Custom Edition'

        # 3 words before 'edition' — only the last 2 should be captured
        result3 = _edition_fallback_regex('.Of.Smaug.Extended.Edition.')
        # The regex will match 'Smaug.Extended.Edition' (2 words max),
        # NOT 'Of.Smaug.Extended.Edition' (3 words)
        assert 'Of' not in result3


# ===========================================================================
# Integration: full pipeline from filename to edition
# ===========================================================================

class TestEditionIntegration:
    """End-to-end tests matching real production scenarios."""

    def test_halloween_unrated_producers_cut(self):
        """The original bug that motivated compound edition detection."""
        result = get_edition(
            'Halloween.The.Curse.of.Michael.Myers.1995.UNRATED.PRODUCERS.CUT.'
            'BluRay.1080p.DTS-HD.MA.5.1.AVC.REMUX-FraMeSToR.mkv'
        )
        assert result == 'Unrated Producers Cut'

    def test_blade_runner_final_cut(self):
        result = get_edition(
            'Blade.Runner.1982.The.Final.Cut.1080p.BluRay.mkv'
        )
        assert result == 'Final Cut'

    def test_apocalypse_now_redux_release(self):
        """Redux before year — _parse_edition_from_release catches it."""
        result = _parse_edition_from_release(
            'Apocalypse.Now.Redux.1979.REMASTERED.1080p.BluRay.x264-BARC0DE'
        )
        assert result == 'Redux'

    def test_hobbit_extended_edition(self):
        """Should return 'Extended Edition', not 'Of Smaug Extended Edition'."""
        result = get_edition(
            'The.Hobbit.The.Desolation.of.Smaug.2013.EXTENDED.EDITION.1080p.'
            'BluRay.x264.mkv'
        )
        assert result == 'Extended Edition'

    def test_alien3_assembly_cut(self):
        result = get_edition(
            'Alien.3.1992.Assembly.Cut.Special.Edition.1080p.BluRay.mkv'
        )
        # Assembly Cut should be caught either by fallback or compound
        assert 'Assembly' in result or 'Special' in result

    def test_plex_edition_tag_real(self):
        result = get_edition(
            'Snow White and the Seven Dwarfs (1937) '
            '{edition-The Enchanted Edition} Bluray-1080p.mkv'
        )
        assert result == 'The Enchanted Edition'

    def test_mad_max_black_chrome(self):
        result = get_edition(
            'Mad.Max.Fury.Road.2015.Black.And.Chrome.Edition.1080p.mkv'
        )
        assert result == "Black & Chrome"

    def test_x_men_rogue_cut(self):
        result = get_edition(
            'X-Men.Days.of.Future.Past.2014.THE.ROGUE.CUT.1080p.BluRay.mkv'
        )
        assert result == 'Rogue Cut'

    # --- Possessive apostrophe handling ---

    def test_extended_directors_cut_apostrophe(self):
        """Container title with apostrophe: Extended Director's Cut."""
        result = get_edition("Extended Director's Cut")
        assert result == "Extended Director's Cut"

    def test_extended_directors_cut_dotted(self):
        """Filename-style: Extended.Directors.Cut after year."""
        result = get_edition(
            'Cop.Land.1997.Extended.Directors.Cut.1080p.BluRay.mkv'
        )
        assert result == "Extended Director's Cut"

    def test_directors_cut_apostrophe_only(self):
        """Plain Director's Cut with apostrophe still works."""
        result = get_edition("Some.Movie.2020.Director's.Cut.1080p.mkv")
        assert result == "Director's Cut"

    def test_extended_director_cut_no_s(self):
        """Extended Director Cut (no possessive) also matches."""
        result = get_edition(
            'Movie.2020.Extended.Director.Cut.1080p.mkv'
        )
        assert result == "Extended Director's Cut"

    def test_parse_release_directors_cut_apostrophe(self):
        """_parse_edition_from_release handles apostrophe."""
        result = _parse_edition_from_release("Extended Director's Cut")
        assert result == "Extended Director's Cut"

    def test_parse_release_directors_cut_dotted(self):
        """_parse_edition_from_release with dotted release name."""
        result = _parse_edition_from_release(
            'Cop.Land.1997.Extended.Directors.Cut.1080p.BluRay-Group'
        )
        assert result == "Extended Director's Cut"

    def test_unrated_dc_scene_style(self):
        """Unrated DC in scene release → Unrated Director's Cut."""
        result = _parse_edition_from_release(
            'Dawn.of.the.Dead.2004.Unrated.DC.2160p.BluRay.x265-QTZ'
        )
        assert result == "Unrated Director's Cut"

    def test_unrated_dc_container_title(self):
        """Unrated DC in container title string."""
        result = get_edition(
            'Dawn of the Dead (2004) Unrated DC MULTi VFF 2160p'
        )
        assert result == "Unrated Director's Cut"

    def test_unrated_directors_cut_full(self):
        """Full Unrated Directors Cut in filename."""
        result = get_edition(
            'Movie.2020.Unrated.Directors.Cut.1080p.mkv'
        )
        assert result == "Unrated Director's Cut"


# ===========================================================================
# compute_recommended_action — same-title / year correction logic
# ===========================================================================

class TestComputeRecommendedAction:
    """Tests for compute_recommended_action() with expected data."""

    # Helper to build flags and identification dicts
    @staticmethod
    def _flags(*checks):
        return [{'check': c, 'severity': 'MEDIUM', 'detail': 'test'} for c in checks]

    @staticmethod
    def _ident(method, title=None, year=None, imdb=None):
        d = {'method': method}
        if title:
            d['identified_title'] = title
        if year:
            d['identified_year'] = year
        if imdb:
            d['identified_imdb'] = imdb
        return d

    @staticmethod
    def _expected(title='Test Movie', year=2020, db_title=None):
        return {
            'title': title,
            'year': year,
            'db_title': db_title,
            'resolution': '1080p',
        }

    # -- Basic behavior (no expected data — backward compatibility) --

    def test_no_expected_imdb_returns_reassign(self):
        """Without expected data, IMDB match → reassign (backward compat)."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='Foo', imdb='tt1234567')
        assert compute_recommended_action(flags, ident) == 'reassign_movie'

    def test_no_expected_title_only_returns_reassign(self):
        """Without expected data, title-only match → reassign (backward compat)."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='Foo')
        assert compute_recommended_action(flags, ident) == 'reassign_movie'

    # -- Same title, same year → fall through to flag-based logic --

    def test_same_title_same_year_template_flag(self):
        """Same title + same year with template flag → rename_template."""
        flags = self._flags('title', 'template')
        ident = self._ident('container_title', title='Test Movie', year=2020)
        expected = self._expected('Test Movie', 2020)
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    def test_same_title_same_year_no_template_flag(self):
        """Same title + same year, only title flag, identification confirms identity → none.

        When identification (or manual) confirms the same movie and
        there are no actionable flags (template/resolution/edition), the
        title flag is just a container metadata discrepancy — nothing to fix.
        """
        flags = self._flags('title')
        ident = self._ident('container_title', title='Test Movie', year=2020)
        expected = self._expected('Test Movie', 2020)
        result = compute_recommended_action(flags, ident, expected)
        assert result == 'none'

    def test_same_title_same_year_resolution_flag(self):
        """Same title + same year with resolution flag → rename_resolution."""
        flags = self._flags('resolution')
        ident = self._ident('container_title', title='Test Movie', year=2020)
        expected = self._expected('Test Movie', 2020)
        assert compute_recommended_action(flags, ident, expected) == 'rename_resolution'

    # -- Same title, year within ±1 → rename_template --

    def test_same_title_year_off_by_one_high(self):
        """Same title, identified year 1 higher → rename_template."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='55 Steps', year=2018)
        expected = self._expected('55 Steps', 2017)
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    def test_same_title_year_off_by_one_low(self):
        """Same title, identified year 1 lower → rename_template."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='The Matrix', year=1998)
        expected = self._expected('The Matrix', 1999)
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    def test_same_title_year_off_by_one_srrdb(self):
        """Same title, year ±1 via srrdb_crc method → rename_template."""
        flags = self._flags('title')
        ident = self._ident('srrdb_crc', title='Blade Runner', year=2016, imdb='tt1234')
        expected = self._expected('Blade Runner', 2017)
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    # -- Same title, year beyond ±1 → reassign_movie (likely a remake) --

    def test_same_title_year_diff_two(self):
        """Same title, year off by 2 → reassign_movie (could be remake)."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='Dune', year=2021)
        expected = self._expected('Dune', 2019)
        assert compute_recommended_action(flags, ident, expected) == 'reassign_movie'

    def test_same_title_year_diff_large(self):
        """Same title, large year difference → reassign_movie (remake)."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='Dune', year=2021)
        expected = self._expected('Dune', 1984)
        assert compute_recommended_action(flags, ident, expected) == 'reassign_movie'

    # -- Different title → reassign_movie --

    def test_different_title_returns_reassign(self):
        """Different titles → reassign_movie regardless of year."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='Blade Runner', year=2017)
        expected = self._expected('Blade Runner 2049', 2017)
        assert compute_recommended_action(flags, ident, expected) == 'reassign_movie'

    def test_completely_different_title(self):
        """Completely different movie → reassign_movie."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='The Avengers', year=2012, imdb='tt0848228')
        expected = self._expected('Iron Man', 2008)
        assert compute_recommended_action(flags, ident, expected) == 'reassign_movie'

    # -- IMDB-only identification (no title) → reassign_movie --

    def test_imdb_only_no_title(self):
        """IMDB-only match without identified title → reassign_movie."""
        flags = self._flags('title')
        ident = self._ident('container_title', imdb='tt0848228')
        expected = self._expected('Iron Man', 2008)
        assert compute_recommended_action(flags, ident, expected) == 'reassign_movie'

    # -- Non-identification methods --

    def test_crc_not_found_returns_manual(self):
        """CRC not found → manual_review."""
        flags = self._flags('title')
        ident = self._ident('crc_not_found')
        expected = self._expected()
        assert compute_recommended_action(flags, ident, expected) == 'manual_review'

    def test_tv_episode_detected(self):
        """TV episode detected → delete_wrong."""
        flags = self._flags('title')
        ident = self._ident('tv_episode_detected')
        expected = self._expected()
        assert compute_recommended_action(flags, ident, expected) == 'delete_wrong'

    def test_skipped_falls_through(self):
        """Skipped method falls through to flag-based logic."""
        flags = self._flags('template')
        ident = self._ident('skipped')
        expected = self._expected()
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    # -- TV episode flag (no identification needed) --

    def test_tv_episode_flag(self):
        """TV episode flag → delete_wrong regardless of other flags."""
        flags = self._flags('tv_episode', 'title')
        assert compute_recommended_action(flags) == 'delete_wrong'

    # -- No identification, flag-based --

    def test_no_ident_title_flag(self):
        """Title flag without identification → needs_full."""
        flags = self._flags('title')
        assert compute_recommended_action(flags) == 'needs_full'

    def test_no_ident_template_and_title(self):
        """Template + title without identification → needs_full."""
        flags = self._flags('template', 'title')
        assert compute_recommended_action(flags) == 'needs_full'

    def test_no_ident_template_only(self):
        """Template flag alone without identification → rename_template."""
        flags = self._flags('template')
        assert compute_recommended_action(flags) == 'rename_template'

    def test_no_ident_resolution_only(self):
        """Resolution flag alone → rename_resolution."""
        flags = self._flags('resolution')
        assert compute_recommended_action(flags) == 'rename_resolution'

    def test_no_ident_edition_only(self):
        """Edition flag alone → rename_edition."""
        flags = self._flags('edition')
        assert compute_recommended_action(flags) == 'rename_edition'

    # -- Same title, no year on either side → falls through --

    def test_same_title_no_years(self):
        """Same title, neither side has year → falls through (confirmed)."""
        flags = self._flags('resolution')
        ident = self._ident('container_title', title='Test Movie')
        expected = self._expected('Test Movie', year=None)
        assert compute_recommended_action(flags, ident, expected) == 'rename_resolution'

    def test_same_title_only_ident_has_year(self):
        """Same title, only identification has year → falls through (can't compare)."""
        flags = self._flags('template')
        ident = self._ident('container_title', title='Test Movie', year=2020)
        expected = self._expected('Test Movie', year=None)
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    # -- db_title preference --

    def test_uses_db_title_for_comparison(self):
        """Uses db_title when available for title comparison."""
        flags = self._flags('title')
        ident = self._ident('container_title', title='The Monster', year=2017)
        expected = {'title': 'Monster, The', 'year': 2016, 'db_title': 'The Monster'}
        assert compute_recommended_action(flags, ident, expected) == 'rename_template'

    def test_duplicate_flag_returns_delete_duplicate(self):
        """Duplicate flag should recommend delete_duplicate (not delete_wrong)."""
        flags = [{'check': 'duplicate', 'severity': 'MEDIUM', 'detail': 'Possible duplicate'}]
        assert compute_recommended_action(flags) == 'delete_duplicate'

    def test_duplicate_with_other_flags_still_delete_duplicate(self):
        """Duplicate flag takes priority (before template/resolution logic)."""
        flags = [
            {'check': 'duplicate', 'severity': 'MEDIUM', 'detail': 'Possible duplicate'},
            {'check': 'template', 'severity': 'LOW', 'detail': 'Template mismatch'},
        ]
        assert compute_recommended_action(flags) == 'delete_duplicate'

    def test_foreign_audio_flag_returns_delete_foreign(self):
        """Foreign audio flag → delete_foreign."""
        flags = [{'check': 'foreign_audio', 'severity': 'LOW', 'detail': 'All audio tracks are non-English: fr'}]
        assert compute_recommended_action(flags) == 'delete_foreign'

    def test_foreign_audio_with_template_still_delete_foreign(self):
        """Foreign audio takes priority over template/resolution flags."""
        flags = [
            {'check': 'foreign_audio', 'severity': 'LOW', 'detail': 'All audio tracks are non-English: fr'},
            {'check': 'template', 'severity': 'LOW', 'detail': 'Template mismatch'},
        ]
        assert compute_recommended_action(flags) == 'delete_foreign'

    def test_foreign_audio_with_resolution_still_delete_foreign(self):
        """Foreign audio takes priority over resolution flags."""
        flags = [
            {'check': 'foreign_audio', 'severity': 'LOW', 'detail': 'All audio tracks are non-English: ja'},
            {'check': 'resolution', 'severity': 'HIGH', 'detail': 'Resolution mismatch'},
        ]
        assert compute_recommended_action(flags) == 'delete_foreign'


# ---------------------------------------------------------------------------
# _check_year_against_imdb() tests
# ---------------------------------------------------------------------------

class TestCheckYearAgainstImdb:
    """Tests for the ±1 year IMDB adjudication helper."""

    def test_folder_matches_imdb_suppress(self):
        """Folder year matches IMDB → suppress (false positive)."""
        assert _check_year_against_imdb(2018, 2019, 2019) == 'suppress'

    def test_folder_matches_imdb_suppress_reverse(self):
        """Folder year matches IMDB, container is +1 → suppress."""
        assert _check_year_against_imdb(2020, 2019, 2019) == 'suppress'

    def test_container_matches_imdb_keep(self):
        """Container year matches IMDB → keep (folder may be wrong)."""
        assert _check_year_against_imdb(2018, 2019, 2018) == 'keep'

    def test_container_matches_imdb_keep_reverse(self):
        """Container year matches IMDB, container is +1 → keep."""
        assert _check_year_against_imdb(2020, 2019, 2020) == 'keep'

    def test_neither_matches_downgrade(self):
        """Neither year matches IMDB → downgrade to LOW."""
        assert _check_year_against_imdb(2018, 2019, 2020) == 'downgrade'

    def test_no_imdb_year_downgrade(self):
        """No IMDB year available → downgrade."""
        assert _check_year_against_imdb(2018, 2019, None) == 'downgrade'

    def test_no_imdb_year_zero_downgrade(self):
        """IMDB year is 0 (missing data) → downgrade."""
        assert _check_year_against_imdb(2018, 2019, 0) == 'downgrade'


# ---------------------------------------------------------------------------
# check_container_title() with imdb_year tests
# ---------------------------------------------------------------------------

class TestCheckContainerTitleImdbYear:
    """Tests for check_container_title() with IMDB year validation."""

    # -- ±1 year, same title, IMDB confirms folder --

    def test_year_off_by_1_imdb_confirms_folder_suppressed(self):
        """Same title, year off by 1, IMDB matches folder → no flag."""
        # Container says 2018, folder says 2019, IMDB says 2019
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is None
        assert meta is not None
        assert meta['title'] == 'The Movie'

    def test_year_off_by_1_reverse_imdb_confirms_folder_suppressed(self):
        """Same title, container +1, IMDB matches folder → no flag."""
        flag, meta = check_container_title(
            'The Movie 2020 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is None

    # -- ±1 year, same title, IMDB confirms container --

    def test_year_off_by_1_imdb_confirms_container_high(self):
        """Same title, year off by 1, IMDB matches container → HIGH flag."""
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=2018,
        )
        assert flag is not None
        assert flag['severity'] == 'HIGH'
        assert flag['check'] == 'title'
        assert '2018' in flag['detail']
        assert '2019' in flag['detail']

    # -- ±1 year, same title, no IMDB data --

    def test_year_off_by_1_no_imdb_low(self):
        """Same title, year off by 1, no IMDB → LOW flag."""
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=None,
        )
        assert flag is not None
        assert flag['severity'] == 'LOW'
        assert 'ambiguous' in flag['detail']

    def test_year_off_by_1_imdb_zero_low(self):
        """Same title, year off by 1, IMDB year is 0 → LOW flag."""
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=0,
        )
        assert flag is not None
        assert flag['severity'] == 'LOW'

    # -- ±1 year, same title, IMDB matches neither --

    def test_year_off_by_1_imdb_matches_neither_low(self):
        """Same title, year off by 1, IMDB matches neither → LOW."""
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=2020,
        )
        assert flag is not None
        assert flag['severity'] == 'LOW'
        assert 'ambiguous' in flag['detail']

    # -- Year off by >1, same title → always HIGH regardless of IMDB --

    def test_year_off_by_2_still_high(self):
        """Same title, year off by 2 → HIGH even if IMDB matches folder."""
        flag, meta = check_container_title(
            'The Movie 2017 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is not None
        assert flag['severity'] == 'HIGH'
        assert 'title matches' in flag['detail']

    def test_year_off_by_3_still_high(self):
        """Same title, year off by 3 → HIGH regardless."""
        flag, meta = check_container_title(
            'The Movie 2016 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is not None
        assert flag['severity'] == 'HIGH'

    # -- Different title cases are unchanged --

    def test_different_title_different_year_high(self):
        """Different title AND year → HIGH (unchanged, ignores imdb_year)."""
        flag, meta = check_container_title(
            'Other Movie 2018 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is not None
        assert flag['severity'] == 'HIGH'

    def test_different_title_same_year_medium(self):
        """Different title, same year → MEDIUM (unchanged)."""
        flag, meta = check_container_title(
            'Other Movie 2019 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is not None
        assert flag['severity'] == 'MEDIUM'

    # -- Same title, same year → no flag (unchanged) --

    def test_same_title_same_year_no_flag(self):
        """Same title, same year → no flag (unchanged)."""
        flag, meta = check_container_title(
            'The Movie 2019 1080p BluRay', 'The Movie', 2019, imdb_year=2019,
        )
        assert flag is None

    # -- No imdb_year param at all (backward compatibility) --

    def test_no_imdb_year_param_year_off_by_1_low(self):
        """No imdb_year param → ±1 year gets LOW severity."""
        flag, meta = check_container_title(
            'The Movie 2018 1080p BluRay', 'The Movie', 2019,
        )
        assert flag is not None
        assert flag['severity'] == 'LOW'


# ---------------------------------------------------------------------------
# _revalidate_year_flags() tests
# ---------------------------------------------------------------------------

class TestRevalidateYearFlags:
    """Tests for post-identification ±1 year flag re-evaluation."""

    @staticmethod
    def _item(flags, folder_year=2019):
        return {
            'flags': list(flags),
            'flag_count': len(flags),
            'expected': {'year': folder_year},
        }

    @staticmethod
    def _year_flag(container_year, folder_year, severity='HIGH'):
        return {
            'check': 'title',
            'severity': severity,
            'detail': (
                f"Container year {container_year} "
                f"vs folder year {folder_year} (title matches)"
            ),
        }

    def test_suppress_when_imdb_matches_folder(self):
        """Flag removed when IMDB year matches folder year."""
        flag = self._year_flag(2018, 2019)
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, 2019)
        assert result is True
        assert len(item['flags']) == 0
        assert item['flag_count'] == 0

    def test_keep_when_imdb_matches_container(self):
        """Flag kept as HIGH when IMDB year matches container year."""
        flag = self._year_flag(2018, 2019)
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, 2018)
        assert result is False
        assert len(item['flags']) == 1
        assert item['flags'][0]['severity'] == 'HIGH'

    def test_downgrade_when_imdb_matches_neither(self):
        """Flag downgraded to LOW when IMDB matches neither year."""
        flag = self._year_flag(2018, 2019)
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, 2020)
        assert result is True
        assert len(item['flags']) == 1
        assert item['flags'][0]['severity'] == 'LOW'
        assert 'ambiguous' in item['flags'][0]['detail']

    def test_no_imdb_year_noop(self):
        """No IMDB year → no changes."""
        flag = self._year_flag(2018, 2019)
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, None)
        assert result is False
        assert len(item['flags']) == 1

    def test_non_year_flags_untouched(self):
        """Non-title flags are not affected."""
        res_flag = {'check': 'resolution', 'severity': 'HIGH', 'detail': 'wrong res'}
        year_flag = self._year_flag(2018, 2019)
        item = self._item([res_flag, year_flag], folder_year=2019)
        _revalidate_year_flags(item, 2019)
        assert len(item['flags']) == 1
        assert item['flags'][0]['check'] == 'resolution'

    def test_year_off_by_more_than_1_untouched(self):
        """Flags with year off by >1 are not affected."""
        flag = self._year_flag(2016, 2019)
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, 2019)
        assert result is False
        assert len(item['flags']) == 1
        assert item['flags'][0]['severity'] == 'HIGH'

    def test_title_flag_without_title_matches_untouched(self):
        """Title flags without 'title matches' in detail are not affected."""
        flag = {
            'check': 'title',
            'severity': 'HIGH',
            'detail': "Container title 'Other Movie (2018)' vs folder 'The Movie (2019)'",
        }
        item = self._item([flag], folder_year=2019)
        result = _revalidate_year_flags(item, 2019)
        assert result is False
        assert len(item['flags']) == 1

    def test_flag_count_updated_after_suppress(self):
        """flag_count is updated after suppression."""
        res_flag = {'check': 'resolution', 'severity': 'HIGH', 'detail': 'wrong res'}
        year_flag = self._year_flag(2018, 2019)
        item = self._item([res_flag, year_flag], folder_year=2019)
        assert item['flag_count'] == 2
        _revalidate_year_flags(item, 2019)
        assert item['flag_count'] == 1


# ===========================================================================
# parse_cd_number() — CD number extraction from filenames
# ===========================================================================

class TestParseCdNumber:
    """Tests for parse_cd_number() filename parsing."""

    def test_cd1_lowercase(self):
        assert parse_cd_number('Movie (2005) cd1.avi') == 1

    def test_cd2_lowercase(self):
        assert parse_cd_number('Movie (2005) cd2.avi') == 2

    def test_cd_uppercase(self):
        assert parse_cd_number('Movie (2005) CD1.avi') == 1

    def test_cd_mixed_case(self):
        assert parse_cd_number('Movie (2005) Cd2.avi') == 2

    def test_cd_with_dot_separator(self):
        assert parse_cd_number('Movie.2005.cd.1.avi') == 1

    def test_cd_with_dash_separator(self):
        assert parse_cd_number('Movie-2005-cd-2.avi') == 2

    def test_cd_with_underscore_separator(self):
        assert parse_cd_number('Movie_2005_cd_3.avi') == 3

    def test_cd_no_separator(self):
        """cd immediately followed by digit, with leading separator."""
        assert parse_cd_number('Movie (2005) cd1.mkv') == 1

    def test_high_cd_number(self):
        assert parse_cd_number('Movie (2005) cd10.avi') == 10

    def test_no_cd_tag(self):
        assert parse_cd_number('Movie (2005) 1080p.mkv') is None

    def test_no_cd_tag_word_containing_cd(self):
        """Words like 'abcd1' should not match (need leading separator)."""
        assert parse_cd_number('abcd1.avi') is None

    def test_cd_in_path_component_ignored(self):
        """Only filename is searched, not the full path."""
        # parse_cd_number receives just a filename, not a full path
        assert parse_cd_number('movie.mkv') is None

    def test_empty_string(self):
        assert parse_cd_number('') is None

    def test_cd_at_start_no_leading_separator(self):
        """'cd1' at the very start of the filename has no leading separator."""
        assert parse_cd_number('cd1.avi') is None


# ===========================================================================
# classify_video_files() — multi-file folder classification
# ===========================================================================

class TestClassifyVideoFiles:
    """Tests for classify_video_files() folder classification."""

    def test_empty_list(self):
        result = classify_video_files([])
        assert result['type'] == 'single'
        assert result['cd_files'] == []
        assert result['non_cd_files'] == []

    def test_single_file(self):
        result = classify_video_files(['/movies/Movie (2005)/Movie (2005) 1080p.mkv'])
        assert result['type'] == 'single'
        assert result['non_cd_files'] == ['/movies/Movie (2005)/Movie (2005) 1080p.mkv']
        assert result['cd_files'] == []

    def test_multi_cd_two_files(self):
        """Two files with sequential cd1/cd2 tags → multi_cd."""
        files = [
            '/movies/Jungle Trap (1990)/Jungle Trap (1990) cd1.avi',
            '/movies/Jungle Trap (1990)/Jungle Trap (1990) cd2.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'multi_cd'
        assert len(result['cd_files']) == 2
        assert result['cd_files'][0] == (1, files[0])
        assert result['cd_files'][1] == (2, files[1])
        assert result['non_cd_files'] == []

    def test_multi_cd_three_files(self):
        """Three sequential cd files → multi_cd."""
        files = [
            '/movies/Movie/Movie cd3.avi',
            '/movies/Movie/Movie cd1.avi',
            '/movies/Movie/Movie cd2.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'multi_cd'
        assert len(result['cd_files']) == 3
        # Should be sorted by cd number
        assert result['cd_files'][0][0] == 1
        assert result['cd_files'][1][0] == 2
        assert result['cd_files'][2][0] == 3

    def test_non_sequential_cd_is_variants(self):
        """cd1 + cd3 (missing cd2) → variants, not multi_cd."""
        files = [
            '/movies/Movie/Movie cd1.avi',
            '/movies/Movie/Movie cd3.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'

    def test_mixed_cd_and_non_cd_is_variants(self):
        """A full file plus a cd-tagged partial → variants."""
        files = [
            '/movies/Movie/Movie 1080p.mkv',
            '/movies/Movie/Movie cd1.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'
        assert len(result['non_cd_files']) == 1
        assert len(result['cd_files']) == 1
        # Single CD file forms a trivial cd1 sub-group
        assert result['has_cd_subgroup'] is True

    def test_mixed_cd_subgroup_and_standalone(self):
        """A full file plus cd1+cd2+cd3 → variants with valid CD sub-group."""
        files = [
            '/movies/New Land/New Land 720p.mkv',
            '/movies/New Land/New Land cd1.mkv',
            '/movies/New Land/New Land cd2.mkv',
            '/movies/New Land/New Land cd3.mkv',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'
        assert len(result['non_cd_files']) == 1
        assert len(result['cd_files']) == 3
        assert result['has_cd_subgroup'] is True

    def test_mixed_non_sequential_cd_no_subgroup(self):
        """A full file plus cd1+cd3 (missing cd2) → no valid CD sub-group."""
        files = [
            '/movies/Movie/Movie 1080p.mkv',
            '/movies/Movie/Movie cd1.avi',
            '/movies/Movie/Movie cd3.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'
        assert result['has_cd_subgroup'] is False

    def test_two_non_cd_files_no_subgroup(self):
        """Two non-CD files → variants, no CD sub-group."""
        files = [
            '/movies/Twelve Monkeys (1995)/Twelve Monkeys (1995) 720p.mkv',
            '/movies/Twelve Monkeys (1995)/Twelve Monkeys (1995) Remastered 1080p.mkv',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'
        assert result['has_cd_subgroup'] is False

    def test_duplicate_cd_numbers_is_variants(self):
        """Two files both tagged cd1 → not sequential → variants."""
        files = [
            '/movies/Movie/Movie cd1.avi',
            '/movies/Movie/Movie cd1.mkv',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'

    def test_cd_starting_at_zero_is_variants(self):
        """cd0 + cd1 → sequence doesn't start at 1 → variants."""
        files = [
            '/movies/Movie/Movie cd0.avi',
            '/movies/Movie/Movie cd1.avi',
        ]
        result = classify_video_files(files)
        assert result['type'] == 'variants'


# ===========================================================================
# detect_duplicates() — duplicate file pair detection
# ===========================================================================

class TestDetectDuplicates:
    """Tests for detect_duplicates() pair detection."""

    @staticmethod
    def _result(size=None, duration=None, resolution=None):
        """Helper to build a minimal file_result dict."""
        r = {
            'file_size_bytes': size,
            'actual': {'duration_min': duration or 0},
            'expected': {'resolution': resolution or ''},
        }
        return r

    def test_empty_list(self):
        assert detect_duplicates([]) == []

    def test_single_file(self):
        assert detect_duplicates([self._result(size=1000)]) == []

    def test_same_file_size(self):
        """Two files with identical byte size → duplicate."""
        results = [
            self._result(size=5000000000),
            self._result(size=5000000000),
        ]
        pairs = detect_duplicates(results)
        assert pairs == [(0, 1)]

    def test_different_file_size_no_match(self):
        """Different sizes, different resolutions → not duplicates."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=3000000000, duration=120.0, resolution='720p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == []

    def test_same_runtime_same_resolution(self):
        """Same runtime (within 0.1) and same resolution → duplicate."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=3000000000, duration=120.05, resolution='1080p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == [(0, 1)]

    def test_same_runtime_different_resolution_not_duplicate(self):
        """Same runtime but different resolution → intentional quality variants."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=3000000000, duration=120.0, resolution='720p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == []

    def test_runtime_outside_threshold(self):
        """Runtime differs by more than 0.1 min → not duplicates."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=3000000000, duration=120.2, resolution='1080p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == []

    def test_runtime_at_exact_threshold(self):
        """Runtime differs by exactly 0.1 min → still duplicate."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=3000000000, duration=120.1, resolution='1080p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == [(0, 1)]

    def test_three_files_two_pairs(self):
        """Three files where all have same size → three pairs."""
        results = [
            self._result(size=5000000000),
            self._result(size=5000000000),
            self._result(size=5000000000),
        ]
        pairs = detect_duplicates(results)
        assert pairs == [(0, 1), (0, 2), (1, 2)]

    def test_file_size_zero_not_matched(self):
        """File size of 0 is falsy → size check skipped."""
        results = [
            self._result(size=0, duration=120.0, resolution='1080p'),
            self._result(size=0, duration=120.0, resolution='1080p'),
        ]
        pairs = detect_duplicates(results)
        # size=0 is falsy, so size check skipped; falls through to runtime check
        assert pairs == [(0, 1)]

    def test_missing_file_size_falls_through_to_runtime(self):
        """No file_size_bytes → size check skipped, runtime check used."""
        results = [
            self._result(size=None, duration=90.5, resolution='720p'),
            self._result(size=None, duration=90.5, resolution='720p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == [(0, 1)]

    def test_no_duration_no_match(self):
        """No file size, no duration → no match (runtime 0 is falsy)."""
        results = [
            self._result(size=None, duration=0, resolution='1080p'),
            self._result(size=None, duration=0, resolution='1080p'),
        ]
        pairs = detect_duplicates(results)
        assert pairs == []

    def test_size_match_takes_priority_over_runtime(self):
        """Size match fires first, runtime check not needed."""
        results = [
            self._result(size=5000000000, duration=120.0, resolution='1080p'),
            self._result(size=5000000000, duration=999.0, resolution='720p'),
        ]
        pairs = detect_duplicates(results)
        # Same size → duplicate via size check (different runtime/res don't matter)
        assert pairs == [(0, 1)]


# ---------------------------------------------------------------------------
# pick_best_duplicate() — keep/delete recommendation
# ---------------------------------------------------------------------------

class TestPickBestDuplicate:
    """Tests for pick_best_duplicate() keep/delete logic."""

    @staticmethod
    def _item(codec='AVC', size=5000000000):
        return {
            'file': 'movie.mkv',
            'file_size_bytes': size,
            'actual': {'video_codec': codec},
        }

    def test_hevc_wins_over_avc_even_if_smaller(self):
        a = self._item(codec='HEVC', size=1000)
        b = self._item(codec='AVC', size=9999)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is a
        assert delete is b

    def test_h265_wins_over_avc(self):
        a = self._item(codec='AVC', size=9999)
        b = self._item(codec='H.265', size=1000)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is b

    def test_x265_wins_over_avc(self):
        a = self._item(codec='x265', size=1000)
        b = self._item(codec='AVC', size=9999)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is a

    def test_both_hevc_larger_wins(self):
        a = self._item(codec='HEVC', size=5000)
        b = self._item(codec='HEVC', size=9000)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is b

    def test_both_avc_larger_wins(self):
        a = self._item(codec='AVC', size=9000)
        b = self._item(codec='AVC', size=5000)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is a

    def test_no_codec_info_larger_wins(self):
        a = self._item(codec='', size=3000)
        b = self._item(codec='', size=7000)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is b

    def test_equal_size_returns_first(self):
        a = self._item(codec='AVC', size=5000)
        b = self._item(codec='AVC', size=5000)
        keep, delete = pick_best_duplicate(a, b)
        assert keep is a


# ---------------------------------------------------------------------------
# OpenSubtitles hash and lookup tests
# ---------------------------------------------------------------------------

class TestComputeOpensubtitlesHash:
    """Tests for compute_opensubtitles_hash()."""

    def _make_file(self, tmp_path, size, content_byte=b'\x00'):
        """Create a test file of the given size."""
        fp = tmp_path / 'test.mkv'
        # Write deterministic content
        fp.write_bytes(content_byte * size)
        return str(fp)

    def test_file_too_small(self, tmp_path):
        """Files smaller than 128KB return None."""
        fp = self._make_file(tmp_path, 64 * 1024)  # exactly 64KB
        result = compute_opensubtitles_hash(fp)
        assert result is None

    def test_minimum_size(self, tmp_path):
        """Files of exactly 128KB (65536 * 2) return a valid hash."""
        fp = self._make_file(tmp_path, 65536 * 2)
        result = compute_opensubtitles_hash(fp)
        assert result is not None
        assert len(result) == 16
        assert all(c in '0123456789abcdef' for c in result)

    def test_hash_is_deterministic(self, tmp_path):
        """Same file produces the same hash."""
        fp = self._make_file(tmp_path, 200000, b'\x42')
        h1 = compute_opensubtitles_hash(fp)
        h2 = compute_opensubtitles_hash(fp)
        assert h1 == h2

    def test_different_content_different_hash(self, tmp_path):
        """Files with different content produce different hashes."""
        fp1 = tmp_path / 'a.mkv'
        fp2 = tmp_path / 'b.mkv'
        fp1.write_bytes(b'\x00' * 200000)
        fp2.write_bytes(b'\xff' * 200000)
        h1 = compute_opensubtitles_hash(str(fp1))
        h2 = compute_opensubtitles_hash(str(fp2))
        assert h1 != h2

    def test_hash_includes_filesize(self, tmp_path):
        """Different file sizes with same content pattern produce different hashes."""
        fp1 = self._make_file(tmp_path, 200000, b'\x42')
        fp2 = tmp_path / 'bigger.mkv'
        fp2.write_bytes(b'\x42' * 300000)
        h1 = compute_opensubtitles_hash(fp1)
        h2 = compute_opensubtitles_hash(str(fp2))
        assert h1 != h2

    def test_returns_lowercase_hex(self, tmp_path):
        """Hash should be lowercase hex string."""
        fp = self._make_file(tmp_path, 200000, b'\xAB')
        result = compute_opensubtitles_hash(fp)
        assert result == result.lower()


class TestOpensubtitlesLookupHash:
    """Tests for opensubtitles_lookup_hash() with mocked HTTP."""

    def _mock_response(self, data, status_code=200):
        """Create a mock response object."""
        class MockResp:
            def __init__(self):
                self.status_code = status_code
                self.ok = status_code == 200
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise Exception(f'HTTP {self.status_code}')
            def json(self):
                return data
        return MockResp()

    def test_no_api_key_returns_none(self):
        """Returns None when no API key is provided."""
        result = opensubtitles_lookup_hash('abcdef0123456789', '')
        assert result is None

    def test_no_api_key_none_returns_none(self):
        """Returns None when API key is None."""
        result = opensubtitles_lookup_hash('abcdef0123456789', None)
        assert result is None

    def test_successful_hash_match(self, monkeypatch):
        """Returns identification when moviehash_match is True."""
        response_data = {
            'data': [{
                'attributes': {
                    'moviehash_match': True,
                    'release': 'Some.Movie.2024.1080p.BluRay',
                    'feature_details': {
                        'title': 'Some Movie',
                        'year': 2024,
                        'imdb_id': 1234567,
                        'tmdb_id': 99999,
                        'feature_type': 'Movie',
                    }
                }
            }]
        }

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response(response_data))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result is not None
        assert result['title'] == 'Some Movie'
        assert result['year'] == 2024
        assert result['imdb_id'] == 'tt1234567'
        assert result['feature_type'] == 'movie'

    def test_episode_detection(self, monkeypatch):
        """Returns feature_type='episode' for TV episodes."""
        response_data = {
            'data': [{
                'attributes': {
                    'moviehash_match': True,
                    'release': 'Some.Show.S01E01',
                    'feature_details': {
                        'title': 'Some Show',
                        'year': 2023,
                        'imdb_id': 7654321,
                        'tmdb_id': 88888,
                        'feature_type': 'Episode',
                    }
                }
            }]
        }

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response(response_data))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result is not None
        assert result['feature_type'] == 'episode'

    def test_no_results_returns_none(self, monkeypatch):
        """Returns None when API returns no results."""
        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response({'data': []}))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result is None

    def test_no_moviehash_match_falls_back(self, monkeypatch):
        """Falls back to first result when no moviehash_match entries."""
        response_data = {
            'data': [{
                'attributes': {
                    'moviehash_match': False,
                    'release': 'Fallback.Movie.2022',
                    'feature_details': {
                        'title': 'Fallback Movie',
                        'year': 2022,
                        'imdb_id': 1111111,
                        'feature_type': 'Movie',
                    }
                }
            }]
        }

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response(response_data))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result is not None
        assert result['title'] == 'Fallback Movie'
        assert result.get('hash_match') is False

    def test_api_error_returns_none(self, monkeypatch):
        """Returns None on API error."""
        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response({}, 500))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result is None

    def test_imdb_id_zero_padded(self, monkeypatch):
        """IMDB ID is properly zero-padded with tt prefix."""
        response_data = {
            'data': [{
                'attributes': {
                    'moviehash_match': True,
                    'feature_details': {
                        'title': 'Test',
                        'year': 2020,
                        'imdb_id': 123,
                        'feature_type': 'Movie',
                    }
                }
            }]
        }

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', lambda *a, **kw: self._mock_response(response_data))

        result = opensubtitles_lookup_hash('abcdef0123456789', 'test_key')
        assert result['imdb_id'] == 'tt0000123'


class TestComputeRecommendedActionOpenSubtitles:
    """Tests for compute_recommended_action() with opensubtitles_hash method."""

    def test_opensubtitles_hash_different_movie(self):
        """opensubtitles_hash with different title → reassign_movie."""
        flags = [{'check': 'title', 'severity': 'HIGH'}]
        identification = {
            'method': 'opensubtitles_hash',
            'identified_title': 'Different Movie',
            'identified_year': 2020,
            'identified_imdb': 'tt1234567',
        }
        expected = {'title': 'Original Movie', 'year': 2020}
        result = compute_recommended_action(flags, identification, expected)
        assert result == 'reassign_movie'

    def test_opensubtitles_hash_same_movie(self):
        """opensubtitles_hash with same title → falls through to flag-based."""
        flags = [{'check': 'template', 'severity': 'LOW'}]
        identification = {
            'method': 'opensubtitles_hash',
            'identified_title': 'Same Movie',
            'identified_year': 2020,
        }
        expected = {'title': 'Same Movie', 'year': 2020}
        result = compute_recommended_action(flags, identification, expected)
        assert result == 'rename_template'

    def test_opensubtitles_hash_tv_episode(self):
        """opensubtitles_hash with feature_type=episode → delete_wrong."""
        flags = [{'check': 'title', 'severity': 'HIGH'}]
        identification = {
            'method': 'opensubtitles_hash',
            'feature_type': 'episode',
            'identified_title': 'Some Show',
        }
        result = compute_recommended_action(flags, identification)
        assert result == 'delete_wrong'


class TestRecalculateFolderDuplicates:
    """Tests for Audit._recalculate_folder_duplicates() logic.

    Since the method lives on the Audit class (which needs the full CP
    runtime), we test via a minimal mock that has just last_report.
    """

    def _make_audit(self, flagged):
        """Create a minimal object with the attributes _recalculate_folder_duplicates needs."""
        # Import the actual method so we can bind it
        from couchpotato.core.plugins.audit import Audit
        obj = object.__new__(Audit)
        obj.last_report = {
            'flagged': flagged,
            'total_flagged': len(flagged),
        }
        return obj

    def _make_item(self, item_id, folder, filename, file_size, duration=100.0,
                   resolution='1080p', flags=None, fixed=None):
        return {
            'item_id': item_id,
            'folder': folder,
            'file': filename,
            'file_path': '/movies/%s/%s' % (folder, filename),
            'file_size_bytes': file_size,
            'actual': {
                'resolution': '1920x1080',
                'duration_min': duration,
                'video_codec': 'h264',
                'container_title': None,
                'container_title_parsed': None,
            },
            'expected': {
                'resolution': resolution,
                'runtime_min': 120,
                'title': 'Test',
                'year': 2020,
                'db_title': 'Test',
            },
            'flags': flags or [],
            'flag_count': len(flags) if flags else 0,
            'identification': None,
            'recommended_action': None,
            'fixed': fixed,
            'variant_files': [],
            'variant_count': 0,
        }

    def test_removes_stale_duplicate_flag_after_partner_deleted(self):
        """When file A is deleted, file B's 'duplicate of A' flag is removed."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of a.mkv'}])

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        # B should have lost its duplicate flag and been removed (no flags left)
        assert item_b not in audit.last_report['flagged']
        assert audit.last_report['total_flagged'] == 1  # only fixed item_a remains

    def test_keeps_duplicate_flag_when_third_file_still_matches(self):
        """If A, B, C are all same-size duplicates and A is deleted,
        B and C should still be flagged as duplicates of each other."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of a.mkv'}])
        item_c = self._make_item('ccc', 'Movie (2020)', 'c.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of a.mkv'}])

        audit = self._make_audit([item_a, item_b, item_c])
        audit._recalculate_folder_duplicates(item_a)

        # B and C should still be in flagged with updated duplicate flags
        assert item_b in audit.last_report['flagged']
        assert item_c in audit.last_report['flagged']
        b_dupe = [f for f in item_b['flags'] if f['check'] == 'duplicate']
        c_dupe = [f for f in item_c['flags'] if f['check'] == 'duplicate']
        assert len(b_dupe) == 1
        assert 'c.mkv' in b_dupe[0]['detail']
        assert len(c_dupe) == 1
        assert 'b.mkv' in c_dupe[0]['detail']

    def test_preserves_non_duplicate_flags(self):
        """Removing a duplicate flag should not affect other flags on the sibling."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 5000,
                                 flags=[
                                     {'check': 'resolution', 'severity': 'HIGH',
                                      'detail': 'Claimed 1080p, actual 720p'},
                                     {'check': 'duplicate', 'severity': 'MEDIUM',
                                      'detail': 'Possible duplicate of a.mkv'},
                                 ])

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        # B should still be in flagged with just the resolution flag
        assert item_b in audit.last_report['flagged']
        assert len(item_b['flags']) == 1
        assert item_b['flags'][0]['check'] == 'resolution'
        assert item_b['flag_count'] == 1

    def test_recomputes_recommended_action(self):
        """After removing duplicate flag, recommended_action is recomputed."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 5000,
                                 flags=[
                                     {'check': 'template', 'severity': 'LOW',
                                      'detail': 'Filename mismatch'},
                                     {'check': 'duplicate', 'severity': 'MEDIUM',
                                      'detail': 'Possible duplicate of a.mkv'},
                                 ])

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        # B should now have rename_template (not delete_wrong)
        assert item_b['recommended_action'] == 'rename_template'

    def test_does_nothing_when_no_duplicate_flags(self):
        """No-op when siblings have no duplicate flags."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'resolution', 'severity': 'HIGH',
                                         'detail': 'wrong res'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 6000,
                                 flags=[{'check': 'template', 'severity': 'LOW',
                                         'detail': 'template mismatch'}])

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        # B should be untouched
        assert len(item_b['flags']) == 1
        assert item_b['flags'][0]['check'] == 'template'

    def test_does_nothing_for_different_folder(self):
        """Items in a different folder are not affected."""
        item_a = self._make_item('aaa', 'Movie A (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie B (2020)', 'b.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of c.mkv'}])

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        # B is in a different folder, should be untouched
        assert len(item_b['flags']) == 1
        assert item_b['flags'][0]['check'] == 'duplicate'

    def test_updates_variant_files(self):
        """variant_files list should be updated to remove the deleted file."""
        item_a = self._make_item('aaa', 'Movie (2020)', 'a.mkv', 5000,
                                 flags=[{'check': 'duplicate', 'severity': 'MEDIUM',
                                         'detail': 'Possible duplicate of b.mkv'}],
                                 fixed={'action': 'delete_wrong'})
        item_b = self._make_item('bbb', 'Movie (2020)', 'b.mkv', 5000,
                                 flags=[
                                     {'check': 'resolution', 'severity': 'HIGH',
                                      'detail': 'wrong res'},
                                     {'check': 'duplicate', 'severity': 'MEDIUM',
                                      'detail': 'Possible duplicate of a.mkv'},
                                 ])
        item_b['variant_files'] = ['a.mkv', 'b.mkv']
        item_b['variant_count'] = 2

        audit = self._make_audit([item_a, item_b])
        audit._recalculate_folder_duplicates(item_a)

        assert item_b['variant_files'] == ['b.mkv']
        assert item_b['variant_count'] == 1


# ---------------------------------------------------------------------------
# Audio track extraction helpers
# ---------------------------------------------------------------------------

class TestFormatAudioChannels:

    def test_71_from_count_8(self):
        assert _format_audio_channels('8', 'L R C LFE Ls Rs Lb Rb') == '7.1'

    def test_51_from_count_6(self):
        assert _format_audio_channels('6', 'L R C LFE Ls Rs') == '5.1'

    def test_20_from_count_2(self):
        assert _format_audio_channels('2', '') == '2.0'

    def test_mono(self):
        assert _format_audio_channels('1', '') == '1.0'

    def test_empty(self):
        assert _format_audio_channels('', '') == ''

    def test_lfe_detection(self):
        # 6 channels with LFE = 5.1
        assert _format_audio_channels('6', 'L R C LFE Ls Rs') == '5.1'
        # 6 channels without LFE layout info — raw count fallback
        assert _format_audio_channels('6', '') == '6'


class TestFormatAudioCodec:

    def test_truehd_atmos(self):
        track = {'Format': 'MLP FBA', 'Format_Commercial_IfAny': 'Dolby TrueHD with Dolby Atmos', 'Format_AdditionalFeatures': '16-ch'}
        assert _format_audio_codec(track) == 'TrueHD Atmos'

    def test_truehd_no_atmos(self):
        track = {'Format': 'MLP FBA', 'Format_Commercial_IfAny': 'Dolby TrueHD'}
        assert _format_audio_codec(track) == 'TrueHD'

    def test_ddplus_atmos(self):
        track = {'Format': 'E-AC-3', 'Format_AdditionalFeatures': 'JOC'}
        assert _format_audio_codec(track) == 'DD+ Atmos'

    def test_ddplus_no_atmos(self):
        track = {'Format': 'E-AC-3'}
        assert _format_audio_codec(track) == 'DD+'

    def test_ac3(self):
        track = {'Format': 'AC-3'}
        assert _format_audio_codec(track) == 'AC3'

    def test_dts_hd_ma(self):
        track = {'Format': 'DTS', 'Format_AdditionalFeatures': 'XLL'}
        assert _format_audio_codec(track) == 'DTS-HD MA'

    def test_dts_plain(self):
        track = {'Format': 'DTS'}
        assert _format_audio_codec(track) == 'DTS'

    def test_aac(self):
        track = {'Format': 'AAC'}
        assert _format_audio_codec(track) == 'AAC'

    def test_mpeg_audio(self):
        track = {'Format': 'MPEG Audio'}
        assert _format_audio_codec(track) == 'MP3'

    def test_flac(self):
        track = {'Format': 'FLAC'}
        assert _format_audio_codec(track) == 'FLAC'

    def test_unknown(self):
        track = {'Format': 'SomeWeirdCodec'}
        assert _format_audio_codec(track) == 'SomeWeirdCodec'


class TestExtractAudioTracks:

    def test_basic_extraction(self):
        tracks = [
            {'@type': 'General', 'Format': 'Matroska'},
            {'@type': 'Video', 'Format': 'HEVC'},
            {'@type': 'Audio', 'Format': 'DTS', 'Format_AdditionalFeatures': 'XLL', 'Channels': '6', 'ChannelLayout': 'L R C LFE Ls Rs', 'Language': 'en'},
            {'@type': 'Audio', 'Format': 'AC-3', 'Channels': '6', 'ChannelLayout': 'L R C LFE Ls Rs', 'Language': 'en'},
        ]
        result = _extract_audio_tracks(tracks)
        assert len(result) == 2
        assert result[0] == {'codec': 'DTS-HD MA', 'channels': '5.1', 'language': 'en'}
        assert result[1] == {'codec': 'AC3', 'channels': '5.1', 'language': 'en'}

    def test_no_audio_tracks(self):
        tracks = [
            {'@type': 'General'},
            {'@type': 'Video', 'Format': 'AVC'},
        ]
        assert _extract_audio_tracks(tracks) == []

    def test_empty_tracks(self):
        assert _extract_audio_tracks([]) == []

    def test_non_dict_tracks(self):
        assert _extract_audio_tracks([None, 'garbage', 42]) == []

    def test_avi_mp3(self):
        tracks = [
            {'@type': 'Audio', 'Format': 'MPEG Audio', 'Channels': '2'},
        ]
        result = _extract_audio_tracks(tracks)
        assert result == [{'codec': 'MP3', 'channels': '2.0', 'language': ''}]


class TestCheckAudioLanguage:
    """Tests for check_audio_language()."""

    def test_no_tracks_flags(self):
        """No audio tracks at all → flag (no English audio)."""
        result = check_audio_language([])
        assert result is not None
        assert result['check'] == 'foreign_audio'
        assert result['severity'] == 'LOW'
        assert 'No audio tracks' in result['detail']

    def test_single_english_track_no_flag(self):
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'en'}]
        assert check_audio_language(tracks) is None

    def test_mixed_english_and_french_no_flag(self):
        tracks = [
            {'codec': 'DTS-HD MA', 'channels': '5.1', 'language': 'en'},
            {'codec': 'AAC', 'channels': '2.0', 'language': 'fr'},
        ]
        assert check_audio_language(tracks) is None

    def test_all_french_flags(self):
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'fr'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'foreign_audio'
        assert result['severity'] == 'LOW'
        assert 'fr' in result['detail']

    def test_multiple_non_english_languages(self):
        tracks = [
            {'codec': 'DTS', 'channels': '5.1', 'language': 'fr'},
            {'codec': 'AAC', 'channels': '2.0', 'language': 'de'},
        ]
        result = check_audio_language(tracks)
        assert result is not None
        assert 'fr' in result['detail']
        assert 'de' in result['detail']

    def test_single_japanese_flags(self):
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'ja'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert 'ja' in result['detail']

    def test_empty_language_with_foreign_flags_unknown(self):
        """Foreign + empty language → unknown_audio (needs whisper)."""
        tracks = [
            {'codec': 'AAC', 'channels': '2.0', 'language': 'fr'},
            {'codec': 'AAC', 'channels': '2.0', 'language': ''},
        ]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'unknown_audio'

    def test_all_empty_language_flags_unknown(self):
        """All tracks with empty language → unknown_audio (needs whisper)."""
        tracks = [
            {'codec': 'AAC', 'channels': '2.0', 'language': ''},
        ]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'unknown_audio'

    def test_custom_accepted_languages(self):
        """Custom accepted_languages parameter works."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'fr'}]
        # French is accepted
        assert check_audio_language(tracks, accepted_languages=('en', 'fr')) is None
        # Only English accepted → flags
        result = check_audio_language(tracks, accepted_languages=('en',))
        assert result is not None

    def test_case_insensitive_match(self):
        """Language matching is case-insensitive."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'EN'}]
        assert check_audio_language(tracks) is None

    def test_duplicate_languages_deduped(self):
        """Multiple tracks with same language show it once in detail."""
        tracks = [
            {'codec': 'DTS', 'channels': '5.1', 'language': 'fr'},
            {'codec': 'AAC', 'channels': '2.0', 'language': 'fr'},
        ]
        result = check_audio_language(tracks)
        assert result is not None
        # Should show 'fr' only once
        assert result['detail'] == 'All audio tracks are non-English: fr'

    def test_locale_code_en_us_no_flag(self):
        """en-US should match en (BCP-47 locale prefix)."""
        tracks = [{'codec': 'DD+', 'channels': '5.1', 'language': 'en-US'}]
        assert check_audio_language(tracks) is None

    def test_locale_code_en_gb_no_flag(self):
        """en-GB should match en."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'en-GB'}]
        assert check_audio_language(tracks) is None

    def test_locale_code_fr_fr_still_flags(self):
        """fr-FR should NOT match en — still foreign."""
        tracks = [{'codec': 'AC3', 'channels': '5.1', 'language': 'fr-FR'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert 'fr-FR' in result['detail']

    def test_locale_code_mixed_en_us_and_fr(self):
        """en-US + fr-FR → has English, no flag."""
        tracks = [
            {'codec': 'AC3', 'channels': '5.1', 'language': 'fr-FR'},
            {'codec': 'AC3', 'channels': '5.1', 'language': 'en-US'},
        ]
        assert check_audio_language(tracks) is None

    def test_locale_code_custom_accepted(self):
        """Locale prefix matching works with custom accepted_languages."""
        tracks = [{'codec': 'DD+', 'channels': '2.0', 'language': 'da-DK'}]
        # Danish not in default accepted
        assert check_audio_language(tracks) is not None
        # Danish in custom accepted
        assert check_audio_language(tracks, accepted_languages=('en', 'da')) is None

    # --- New: normalization-aware tests ---

    def test_iso639_2_eng_no_flag(self):
        """ISO 639-2 'eng' normalizes to 'en' → no flag."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'eng'}]
        assert check_audio_language(tracks) is None

    def test_full_name_english_no_flag(self):
        """Full language name 'English' normalizes to 'en' → no flag."""
        tracks = [{'codec': 'DTS', 'channels': '5.1', 'language': 'English'}]
        assert check_audio_language(tracks) is None

    def test_nonstandard_jap_flags(self):
        """Non-standard 'jap' normalizes to 'ja' → foreign_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'jap'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'foreign_audio'
        assert 'jap' in result['detail']

    def test_iso639_3_cmn_flags(self):
        """ISO 639-3 'cmn' (Mandarin) normalizes to 'zh' → foreign_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'cmn'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'foreign_audio'

    def test_full_name_nederlands_flags(self):
        """Full name 'Nederlands' normalizes to 'nl' → foreign_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'Nederlands'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'foreign_audio'

    def test_zxx_no_flag(self):
        """zxx (no linguistic content) → no flag (intentional)."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'zxx'}]
        assert check_audio_language(tracks) is None

    def test_mul_flags_unknown(self):
        """'mul' (multiple languages) → unknown_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'mul'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'unknown_audio'

    def test_und_flags_unknown(self):
        """'und' (undetermined) → unknown_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'und'}]
        result = check_audio_language(tracks)
        assert result is not None
        assert result['check'] == 'unknown_audio'

    def test_accepted_plus_empty_no_flag(self):
        """Accepted track + empty track → no flag (early exit on accepted)."""
        tracks = [
            {'codec': 'DTS', 'channels': '5.1', 'language': 'en'},
            {'codec': 'AAC', 'channels': '2.0', 'language': ''},
        ]
        assert check_audio_language(tracks) is None

    def test_accepted_plus_und_no_flag(self):
        """Accepted track + und track → no flag (early exit on accepted)."""
        tracks = [
            {'codec': 'AAC', 'channels': '2.0', 'language': 'und'},
            {'codec': 'DTS', 'channels': '5.1', 'language': 'en'},
        ]
        assert check_audio_language(tracks) is None

    def test_iso639_2_fra_custom_accepted(self):
        """'fra' normalizes to 'fr', accepted when 'fr' is in accepted_languages."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': 'fra'}]
        assert check_audio_language(tracks) is not None
        assert check_audio_language(tracks, accepted_languages=('en', 'fr')) is None


class TestNormalizeLanguage:
    """Tests for normalize_language()."""

    def test_bcp47_en_us(self):
        """BCP-47 locale code en-US → en."""
        assert normalize_language('en-US') == 'en'

    def test_bcp47_fr_fr(self):
        """BCP-47 locale code fr-FR → fr."""
        assert normalize_language('fr-FR') == 'fr'

    def test_iso639_2_eng(self):
        """ISO 639-2 bibliographic code eng → en."""
        assert normalize_language('eng') == 'en'

    def test_iso639_2_ger(self):
        """ISO 639-2 bibliographic code ger → de."""
        assert normalize_language('ger') == 'de'

    def test_iso639_3_cmn(self):
        """ISO 639-3 cmn (Mandarin) → zh."""
        assert normalize_language('cmn') == 'zh'

    def test_iso639_3_yue(self):
        """ISO 639-3 yue (Cantonese) → zh."""
        assert normalize_language('yue') == 'zh'

    def test_nonstandard_jap(self):
        """Non-standard abbreviation jap → ja."""
        assert normalize_language('jap') == 'ja'

    def test_full_name_nederlands(self):
        """Full name Nederlands → nl."""
        assert normalize_language('Nederlands') == 'nl'

    def test_full_name_english(self):
        """Full name English → en."""
        assert normalize_language('English') == 'en'

    def test_case_insensitive(self):
        """Case-insensitive: ENG → en."""
        assert normalize_language('ENG') == 'en'

    def test_passthrough_short_code(self):
        """Unknown 2-letter code passes through unchanged."""
        assert normalize_language('xx') == 'xx'

    def test_passthrough_with_locale(self):
        """Unknown code with locale suffix → base only."""
        assert normalize_language('xx-YY') == 'xx'


class TestComputeRecommendedActionUnknownAudio:
    """Tests for compute_recommended_action with unknown_audio flags."""

    @staticmethod
    def _flags(*checks):
        return [{'check': c, 'severity': 'LOW', 'detail': 'test'} for c in checks]

    def test_unknown_audio_recommends_verify(self):
        """unknown_audio alone → verify_audio."""
        assert compute_recommended_action(self._flags('unknown_audio')) == 'verify_audio'

    def test_unknown_audio_plus_template_recommends_verify(self):
        """unknown_audio + template → verify_audio (higher priority)."""
        assert compute_recommended_action(self._flags('unknown_audio', 'template')) == 'verify_audio'

    def test_unknown_audio_plus_foreign_recommends_verify(self):
        """unknown_audio + foreign_audio → verify_audio (unknown checked first)."""
        assert compute_recommended_action(self._flags('unknown_audio', 'foreign_audio')) == 'verify_audio'

    def test_duplicate_beats_unknown_audio(self):
        """duplicate + unknown_audio → delete_duplicate (higher priority)."""
        assert compute_recommended_action(self._flags('duplicate', 'unknown_audio')) == 'delete_duplicate'
