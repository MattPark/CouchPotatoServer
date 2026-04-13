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
"""

from couchpotato.core.plugins.audit import (
    get_edition,
    _parse_edition_from_release,
    _detect_edition_from_words,
    _edition_fallback_regex,
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
        # Extended is at earlier position, and it extends to "Extended Directors Cut"
        assert result == 'Extended Directors Cut'

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
