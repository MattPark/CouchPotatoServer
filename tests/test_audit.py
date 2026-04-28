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
    whisper_verify_audio,
    _run_whisper_detection,
    _scan_single_file,
    needs_identification,
    ISO_639_1_TO_639_2,
    load_cp_database,
    validate_srrdb_imdb,
    identify_flagged_file,
    _preview_delete_wrong,
    _preview_rename_resolution,
    _preview_rename_edition,
    execute_fix_delete_wrong,
    execute_fix_rename_resolution,
    execute_fix_rename_edition,
    generate_fix_preview,
    parse_guessit_tokens,
)
import json
import os
import re
import subprocess
from unittest import mock


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


class TestFileKnowledge:
    """Tests for the DB-backed file knowledge system.

    Uses a real TinyDB instance (temp file) and mocks get_db() to return it.
    """

    @staticmethod
    def _make_db(tmp_path):
        """Create a real CouchDB instance backed by a temp directory."""
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        """Create a minimal mock that has the Audit DB methods bound."""
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            pass

        p = _MockPlugin()
        for name in ('_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_is_ignored',
                     '_get_knowledge_stats', '_upsert_scan_knowledge',
                     '_prune_file_knowledge', '_ensure_original_hashes',
                     '_post_modification_update', '_cache_identification'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_get_knowledge_not_found(self, tmp_path):
        """Missing fingerprint returns None."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            assert p._get_knowledge('nonexistent:fp') is None

    def test_get_or_create_creates_new(self, tmp_path):
        """First call creates a new doc."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv',
                                             release_id='rel1', media_id='med1')
            assert doc is not None
            assert doc['_t'] == 'file_knowledge'
            assert doc['original_fingerprint'] == '100:aabbccdd'
            assert doc['current_fingerprint'] == '100:aabbccdd'
            assert doc['release_id'] == 'rel1'
            assert doc['media_id'] == 'med1'
            assert doc['file_path'] == '/movies/a.mkv'
            assert doc['crc32'] is None
            assert doc['opensubtitles_hash'] is None
            assert doc['ignored'] is None
            assert doc['whisper'] is None
            assert doc['modified'] is False
            assert doc['modifications'] == []

    def test_get_or_create_returns_existing(self, tmp_path):
        """Second call returns existing doc, updates last_seen."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc1 = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv')
            first_seen = doc1['first_seen']
            doc2 = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv')
            assert doc2['_id'] == doc1['_id']
            assert doc2['first_seen'] == first_seen

    def test_get_or_create_updates_path(self, tmp_path):
        """If file_path changed (file moved), it's updated."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc1 = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv')
            doc2 = p._get_or_create_knowledge('100:aabbccdd', '/movies/b.mkv')
            assert doc2['file_path'] == '/movies/b.mkv'
            assert doc2['_id'] == doc1['_id']

    def test_get_or_create_backfills_ids(self, tmp_path):
        """Missing release_id/media_id are backfilled on subsequent call."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc1 = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv')
            assert doc1['release_id'] is None
            doc2 = p._get_or_create_knowledge('100:aabbccdd', '/movies/a.mkv',
                                              release_id='rel1', media_id='med1')
            assert doc2['release_id'] == 'rel1'
            assert doc2['media_id'] == 'med1'

    def test_is_ignored_empty(self, tmp_path):
        """No docs → nothing is ignored."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            assert p._is_ignored('123:abc') is False

    def test_is_ignored_with_entry(self, tmp_path):
        """Doc with ignored field → is ignored."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('123:abc', '/movies/a.mkv')
            doc['ignored'] = {'reason': 'test'}
            p._update_knowledge(doc)
            assert p._is_ignored('123:abc') is True

    def test_is_ignored_entry_without_ignored_key(self, tmp_path):
        """Doc without ignored field (e.g. only whisper data) → not ignored."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('123:abc', '/movies/a.mkv')
            doc['whisper'] = {'language': 'en'}
            p._update_knowledge(doc)
            assert p._is_ignored('123:abc') is False

    def test_is_ignored_null_fingerprint(self, tmp_path):
        """None fingerprint → not ignored (no crash)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            assert p._is_ignored(None) is False
            assert p._is_ignored('') is False

    def test_get_knowledge_stats(self, tmp_path):
        """Stats reflect actual DB contents."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            # Empty
            stats = p._get_knowledge_stats()
            assert stats['total'] == 0

            # Add some docs
            doc1 = p._get_or_create_knowledge('aaa:1111', '/a.mkv')
            doc1['ignored'] = {'reason': ''}
            p._update_knowledge(doc1)

            doc2 = p._get_or_create_knowledge('bbb:2222', '/b.mkv')
            doc2['whisper'] = {'language': 'en'}
            p._update_knowledge(doc2)

            doc3 = p._get_or_create_knowledge('ccc:3333', '/c.mkv')
            doc3['identification'] = {'method': 'srrdb_crc'}
            p._update_knowledge(doc3)

            stats = p._get_knowledge_stats()
            assert stats['total'] == 3
            assert stats['ignored'] == 1
            assert stats['whisper_verified'] == 1
            assert stats['identified'] == 1
            assert stats['modified'] == 0

    def test_upsert_scan_knowledge(self, tmp_path):
        """Upsert creates docs for all seen files."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            seen_fps = {
                'aaa:1111': '/movies/a.mkv',
                'bbb:2222': '/movies/b.mkv',
                'ccc:3333': '/movies/c.mkv',
            }
            release_by_filepath = {
                '/movies/a.mkv': {'release_id': 'r1', 'media_id': 'm1'},
                '/movies/b.mkv': {'release_id': 'r2', 'media_id': 'm2'},
            }
            p._upsert_scan_knowledge(seen_fps, release_by_filepath, [])

            # All three should exist
            assert p._get_knowledge('aaa:1111') is not None
            assert p._get_knowledge('bbb:2222') is not None
            assert p._get_knowledge('ccc:3333') is not None

            # Release info populated where available
            doc_a = p._get_knowledge('aaa:1111')
            assert doc_a['release_id'] == 'r1'
            doc_c = p._get_knowledge('ccc:3333')
            assert doc_c['release_id'] is None  # not in release_by_filepath


class TestWhisperVerifyAudio:
    """Tests for whisper language verification functions."""

    def test_file_not_found(self):
        """Non-existent file → error."""
        result = whisper_verify_audio('/nonexistent/movie.mkv')
        assert result['error'] == 'File not found'
        assert result['language'] is None

    def test_model_not_found(self, tmp_path):
        """Non-existent model → error."""
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        result = whisper_verify_audio(str(f), model_path='/nonexistent/model.bin')
        assert 'model not found' in result['error'].lower()
        assert result['language'] is None

    @mock.patch('couchpotato.core.plugins.audit._get_media_duration', return_value=5.0)
    def test_file_too_short(self, mock_dur, tmp_path):
        """File shorter than 10 seconds → error."""
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        model = tmp_path / 'model.bin'
        model.write_bytes(b'\x00' * 100)
        result = whisper_verify_audio(str(f), model_path=str(model))
        assert 'too short' in result['error'].lower()

    @mock.patch('couchpotato.core.plugins.audit.shutil.rmtree')
    @mock.patch('couchpotato.core.plugins.audit._run_whisper_detection',
                return_value=('en', 0.95))
    @mock.patch('couchpotato.core.plugins.audit._extract_audio_sample',
                return_value=True)
    @mock.patch('couchpotato.core.plugins.audit._get_media_duration',
                return_value=120.0)
    def test_high_confidence_single_sample(self, mock_dur, mock_extract,
                                           mock_whisper, mock_rmtree, tmp_path):
        """High confidence (>0.70) on first sample → returns immediately."""
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        model = tmp_path / 'model.bin'
        model.write_bytes(b'\x00' * 100)

        result = whisper_verify_audio(str(f), model_path=str(model))

        assert result['language'] == 'en'
        assert result['confidence'] == 0.95
        assert len(result['tracks']) == 1
        assert result['tracks'][0]['samples'][0]['offset_pct'] == 50
        # Only one whisper call (no retry)
        assert mock_whisper.call_count == 1

    @mock.patch('couchpotato.core.plugins.audit.shutil.rmtree')
    @mock.patch('couchpotato.core.plugins.audit._run_whisper_detection')
    @mock.patch('couchpotato.core.plugins.audit._extract_audio_sample',
                return_value=True)
    @mock.patch('couchpotato.core.plugins.audit._get_media_duration',
                return_value=120.0)
    def test_low_confidence_triggers_retry(self, mock_dur, mock_extract,
                                            mock_whisper, mock_rmtree, tmp_path):
        """Low confidence on first sample → retries at 25% and 75%."""
        # First call: low confidence. Second and third: better.
        mock_whisper.side_effect = [
            ('en', 0.50),   # 50% — low
            ('en', 0.85),   # 25% — high
            ('en', 0.80),   # 75% — decent
        ]
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        model = tmp_path / 'model.bin'
        model.write_bytes(b'\x00' * 100)

        result = whisper_verify_audio(str(f), model_path=str(model))

        assert result['language'] == 'en'
        assert result['confidence'] == 0.85  # best of all samples
        assert len(result['tracks'][0]['samples']) == 3
        assert mock_whisper.call_count == 3

    @mock.patch('couchpotato.core.plugins.audit.shutil.rmtree')
    @mock.patch('couchpotato.core.plugins.audit._run_whisper_detection',
                return_value=(None, 0.0))
    @mock.patch('couchpotato.core.plugins.audit._extract_audio_sample',
                return_value=True)
    @mock.patch('couchpotato.core.plugins.audit._get_media_duration',
                return_value=120.0)
    def test_whisper_fails_all_samples(self, mock_dur, mock_extract,
                                       mock_whisper, mock_rmtree, tmp_path):
        """Whisper returns nothing for all samples → language is None."""
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        model = tmp_path / 'model.bin'
        model.write_bytes(b'\x00' * 100)

        result = whisper_verify_audio(str(f), model_path=str(model))

        assert result['language'] is None
        assert result['confidence'] == 0.0
        assert len(result['tracks'][0]['samples']) == 3

    @mock.patch('couchpotato.core.plugins.audit.shutil.rmtree')
    @mock.patch('couchpotato.core.plugins.audit._extract_audio_sample',
                return_value=False)
    @mock.patch('couchpotato.core.plugins.audit._get_media_duration',
                return_value=120.0)
    def test_extract_fails(self, mock_dur, mock_extract,
                            mock_rmtree, tmp_path):
        """Audio extraction failure → error."""
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        model = tmp_path / 'model.bin'
        model.write_bytes(b'\x00' * 100)

        result = whisper_verify_audio(str(f), model_path=str(model))

        assert 'error' in result
        assert 'extract' in result['error'].lower()


class TestRunWhisperDetection:
    """Tests for _run_whisper_detection output parsing."""

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    def test_parses_language_from_stderr(self, mock_run):
        """Parses 'auto-detected language: en (p = 0.95)' from stderr."""
        mock_run.return_value = mock.Mock(
            stderr='whisper_full_with_state: auto-detected language: en (p = 0.95)\n',
            stdout='[00:00:00.000 --> 00:00:05.000] Hello world\n',
        )
        lang, conf = _run_whisper_detection('/tmp/test.wav', '/models/model.bin')
        assert lang == 'en'
        assert abs(conf - 0.95) < 0.001

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    def test_no_match_returns_none(self, mock_run):
        """No language detection line → returns (None, 0.0)."""
        mock_run.return_value = mock.Mock(
            stderr='whisper_init_from_file: loaded model\n',
            stdout='',
        )
        lang, conf = _run_whisper_detection('/tmp/test.wav', '/models/model.bin')
        assert lang is None
        assert conf == 0.0

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run',
                side_effect=Exception('command not found'))
    def test_exception_returns_none(self, mock_run):
        """Exception during subprocess → returns (None, 0.0)."""
        lang, conf = _run_whisper_detection('/tmp/test.wav', '/models/model.bin')
        assert lang is None
        assert conf == 0.0


# ---------------------------------------------------------------------------
# Smart skip logic for identification
# ---------------------------------------------------------------------------

class TestNeedsIdentification:
    """Tests for needs_identification() smart skip logic."""

    @staticmethod
    def _flags(*checks):
        return [{'check': c, 'severity': 'LOW', 'detail': 'test'} for c in checks]

    def test_title_needs_identification(self):
        assert needs_identification(self._flags('title')) is True

    def test_runtime_needs_identification(self):
        assert needs_identification(self._flags('runtime')) is True

    def test_title_and_runtime(self):
        assert needs_identification(self._flags('title', 'runtime')) is True

    def test_template_only_skips(self):
        assert needs_identification(self._flags('template')) is False

    def test_resolution_only_skips(self):
        assert needs_identification(self._flags('resolution')) is False

    def test_edition_only_skips(self):
        assert needs_identification(self._flags('edition')) is False

    def test_naming_combo_skips(self):
        assert needs_identification(self._flags('template', 'resolution', 'edition')) is False

    def test_tv_episode_skips(self):
        assert needs_identification(self._flags('tv_episode', 'title')) is False

    def test_foreign_audio_only_skips(self):
        assert needs_identification(self._flags('foreign_audio')) is False

    def test_unknown_audio_only_skips(self):
        assert needs_identification(self._flags('unknown_audio')) is False

    def test_foreign_audio_plus_template_skips(self):
        assert needs_identification(self._flags('foreign_audio', 'template')) is False

    def test_foreign_audio_plus_title_needs(self):
        """foreign_audio + title mismatch still needs identification."""
        assert needs_identification(self._flags('foreign_audio', 'title')) is True

    def test_unknown_audio_plus_resolution_skips(self):
        assert needs_identification(self._flags('unknown_audio', 'resolution')) is False

    def test_all_skip_checks_combined(self):
        assert needs_identification(self._flags(
            'resolution', 'edition', 'template', 'foreign_audio', 'unknown_audio'
        )) is False


class TestApplyWhisperResult:
    """Tests for _apply_whisper_result flag reclassification.

    Uses a real TinyDB instance and mocks get_db() to provide DB-backed
    file_knowledge lookups (_get_or_create_knowledge / _update_knowledge).
    """

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            last_report = None

        p = _MockPlugin()
        for name in ('_apply_whisper_result',
                      '_get_knowledge', '_get_or_create_knowledge',
                      '_update_knowledge'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_english_detected_clears_flag(self, tmp_path):
        """Whisper detects English but tag is wrong → audio_mislabeled flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test123',
            'file_fingerprint': '100:aabb',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'en', 'confidence': 0.95,
                  'tracks': [{'track_index': 0, 'tagged_language': '',
                              'language': 'en', 'confidence': 0.95}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'unknown_audio' not in checks
        assert 'foreign_audio' not in checks
        # English detected but tag is empty → audio_mislabeled (needs tag fix)
        assert 'audio_mislabeled' in checks
        assert item['flag_count'] == 1
        assert item['recommended_action'] == 'set_audio_language'

    def test_foreign_detected_reclassifies(self, tmp_path):
        """Whisper detects French → unknown_audio replaced with foreign_audio."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test456',
            'file_fingerprint': '200:ccdd',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'fr', 'confidence': 0.92,
                  'tracks': [{'track_index': 0, 'tagged_language': 'fr',
                              'language': 'fr', 'confidence': 0.92}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'unknown_audio' not in checks
        assert 'foreign_audio' in checks
        assert 'Whisper' in item['flags'][0]['detail']

    def test_stores_in_file_knowledge(self, tmp_path):
        """Result is stored in file_knowledge DB doc."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test789',
            'file_fingerprint': '300:eeff',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'en', 'confidence': 0.95,
                  'tracks': [{'track_index': 0, 'tagged_language': '',
                              'language': 'en', 'confidence': 0.95,
                              'samples': [{'offset_pct': 50, 'language': 'en', 'confidence': 0.95}]}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

            doc = p._get_knowledge('300:eeff')
        assert doc is not None
        whisper_data = doc['whisper']
        assert whisper_data['language'] == 'en'
        assert whisper_data['confidence'] == 0.95
        assert len(whisper_data['tracks']) == 1

    def test_failed_detection_leaves_flags(self, tmp_path):
        """Whisper returns None language → flags unchanged."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'testfail',
            'file_fingerprint': '400:1122',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': None, 'confidence': 0.0, 'samples': []}

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'unknown_audio' in checks

    def test_existing_foreign_audio_not_duplicated(self, tmp_path):
        """Item with existing foreign_audio + verify → single foreign_audio flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'testdedup',
            'file_fingerprint': '500:3344',
            'flags': [
                {'check': 'foreign_audio', 'severity': 'LOW',
                 'detail': 'Audio language is ja, expected en'},
            ],
            'flag_count': 1,
            'recommended_action': 'delete_foreign',
        }
        result = {
            'language': 'ja', 'confidence': 0.97,
            'tracks': [{'track_index': 0, 'tagged_language': 'ja',
                         'language': 'ja', 'confidence': 0.97, 'samples': []}],
        }

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

        foreign_flags = [f for f in item['flags'] if f['check'] == 'foreign_audio']
        assert len(foreign_flags) == 1, 'Should have exactly one foreign_audio flag'
        assert 'Whisper' in foreign_flags[0]['detail']

    def test_whisper_english_clears_foreign_audio(self, tmp_path):
        """Item with foreign_audio + whisper says English → audio_mislabeled (tag wrong)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'testclear',
            'file_fingerprint': '600:5566',
            'flags': [
                {'check': 'foreign_audio', 'severity': 'LOW',
                 'detail': 'Audio language is de, expected en'},
            ],
            'flag_count': 1,
            'recommended_action': 'delete_foreign',
        }
        result = {
            'language': 'en', 'confidence': 0.99,
            'tracks': [{'track_index': 0, 'tagged_language': 'de',
                         'language': 'en', 'confidence': 0.99, 'samples': []}],
        }

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' not in checks
        # Tagged 'de' but actually English → audio_mislabeled
        assert 'audio_mislabeled' in checks
        assert item['flag_count'] == 1
        assert item['recommended_action'] == 'set_audio_language'


# ---------------------------------------------------------------------------
# Foreign film detection via original_language
# ---------------------------------------------------------------------------

class TestForeignFilmDetection:
    """Tests for original_language propagation and foreign film logic."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            last_report = None

        p = _MockPlugin()
        for name in ('_apply_whisper_result',
                      '_get_knowledge', '_get_or_create_knowledge',
                      '_update_knowledge'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_load_cp_database_includes_original_language(self, tmp_path):
        """load_cp_database extracts original_language from media info."""
        db_file = tmp_path / 'db.json'
        db_file.write_text(json.dumps({'_default': {
            '1': {
                '_t': 'media', 'type': 'movie',
                'title': 'Foreign Movie',
                'identifiers': {'imdb': 'tt1234567'},
                'info': {'year': 2020, 'runtime': 120,
                         'original_language': 'ko'},
                '_id': 'abc123',
            }
        }}))
        media_by_imdb, _, _, _ = load_cp_database(str(db_file))
        assert media_by_imdb['tt1234567']['original_language'] == 'ko'

    def test_load_cp_database_missing_original_language(self, tmp_path):
        """load_cp_database defaults to empty string when original_language absent."""
        db_file = tmp_path / 'db.json'
        db_file.write_text(json.dumps({'_default': {
            '1': {
                '_t': 'media', 'type': 'movie',
                'title': 'Old Movie',
                'identifiers': {'imdb': 'tt0000001'},
                'info': {'year': 2010, 'runtime': 90},
                '_id': 'def456',
            }
        }}))
        media_by_imdb, _, _, _ = load_cp_database(str(db_file))
        assert media_by_imdb['tt0000001']['original_language'] == ''

    @mock.patch('couchpotato.core.plugins.audit.extract_file_meta')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='12345:abcdef0123456789')
    def test_scan_single_file_propagates_original_language(
            self, mock_fp, mock_meta, tmp_path):
        """_scan_single_file includes original_language from db_entry."""
        f = tmp_path / 'Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        mock_meta.return_value = {
            'resolution_width': 720, 'resolution_height': 480,
            'duration_min': 120.0, 'video_codec': 'H.264',
            'container_title': None,
            'audio_tracks': [{'codec': 'AAC', 'channels': '2.0',
                              'language': 'en'}],
        }
        db_entry = {'title': 'Movie', 'runtime': 120,
                    'original_language': 'fr'}
        result = _scan_single_file(
            str(f), folder_title='Movie', folder_year=2020,
            imdb_id='tt0000001', db_entry=db_entry,
            expected_runtime=120, renamer_template=None,
            renamer_replace_doubles=True, renamer_separator='',
        )
        assert result is not None
        assert result['original_language'] == 'fr'

    @mock.patch('couchpotato.core.plugins.audit.extract_file_meta')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='12345:abcdef0123456789')
    def test_scan_single_file_no_db_entry_empty_language(
            self, mock_fp, mock_meta, tmp_path):
        """_scan_single_file returns empty original_language when no db_entry."""
        f = tmp_path / 'Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        mock_meta.return_value = {
            'resolution_width': 720, 'resolution_height': 480,
            'duration_min': 120.0, 'video_codec': 'H.264',
            'container_title': None,
            'audio_tracks': [{'codec': 'AAC', 'channels': '2.0',
                              'language': 'en'}],
        }
        result = _scan_single_file(
            str(f), folder_title='Movie', folder_year=2020,
            imdb_id='tt0000001', db_entry=None,
            expected_runtime=120, renamer_template=None,
            renamer_replace_doubles=True, renamer_separator='',
        )
        assert result is not None
        assert result['original_language'] == ''

    def test_whisper_foreign_film_enriches_detail(self, tmp_path):
        """Whisper foreign result on a foreign film includes original_language in detail."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test_foreign',
            'file_fingerprint': '100:aabb',
            'file_path': '/tmp/test.mkv',
            'original_language': 'ko',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW',
                        'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'ko', 'confidence': 0.95,
                  'tracks': [{'track_index': 0, 'tagged_language': 'ko',
                              'language': 'ko', 'confidence': 0.95}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db',
                        return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' in checks
        assert item['recommended_action'] == 'delete_foreign'
        # Detail should mention original language
        detail = item['flags'][0]['detail']
        assert 'original language: ko' in detail

    def test_whisper_foreign_on_english_film_no_original_lang_detail(
            self, tmp_path):
        """Whisper foreign result on film without original_language uses generic detail."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test_nolang',
            'file_fingerprint': '200:ccdd',
            'file_path': '/tmp/test2.mkv',
            'original_language': '',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW',
                        'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'fr', 'confidence': 0.90,
                  'tracks': [{'track_index': 0, 'tagged_language': 'fr',
                              'language': 'fr', 'confidence': 0.90}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db',
                        return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' in checks
        detail = item['flags'][0]['detail']
        assert 'original language' not in detail
        assert 'Whisper:' in detail

    def test_whisper_foreign_on_english_film_flags_correctly(self, tmp_path):
        """Whisper detects French on an English-original film — still foreign_audio."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        item = {
            'item_id': 'test_en_film_fr_audio',
            'file_fingerprint': '300:eeff',
            'file_path': '/tmp/test3.mkv',
            'original_language': 'en',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW',
                        'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }
        result = {'language': 'fr', 'confidence': 0.88,
                  'tracks': [{'track_index': 0, 'tagged_language': 'fr',
                              'language': 'fr', 'confidence': 0.88}]}

        with mock.patch('couchpotato.core.plugins.audit.get_db',
                        return_value=db):
            p._apply_whisper_result(item, result)

        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' in checks
        # English-original film with French audio — not marked as foreign film
        detail = item['flags'][0]['detail']
        assert 'original language' not in detail

class TestSeenFingerprints:
    """Tests that _scan_single_file collects fingerprints for all files."""

    @mock.patch('couchpotato.core.plugins.audit.extract_file_meta')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='12345:abcdef0123456789')
    def test_clean_file_adds_fingerprint(self, mock_fp, mock_meta, tmp_path):
        """Clean (unflagged) files still add their fingerprint to the dict."""
        f = tmp_path / 'Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        mock_meta.return_value = {
            'resolution_width': 1920, 'resolution_height': 1080,
            'duration_min': 120.0, 'video_codec': 'HEVC',
            'container_title': None, 'audio_tracks': [
                {'codec': 'AAC', 'channels': '2.0', 'language': 'en'}
            ],
        }
        seen = {}
        result = _scan_single_file(
            str(f), folder_title='Movie', folder_year=2020,
            imdb_id='tt0000001', db_entry={'title': 'Movie', 'runtime': 120},
            expected_runtime=120, renamer_template=None,
            renamer_replace_doubles=True, renamer_separator='',
            seen_fingerprints=seen,
        )
        assert result is None, 'File should be clean (no flags)'
        assert '12345:abcdef0123456789' in seen
        assert seen['12345:abcdef0123456789'] == str(f)

    @mock.patch('couchpotato.core.plugins.audit.extract_file_meta')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='99999:ffff000011112222')
    def test_flagged_file_adds_fingerprint(self, mock_fp, mock_meta, tmp_path):
        """Flagged files also add their fingerprint to the dict."""
        f = tmp_path / 'Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        mock_meta.return_value = {
            'resolution_width': 1920, 'resolution_height': 1080,
            'duration_min': 120.0, 'video_codec': 'HEVC',
            'container_title': None, 'audio_tracks': [
                {'codec': 'AAC', 'channels': '2.0', 'language': 'fr'}
            ],
        }
        seen = {}
        result = _scan_single_file(
            str(f), folder_title='Movie', folder_year=2020,
            imdb_id='tt0000001', db_entry={'title': 'Movie', 'runtime': 120},
            expected_runtime=120, renamer_template=None,
            renamer_replace_doubles=True, renamer_separator='',
            seen_fingerprints=seen,
        )
        assert result is not None, 'File should be flagged (foreign audio)'
        assert '99999:ffff000011112222' in seen
        assert seen['99999:ffff000011112222'] == str(f)
        assert result['file_fingerprint'] == '99999:ffff000011112222'

    @mock.patch('couchpotato.core.plugins.audit.extract_file_meta')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='12345:abcdef0123456789')
    def test_none_seen_dict_is_safe(self, mock_fp, mock_meta, tmp_path):
        """Passing seen_fingerprints=None doesn't crash."""
        f = tmp_path / 'Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        mock_meta.return_value = {
            'resolution_width': 1920, 'resolution_height': 1080,
            'duration_min': 120.0, 'video_codec': 'HEVC',
            'container_title': None, 'audio_tracks': [
                {'codec': 'AAC', 'channels': '2.0', 'language': 'en'}
            ],
        }
        # Should not raise
        result = _scan_single_file(
            str(f), folder_title='Movie', folder_year=2020,
            imdb_id='tt0000001', db_entry={'title': 'Movie', 'runtime': 120},
            expected_runtime=120, renamer_template=None,
            renamer_replace_doubles=True, renamer_separator='',
            seen_fingerprints=None,
        )
        assert result is None


# ---------------------------------------------------------------------------
# File knowledge pruning
# ---------------------------------------------------------------------------

class TestPruneFileKnowledge:
    """Tests for DB-backed _prune_file_knowledge."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            pass

        p = _MockPlugin()
        for name in ('_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_prune_file_knowledge'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_removes_stale_entries(self, tmp_path):
        """Entries not in seen dict are removed."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('aaa:1111', '/a.mkv')
            p._get_or_create_knowledge('bbb:2222', '/b.mkv')
            p._get_or_create_knowledge('ccc:3333', '/c.mkv')

            seen = {'aaa:1111': '/a.mkv', 'ccc:3333': '/c.mkv'}
            p._prune_file_knowledge(seen)

            assert p._get_knowledge('bbb:2222') is None
            assert p._get_knowledge('aaa:1111') is not None
            assert p._get_knowledge('ccc:3333') is not None

    def test_no_stale_entries(self, tmp_path):
        """When all entries are in seen dict, nothing is removed."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('aaa:1111', '/a.mkv')

            seen = {'aaa:1111': '/a.mkv', 'bbb:2222': '/b.mkv'}
            p._prune_file_knowledge(seen)

            assert p._get_knowledge('aaa:1111') is not None

    def test_empty_knowledge(self, tmp_path):
        """Empty DB is a no-op."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._prune_file_knowledge({'aaa:1111': '/a.mkv'})
            # Should not raise

    def test_empty_seen_dict(self, tmp_path):
        """Empty seen dict is a no-op (safety: don't wipe everything)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('aaa:1111', '/a.mkv')
            p._prune_file_knowledge({})
            assert p._get_knowledge('aaa:1111') is not None

    def test_all_stale(self, tmp_path):
        """All entries stale — all removed."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('aaa:1111', '/a.mkv')
            p._get_or_create_knowledge('bbb:2222', '/b.mkv')

            seen = {'ccc:3333': '/c.mkv', 'ddd:4444': '/d.mkv'}
            p._prune_file_knowledge(seen)

            assert p._get_knowledge('aaa:1111') is None
            assert p._get_knowledge('bbb:2222') is None


class TestEnsureOriginalHashes:
    """Tests for _ensure_original_hashes pre-modification guard."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            pass

        p = _MockPlugin()
        for name in ('_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_ensure_original_hashes',
                     '_post_modification_update', '_cache_identification'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef0123456789')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_computes_missing_hashes(self, mock_crc, mock_os_hash, tmp_path):
        """Computes CRC32 and OS hash when both are None."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            assert doc['crc32'] is None
            assert doc['opensubtitles_hash'] is None

            p._ensure_original_hashes(doc, '/movies/a.mkv')

            # Verify hashes stored
            refreshed = p._get_knowledge('100:aabb')
            assert refreshed['crc32'] == 'DEADBEEF'
            assert refreshed['opensubtitles_hash'] == 'abcdef0123456789'
            mock_crc.assert_called_once_with('/movies/a.mkv')
            mock_os_hash.assert_called_once_with('/movies/a.mkv')

    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32')
    def test_skips_when_already_present(self, mock_crc, mock_os_hash, tmp_path):
        """No-op when both hashes already exist."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            doc['crc32'] = 'EXISTING'
            doc['opensubtitles_hash'] = 'EXISTING_HASH'
            p._update_knowledge(doc)

            p._ensure_original_hashes(doc, '/movies/a.mkv')

            mock_crc.assert_not_called()
            mock_os_hash.assert_not_called()

    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:newfingerprint')
    def test_post_modification_update(self, mock_fp, tmp_path):
        """After modification, current_fingerprint changes, original stays."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            doc['crc32'] = 'DEADBEEF'
            p._update_knowledge(doc)

            p._post_modification_update(doc, '/movies/a.mkv',
                                        'relabel_audio', 'Track 0: de -> en')

            # Lookup by NEW fingerprint should work
            updated = p._get_knowledge('999:newfingerprint')
            assert updated is not None
            assert updated['original_fingerprint'] == '100:aabb'
            assert updated['current_fingerprint'] == '999:newfingerprint'
            assert updated['modified'] is True
            assert len(updated['modifications']) == 1
            assert updated['modifications'][0]['type'] == 'relabel_audio'
            # Original hash preserved
            assert updated['crc32'] == 'DEADBEEF'

            # Old fingerprint no longer resolves
            old = p._get_knowledge('100:aabb')
            assert old is None

    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:newfingerprint')
    def test_post_modification_clears_identification(self, mock_fp, tmp_path):
        """After modification, cached identification is cleared."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            doc['crc32'] = 'DEADBEEF'
            doc['identification'] = {
                'method': 'srrdb_crc',
                'identified_title': 'Test Movie',
                'confidence': 'high',
            }
            p._update_knowledge(doc)

            p._post_modification_update(doc, '/movies/a.mkv',
                                        'relabel_audio', 'Track 0: de -> en')

            updated = p._get_knowledge('999:newfingerprint')
            assert updated['identification'] is None
            # Original hash preserved
            assert updated['crc32'] == 'DEADBEEF'


class TestCacheIdentification:
    """Tests for _cache_identification method."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            pass

        p = _MockPlugin()
        for name in ('_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_cache_identification'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_caches_positive_identification(self, tmp_path):
        """Positive identification (srrdb_crc) is cached in knowledge doc."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')

            ident = {
                'method': 'srrdb_crc',
                'identified_title': 'Test Movie',
                'identified_year': 2020,
                'confidence': 'high',
                'crc32': 'DEADBEEF',
            }
            p._cache_identification('100:aabb', ident)

            doc = p._get_knowledge('100:aabb')
            assert doc['identification'] == ident
            assert doc['crc32'] == 'DEADBEEF'

    def test_caches_opensubtitles_hash(self, tmp_path):
        """OpenSubtitles hash is extracted and cached."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')

            ident = {
                'method': 'opensubtitles_hash',
                'identified_title': 'Test Movie',
                'confidence': 'high',
                'moviehash': 'abcdef0123456789',
            }
            p._cache_identification('100:aabb', ident)

            doc = p._get_knowledge('100:aabb')
            assert doc['identification'] == ident
            assert doc['opensubtitles_hash'] == 'abcdef0123456789'

    def test_caches_container_title(self, tmp_path):
        """Container title identification is cached."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')

            ident = {
                'method': 'container_title',
                'identified_title': 'Test Movie',
                'confidence': 'high',
            }
            p._cache_identification('100:aabb', ident)

            doc = p._get_knowledge('100:aabb')
            assert doc['identification'] == ident

    def test_skips_crc_not_found_identification(self, tmp_path):
        """crc_not_found is NOT cached as identification (retry on next scan)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')

            ident = {
                'method': 'crc_not_found',
                'confidence': 'none',
                'detail': 'CRC32 DEADBEEF not found in srrDB',
                'crc32': 'DEADBEEF',
            }
            p._cache_identification('100:aabb', ident)

            doc = p._get_knowledge('100:aabb')
            # Identification NOT cached
            assert doc['identification'] is None
            # But CRC32 hash IS cached (avoid recomputing)
            assert doc['crc32'] == 'DEADBEEF'

    def test_does_not_overwrite_existing_hashes(self, tmp_path):
        """Existing hashes are preserved (e.g. from pre-modification guard)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            doc['crc32'] = 'ORIGINAL_CRC'
            doc['opensubtitles_hash'] = 'ORIGINAL_OSH'
            p._update_knowledge(doc)

            ident = {
                'method': 'srrdb_crc',
                'identified_title': 'Test Movie',
                'confidence': 'high',
                'crc32': 'NEW_CRC',
            }
            p._cache_identification('100:aabb', ident)

            doc = p._get_knowledge('100:aabb')
            # Identification is updated
            assert doc['identification'] == ident
            # But existing hashes are NOT overwritten
            assert doc['crc32'] == 'ORIGINAL_CRC'
            assert doc['opensubtitles_hash'] == 'ORIGINAL_OSH'

    def test_noop_for_none_fingerprint(self, tmp_path):
        """None fingerprint does nothing (no crash)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._cache_identification(None, {'method': 'srrdb_crc'})
            p._cache_identification('', {'method': 'srrdb_crc'})

    def test_noop_for_none_identification(self, tmp_path):
        """None identification does nothing."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._get_or_create_knowledge('100:aabb', '/movies/a.mkv')
            p._cache_identification('100:aabb', None)
            doc = p._get_knowledge('100:aabb')
            assert doc['identification'] is None

    def test_noop_for_missing_doc(self, tmp_path):
        """Fingerprint not in DB → does nothing (no crash)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._cache_identification('nonexistent:fp', {'method': 'srrdb_crc'})


class TestIdentifyCachedHashes:
    """Tests for identify_flagged_file with cached hashes."""

    @mock.patch('couchpotato.core.plugins.audit.srrdb_lookup_crc')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32')
    @mock.patch('couchpotato.core.plugins.audit.opensubtitles_lookup_hash',
                return_value=None)
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='aaaa')
    def test_uses_cached_crc32(self, mock_os_hash, mock_os_lookup,
                               mock_crc, mock_srr, tmp_path):
        """When cached_crc32 is provided, compute_crc32 is not called."""
        from couchpotato.core.plugins.audit import identify_flagged_file
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 1000)

        mock_srr.return_value = {
            'release': 'Test.Movie.2020.1080p',
            'imdb_id': 'tt1234567',
        }

        with mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', False):
            result = identify_flagged_file(
                str(f),
                [{'check': 'title', 'severity': 'HIGH'}],
                None,
                cached_crc32='CACHEDCRC',
            )

        mock_crc.assert_not_called()
        mock_srr.assert_called_once_with('CACHEDCRC')
        assert result['method'] == 'srrdb_crc'
        assert result['crc32'] == 'CACHEDCRC'

    @mock.patch('couchpotato.core.plugins.audit.opensubtitles_lookup_hash')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash')
    def test_uses_cached_opensubtitles_hash(self, mock_compute_osh,
                                             mock_os_lookup, tmp_path):
        """When cached_opensubtitles_hash is provided, compute is not called."""
        from couchpotato.core.plugins.audit import identify_flagged_file
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 1000)

        mock_os_lookup.return_value = {
            'title': 'Test Movie',
            'year': 2020,
            'imdb_id': 'tt1234567',
        }

        with mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', False), \
             mock.patch('couchpotato.core.plugins.audit.OS_APP_API_KEY',
                        'test-key', create=True):
            # Need to mock the import path for standalone mode
            import couchpotato.core.media.movie.providers.info.opensubtitles as os_mod
            orig = getattr(os_mod, 'OS_APP_API_KEY', None)
            os_mod.OS_APP_API_KEY = 'test-key'
            try:
                result = identify_flagged_file(
                    str(f),
                    [{'check': 'title', 'severity': 'HIGH'}],
                    None,
                    cached_opensubtitles_hash='CACHED_OSH',
                )
            finally:
                if orig is None:
                    delattr(os_mod, 'OS_APP_API_KEY')
                else:
                    os_mod.OS_APP_API_KEY = orig

        mock_compute_osh.assert_not_called()
        mock_os_lookup.assert_called_once_with('CACHED_OSH', 'test-key',
                                                filepath=str(f))
        assert result['method'] == 'opensubtitles_hash'
        assert result['moviehash'] == 'CACHED_OSH'


# ---------------------------------------------------------------------------
# TestSetAudioLanguage — set_audio_language feature tests
# ---------------------------------------------------------------------------

class TestSetAudioLanguage:
    """Tests for the set_audio_language feature.

    Covers:
      A. compute_recommended_action with audio_mislabeled flag
      B. _apply_whisper_result flag reclassification to audio_mislabeled
      C. _preview_set_audio_language
      D. _execute_set_audio_language (MKV + non-MKV paths)
      E. ISO_639_1_TO_639_2 mapping
      F. Integration / edge cases
    """

    @staticmethod
    def _flags(*checks):
        return [{'check': c, 'severity': 'MEDIUM', 'detail': 'test'} for c in checks]

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            last_report = None

        p = _MockPlugin()
        for name in ('_apply_whisper_result',
                     '_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_ensure_original_hashes',
                     '_post_modification_update', '_cache_identification',
                     '_preview_set_audio_language',
                     '_execute_set_audio_language',
                     '_execute_set_audio_language_single'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    # -----------------------------------------------------------------------
    # A. compute_recommended_action with audio_mislabeled
    # -----------------------------------------------------------------------

    def test_audio_mislabeled_recommends_set_audio_language(self):
        assert compute_recommended_action(
            self._flags('audio_mislabeled')) == 'set_audio_language'

    def test_audio_mislabeled_with_title_flag(self):
        """audio_mislabeled takes priority over title flag."""
        assert compute_recommended_action(
            self._flags('audio_mislabeled', 'title')) == 'set_audio_language'

    def test_audio_mislabeled_loses_to_tv_episode(self):
        assert compute_recommended_action(
            self._flags('tv_episode', 'audio_mislabeled')) == 'delete_wrong'

    def test_audio_mislabeled_loses_to_duplicate(self):
        assert compute_recommended_action(
            self._flags('duplicate', 'audio_mislabeled')) == 'delete_duplicate'

    def test_unknown_audio_still_returns_verify(self):
        """Existing unknown_audio behavior unchanged."""
        assert compute_recommended_action(
            self._flags('unknown_audio')) == 'verify_audio'

    # -----------------------------------------------------------------------
    # B. _apply_whisper_result — audio_mislabeled flag logic
    # -----------------------------------------------------------------------

    def test_english_mislabeled_single_track(self, tmp_path):
        """English detected but tagged 'und' → audio_mislabeled flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_1', 'file_fingerprint': '100:aa',
            'file_path': '/movies/a.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [{'track_index': 0, 'tagged_language': 'und',
                         'language': 'en', 'confidence': 0.95}],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks
        assert 'unknown_audio' not in checks
        assert 'foreign_audio' not in checks
        assert item['recommended_action'] == 'set_audio_language'

    def test_english_mislabeled_from_foreign_audio(self, tmp_path):
        """File tagged 'de' but actually English → audio_mislabeled."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_2', 'file_fingerprint': '200:bb',
            'file_path': '/movies/b.mkv',
            'flags': [{'check': 'foreign_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'delete_foreign',
        }
        result = {
            'language': 'en', 'confidence': 0.99,
            'tracks': [{'track_index': 0, 'tagged_language': 'de',
                         'language': 'en', 'confidence': 0.99}],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks
        assert 'foreign_audio' not in checks
        assert item['recommended_action'] == 'set_audio_language'

    def test_english_correctly_tagged_clears_flags(self, tmp_path):
        """English detected and already tagged 'eng' → no audio flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_3', 'file_fingerprint': '300:cc',
            'file_path': '/movies/c.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.98,
            'tracks': [{'track_index': 0, 'tagged_language': 'eng',
                         'language': 'en', 'confidence': 0.98}],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        audio_flags = {f['check'] for f in item['flags']
                       if f['check'] in ('unknown_audio', 'foreign_audio', 'audio_mislabeled')}
        assert len(audio_flags) == 0

    def test_foreign_confirmed_single_track(self, tmp_path):
        """Foreign detected → foreign_audio (existing behavior)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_4', 'file_fingerprint': '400:dd',
            'file_path': '/movies/d.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'fr', 'confidence': 0.92,
            'tracks': [{'track_index': 0, 'tagged_language': 'und',
                         'language': 'fr', 'confidence': 0.92}],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' in checks
        assert 'audio_mislabeled' not in checks
        assert item['recommended_action'] == 'delete_foreign'

    def test_multi_track_all_und_all_english(self, tmp_path):
        """3 tracks all 'und', all detected English → audio_mislabeled."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_5', 'file_fingerprint': '500:ee',
            'file_path': '/movies/e.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                {'track_index': 1, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.90},
                {'track_index': 2, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.93},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks
        assert item['recommended_action'] == 'set_audio_language'

    def test_multi_track_all_und_mixed_languages(self, tmp_path):
        """2 tracks 'und': English + French → audio_mislabeled (has English)."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_6', 'file_fingerprint': '600:ff',
            'file_path': '/movies/f.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                {'track_index': 1, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.88},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks
        assert item['recommended_action'] == 'set_audio_language'

    def test_multi_track_one_correct_one_wrong(self, tmp_path):
        """Track 1 correctly tagged 'eng', track 2 'und' → audio_mislabeled."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_7', 'file_fingerprint': '700:gg',
            'file_path': '/movies/g.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'eng', 'language': 'en', 'confidence': 0.97},
                {'track_index': 1, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.90},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks

    def test_multi_track_all_correctly_tagged(self, tmp_path):
        """Both tracks tagged correctly → no audio flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_8', 'file_fingerprint': '800:hh',
            'file_path': '/movies/h.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'eng', 'language': 'en', 'confidence': 0.97},
                {'track_index': 1, 'tagged_language': 'fre', 'language': 'fr', 'confidence': 0.92},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        audio_flags = {f['check'] for f in item['flags']
                       if f['check'] in ('unknown_audio', 'foreign_audio', 'audio_mislabeled')}
        assert len(audio_flags) == 0

    def test_multi_track_no_english_all_foreign(self, tmp_path):
        """2 tracks 'und', both foreign → foreign_audio."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_9', 'file_fingerprint': '900:ii',
            'file_path': '/movies/i.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'fr', 'confidence': 0.93,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.93},
                {'track_index': 1, 'tagged_language': 'und', 'language': 'de', 'confidence': 0.88},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'foreign_audio' in checks
        assert 'audio_mislabeled' not in checks

    def test_multi_track_english_plus_foreign_both_tagged_wrong(self, tmp_path):
        """Track tagged 'fre' is actually English → audio_mislabeled."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_10', 'file_fingerprint': '1000:jj',
            'file_path': '/movies/j.mkv',
            'flags': [{'check': 'foreign_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'delete_foreign',
        }
        result = {
            'language': 'en', 'confidence': 0.96,
            'tracks': [
                {'track_index': 0, 'tagged_language': 'fre', 'language': 'en', 'confidence': 0.96},
                {'track_index': 1, 'tagged_language': 'und', 'language': 'ja', 'confidence': 0.85},
                {'track_index': 2, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.91},
            ],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' in checks
        assert 'foreign_audio' not in checks

    def test_failed_detection_preserves_flags(self, tmp_path):
        """Whisper returns None → keep original unknown_audio flag."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_11', 'file_fingerprint': '1100:kk',
            'file_path': '/movies/k.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {'language': None, 'confidence': 0.0, 'tracks': []}
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
        checks = {f['check'] for f in item['flags']}
        assert 'unknown_audio' in checks

    def test_stores_whisper_in_knowledge_with_mislabeled(self, tmp_path):
        """Whisper data stored in knowledge doc even when audio_mislabeled."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        item = {
            'item_id': 'sal_12', 'file_fingerprint': '1200:ll',
            'file_path': '/movies/l.mkv',
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
            'flag_count': 1, 'recommended_action': 'verify_audio',
        }
        result = {
            'language': 'en', 'confidence': 0.95,
            'tracks': [{'track_index': 0, 'tagged_language': '',
                         'language': 'en', 'confidence': 0.95,
                         'samples': [{'offset_pct': 50, 'language': 'en', 'confidence': 0.95}]}],
        }
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            p._apply_whisper_result(item, result)
            doc = p._get_knowledge('1200:ll')
        assert doc is not None
        assert doc['whisper']['language'] == 'en'
        assert len(doc['whisper']['tracks']) == 1
        # And the flag should be audio_mislabeled
        assert any(f['check'] == 'audio_mislabeled' for f in item['flags'])

    # -----------------------------------------------------------------------
    # C. _preview_set_audio_language
    # -----------------------------------------------------------------------

    def test_preview_mkv_single_track_change(self, tmp_path):
        """MKV file, 1 track und→eng: preview shows change."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aa', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '100:aa',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
            }
            preview = p._preview_set_audio_language(item)
        assert preview['action'] == 'set_audio_language'
        assert preview['is_mkv'] is True
        assert preview['remux'] is False
        assert preview['method'] == 'mkvpropedit'
        assert len(preview['tracks']) == 1
        assert preview['tracks'][0]['needs_change'] is True
        assert preview['tracks'][0]['new_tag'] == 'eng'
        assert preview['tracks'][0]['current_tag'] == 'und'

    def test_preview_mkv_multi_track_mixed(self, tmp_path):
        """MKV, 3 tracks: one needs change, one correct, one needs change."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'multi.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('200:bb', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [
                    {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                    {'track_index': 1, 'tagged_language': 'fre', 'language': 'fr', 'confidence': 0.90},
                    {'track_index': 2, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.88},
                ],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '200:bb',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                    {'codec': 'DTS', 'channels': '5.1', 'language': 'fr'},
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
            }
            preview = p._preview_set_audio_language(item)
        assert len(preview['tracks']) == 3
        assert preview['tracks'][0]['needs_change'] is True
        assert preview['tracks'][1]['needs_change'] is False  # fre matches fr
        assert preview['tracks'][2]['needs_change'] is True

    def test_preview_non_mkv_shows_remux(self, tmp_path):
        """MP4 file → preview indicates remux to .mkv."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mp4'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('300:cc', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '300:cc',
                'actual': {'audio_tracks': [
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
            }
            preview = p._preview_set_audio_language(item)
        assert preview['remux'] is True
        assert preview['is_mkv'] is False
        assert preview['method'] == 'ffmpeg_remux'
        assert preview['new_path'].endswith('.mkv')
        assert not preview['new_path'].endswith('.mp4')

    def test_preview_non_mkv_avi(self, tmp_path):
        """AVI file → same remux behavior as MP4."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.avi'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('400:dd', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': '',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '400:dd',
                'actual': {'audio_tracks': [
                    {'codec': 'MP3', 'channels': '2.0', 'language': ''},
                ]},
            }
            preview = p._preview_set_audio_language(item)
        assert preview['remux'] is True
        assert preview['new_path'].endswith('.mkv')
        assert '.avi' in preview['description']

    def test_preview_no_whisper_result_errors(self, tmp_path):
        """No whisper data in knowledge → error."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            item = {
                'file_path': str(f), 'file_fingerprint': '500:ee',
                'actual': {'audio_tracks': []},
            }
            preview = p._preview_set_audio_language(item)
        assert 'error' in preview
        assert 'whisper' in preview['error'].lower() or 'Verify Audio' in preview['error']

    def test_preview_file_not_found_errors(self):
        """File doesn't exist → error."""
        p = self._make_plugin()
        item = {
            'file_path': '/nonexistent/movie.mkv', 'file_fingerprint': '600:ff',
            'actual': {'audio_tracks': []},
        }
        preview = p._preview_set_audio_language(item)
        assert 'error' in preview

    # -----------------------------------------------------------------------
    # D. _execute_set_audio_language
    # -----------------------------------------------------------------------

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_mkv_single_track_mkvpropedit_args(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """MKV, 1 track und→eng: mkvpropedit called with correct args."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('100:aa', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '100:aa',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        assert details['method'] == 'mkvpropedit'
        assert details['tracks_changed'] == 1
        # Verify mkvpropedit args
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == 'mkvpropedit'
        assert cmd[1] == str(f)
        assert '--edit' in cmd
        assert 'track:a1' in cmd
        assert 'language=eng' in cmd
        assert 'language-ietf=en' in cmd

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_mkv_multi_track_only_wrong_tracks(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """MKV, 3 tracks: only tracks with wrong tags in mkvpropedit cmd."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('200:bb', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [
                    {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                    {'track_index': 1, 'tagged_language': 'fre', 'language': 'fr', 'confidence': 0.90},
                    {'track_index': 2, 'tagged_language': 'und', 'language': 'ja', 'confidence': 0.85},
                ],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '200:bb',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                    {'codec': 'DTS', 'channels': '5.1', 'language': 'fr'},
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        assert details['tracks_changed'] == 2  # track 0 and 2, not 1
        cmd = mock_run.call_args[0][0]
        assert 'track:a1' in cmd  # track_index 0
        assert 'track:a2' not in cmd  # track_index 1 (correctly tagged)
        assert 'track:a3' in cmd  # track_index 2

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_mkv_multi_track_all_need_fix(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """MKV, 2 tracks both 'und' → both in single mkvpropedit cmd."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('300:cc', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [
                    {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                    {'track_index': 1, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.88},
                ],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '300:cc',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                    {'codec': 'DTS', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        assert details['tracks_changed'] == 2
        cmd = mock_run.call_args[0][0]
        assert 'track:a1' in cmd
        assert 'track:a2' in cmd
        # Single subprocess call
        assert mock_run.call_count == 1

    @mock.patch('couchpotato.core.plugins.audit.os.remove')
    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_non_mkv_ffmpeg_remux_args(
            self, mock_crc, mock_osh, mock_fp, mock_run, mock_remove, tmp_path):
        """MP4, 1 track: ffmpeg remux called, original deleted."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mp4'
        f.write_bytes(b'\x00' * 100)
        mkv_path = tmp_path / 'movie.mkv'
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('400:dd', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '400:dd',
                'actual': {'audio_tracks': [
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        assert details['method'] == 'ffmpeg_remux'
        assert details['remuxed'] is True
        # Check ffmpeg command
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == 'ffmpeg'
        assert '-c' in cmd and 'copy' in cmd
        assert '-map' in cmd and '0' in cmd
        assert '-metadata:s:a:0' in cmd
        assert 'language=eng' in cmd
        # Original mp4 deleted
        mock_remove.assert_called_once_with(str(f))
        # Item path updated
        assert item['file_path'] == str(mkv_path)
        assert item['file'].endswith('.mkv')

    @mock.patch('couchpotato.core.plugins.audit.os.remove')
    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_non_mkv_updates_all_paths(
            self, mock_crc, mock_osh, mock_fp, mock_run, mock_remove, tmp_path):
        """Non-MKV remux updates item paths and knowledge doc file_path."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mp4'
        f.write_bytes(b'\x00' * 100)
        mkv_path = tmp_path / 'movie.mkv'
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('500:ee', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '500:ee',
                'actual': {'audio_tracks': [
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
            # Check knowledge doc path updated
            updated_doc = p._get_knowledge('999:new')
        assert success is True
        assert item['file_path'] == str(mkv_path)
        assert item['file'] == 'movie.mkv'
        assert updated_doc is not None
        assert updated_doc['file_path'] == str(mkv_path)

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_crc32_guard_called_before_mod(
            self, mock_crc, mock_osh, mock_run, tmp_path):
        """_ensure_original_hashes called BEFORE subprocess.run."""
        call_order = []
        mock_crc.side_effect = lambda f: (call_order.append('crc32'), 'DEADBEEF')[1]
        mock_osh.side_effect = lambda f: (call_order.append('osh'), 'abcdef')[1]
        mock_run.side_effect = lambda *a, **kw: (
            call_order.append('subprocess'),
            mock.Mock(returncode=0, stderr='', stdout=''))[1]
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db), \
             mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                        return_value='999:new'):
            doc = p._get_or_create_knowledge('600:ff', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '600:ff',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            p._execute_set_audio_language(item)
        # CRC32 and OSH must come before subprocess
        assert call_order.index('crc32') < call_order.index('subprocess')
        assert call_order.index('osh') < call_order.index('subprocess')

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_fingerprint_updated_after_mod(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """After MKV edit: original fingerprint preserved, current updated."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('700:gg', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '700:gg',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
            updated = p._get_knowledge('999:new')
        assert success is True
        assert updated is not None
        assert updated['original_fingerprint'] == '700:gg'
        assert updated['current_fingerprint'] == '999:new'
        assert updated['modified'] is True
        assert len(updated['modifications']) == 1
        assert updated['modifications'][0]['type'] == 'set_audio_language'

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_audio_tracks_updated_in_item(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """After fix: item audio_tracks have new language tags."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('800:hh', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [
                    {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                    {'track_index': 1, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.88},
                ],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '800:hh',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                    {'codec': 'DTS', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            p._execute_set_audio_language(item)
        assert item['actual']['audio_tracks'][0]['language'] == 'en'
        assert item['actual']['audio_tracks'][1]['language'] == 'fr'

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_flag_removed_action_recomputed(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """After fix: audio_mislabeled flag removed, action recomputed."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('900:ii', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '900:ii',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [
                    {'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'},
                    {'check': 'title', 'severity': 'MEDIUM', 'detail': 'title mismatch'},
                ],
                'flag_count': 2, 'recommended_action': 'set_audio_language',
            }
            success, _ = p._execute_set_audio_language(item)
        assert success is True
        checks = {f['check'] for f in item['flags']}
        assert 'audio_mislabeled' not in checks
        assert 'title' in checks  # other flags preserved
        assert item['flag_count'] == 1
        # recommended_action recomputed — title without identification → needs_full
        assert item['recommended_action'] != 'set_audio_language'

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_subprocess_failure_no_side_effects(
            self, mock_crc, mock_osh, mock_run, tmp_path):
        """mkvpropedit fails → no fingerprint update, no flag removal."""
        mock_run.return_value = mock.Mock(returncode=1, stderr='error', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1000:jj', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1000:jj',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is False
        assert 'error' in details
        # Flags unchanged
        assert any(f['check'] == 'audio_mislabeled' for f in item['flags'])
        assert item['flag_count'] == 1

    @mock.patch('couchpotato.core.plugins.audit.os.remove')
    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_non_mkv_remux_failure_original_preserved(
            self, mock_crc, mock_osh, mock_run, mock_remove, tmp_path):
        """ffmpeg fails → original .mp4 NOT deleted."""
        mock_run.return_value = mock.Mock(returncode=1, stderr='error', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mp4'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1100:kk', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1100:kk',
                'actual': {'audio_tracks': [
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is False
        # Original NOT deleted
        mock_remove.assert_not_called()
        # Path unchanged
        assert item['file_path'] == str(f)

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_language_code_mapping(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """Whisper 'ja' → mkvpropedit uses 'jpn' and 'ja'."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1200:ll', str(f))
            doc['whisper'] = {
                'language': 'ja', 'confidence': 0.92,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'ja', 'confidence': 0.92}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1200:ll',
                'actual': {'audio_tracks': [
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        cmd = mock_run.call_args[0][0]
        assert 'language=jpn' in cmd
        assert 'language-ietf=ja' in cmd

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_file_path_with_spaces(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """Path with spaces: subprocess list handles quoting automatically."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        d = tmp_path / 'A Movie (2020)'
        d.mkdir()
        f = d / 'A Movie (2020) 1080p.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1300:mm', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1300:mm',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, _ = p._execute_set_audio_language(item)
        assert success is True
        cmd = mock_run.call_args[0][0]
        # The path with spaces is passed as a single list element
        assert str(f) in cmd

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_identification_cleared_after_mod(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """After set_audio_language, cached identification is cleared."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1400:nn', str(f))
            doc['crc32'] = 'EXISTING_CRC'
            doc['opensubtitles_hash'] = 'EXISTING_OSH'
            doc['identification'] = {
                'method': 'srrdb_crc',
                'identified_title': 'Test Movie',
                'confidence': 'high',
            }
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1400:nn',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, _ = p._execute_set_audio_language(item)
            updated = p._get_knowledge('999:new')
        assert success is True
        assert updated['identification'] is None
        # CRC32 preserved (original hashes kept)
        assert updated['crc32'] == 'EXISTING_CRC'

    # -----------------------------------------------------------------------
    # E. ISO_639_1_TO_639_2 mapping
    # -----------------------------------------------------------------------

    def test_iso_mapping_common_languages(self):
        """Core languages have correct 3-letter codes."""
        expected = {
            'en': 'eng', 'fr': 'fre', 'de': 'ger', 'es': 'spa',
            'ja': 'jpn', 'zh': 'chi', 'ko': 'kor', 'it': 'ita',
            'pt': 'por', 'ru': 'rus',
        }
        for iso1, iso2 in expected.items():
            assert ISO_639_1_TO_639_2[iso1] == iso2, \
                '%s should map to %s' % (iso1, iso2)

    def test_iso_mapping_covers_whisper_languages(self):
        """All common Whisper-detected languages have mapping entries."""
        whisper_langs = [
            'en', 'fr', 'de', 'es', 'it', 'pt', 'ru', 'ja', 'ko', 'zh',
            'nl', 'ar', 'hi', 'sv', 'no', 'da', 'fi', 'pl', 'tr', 'he',
            'th', 'ro', 'hu', 'cs', 'el', 'fa', 'id', 'uk', 'vi',
        ]
        for lang in whisper_langs:
            assert lang in ISO_639_1_TO_639_2, \
                'Missing mapping for Whisper language: %s' % lang

    # -----------------------------------------------------------------------
    # F. Integration / edge cases
    # -----------------------------------------------------------------------

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_full_flow_verify_then_set_language(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """End-to-end: unknown_audio → whisper → audio_mislabeled → fix."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            # Start: unknown_audio flag
            item = {
                'item_id': 'flow_1', 'file_fingerprint': '1500:oo',
                'file_path': str(f),
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                ]},
                'flags': [{'check': 'unknown_audio', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'verify_audio',
            }
            # Step 1: Apply whisper result (English detected, tag wrong)
            whisper_result = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'und',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._apply_whisper_result(item, whisper_result)
            assert item['recommended_action'] == 'set_audio_language'
            assert any(f['check'] == 'audio_mislabeled' for f in item['flags'])

            # Step 2: Preview
            preview = p._preview_set_audio_language(item)
            assert preview['action'] == 'set_audio_language'
            assert preview['tracks'][0]['needs_change'] is True

            # Step 3: Execute
            success, details = p._execute_set_audio_language(item)
            assert success is True
            assert details['tracks_changed'] == 1
            # Flag removed, action recomputed
            assert not any(f['check'] == 'audio_mislabeled' for f in item['flags'])

    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_no_tracks_need_change(
            self, mock_crc, mock_osh, mock_fp, mock_run, tmp_path):
        """All tracks already correctly tagged → success with no subprocess."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.mkv'
        f.write_bytes(b'\x00' * 100)
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1600:pp', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'tagged_language': 'eng',
                             'language': 'en', 'confidence': 0.95}],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1600:pp',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': 'en'},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        assert details['tracks_changed'] == 0
        mock_run.assert_not_called()  # No subprocess needed

    @mock.patch('couchpotato.core.plugins.audit.os.remove')
    @mock.patch('couchpotato.core.plugins.audit.subprocess.run')
    @mock.patch('couchpotato.core.plugins.audit.compute_file_fingerprint',
                return_value='999:new')
    @mock.patch('couchpotato.core.plugins.audit.compute_opensubtitles_hash',
                return_value='abcdef')
    @mock.patch('couchpotato.core.plugins.audit.compute_crc32',
                return_value='DEADBEEF')
    def test_execute_non_mkv_multi_track_ffmpeg_args(
            self, mock_crc, mock_osh, mock_fp, mock_run, mock_remove, tmp_path):
        """AVI with 3 tracks → ffmpeg has all -metadata:s:a:N args."""
        mock_run.return_value = mock.Mock(returncode=0, stderr='', stdout='')
        db = self._make_db(tmp_path)
        p = self._make_plugin()
        f = tmp_path / 'movie.avi'
        f.write_bytes(b'\x00' * 100)
        mkv_path = tmp_path / 'movie.mkv'
        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            doc = p._get_or_create_knowledge('1700:qq', str(f))
            doc['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [
                    {'track_index': 0, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.95},
                    {'track_index': 1, 'tagged_language': 'und', 'language': 'fr', 'confidence': 0.88},
                    {'track_index': 2, 'tagged_language': 'und', 'language': 'en', 'confidence': 0.91},
                ],
            }
            p._update_knowledge(doc)
            item = {
                'file_path': str(f), 'file_fingerprint': '1700:qq',
                'actual': {'audio_tracks': [
                    {'codec': 'AC3', 'channels': '5.1', 'language': ''},
                    {'codec': 'DTS', 'channels': '5.1', 'language': ''},
                    {'codec': 'AAC', 'channels': '2.0', 'language': ''},
                ]},
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW', 'detail': 'x'}],
                'flag_count': 1, 'recommended_action': 'set_audio_language',
            }
            success, details = p._execute_set_audio_language(item)
        assert success is True
        cmd = mock_run.call_args[0][0]
        # All 3 tracks get metadata in ffmpeg command
        assert '-metadata:s:a:0' in cmd
        assert '-metadata:s:a:1' in cmd
        assert '-metadata:s:a:2' in cmd
        assert cmd[-1] == str(mkv_path)
        # Verify language values
        idx_a0 = cmd.index('-metadata:s:a:0')
        assert cmd[idx_a0 + 1] == 'language=eng'
        idx_a1 = cmd.index('-metadata:s:a:1')
        assert cmd[idx_a1 + 1] == 'language=fre'
        idx_a2 = cmd.index('-metadata:s:a:2')
        assert cmd[idx_a2 + 1] == 'language=eng'


# ---------------------------------------------------------------------------
# Foreign (No English Audio) compound filter
# ---------------------------------------------------------------------------

class TestForeignNoEnglishFilter:
    """Tests for the foreign_no_english compound filter in _filter_and_sort."""

    @staticmethod
    def _make_item(original_language='en', audio_languages=None,
                   flags=None, spoken_languages=None,
                   production_countries=None, **kw):
        """Build a minimal flagged item for filter testing."""
        if audio_languages is None:
            audio_languages = ['en']
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': lang}
                   for lang in audio_languages]
        if flags is None:
            flags = [{'check': 'foreign_audio', 'severity': 'HIGH', 'detail': 'test'}]
        item = {
            'item_id': 'test123',
            'file_fingerprint': '100:abc',
            'folder': 'Test Movie (2024)',
            'file': 'test.mkv',
            'file_path': '/movies/test.mkv',
            'original_language': original_language,
            'spoken_languages': spoken_languages or [],
            'production_countries': production_countries or [],
            'actual': {'audio_tracks': tracks},
            'expected': {'title': 'Test Movie', 'year': 2024},
            'flags': flags,
            'flag_count': len(flags),
            'recommended_action': 'delete_foreign',
        }
        item.update(kw)
        return item

    @staticmethod
    def _filter(items, filter_check='foreign_no_english'):
        """Run _filter_and_sort on items with the given filter."""
        from couchpotato.core.plugins.audit import Audit
        from unittest.mock import patch

        plugin = object.__new__(Audit)
        plugin.last_report = {'flagged': items}
        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            return plugin._filter_and_sort(filter_check=filter_check, filter_fixed='all')

    def test_foreign_language_no_english_audio_included(self):
        items = [self._make_item(original_language='ko', audio_languages=['ko'])]
        result = self._filter(items)
        assert len(result) == 1

    def test_english_language_excluded(self):
        items = [self._make_item(original_language='en', audio_languages=['en'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_foreign_language_with_english_audio_excluded(self):
        """Foreign film but has an English audio track — user can watch it."""
        items = [self._make_item(original_language='ko', audio_languages=['ko', 'en'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_foreign_language_with_eng_code_excluded(self):
        items = [self._make_item(original_language='ja', audio_languages=['ja', 'eng'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_no_original_language_excluded(self):
        """If original_language is missing, can't confirm it's foreign."""
        items = [self._make_item(original_language='', audio_languages=['ko'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_none_original_language_excluded(self):
        items = [self._make_item(original_language=None, audio_languages=['ko'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_mixed_items_filtered_correctly(self):
        items = [
            self._make_item(original_language='ko', audio_languages=['ko'],
                            item_id='a', file_fingerprint='1:a'),
            self._make_item(original_language='en', audio_languages=['en'],
                            item_id='b', file_fingerprint='2:b'),
            self._make_item(original_language='ja', audio_languages=['ja'],
                            item_id='c', file_fingerprint='3:c'),
            self._make_item(original_language='fr', audio_languages=['fr', 'en'],
                            item_id='d', file_fingerprint='4:d'),
        ]
        result = self._filter(items)
        ids = [i['item_id'] for i in result]
        assert ids == ['a', 'c']

    def test_no_audio_tracks_included(self):
        """Foreign film with no audio track info — include it (suspicious)."""
        item = self._make_item(original_language='zh', audio_languages=[])
        result = self._filter([item])
        assert len(result) == 1

    def test_empty_language_audio_track_included(self):
        """Foreign film with unlabeled audio — no English confirmed."""
        item = self._make_item(original_language='hi', audio_languages=[''])
        result = self._filter([item])
        assert len(result) == 1

    def test_case_insensitive_english_check(self):
        items = [self._make_item(original_language='ko', audio_languages=['EN'])]
        result = self._filter(items)
        assert len(result) == 0

    # --- Whisper override tests ---

    def test_whisper_english_override_excludes(self):
        """Item with audio_mislabeled flag = Whisper detected English — exclude."""
        flags = [{'check': 'audio_mislabeled', 'severity': 'LOW',
                  'detail': 'Whisper verified: Track 0 (): en 90%'}]
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 flags=flags)]
        result = self._filter(items)
        assert len(result) == 0

    def test_whisper_foreign_confirmed_still_included(self):
        """Item with foreign_audio flag (Whisper confirmed foreign) — include."""
        flags = [{'check': 'foreign_audio', 'severity': 'HIGH',
                  'detail': 'Whisper verified: Track 0 (): ja 95%'}]
        items = [self._make_item(original_language='ja', audio_languages=[''],
                                 flags=flags)]
        result = self._filter(items)
        assert len(result) == 1

    def test_whisper_override_with_multiple_flags(self):
        """audio_mislabeled among other flags — still excludes."""
        flags = [
            {'check': 'resolution', 'severity': 'LOW', 'detail': 'SD copy'},
            {'check': 'audio_mislabeled', 'severity': 'LOW',
             'detail': 'Whisper verified: Track 0 (): en 85%'},
        ]
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 flags=flags)]
        result = self._filter(items)
        assert len(result) == 0

    # --- spoken_languages override tests ---

    def test_spoken_languages_english_excludes(self):
        """TMDB lists English in spoken_languages — exclude."""
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 spoken_languages=['it', 'en'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_spoken_languages_no_english_still_included(self):
        """TMDB spoken_languages has no English — still included."""
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 spoken_languages=['it', 'es'])]
        result = self._filter(items)
        assert len(result) == 1

    def test_spoken_languages_eng_code_excludes(self):
        """spoken_languages with 'eng' code — exclude."""
        items = [self._make_item(original_language='ja', audio_languages=['ja'],
                                 spoken_languages=['ja', 'eng'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_spoken_languages_empty_still_uses_audio(self):
        """No spoken_languages — falls through to audio track check."""
        items = [self._make_item(original_language='ko', audio_languages=['ko'],
                                 spoken_languages=[])]
        result = self._filter(items)
        assert len(result) == 1

    def test_spoken_languages_none_still_uses_audio(self):
        """spoken_languages is None — falls through to audio track check."""
        items = [self._make_item(original_language='ko', audio_languages=['ko'],
                                 spoken_languages=None)]
        result = self._filter(items)
        assert len(result) == 1

    # --- Combined scenario tests ---

    def test_coproduction_whisper_override(self):
        """Co-production: foreign orig_lang, no English tags, but Whisper found English."""
        flags = [{'check': 'audio_mislabeled', 'severity': 'LOW',
                  'detail': 'Whisper verified: Track 0 (): en 90%'}]
        # Simulates GBTU: orig=it, no English audio tags, TMDB spoken=[it]
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 spoken_languages=['it'], flags=flags)]
        result = self._filter(items)
        assert len(result) == 0

    def test_coproduction_spoken_languages_override(self):
        """Co-production: foreign orig_lang, no English tags, but TMDB lists English spoken."""
        # Simulates "Once Upon a Time in America": orig=it, spoken=[en]
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 spoken_languages=['en'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_mixed_items_with_overrides(self):
        """Mix of items: some with overrides, some without."""
        items = [
            # Genuine foreign film — included
            self._make_item(original_language='ko', audio_languages=['ko'],
                            item_id='a', file_fingerprint='1:a'),
            # Whisper override — excluded
            self._make_item(original_language='it', audio_languages=[''],
                            item_id='b', file_fingerprint='2:b',
                            flags=[{'check': 'audio_mislabeled', 'severity': 'LOW',
                                    'detail': 'Whisper: en 90%'}]),
            # spoken_languages override — excluded
            self._make_item(original_language='fr', audio_languages=[''],
                            item_id='c', file_fingerprint='3:c',
                            spoken_languages=['fr', 'en']),
            # Genuine foreign, no overrides — included
            self._make_item(original_language='ja', audio_languages=['ja'],
                            item_id='d', file_fingerprint='4:d'),
        ]
        result = self._filter(items)
        ids = [i['item_id'] for i in result]
        assert ids == ['a', 'd']

    # --- Production countries override tests ---

    def test_production_country_us_excludes(self):
        """US in production_countries — exclude."""
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 production_countries=['IT', 'US'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_production_country_gb_excludes(self):
        """GB in production_countries — exclude."""
        items = [self._make_item(original_language='fr', audio_languages=['fr'],
                                 production_countries=['FR', 'GB'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_production_country_au_excludes(self):
        """AU in production_countries — exclude."""
        items = [self._make_item(original_language='zh', audio_languages=['zh'],
                                 production_countries=['CN', 'AU'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_production_country_nz_ca_ie_excludes(self):
        """NZ, CA, IE each exclude."""
        for country in ('NZ', 'CA', 'IE'):
            items = [self._make_item(original_language='de', audio_languages=['de'],
                                     production_countries=[country])]
            result = self._filter(items)
            assert len(result) == 0, f'{country} should exclude'

    def test_production_country_non_english_still_included(self):
        """Only non-English countries — still included."""
        items = [self._make_item(original_language='it', audio_languages=['it'],
                                 production_countries=['IT', 'ES', 'DE'])]
        result = self._filter(items)
        assert len(result) == 1

    def test_production_country_empty_falls_through(self):
        """Empty production_countries — falls through to audio check."""
        items = [self._make_item(original_language='ko', audio_languages=['ko'],
                                 production_countries=[])]
        result = self._filter(items)
        assert len(result) == 1

    def test_production_country_none_falls_through(self):
        """None production_countries — falls through to audio check."""
        items = [self._make_item(original_language='ko', audio_languages=['ko'],
                                 production_countries=None)]
        result = self._filter(items)
        assert len(result) == 1

    def test_production_country_case_insensitive(self):
        """Lowercase country codes still match."""
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 production_countries=['it', 'us'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_gbtu_scenario(self):
        """Co-production: orig=it, spoken=[it], countries=[US,IT,ES,DE] — excluded."""
        items = [self._make_item(original_language='it', audio_languages=[''],
                                 spoken_languages=['it'],
                                 production_countries=['US', 'IT', 'ES', 'DE'])]
        result = self._filter(items)
        assert len(result) == 0

    def test_triple_negative_included(self):
        """All three layers negative — genuinely foreign, included."""
        items = [self._make_item(original_language='ko', audio_languages=['ko'],
                                 spoken_languages=['ko'],
                                 production_countries=['KR'])]
        result = self._filter(items)
        assert len(result) == 1

    def test_mixed_items_all_three_layers(self):
        """Mix of items excluded by different override layers."""
        items = [
            # Genuine foreign — included
            self._make_item(original_language='ko', audio_languages=['ko'],
                            spoken_languages=['ko'], production_countries=['KR'],
                            item_id='a', file_fingerprint='1:a'),
            # Whisper override — excluded
            self._make_item(original_language='it', audio_languages=[''],
                            item_id='b', file_fingerprint='2:b',
                            flags=[{'check': 'audio_mislabeled', 'severity': 'LOW',
                                    'detail': 'Whisper: en 90%'}]),
            # spoken_languages override — excluded
            self._make_item(original_language='fr', audio_languages=[''],
                            item_id='c', file_fingerprint='3:c',
                            spoken_languages=['fr', 'en']),
            # production_countries override — excluded
            self._make_item(original_language='it', audio_languages=[''],
                            item_id='d', file_fingerprint='4:d',
                            spoken_languages=['it'],
                            production_countries=['US', 'IT', 'ES', 'DE']),
            # Genuine foreign, no overrides — included
            self._make_item(original_language='ja', audio_languages=['ja'],
                            spoken_languages=['ja'], production_countries=['JP'],
                            item_id='e', file_fingerprint='5:e'),
        ]
        result = self._filter(items)
        ids = [i['item_id'] for i in result]
        assert ids == ['a', 'e']


class TestBatchVerifyUnified:
    """Tests that verify_audio batch action goes through the unified batch pipeline
    and respects filters, uses whisper cache, etc."""

    @staticmethod
    def _make_plugin(flagged_items):
        """Create a minimal Audit plugin with mocked dependencies."""
        from couchpotato.core.plugins.audit import Audit

        plugin = object.__new__(Audit)
        plugin.last_report = {'flagged': flagged_items}
        plugin.fix_in_progress = False
        plugin._knowledge_cache = {}
        return plugin

    @staticmethod
    def _make_item(item_id='test1', fingerprint='100:abc', check='unknown_audio',
                   audio_languages=None):
        """Build a flagged item for batch testing."""
        if audio_languages is None:
            audio_languages = ['']
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': lang}
                   for lang in audio_languages]
        return {
            'item_id': item_id,
            'file_fingerprint': fingerprint,
            'folder': 'Test Movie (2024)',
            'file': 'test.mkv',
            'file_path': '/movies/test.mkv',
            'original_language': 'en',
            'actual': {'audio_tracks': tracks},
            'expected': {'title': 'Test Movie', 'year': 2024},
            'flags': [{'check': check, 'severity': 'LOW', 'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }

    def test_verify_audio_in_valid_fix_actions(self):
        """verify_audio is a valid batch fix action."""
        from couchpotato.core.plugins.audit import VALID_FIX_ACTIONS
        assert 'verify_audio' in VALID_FIX_ACTIONS

    def test_fixBatchView_accepts_verify_audio(self):
        """fixBatchView accepts action=verify_audio with filters."""
        from unittest.mock import patch

        items = [
            self._make_item(item_id='a', fingerprint='1:a', check='unknown_audio'),
            self._make_item(item_id='b', fingerprint='2:b', check='resolution'),
        ]
        plugin = self._make_plugin(items)

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            # Dry run with filter_check — should only match the unknown_audio item
            result = plugin.fixBatchView(
                action='verify_audio',
                filter_check='unknown_audio',
                confirm='1',
                dry_run='1',
            )

        assert result['success'] is True
        assert result['dry_run'] is True
        assert result['total'] == 1
        assert result['previews'][0]['item_id'] == 'a'

    def test_fixBatchView_verify_audio_no_filter_gets_all(self):
        """Without filter_action, verify_audio dry run returns all unfixed items
        regardless of recommended_action."""
        from unittest.mock import patch

        items = [
            self._make_item(item_id='a', fingerprint='1:a', check='unknown_audio'),
            self._make_item(item_id='b', fingerprint='2:b', check='unknown_audio'),
        ]
        plugin = self._make_plugin(items)

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            result = plugin.fixBatchView(
                action='verify_audio',
                confirm='1',
                dry_run='1',
            )

        assert result['success'] is True
        assert result['total'] == 2

    def test_run_batch_fix_verify_audio_uses_cache(self):
        """_run_batch_fix with verify_audio checks whisper cache before running."""
        from unittest.mock import patch

        item = self._make_item()
        plugin = self._make_plugin([item])

        cached_whisper = {
            'language': 'en',
            'confidence': 0.95,
            'verified_at': '2024-01-01T00:00:00',
            'tracks': [{'track_index': 0, 'language': 'en',
                         'confidence': 0.95, 'tagged_language': ''}],
        }

        mock_doc = {'whisper': cached_whisper, 'current_fingerprint': '100:abc'}

        with patch.object(plugin, '_get_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_apply_whisper_result') as mock_apply, \
             patch.object(plugin, '_mark_fixed') as mock_mark, \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'), \
             patch('os.path.isfile', return_value=True), \
             patch('couchpotato.core.plugins.audit.whisper_verify_audio') as mock_whisper:

            results = plugin._run_batch_fix(
                action='verify_audio',
                items=[item],
                dry_run=False,
            )

        # Whisper should NOT have been called (cache hit)
        mock_whisper.assert_not_called()
        # But apply_whisper_result should have been called with cached data
        mock_apply.assert_called_once_with(item, cached_whisper)
        mock_mark.assert_called_once()

    def test_run_batch_fix_verify_audio_runs_whisper_on_cache_miss(self):
        """_run_batch_fix runs whisper when no cache exists."""
        from unittest.mock import patch

        item = self._make_item()
        plugin = self._make_plugin([item])

        whisper_result = {
            'language': 'ja',
            'confidence': 0.88,
            'tracks': [{'track_index': 0, 'language': 'ja',
                         'confidence': 0.88, 'tagged_language': ''}],
        }

        # No whisper cache
        mock_doc = {'current_fingerprint': '100:abc'}

        with patch.object(plugin, '_get_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_apply_whisper_result') as mock_apply, \
             patch.object(plugin, '_mark_fixed'), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'), \
             patch('couchpotato.core.plugins.audit.whisper_verify_audio',
                   return_value=whisper_result) as mock_whisper, \
             patch('os.path.isfile', return_value=True):

            results = plugin._run_batch_fix(
                action='verify_audio',
                items=[item],
                dry_run=False,
            )

        # Whisper should have been called
        mock_whisper.assert_called_once_with('/movies/test.mkv',
                                             audio_tracks=item['actual']['audio_tracks'])
        mock_apply.assert_called_once_with(item, whisper_result)

    def test_fixBatchView_verify_audio_respects_severity_filter(self):
        """verify_audio batch respects filter_severity just like other actions."""
        from unittest.mock import patch

        items = [
            self._make_item(item_id='a', fingerprint='1:a', check='unknown_audio'),
        ]
        # Override severity to HIGH
        items[0]['flags'][0]['severity'] = 'HIGH'

        plugin = self._make_plugin(items)

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            # Filter for LOW severity only — should not match the HIGH item
            result = plugin.fixBatchView(
                action='verify_audio',
                filter_severity='LOW',
                confirm='1',
                dry_run='1',
            )

        # Item has HIGH severity, filter asks for LOW — no match
        assert result.get('success') is False or result.get('total', 0) == 0

    def test_fixBatchView_verify_audio_with_filter_action(self):
        """verify_audio batch with filter_action only returns items matching
        that action, not just verify_audio items."""
        from unittest.mock import patch

        item_verify = self._make_item(item_id='a', fingerprint='1:a',
                                       check='unknown_audio')
        item_verify['recommended_action'] = 'verify_audio'

        item_set = self._make_item(item_id='b', fingerprint='2:b',
                                    check='foreign_audio')
        item_set['recommended_action'] = 'set_audio_language'

        plugin = self._make_plugin([item_verify, item_set])

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            result = plugin.fixBatchView(
                action='verify_audio',
                filter_action='set_audio_language',
                confirm='1',
                dry_run='1',
            )

        assert result['success'] is True
        assert result['total'] == 1
        assert result['previews'][0]['item_id'] == 'b'

    def test_fixBatchView_verify_audio_no_filter_action_returns_all(self):
        """verify_audio batch with no filter_action returns ALL unfixed items,
        not just those with recommended_action=verify_audio."""
        from unittest.mock import patch

        item_verify = self._make_item(item_id='a', fingerprint='1:a',
                                       check='unknown_audio')
        item_verify['recommended_action'] = 'verify_audio'

        item_set = self._make_item(item_id='b', fingerprint='2:b',
                                    check='foreign_audio')
        item_set['recommended_action'] = 'set_audio_language'

        item_rename = self._make_item(item_id='c', fingerprint='3:c',
                                       check='resolution')
        item_rename['recommended_action'] = 'rename_resolution'

        plugin = self._make_plugin([item_verify, item_set, item_rename])

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            result = plugin.fixBatchView(
                action='verify_audio',
                confirm='1',
                dry_run='1',
            )

        assert result['success'] is True
        assert result['total'] == 3

    def test_fixBatchView_destructive_action_ignores_filter_action(self):
        """Destructive actions always filter by their own action, ignoring
        any filter_action parameter to prevent accidental cross-action deletes."""
        from unittest.mock import patch

        item_delete = self._make_item(item_id='a', fingerprint='1:a',
                                       check='foreign_audio')
        item_delete['recommended_action'] = 'delete_foreign'

        item_rename = self._make_item(item_id='b', fingerprint='2:b',
                                       check='resolution')
        item_rename['recommended_action'] = 'rename_resolution'

        plugin = self._make_plugin([item_delete, item_rename])

        with patch.object(plugin, '_build_ignored_set', return_value=set()):
            # Try to pass filter_action that includes both — should be ignored
            result = plugin.fixBatchView(
                action='delete_foreign',
                filter_action='rename_resolution',
                confirm='1',
                dry_run='1',
            )

        # Only the delete_foreign item should match
        assert result['success'] is True
        assert result['total'] == 1
        assert result['previews'][0]['item_id'] == 'a'


# ---------------------------------------------------------------------------
# validate_srrdb_imdb — TMDB cross-validation of srrDB IMDB IDs
# ---------------------------------------------------------------------------

class TestValidateSrrdbImdb:
    """Tests for validate_srrdb_imdb() TMDB cross-validation."""

    MODULE = 'couchpotato.core.plugins.audit'

    @staticmethod
    def _mock_response(data, status_code=200):
        class MockResp:
            def __init__(self):
                self.status_code = status_code
                self.ok = status_code == 200
            def json(self):
                return data
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise Exception(f'HTTP {self.status_code}')
        return MockResp()

    # -- Edge cases / early returns --

    def test_returns_none_when_no_imdb(self):
        """Returns None when srrdb_imdb_id is missing."""
        assert validate_srrdb_imdb('Some Movie', 2020, None) is None
        assert validate_srrdb_imdb('Some Movie', 2020, '') is None

    def test_returns_none_when_no_title(self):
        """Returns None when identified_title is missing."""
        assert validate_srrdb_imdb('', 2020, 'tt1234567') is None
        assert validate_srrdb_imdb(None, 2020, 'tt1234567') is None

    def test_returns_none_when_no_requests(self, monkeypatch):
        """Returns None when requests module is unavailable."""
        monkeypatch.setattr(self.MODULE + '.requests', None)
        assert validate_srrdb_imdb('Movie', 2020, 'tt1234567') is None

    # -- Title match (valid srrDB IMDB) --

    def test_valid_imdb_unchanged(self, monkeypatch):
        """When titles match, returns the original IMDB ID with corrected=False."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Cinema Paradiso', 'year': 1988,
                                   'imdb_id': 'tt0095765'},
        )
        result = validate_srrdb_imdb('Cinema Paradiso', 1988, 'tt0095765')
        assert result is not None
        assert result['imdb_id'] == 'tt0095765'
        assert result['corrected'] is False

    def test_valid_imdb_title_match_fuzzy(self, monkeypatch):
        """Fuzzy title matching still validates (e.g. article reordering)."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': "Avengers: Infinity War",
                                   'year': 2018, 'imdb_id': 'tt4154756'},
        )
        # Container title doesn't have "The" prefix or colon
        result = validate_srrdb_imdb('The Avengers Infinity War', 2018,
                                     'tt4154756')
        assert result is not None
        assert result['imdb_id'] == 'tt4154756'
        assert result['corrected'] is False

    def test_valid_imdb_year_within_tolerance(self, monkeypatch):
        """Year ±1 is still considered valid."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Crumb Catcher', 'year': 2024,
                                   'imdb_id': 'tt7178516'},
        )
        # Guessit parsed year as 2023 from release name, TMDB says 2024
        result = validate_srrdb_imdb('Crumb Catcher', 2023, 'tt7178516')
        assert result is not None
        assert result['imdb_id'] == 'tt7178516'
        assert result['corrected'] is False

    def test_valid_imdb_no_year_available(self, monkeypatch):
        """No year to compare — trusts the title match."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Eraserhead', 'year': 1977,
                                   'imdb_id': 'tt0074486'},
        )
        result = validate_srrdb_imdb('Eraserhead', None, 'tt0074486')
        assert result is not None
        assert result['imdb_id'] == 'tt0074486'
        assert result['corrected'] is False

    # -- Title mismatch (wrong srrDB IMDB) --

    def test_corrects_wrong_imdb(self, monkeypatch):
        """Snatch N Grab / The Buddha case: srrDB has wrong IMDB, corrected via TMDB."""
        # lookup_imdb_id returns "The Buddha" for srrDB's tt1478841
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'The Buddha', 'year': 2010,
                                   'imdb_id': 'tt1478841'},
        )

        call_log = []

        def mock_get(url, **kwargs):
            call_log.append(url)
            if 'search/movie' in url:
                return TestValidateSrrdbImdb._mock_response({
                    'results': [{'id': 274882, 'title': 'Snatch N Grab',
                                 'release_date': '2010-01-01'}]
                })
            elif '/movie/274882' in url:
                return TestValidateSrrdbImdb._mock_response({
                    'title': 'Snatch N Grab',
                    'imdb_id': 'tt1726758',
                })
            return TestValidateSrrdbImdb._mock_response({}, 404)

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', mock_get)

        result = validate_srrdb_imdb('Snatch N Grab', 2010, 'tt1478841')
        assert result is not None
        assert result['imdb_id'] == 'tt1726758'
        assert result['corrected'] is True
        assert 'tt1478841' in result['detail']
        assert 'tt1726758' in result['detail']

    def test_year_mismatch_triggers_correction(self, monkeypatch):
        """Same title but year >1 apart triggers TMDB search (Faust 1926 vs 2011)."""
        # srrDB says tt0016847 (Faust 1926), but guessit parsed year 2011
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Faust', 'year': 1926,
                                   'imdb_id': 'tt0016847'},
        )

        def mock_get(url, **kwargs):
            if 'search/movie' in url:
                return TestValidateSrrdbImdb._mock_response({
                    'results': [{'id': 83193, 'title': 'Faust',
                                 'release_date': '2011-09-08'}]
                })
            elif '/movie/83193' in url:
                return TestValidateSrrdbImdb._mock_response({
                    'title': 'Faust',
                    'imdb_id': 'tt1437357',
                })
            return TestValidateSrrdbImdb._mock_response({}, 404)

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', mock_get)

        result = validate_srrdb_imdb('Faust', 2011, 'tt0016847')
        assert result is not None
        assert result['imdb_id'] == 'tt1437357'
        assert result['corrected'] is True

    # -- TMDB failure fallbacks --

    def test_lookup_failure_returns_none(self, monkeypatch):
        """When lookup_imdb_id returns None, can't validate — returns None."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: None,
        )
        result = validate_srrdb_imdb('Some Movie', 2020, 'tt9999999')
        assert result is None

    def test_tmdb_search_http_error_returns_none(self, monkeypatch):
        """TMDB search HTTP error returns None (caller keeps original)."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Wrong Movie', 'year': 2020,
                                   'imdb_id': 'tt9999999'},
        )

        import requests as req_module
        monkeypatch.setattr(req_module, 'get',
                            lambda *a, **kw: self._mock_response({}, 500))

        result = validate_srrdb_imdb('Correct Movie', 2020, 'tt9999999')
        assert result is None

    def test_tmdb_search_no_results_returns_none(self, monkeypatch):
        """TMDB search returns empty results — returns None."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Wrong Movie', 'year': 2020,
                                   'imdb_id': 'tt9999999'},
        )

        import requests as req_module
        monkeypatch.setattr(req_module, 'get',
                            lambda *a, **kw: self._mock_response({'results': []}))

        result = validate_srrdb_imdb('Obscure Film', 2020, 'tt9999999')
        assert result is None

    def test_tmdb_movie_details_failure_returns_none(self, monkeypatch):
        """TMDB search succeeds but movie details fails — returns None."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Wrong Movie', 'year': 2020,
                                   'imdb_id': 'tt9999999'},
        )

        call_count = {'n': 0}

        def mock_get(url, **kwargs):
            call_count['n'] += 1
            if 'search/movie' in url:
                return self._mock_response({
                    'results': [{'id': 12345, 'title': 'Right Movie'}]
                })
            # Movie details call fails
            return self._mock_response({}, 500)

        import requests as req_module
        monkeypatch.setattr(req_module, 'get', mock_get)

        result = validate_srrdb_imdb('Right Movie', 2020, 'tt9999999')
        assert result is None

    def test_exception_during_tmdb_returns_none(self, monkeypatch):
        """Network exception during TMDB calls returns None gracefully."""
        monkeypatch.setattr(
            self.MODULE + '.lookup_imdb_id',
            lambda imdb_id, **kw: {'title': 'Wrong Movie', 'year': 2020,
                                   'imdb_id': 'tt9999999'},
        )

        import requests as req_module
        def explode(*a, **kw):
            raise ConnectionError('network down')
        monkeypatch.setattr(req_module, 'get', explode)

        result = validate_srrdb_imdb('Right Movie', 2020, 'tt9999999')
        assert result is None


class TestValidateSrrdbImdbIntegration:
    """Integration tests: validate_srrdb_imdb called from identify_flagged_file."""

    MODULE = 'couchpotato.core.plugins.audit'

    @staticmethod
    def _mock_response(data, status_code=200):
        class MockResp:
            def __init__(self):
                self.status_code = status_code
                self.ok = status_code == 200
            def json(self):
                return data
            def raise_for_status(self):
                if self.status_code >= 400:
                    raise Exception(f'HTTP {self.status_code}')
        return MockResp()

    def test_strategy_c_corrects_imdb(self, monkeypatch, tmp_path):
        """Strategy C (srrdb_crc) uses validate_srrdb_imdb to correct wrong IMDB."""
        # Create a dummy file
        video = tmp_path / 'movie.mkv'
        video.write_bytes(b'\x00' * 1024)

        flags = [{'check': 'runtime', 'severity': 'HIGH',
                  'detail': 'Expected 120, actual 89'}]

        # Mock srrdb_lookup_crc to return the wrong IMDB
        monkeypatch.setattr(
            self.MODULE + '.srrdb_lookup_crc',
            lambda crc: {'release': 'Snatch.N.Grab.2010.1080p.BluRay.x264-SADPANDA',
                         'imdb_id': 'tt1478841', 'size': 5924366555},
        )

        # Mock validate_srrdb_imdb to return the corrected IMDB
        monkeypatch.setattr(
            self.MODULE + '.validate_srrdb_imdb',
            lambda title, year, imdb, **kw: {
                'imdb_id': 'tt1726758', 'corrected': True,
                'detail': 'srrDB mapped to tt1478841 (The Buddha), '
                          'corrected to tt1726758 (Snatch N Grab) via TMDB',
            },
        )

        # Mock OpenSubtitles to not interfere (no API key)
        monkeypatch.setattr(self.MODULE + '._CP_AVAILABLE', False)

        result = identify_flagged_file(
            str(video), flags, None,
            cached_crc32='6A28BFDF',
        )
        assert result['method'] == 'srrdb_crc'
        assert result['identified_title'] == 'Snatch N Grab'
        assert result['identified_imdb'] == 'tt1726758'
        assert result.get('imdb_correction') is not None
        assert 'tt1478841' in result['imdb_correction']

    def test_strategy_c_keeps_valid_imdb(self, monkeypatch, tmp_path):
        """Strategy C keeps srrDB IMDB when validation confirms it's correct."""
        video = tmp_path / 'movie.mkv'
        video.write_bytes(b'\x00' * 1024)

        flags = [{'check': 'template', 'severity': 'MEDIUM', 'detail': '...'}]

        monkeypatch.setattr(
            self.MODULE + '.srrdb_lookup_crc',
            lambda crc: {'release': 'Cinema.Paradiso.1988.MULTi.1080p.BluRay.x264-ROUGH',
                         'imdb_id': 'tt0095765', 'size': 13129011042},
        )

        monkeypatch.setattr(
            self.MODULE + '.validate_srrdb_imdb',
            lambda title, year, imdb, **kw: {
                'imdb_id': 'tt0095765', 'corrected': False, 'detail': None,
            },
        )

        monkeypatch.setattr(self.MODULE + '._CP_AVAILABLE', False)

        result = identify_flagged_file(
            str(video), flags, None,
            cached_crc32='475349BA',
        )
        assert result['method'] == 'srrdb_crc'
        assert result['identified_imdb'] == 'tt0095765'
        assert 'imdb_correction' not in result

    def test_strategy_a_corrects_imdb(self, monkeypatch, tmp_path):
        """Strategy A (container_title) uses validate_srrdb_imdb to correct wrong IMDB."""
        video = tmp_path / 'Buddha, The (2010)' / 'The Buddha.mkv'
        video.parent.mkdir(parents=True)
        video.write_bytes(b'\x00' * 1024)

        flags = [{'check': 'title', 'severity': 'HIGH',
                  'detail': 'Title mismatch'}]

        container_parsed = {
            'title': 'Snatch N Grab',
            'year': 2010,
            'raw': 'Snatch.N.Grab.2010.1080p.BluRay.x264-SADPANDA',
        }

        # Mock srrDB search returning wrong IMDB
        srrdb_response = TestValidateSrrdbImdbIntegration._mock_response({
            'results': [{'release': 'Snatch.N.Grab.2010.1080p.BluRay.x264-SADPANDA',
                         'imdbId': '1478841'}]
        })
        import requests as req_module
        monkeypatch.setattr(req_module, 'get',
                            lambda *a, **kw: srrdb_response)

        # Mock validate_srrdb_imdb to return corrected IMDB
        monkeypatch.setattr(
            self.MODULE + '.validate_srrdb_imdb',
            lambda title, year, imdb, **kw: {
                'imdb_id': 'tt1726758', 'corrected': True,
                'detail': 'corrected via TMDB',
            },
        )

        result = identify_flagged_file(
            str(video), flags, container_parsed,
        )
        assert result['method'] == 'container_title'
        assert result['identified_imdb'] == 'tt1726758'
        assert result.get('imdb_correction') is not None

    def test_strategy_c_validation_failure_keeps_original(self, monkeypatch,
                                                          tmp_path):
        """When validation returns None, Strategy C keeps the srrDB IMDB."""
        video = tmp_path / 'movie.mkv'
        video.write_bytes(b'\x00' * 1024)

        flags = [{'check': 'runtime', 'severity': 'HIGH', 'detail': '...'}]

        monkeypatch.setattr(
            self.MODULE + '.srrdb_lookup_crc',
            lambda crc: {'release': 'Some.Movie.2020.1080p.BluRay.x264-GRP',
                         'imdb_id': 'tt1111111', 'size': 5000000000},
        )

        # validate returns None (TMDB lookup failed)
        monkeypatch.setattr(
            self.MODULE + '.validate_srrdb_imdb',
            lambda title, year, imdb, **kw: None,
        )

        monkeypatch.setattr(self.MODULE + '._CP_AVAILABLE', False)

        result = identify_flagged_file(
            str(video), flags, None,
            cached_crc32='DEADBEEF',
        )
        assert result['method'] == 'srrdb_crc'
        assert result['identified_imdb'] == 'tt1111111'
        assert 'imdb_correction' not in result


# ===========================================================================
# Multi-CD action handling tests
# ===========================================================================

class TestMultiCDDelete:
    """Tests for multi-CD delete_wrong/delete_duplicate/delete_foreign."""

    @staticmethod
    def _make_multi_cd_item(tmp_path, num_cds=4):
        """Create a multi-CD item with real temp files."""
        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd_files = []
        for i in range(1, num_cds + 1):
            f = folder / ('Movie.2020.cd%d.mkv' % i)
            f.write_bytes(b'\x00' * (1024 * i))  # different sizes for fingerprints
            cd_files.append({
                'cd_number': i,
                'file': f.name,
                'file_path': str(f),
                'file_size_bytes': f.stat().st_size,
                'file_fingerprint': '%d:fp_cd%d' % (f.stat().st_size, i),
                'has_flags': True,
            })
        return {
            'item_id': 'test123',
            'file': cd_files[0]['file'],
            'file_path': cd_files[0]['file_path'],
            'file_fingerprint': cd_files[0]['file_fingerprint'],
            'folder': 'Movie (2020)',
            'multi_cd': True,
            'cd_count': num_cds,
            'cd_files': cd_files,
            'flags': [{'check': 'title', 'severity': 'HIGH', 'detail': 'test'}],
            'expected': {'title': 'Movie', 'resolution': '1080p'},
            'actual': {'resolution': '1920x1080'},
        }

    def test_preview_delete_includes_cd_deletes(self, tmp_path):
        """Preview for multi-CD delete includes cd_deletes list."""
        item = self._make_multi_cd_item(tmp_path)
        preview = _preview_delete_wrong(item)
        assert 'cd_deletes' in preview['changes']['filesystem']
        cd_deletes = preview['changes']['filesystem']['cd_deletes']
        assert len(cd_deletes) == 4
        assert cd_deletes[0]['cd_number'] == 1
        assert cd_deletes[3]['cd_number'] == 4
        # delete_path should be cd1's path
        assert preview['changes']['filesystem']['delete_path'] == item['cd_files'][0]['file_path']

    def test_preview_delete_single_cd_no_cd_deletes(self, tmp_path):
        """Single-file item should not have cd_deletes."""
        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        f = folder / 'Movie.2020.mkv'
        f.write_bytes(b'\x00' * 1024)
        item = {
            'item_id': 'single1',
            'file': f.name,
            'file_path': str(f),
            'expected': {'title': 'Movie'},
        }
        preview = _preview_delete_wrong(item)
        assert 'cd_deletes' not in preview['changes']['filesystem']

    def test_execute_delete_all_cds(self, tmp_path):
        """Execute delete removes all CD files."""
        item = self._make_multi_cd_item(tmp_path)
        # Verify files exist
        for cd in item['cd_files']:
            assert os.path.isfile(cd['file_path'])

        success, details = execute_fix_delete_wrong(item)
        assert success
        assert 'deleted_paths' in details
        assert len(details['deleted_paths']) == 4

        # Verify files deleted
        for cd in item['cd_files']:
            assert not os.path.isfile(cd['file_path'])

    def test_execute_delete_cleans_empty_folder(self, tmp_path):
        """Folder is cleaned up when no video files remain."""
        item = self._make_multi_cd_item(tmp_path, num_cds=2)
        success, details = execute_fix_delete_wrong(item)
        assert success
        assert details['folder_cleaned'] is True
        assert not os.path.isdir(tmp_path / 'Movie (2020)')

    def test_execute_delete_preserves_folder_with_other_videos(self, tmp_path):
        """Folder kept if other video files remain."""
        item = self._make_multi_cd_item(tmp_path, num_cds=2)
        # Add another video file
        other = tmp_path / 'Movie (2020)' / 'bonus.mkv'
        other.write_bytes(b'\x00' * 100)
        success, details = execute_fix_delete_wrong(item)
        assert success
        assert details['folder_cleaned'] is False
        assert os.path.isdir(tmp_path / 'Movie (2020)')

    def test_execute_delete_fails_if_cd_missing(self, tmp_path):
        """Fails fast if any CD file is missing."""
        item = self._make_multi_cd_item(tmp_path)
        os.remove(item['cd_files'][2]['file_path'])  # remove cd3
        success, details = execute_fix_delete_wrong(item)
        assert not success
        assert 'CD file not found' in details['error']
        # cd1 and cd2 should NOT have been deleted (fail-fast before any delete)
        assert os.path.isfile(item['cd_files'][0]['file_path'])
        assert os.path.isfile(item['cd_files'][1]['file_path'])

    def test_generate_fix_preview_routes_to_delete(self, tmp_path):
        """generate_fix_preview correctly routes delete actions for multi-CD."""
        item = self._make_multi_cd_item(tmp_path)
        for action in ('delete_wrong', 'delete_duplicate', 'delete_foreign'):
            preview = generate_fix_preview(item, action)
            assert 'cd_deletes' in preview['changes']['filesystem']


class TestMultiCDRenameResolution:
    """Tests for multi-CD rename_resolution."""

    @staticmethod
    def _make_multi_cd_item(tmp_path, num_cds=2):
        folder = tmp_path / 'Movie (2020)'
        folder.mkdir(exist_ok=True)
        cd_files = []
        for i in range(1, num_cds + 1):
            f = folder / ('Movie.2020.720p.cd%d.mkv' % i)
            f.write_bytes(b'\x00' * (1024 * i))
            cd_files.append({
                'cd_number': i,
                'file': f.name,
                'file_path': str(f),
                'file_size_bytes': f.stat().st_size,
                'file_fingerprint': '%d:fp_cd%d' % (f.stat().st_size, i),
                'has_flags': True,
            })
        return {
            'item_id': 'restest1',
            'file': cd_files[0]['file'],
            'file_path': cd_files[0]['file_path'],
            'file_fingerprint': cd_files[0]['file_fingerprint'],
            'folder': 'Movie (2020)',
            'multi_cd': True,
            'cd_count': num_cds,
            'cd_files': cd_files,
            'flags': [{'check': 'resolution', 'severity': 'MEDIUM',
                       'detail': 'Expected 720p but actual is 1080p'}],
            'expected': {
                'title': 'Movie', 'resolution': '720p',
                'db_title': 'Movie, The',
            },
            'actual': {'resolution': '1920x1080'},
        }

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_preview_resolution_includes_cd_renames(self, mock_env, tmp_path):
        """Preview for multi-CD resolution includes cd_renames."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])

        preview = _preview_rename_resolution(item)
        assert 'cd_renames' in preview['changes']['filesystem']
        cd_renames = preview['changes']['filesystem']['cd_renames']
        assert len(cd_renames) == 2
        assert cd_renames[0]['cd_number'] == 1
        assert cd_renames[1]['cd_number'] == 2
        # Paths should differ from originals
        assert cd_renames[0]['old_path'] != cd_renames[0]['new_path']

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_execute_resolution_renames_all_cds(self, mock_env, tmp_path):
        """Execute resolution rename renames all CD files."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])

        success, details = execute_fix_rename_resolution(item)
        assert success
        assert 'cd_renames' in details
        assert len(details['cd_renames']) == 2
        # Old files should be gone
        for cd in item['cd_files']:
            assert not os.path.isfile(cd['file_path'])
        # New files should exist
        for cd_rename in details['cd_renames']:
            assert os.path.isfile(cd_rename['new_path'])

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_execute_resolution_fails_if_cd_missing(self, mock_env, tmp_path):
        """Fails if any CD source file is missing."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])
        os.remove(item['cd_files'][1]['file_path'])  # remove cd2

        success, details = execute_fix_rename_resolution(item)
        assert not success
        assert 'CD file not found' in details['error']


class TestMultiCDRenameEdition:
    """Tests for multi-CD rename_edition."""

    @staticmethod
    def _make_multi_cd_item(tmp_path, num_cds=2):
        folder = tmp_path / 'Movie (2020)'
        folder.mkdir(exist_ok=True)
        cd_files = []
        for i in range(1, num_cds + 1):
            f = folder / ('Movie.2020.1080p.cd%d.mkv' % i)
            f.write_bytes(b'\x00' * (1024 * i))
            cd_files.append({
                'cd_number': i,
                'file': f.name,
                'file_path': str(f),
                'file_size_bytes': f.stat().st_size,
                'file_fingerprint': '%d:fp_cd%d' % (f.stat().st_size, i),
                'has_flags': True,
            })
        return {
            'item_id': 'edtest1',
            'file': cd_files[0]['file'],
            'file_path': cd_files[0]['file_path'],
            'file_fingerprint': cd_files[0]['file_fingerprint'],
            'folder': 'Movie (2020)',
            'multi_cd': True,
            'cd_count': num_cds,
            'cd_files': cd_files,
            'detected_edition': 'Criterion',
            'flags': [{'check': 'edition', 'severity': 'LOW',
                       'detail': 'Edition detected but not in filename'}],
            'expected': {
                'title': 'Movie', 'resolution': '1080p',
                'db_title': 'Movie, The',
            },
            'actual': {'resolution': '1920x1080'},
        }

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_preview_edition_includes_cd_renames(self, mock_env, tmp_path):
        """Preview for multi-CD edition includes cd_renames."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality> <edition><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])

        preview = _preview_rename_edition(item)
        assert 'error' not in preview
        assert 'cd_renames' in preview['changes']['filesystem']
        cd_renames = preview['changes']['filesystem']['cd_renames']
        assert len(cd_renames) == 2

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_execute_edition_renames_all_cds(self, mock_env, tmp_path):
        """Execute edition rename renames all CD files."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality> <edition><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])

        success, details = execute_fix_rename_edition(item)
        assert success
        assert 'cd_renames' in details
        assert len(details['cd_renames']) == 2
        assert details['edition'] == 'Criterion'
        # Old files should be gone
        for cd in item['cd_files']:
            assert not os.path.isfile(cd['file_path'])
        # New files should exist
        for cd_rename in details['cd_renames']:
            assert os.path.isfile(cd_rename['new_path'])

    @mock.patch('couchpotato.core.plugins.audit._CP_AVAILABLE', True)
    @mock.patch('couchpotato.core.plugins.audit.Env')
    def test_execute_edition_fails_if_cd_missing(self, mock_env, tmp_path):
        """Fails if any CD source file is missing."""
        mock_env.setting.side_effect = lambda key, **kw: {
            'file_name': '<thename> (<year>) - <quality> <edition><cd>.<ext>',
            'replace_doubles': False,
            'separator': '',
        }.get(key, kw.get('default', ''))

        item = self._make_multi_cd_item(tmp_path)
        item['expected']['year'] = 2020
        item['imdb_id'] = 'tt1234567'
        item['guessit_tokens'] = parse_guessit_tokens(item['file'])
        os.remove(item['cd_files'][1]['file_path'])

        success, details = execute_fix_rename_edition(item)
        assert not success
        assert 'CD file not found' in details['error']


class TestMultiCDSetAudioLanguage:
    """Tests for multi-CD set_audio_language preview and execute."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            last_report = None

        p = _MockPlugin()
        for name in ('_apply_whisper_result',
                     '_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_ensure_original_hashes',
                     '_post_modification_update', '_cache_identification',
                     '_preview_set_audio_language',
                     '_execute_set_audio_language',
                     '_execute_set_audio_language_single'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        return p

    def test_preview_multi_cd_includes_cd_tracks(self, tmp_path):
        """Preview includes per-CD track info."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd2 = folder / 'Movie.cd2.mkv'
        cd1.write_bytes(b'\x00' * 1024)
        cd2.write_bytes(b'\x00' * 2048)

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            # Create knowledge with whisper for both CDs
            fp1 = '1024:fp_cd1'
            fp2 = '2048:fp_cd2'
            doc1 = p._get_or_create_knowledge(fp1, str(cd1))
            doc1['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'language': 'en',
                            'confidence': 0.95, 'tagged_language': 'de'}],
            }
            p._update_knowledge(doc1)
            doc2 = p._get_or_create_knowledge(fp2, str(cd2))
            doc2['whisper'] = {
                'language': 'fr', 'confidence': 0.88,
                'tracks': [{'track_index': 0, 'language': 'fr',
                            'confidence': 0.88, 'tagged_language': ''}],
            }
            p._update_knowledge(doc2)

            item = {
                'file_path': str(cd1),
                'file_fingerprint': fp1,
                'multi_cd': True,
                'cd_files': [
                    {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                     'file_fingerprint': fp1},
                    {'cd_number': 2, 'file': cd2.name, 'file_path': str(cd2),
                     'file_fingerprint': fp2},
                ],
                'actual': {'audio_tracks': [{'codec': 'AAC', 'channels': '2.0'}]},
            }

            preview = p._preview_set_audio_language(item)
            assert 'cd_tracks' in preview
            assert len(preview['cd_tracks']) == 2
            assert preview['cd_tracks'][0]['cd_number'] == 1
            assert preview['cd_tracks'][0]['has_whisper'] is True
            assert preview['cd_tracks'][1]['cd_number'] == 2
            assert len(preview['cd_tracks'][1]['tracks']) == 1

    def test_preview_multi_cd_missing_whisper_for_one_cd(self, tmp_path):
        """Preview marks CD without whisper as has_whisper=False."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd2 = folder / 'Movie.cd2.mkv'
        cd1.write_bytes(b'\x00' * 1024)
        cd2.write_bytes(b'\x00' * 2048)

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            fp1 = '1024:fp_cd1'
            fp2 = '2048:fp_cd2'
            doc1 = p._get_or_create_knowledge(fp1, str(cd1))
            doc1['whisper'] = {
                'language': 'en', 'confidence': 0.95,
                'tracks': [{'track_index': 0, 'language': 'en',
                            'confidence': 0.95, 'tagged_language': 'de'}],
            }
            p._update_knowledge(doc1)
            # cd2 has no whisper data

            item = {
                'file_path': str(cd1),
                'file_fingerprint': fp1,
                'multi_cd': True,
                'cd_files': [
                    {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                     'file_fingerprint': fp1},
                    {'cd_number': 2, 'file': cd2.name, 'file_path': str(cd2),
                     'file_fingerprint': fp2},
                ],
                'actual': {'audio_tracks': [{'codec': 'AAC', 'channels': '2.0'}]},
            }

            preview = p._preview_set_audio_language(item)
            assert preview['cd_tracks'][1]['has_whisper'] is False
            assert preview['cd_tracks'][1]['tracks'] == []

    @mock.patch('couchpotato.core.plugins.audit.subprocess')
    def test_execute_multi_cd_processes_all_files(self, mock_sub, tmp_path):
        """Execute processes each CD file independently."""
        mock_sub.run.return_value = mock.Mock(returncode=0, stderr='')
        mock_sub.TimeoutExpired = subprocess.TimeoutExpired

        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd2 = folder / 'Movie.cd2.mkv'
        cd1.write_bytes(b'\x00' * 1024)
        cd2.write_bytes(b'\x00' * 2048)

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            fp1 = '1024:fp_cd1'
            fp2 = '2048:fp_cd2'
            for fp, path in [(fp1, str(cd1)), (fp2, str(cd2))]:
                doc = p._get_or_create_knowledge(fp, path)
                doc['whisper'] = {
                    'language': 'en', 'confidence': 0.95,
                    'tracks': [{'track_index': 0, 'language': 'en',
                                'confidence': 0.95, 'tagged_language': 'de'}],
                }
                p._update_knowledge(doc)

            item = {
                'file_path': str(cd1),
                'file': cd1.name,
                'file_fingerprint': fp1,
                'multi_cd': True,
                'cd_files': [
                    {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                     'file_fingerprint': fp1},
                    {'cd_number': 2, 'file': cd2.name, 'file_path': str(cd2),
                     'file_fingerprint': fp2},
                ],
                'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW',
                           'detail': 'test'}],
                'expected': {},
                'actual': {'audio_tracks': []},
            }

            success, details = p._execute_set_audio_language(item)
            assert success
            assert details['tracks_changed'] == 2  # one track per CD
            assert len(details['cd_results']) == 2
            assert details['cd_results'][0]['success'] is True
            assert details['cd_results'][1]['success'] is True

            # audio_mislabeled flag should be removed
            assert all(f['check'] != 'audio_mislabeled' for f in item['flags'])


class TestMultiCDVerifyAudio:
    """Tests for multi-CD verify_audio."""

    @staticmethod
    def _make_db(tmp_path):
        from couchpotato.core.db import CouchDB
        db_dir = str(tmp_path / 'db')
        os.makedirs(db_dir, exist_ok=True)
        db = CouchDB(db_dir)
        db.create()
        return db

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        import types

        class _MockPlugin:
            last_report = {'flagged': []}

        p = _MockPlugin()
        for name in ('_apply_whisper_result',
                     '_get_knowledge', '_get_or_create_knowledge',
                     '_update_knowledge', '_find_item',
                     'verifyView', '_verify_multi_cd',
                     '_save_results'):
            method = getattr(Audit, name)
            setattr(p, name, types.MethodType(method, p))
        # _save_results needs a path — mock it to no-op
        p._save_results = lambda: None
        return p

    def test_verify_multi_cd_merges_tracks(self, tmp_path):
        """Merged whisper results include tracks from all CDs."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd2 = folder / 'Movie.cd2.mkv'
        cd1.write_bytes(b'\x00' * 1024)
        cd2.write_bytes(b'\x00' * 2048)

        item = {
            'item_id': 'verify1',
            'file_path': str(cd1),
            'file_fingerprint': '1024:fp_cd1',
            'multi_cd': True,
            'cd_files': [
                {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                 'file_fingerprint': '1024:fp_cd1'},
                {'cd_number': 2, 'file': cd2.name, 'file_path': str(cd2),
                 'file_fingerprint': '2048:fp_cd2'},
            ],
            'flags': [{'check': 'unknown_audio', 'severity': 'MEDIUM', 'detail': 'test'}],
            'expected': {},
            'actual': {},
        }
        p.last_report = {'flagged': [item]}

        # Mock whisper to return per-file results
        def mock_whisper(file_path, **kw):
            if 'cd1' in file_path:
                return {
                    'language': 'en', 'confidence': 0.95,
                    'tracks': [{'track_index': 0, 'language': 'en',
                                'confidence': 0.95, 'tagged_language': ''}],
                }
            else:
                return {
                    'language': 'fr', 'confidence': 0.80,
                    'tracks': [{'track_index': 0, 'language': 'fr',
                                'confidence': 0.80, 'tagged_language': ''}],
                }

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db), \
             mock.patch('couchpotato.core.plugins.audit.whisper_verify_audio',
                        side_effect=mock_whisper):
            result = p._verify_multi_cd(item)

        assert result['success'] is True
        assert result['whisper']['language'] == 'en'  # best confidence
        assert result['whisper']['confidence'] == 0.95
        assert len(result['whisper']['tracks']) == 2  # merged from both CDs
        assert len(result['cd_results']) == 2

    def test_verify_multi_cd_uses_cache(self, tmp_path):
        """Cached whisper results are used without re-running whisper."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd2 = folder / 'Movie.cd2.mkv'
        cd1.write_bytes(b'\x00' * 1024)
        cd2.write_bytes(b'\x00' * 2048)

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db):
            # Pre-populate whisper cache
            for fp, path, lang in [('1024:fp_cd1', str(cd1), 'en'),
                                    ('2048:fp_cd2', str(cd2), 'en')]:
                doc = p._get_or_create_knowledge(fp, path)
                doc['whisper'] = {
                    'language': lang, 'confidence': 0.95,
                    'tracks': [{'track_index': 0, 'language': lang,
                                'confidence': 0.95, 'tagged_language': ''}],
                }
                p._update_knowledge(doc)

            item = {
                'item_id': 'verify2',
                'file_path': str(cd1),
                'file_fingerprint': '1024:fp_cd1',
                'multi_cd': True,
                'cd_files': [
                    {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                     'file_fingerprint': '1024:fp_cd1'},
                    {'cd_number': 2, 'file': cd2.name, 'file_path': str(cd2),
                     'file_fingerprint': '2048:fp_cd2'},
                ],
                'flags': [{'check': 'unknown_audio', 'severity': 'MEDIUM',
                           'detail': 'test'}],
                'expected': {},
                'actual': {},
            }
            p.last_report = {'flagged': [item]}

            with mock.patch('couchpotato.core.plugins.audit.whisper_verify_audio') \
                    as mock_whisper:
                result = p._verify_multi_cd(item)

            assert result['success'] is True
            assert result['cached'] is True
            # whisper should NOT have been called since cache was hit
            mock_whisper.assert_not_called()

    def test_verify_dispatches_to_multi_cd(self, tmp_path):
        """verifyView routes multi-CD items to _verify_multi_cd."""
        db = self._make_db(tmp_path)
        p = self._make_plugin()

        folder = tmp_path / 'Movie (2020)'
        folder.mkdir()
        cd1 = folder / 'Movie.cd1.mkv'
        cd1.write_bytes(b'\x00' * 1024)

        item = {
            'item_id': 'verify3',
            'file_path': str(cd1),
            'file_fingerprint': '1024:fp_cd1',
            'multi_cd': True,
            'cd_files': [
                {'cd_number': 1, 'file': cd1.name, 'file_path': str(cd1),
                 'file_fingerprint': '1024:fp_cd1'},
            ],
            'flags': [{'check': 'unknown_audio', 'severity': 'MEDIUM',
                       'detail': 'test'}],
            'expected': {},
            'actual': {},
        }
        p.last_report = {'flagged': [item]}

        def mock_whisper(file_path, **kw):
            return {
                'language': 'en', 'confidence': 0.92,
                'tracks': [{'track_index': 0, 'language': 'en',
                            'confidence': 0.92, 'tagged_language': ''}],
            }

        with mock.patch('couchpotato.core.plugins.audit.get_db', return_value=db), \
             mock.patch('couchpotato.core.plugins.audit.whisper_verify_audio',
                        side_effect=mock_whisper):
            result = p.verifyView(item_id='verify3')

        assert result['success'] is True
        assert 'cd_results' in result  # multi-CD path was used


# ---------------------------------------------------------------------------
# Verify audio should NOT mark items as fixed when they still need work
# ---------------------------------------------------------------------------

class TestVerifyAudioNotFixedWhenActionChanges:
    """After verify_audio runs, _apply_whisper_result may change
    recommended_action from verify_audio to set_audio_language or
    delete_foreign. In that case, the item should NOT be marked as fixed.
    """

    @staticmethod
    def _make_plugin(flagged_items):
        from couchpotato.core.plugins.audit import Audit
        plugin = object.__new__(Audit)
        plugin.last_report = {'flagged': flagged_items}
        plugin.fix_in_progress = False
        plugin._knowledge_cache = {}
        return plugin

    @staticmethod
    def _make_verify_item(item_id='v1', fingerprint='100:abc',
                          audio_lang='', original_language='en'):
        """Build an item with recommended_action=verify_audio."""
        tracks = [{'codec': 'AAC', 'channels': '2.0', 'language': audio_lang}]
        return {
            'item_id': item_id,
            'file_fingerprint': fingerprint,
            'folder': 'Test Movie (2024)',
            'file': 'test.mkv',
            'file_path': '/movies/test.mkv',
            'original_language': original_language,
            'actual': {'audio_tracks': tracks},
            'expected': {'title': 'Test Movie', 'year': 2024},
            'flags': [{'check': 'unknown_audio', 'severity': 'LOW',
                        'detail': 'test'}],
            'flag_count': 1,
            'recommended_action': 'verify_audio',
        }

    def test_batch_verify_not_fixed_when_set_audio_language(self):
        """Batch verify_audio should NOT mark item as fixed when
        _apply_whisper_result changes recommended_action to set_audio_language."""
        from unittest.mock import patch

        item = self._make_verify_item()
        plugin = self._make_plugin([item])

        # Whisper detects English but tag is wrong → audio_mislabeled
        whisper_result = {
            'language': 'en',
            'confidence': 0.95,
            'tracks': [{'track_index': 0, 'language': 'en',
                         'confidence': 0.95, 'tagged_language': ''}],
        }

        mock_doc = {'whisper': whisper_result, 'current_fingerprint': '100:abc'}

        # Don't mock _apply_whisper_result — let it run for real so it
        # changes recommended_action to set_audio_language
        with patch.object(plugin, '_get_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_update_knowledge'), \
             patch.object(plugin, '_mark_fixed') as mock_mark, \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'), \
             patch('os.path.isfile', return_value=True):

            results = plugin._run_batch_fix(
                action='verify_audio',
                items=[item],
                dry_run=False,
            )

        # Whisper found English but tag was empty → audio_mislabeled →
        # recommended_action is now set_audio_language
        assert item['recommended_action'] == 'set_audio_language'
        # Item should NOT be marked as fixed
        mock_mark.assert_not_called()
        # But the batch should still report success
        assert results[0]['success'] is True

    def test_batch_verify_not_fixed_when_delete_foreign(self):
        """Batch verify_audio should NOT mark item as fixed when
        _apply_whisper_result changes recommended_action to delete_foreign."""
        from unittest.mock import patch

        item = self._make_verify_item(original_language='ja')
        plugin = self._make_plugin([item])

        # Whisper detects Japanese (no English) → foreign_audio → delete_foreign
        whisper_result = {
            'language': 'ja',
            'confidence': 0.90,
            'tracks': [{'track_index': 0, 'language': 'ja',
                         'confidence': 0.90, 'tagged_language': 'ja'}],
        }

        mock_doc = {'whisper': whisper_result, 'current_fingerprint': '100:abc'}

        with patch.object(plugin, '_get_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_update_knowledge'), \
             patch.object(plugin, '_mark_fixed') as mock_mark, \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'), \
             patch('os.path.isfile', return_value=True):

            results = plugin._run_batch_fix(
                action='verify_audio',
                items=[item],
                dry_run=False,
            )

        assert item['recommended_action'] == 'delete_foreign'
        mock_mark.assert_not_called()
        assert results[0]['success'] is True

    def test_batch_verify_still_fixed_when_no_action_change(self):
        """Batch verify_audio SHOULD mark item as fixed when the item
        doesn't need further action (e.g. tags are already correct)."""
        from unittest.mock import patch

        item = self._make_verify_item()
        # Tag already says 'en' — whisper confirms, no mislabel
        item['actual']['audio_tracks'] = [
            {'codec': 'AAC', 'channels': '2.0', 'language': 'en'}
        ]
        plugin = self._make_plugin([item])

        whisper_result = {
            'language': 'en',
            'confidence': 0.95,
            'tracks': [{'track_index': 0, 'language': 'en',
                         'confidence': 0.95, 'tagged_language': 'en'}],
        }

        mock_doc = {'whisper': whisper_result, 'current_fingerprint': '100:abc'}

        with patch.object(plugin, '_get_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=mock_doc), \
             patch.object(plugin, '_update_knowledge'), \
             patch.object(plugin, '_mark_fixed') as mock_mark, \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'), \
             patch('os.path.isfile', return_value=True):

            results = plugin._run_batch_fix(
                action='verify_audio',
                items=[item],
                dry_run=False,
            )

        # Tags matched whisper → no further action needed → mark as fixed
        assert item['recommended_action'] not in ('set_audio_language', 'delete_foreign')
        mock_mark.assert_called_once()

    def test_reconcile_clears_stale_verify_audio_fixed(self, tmp_path):
        """_reconcile_actions clears stale fixed status where verify_audio
        was applied but item still needs set_audio_language."""
        from couchpotato.core.plugins.audit import Audit
        from unittest.mock import patch

        plugin = object.__new__(Audit)
        plugin._knowledge_cache = {}

        # Item already has recommended_action=set_audio_language but was
        # incorrectly marked fixed by verify_audio
        item = {
            'item_id': 'stale1',
            'file_fingerprint': '100:abc',
            'folder': 'Movie (2020)',
            'file': 'movie.mkv',
            'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW',
                        'detail': 'test'}],
            'recommended_action': 'set_audio_language',
            'fixed': {
                'action': 'verify_audio',
                'timestamp': 1000000,
                'details': {'language': 'en', 'confidence': 0.95},
            },
        }
        plugin.last_report = {'flagged': [item]}

        # Empty actions file (already truncated)
        actions_file = tmp_path / 'audit_actions.jsonl'
        actions_file.write_text('')

        with patch.object(plugin, '_get_actions_path',
                          return_value=str(actions_file)), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'):
            plugin._reconcile_actions()

        # Stale fixed should be cleared
        assert item['fixed'] is None

    def test_reconcile_keeps_valid_verify_audio_fixed(self, tmp_path):
        """_reconcile_actions does NOT clear fixed for items where
        verify_audio is actually the final action (no further work needed)."""
        from couchpotato.core.plugins.audit import Audit
        from unittest.mock import patch

        plugin = object.__new__(Audit)
        plugin._knowledge_cache = {}

        # Item has recommended_action that doesn't need further work
        item = {
            'item_id': 'valid1',
            'file_fingerprint': '100:def',
            'folder': 'Movie (2021)',
            'file': 'movie.mkv',
            'flags': [],
            'recommended_action': 'none',
            'fixed': {
                'action': 'verify_audio',
                'timestamp': 1000000,
                'details': {'language': 'en', 'confidence': 0.95},
            },
        }
        plugin.last_report = {'flagged': [item]}

        actions_file = tmp_path / 'audit_actions.jsonl'
        actions_file.write_text('')

        with patch.object(plugin, '_get_actions_path',
                          return_value=str(actions_file)), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'):
            plugin._reconcile_actions()

        # Fixed should remain intact
        assert item['fixed'] is not None
        assert item['fixed']['action'] == 'verify_audio'

    def test_reconcile_does_not_apply_verify_audio_to_set_audio_language_items(self, tmp_path):
        """_reconcile_actions should skip verify_audio action records for
        items whose recommended_action is set_audio_language."""
        from couchpotato.core.plugins.audit import Audit
        from unittest.mock import patch
        import json

        plugin = object.__new__(Audit)
        plugin._knowledge_cache = {}

        item = {
            'item_id': 'new1',
            'file_fingerprint': '100:ghi',
            'folder': 'Movie (2022)',
            'file': 'movie.mkv',
            'flags': [{'check': 'audio_mislabeled', 'severity': 'LOW',
                        'detail': 'test'}],
            'recommended_action': 'set_audio_language',
            'fixed': None,
        }
        plugin.last_report = {'flagged': [item]}

        # Actions file has a verify_audio record for this item
        actions_file = tmp_path / 'audit_actions.jsonl'
        record = {
            'item_id': 'new1',
            'action': 'verify_audio',
            'success': True,
            'timestamp': 1000000,
            'details': {'language': 'en', 'confidence': 0.95},
        }
        actions_file.write_text(json.dumps(record) + '\n')

        with patch.object(plugin, '_get_actions_path',
                          return_value=str(actions_file)), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_truncate_actions'):
            plugin._reconcile_actions()

        # verify_audio record should NOT have been applied as 'fixed'
        assert item['fixed'] is None


# ---------------------------------------------------------------------------
# SD quality label for template — renamer uses 'SD', not '480p'
# ---------------------------------------------------------------------------

class TestQualityLabelForTemplate:
    """The <quality> template token should produce 'SD' for SD-resolution
    content, matching the CouchPotato renamer's quality label."""

    def test_480p_maps_to_sd(self):
        from couchpotato.core.plugins.audit import _quality_label_for_template
        assert _quality_label_for_template(720, 480) == 'SD'

    def test_576p_maps_to_sd(self):
        from couchpotato.core.plugins.audit import _quality_label_for_template
        assert _quality_label_for_template(720, 576) == 'SD'

    def test_1080p_unchanged(self):
        from couchpotato.core.plugins.audit import _quality_label_for_template
        assert _quality_label_for_template(1920, 1080) == '1080p'

    def test_720p_unchanged(self):
        from couchpotato.core.plugins.audit import _quality_label_for_template
        assert _quality_label_for_template(1280, 720) == '720p'

    def test_2160p_unchanged(self):
        from couchpotato.core.plugins.audit import _quality_label_for_template
        assert _quality_label_for_template(3840, 2160) == '2160p'

    def test_build_expected_filename_uses_sd(self):
        """build_expected_filename should use 'SD' for 480p content."""
        from couchpotato.core.plugins.audit import build_expected_filename

        item = {
            'file': 'Two Front Teeth (2006) SD {imdb-tt0498397}.mkv',
            'file_path': '/movies/Two Front Teeth (2006)/Two Front Teeth (2006) SD {imdb-tt0498397}.mkv',
            'imdb_id': 'tt0498397',
            'expected': {'title': 'Two Front Teeth', 'year': 2006,
                         'db_title': 'Two Front Teeth', 'resolution': ''},
            'actual': {'resolution': '720x480'},
            'guessit_tokens': {'video': '', 'audio': '', 'source': '',
                               'group': 'imdb-tt0498397', 'audio_channels': '',
                               'quality_type': 'SD'},
        }
        template = '<thename> (<year>) <quality> {imdb-<imdb_id>}.<ext>'
        result = build_expected_filename(item, template)
        assert result == 'Two Front Teeth (2006) SD {imdb-tt0498397}.mkv'

    def test_check_template_conformance_no_flag_for_sd(self):
        """SD file named with 'SD' should not be flagged for template mismatch."""
        from couchpotato.core.plugins.audit import check_template

        item = {
            'file': 'Two Front Teeth (2006) SD {imdb-tt0498397}.mkv',
            'file_path': '/movies/Two Front Teeth (2006)/Two Front Teeth (2006) SD {imdb-tt0498397}.mkv',
            'imdb_id': 'tt0498397',
            'expected': {'title': 'Two Front Teeth', 'year': 2006,
                         'db_title': 'Two Front Teeth', 'resolution': ''},
            'actual': {'resolution': '720x480'},
            'guessit_tokens': {'video': '', 'audio': '', 'source': '',
                               'group': 'imdb-tt0498397', 'audio_channels': '',
                               'quality_type': 'SD'},
        }
        template = '<thename> (<year>) <quality> {imdb-<imdb_id>}.<ext>'
        result = check_template(item, template)
        # Should not flag — filename already matches
        assert result is None


# ---------------------------------------------------------------------------
# Single-folder scan should merge into existing report, not replace it
# ---------------------------------------------------------------------------

class TestSingleFolderScanMerge:
    """scan_path scans should merge results into existing last_report."""

    @staticmethod
    def _make_plugin():
        from couchpotato.core.plugins.audit import Audit
        plugin = object.__new__(Audit)
        plugin._knowledge_cache = {}
        plugin.fix_in_progress = False
        plugin._cancel = [False]
        plugin.in_progress = False
        return plugin

    def test_scan_path_merges_into_existing_report(self):
        """A scan_path scan should keep existing items and add/replace
        items for the scanned folder only."""
        from unittest.mock import patch

        plugin = self._make_plugin()

        # Existing report with items from two folders
        existing_item_a = {
            'item_id': 'a1', 'folder': 'Movie A (2020)',
            'file': 'a.mkv', 'flags': [{'check': 'resolution', 'severity': 'HIGH'}],
            'recommended_action': 'rename_resolution', 'fixed': None,
        }
        existing_item_b = {
            'item_id': 'b1', 'folder': 'Movie B (2021)',
            'file': 'b.mkv', 'flags': [{'check': 'template', 'severity': 'MEDIUM'}],
            'recommended_action': 'rename_template', 'fixed': None,
        }
        plugin.last_report = {
            'flagged': [existing_item_a, existing_item_b],
            'total_flagged': 2,
            'total_scanned': 100,
            'scan_timestamp': '2026-01-01T00:00:00',
        }

        # Scan returns updated result for Movie A only
        new_item_a = {
            'item_id': 'a2', 'folder': 'Movie A (2020)',
            'file': 'a.mkv', 'flags': [{'check': 'unknown_audio', 'severity': 'LOW'}],
            'recommended_action': 'verify_audio',
        }
        scan_result = {
            'total_scanned': 1,
            'total_flagged': 1,
            'total_errors': 0,
            'cancelled': False,
            'flagged': [new_item_a],
            'seen_fingerprints': {},
            'release_by_filepath': {},
        }

        with patch('couchpotato.core.plugins.audit.scan_library',
                   return_value=scan_result), \
             patch.object(plugin, '_get_movies_dir', return_value='/movies'), \
             patch.object(plugin, '_get_db_path', return_value='/db.json'), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=None), \
             patch('os.path.isfile', return_value=True):
            plugin._run_scan(scan_path='Movie A (2020)')

        flagged = plugin.last_report['flagged']
        # Should have 2 items: updated Movie A + unchanged Movie B
        assert len(flagged) == 2
        folders = {i['folder'] for i in flagged}
        assert folders == {'Movie A (2020)', 'Movie B (2021)'}
        # Movie A should be the new version
        a_items = [i for i in flagged if i['folder'] == 'Movie A (2020)']
        assert a_items[0]['item_id'] == 'a2'
        assert a_items[0]['recommended_action'] == 'verify_audio'
        # Movie B should be unchanged
        b_items = [i for i in flagged if i['folder'] == 'Movie B (2021)']
        assert b_items[0]['item_id'] == 'b1'

    def test_scan_path_removes_folder_when_clean(self):
        """If a folder scan returns no flagged items, old items for that
        folder should be removed from the report."""
        from unittest.mock import patch

        plugin = self._make_plugin()

        existing_item = {
            'item_id': 'c1', 'folder': 'Fixed Movie (2019)',
            'file': 'c.mkv', 'flags': [{'check': 'template', 'severity': 'LOW'}],
            'recommended_action': 'rename_template', 'fixed': None,
        }
        plugin.last_report = {
            'flagged': [existing_item],
            'total_flagged': 1,
            'total_scanned': 100,
            'scan_timestamp': '2026-01-01T00:00:00',
        }

        # Scan returns clean (no flags)
        scan_result = {
            'total_scanned': 1, 'total_flagged': 0, 'total_errors': 0,
            'cancelled': False, 'flagged': [],
            'seen_fingerprints': {}, 'release_by_filepath': {},
        }

        with patch('couchpotato.core.plugins.audit.scan_library',
                   return_value=scan_result), \
             patch.object(plugin, '_get_movies_dir', return_value='/movies'), \
             patch.object(plugin, '_get_db_path', return_value='/db.json'), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=None), \
             patch('os.path.isfile', return_value=True):
            plugin._run_scan(scan_path='Fixed Movie (2019)')

        assert len(plugin.last_report['flagged']) == 0
        assert plugin.last_report['total_flagged'] == 0

    def test_full_scan_replaces_report(self):
        """A full scan (no scan_path) should replace the entire report."""
        from unittest.mock import patch

        plugin = self._make_plugin()

        plugin.last_report = {
            'flagged': [{'item_id': 'old', 'folder': 'Old (2020)'}],
            'total_flagged': 1,
        }

        scan_result = {
            'total_scanned': 50, 'total_flagged': 1, 'total_errors': 0,
            'cancelled': False,
            'flagged': [{'item_id': 'new', 'folder': 'New (2021)',
                         'flags': [], 'file_fingerprint': '100:abc'}],
            'seen_fingerprints': {}, 'release_by_filepath': {},
        }

        with patch('couchpotato.core.plugins.audit.scan_library',
                   return_value=scan_result), \
             patch.object(plugin, '_get_movies_dir', return_value='/movies'), \
             patch.object(plugin, '_get_db_path', return_value='/db.json'), \
             patch.object(plugin, '_save_results'), \
             patch.object(plugin, '_get_or_create_knowledge', return_value=None), \
             patch('os.path.isfile', return_value=True):
            plugin._run_scan()

        # Old items should be gone
        assert len(plugin.last_report['flagged']) == 1
        assert plugin.last_report['flagged'][0]['item_id'] == 'new'
