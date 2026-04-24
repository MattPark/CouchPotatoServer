"""Tests for automation language filtering.

Tests the allowed_languages feature in both:
- isMinimalMovie() in base.py (chart providers)
- addMovies() in automation.py (IMDB Watchlist / catch-all)
"""
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_movie(**overrides):
    """Build a minimal movie dict that passes all existing isMinimalMovie checks."""
    movie = {
        'original_title': 'Test Movie',
        'rating': 8.0,
        'votes': 5000,
        'year': 2024,
        'genres': ['Action', 'Drama'],
        'original_language': 'en',
    }
    movie.update(overrides)
    return movie


def _make_automation_instance(allowed_languages='', required_genres='', ignored_genres='',
                               year=2000, rating=5.0, votes=100):
    """Create an Automation instance with mocked getMinimal."""
    from couchpotato.core.media.movie.providers.automation.base import Automation

    instance = object.__new__(Automation)

    settings = {
        'year': year,
        'rating': rating,
        'votes': votes,
        'required_genres': required_genres,
        'ignored_genres': ignored_genres,
        'allowed_languages': allowed_languages,
    }
    instance.getMinimal = lambda min_type: settings.get(min_type, '')
    return instance


# ---------------------------------------------------------------------------
# isMinimalMovie — language filtering
# ---------------------------------------------------------------------------

class TestIsMinimalMovieLanguageFilter:

    def test_blocks_movie_not_in_allowed_languages(self):
        inst = _make_automation_instance(allowed_languages='en, es, fr')
        movie = _make_movie(original_language='ko')
        assert inst.isMinimalMovie(movie) is False

    def test_allows_movie_in_allowed_languages(self):
        inst = _make_automation_instance(allowed_languages='en, es, fr')
        movie = _make_movie(original_language='en')
        assert inst.isMinimalMovie(movie) is True

    def test_allows_all_when_allowed_languages_empty(self):
        inst = _make_automation_instance(allowed_languages='')
        movie = _make_movie(original_language='ko')
        assert inst.isMinimalMovie(movie) is True

    def test_allows_movie_when_original_language_missing(self):
        """Trakt and other providers may not include original_language."""
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie()
        del movie['original_language']
        assert inst.isMinimalMovie(movie) is True

    def test_allows_movie_when_original_language_is_none(self):
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie(original_language=None)
        assert inst.isMinimalMovie(movie) is True

    def test_case_insensitive_setting(self):
        inst = _make_automation_instance(allowed_languages='EN, ES')
        movie = _make_movie(original_language='en')
        assert inst.isMinimalMovie(movie) is True

    def test_case_insensitive_movie_language(self):
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie(original_language='EN')
        assert inst.isMinimalMovie(movie) is True

    def test_single_allowed_language(self):
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie(original_language='en')
        assert inst.isMinimalMovie(movie) is True

    def test_single_allowed_language_blocks_other(self):
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie(original_language='fr')
        assert inst.isMinimalMovie(movie) is False

    def test_whitespace_in_allowed_languages_handled(self):
        inst = _make_automation_instance(allowed_languages=' en , es , fr ')
        movie = _make_movie(original_language='es')
        assert inst.isMinimalMovie(movie) is True

    def test_language_check_runs_after_genre_check(self):
        """If movie is already blocked by genre, language check is irrelevant."""
        inst = _make_automation_instance(ignored_genres='horror', allowed_languages='en')
        movie = _make_movie(genres=['Horror'], original_language='en')
        assert inst.isMinimalMovie(movie) is False

    def test_empty_string_language_not_blocked(self):
        """If original_language is empty string, don't block (can't verify)."""
        inst = _make_automation_instance(allowed_languages='en')
        movie = _make_movie(original_language='')
        assert inst.isMinimalMovie(movie) is True

    def test_multiple_allowed_languages(self):
        inst = _make_automation_instance(allowed_languages='en, es, fr, de, it')
        movie = _make_movie(original_language='de')
        assert inst.isMinimalMovie(movie) is True

    def test_multiple_allowed_languages_blocks_unlisted(self):
        inst = _make_automation_instance(allowed_languages='en, es, fr, de, it')
        movie = _make_movie(original_language='ja')
        assert inst.isMinimalMovie(movie) is False


# ---------------------------------------------------------------------------
# addMovies — language filtering for watchlist path
# ---------------------------------------------------------------------------

class TestAddMoviesLanguageFilter:

    @patch('couchpotato.core.plugins.automation.Env')
    @patch('couchpotato.core.plugins.automation.fireEvent')
    def test_blocks_movie_not_in_allowed_languages(self, mock_fire, mock_env):
        from couchpotato.core.plugins.automation import Automation as AutomationPlugin

        inst = object.__new__(AutomationPlugin)
        inst.shuttingDown = lambda: False
        inst.conf = lambda key, **kw: kw.get('default', 12)

        mock_fire.side_effect = [
            ['tt1234567'],  # automation.get_movies
            {'original_language': 'ko', 'original_title': 'Test'},  # movie.info
        ]
        mock_env.setting.return_value = 'en'
        mock_env.prop.return_value = False  # not yet added

        result = inst.addMovies()

        assert result is True
        # Should mark as processed but NOT call movie.add
        calls = [str(c) for c in mock_fire.call_args_list]
        assert not any('movie.add' in c for c in calls)
        mock_env.prop.assert_any_call('automation.added.tt1234567', True)

    @patch('couchpotato.core.plugins.automation.Env')
    @patch('couchpotato.core.plugins.automation.fireEvent')
    def test_allows_movie_in_allowed_languages(self, mock_fire, mock_env):
        from couchpotato.core.plugins.automation import Automation as AutomationPlugin

        inst = object.__new__(AutomationPlugin)
        inst.shuttingDown = lambda: False
        inst.conf = lambda key, **kw: kw.get('default', 12)

        mock_fire.side_effect = [
            ['tt1234567'],  # automation.get_movies
            {'original_language': 'en', 'original_title': 'Test'},  # movie.info
            {'_id': 'abc123'},  # movie.add
            {'title': 'Test'},  # media.get
            None,  # movie.searcher.single
        ]
        mock_env.setting.return_value = 'en'
        mock_env.prop.return_value = False

        result = inst.addMovies()

        assert result is True
        add_calls = [c for c in mock_fire.call_args_list if c[0][0] == 'movie.add']
        assert len(add_calls) == 1

    @patch('couchpotato.core.plugins.automation.Env')
    @patch('couchpotato.core.plugins.automation.fireEvent')
    def test_skips_language_check_when_no_allowed_languages(self, mock_fire, mock_env):
        from couchpotato.core.plugins.automation import Automation as AutomationPlugin

        inst = object.__new__(AutomationPlugin)
        inst.shuttingDown = lambda: False
        inst.conf = lambda key, **kw: kw.get('default', 12)

        mock_fire.side_effect = [
            ['tt1234567'],  # automation.get_movies
            # No movie.info call expected since allowed_languages is empty
            {'_id': 'abc123'},  # movie.add
            {'title': 'Test'},  # media.get
            None,  # movie.searcher.single
        ]
        mock_env.setting.return_value = ''  # no allowed languages = allow all
        mock_env.prop.return_value = False

        result = inst.addMovies()

        assert result is True
        info_calls = [c for c in mock_fire.call_args_list if len(c[0]) > 0 and c[0][0] == 'movie.info']
        assert len(info_calls) == 0

    @patch('couchpotato.core.plugins.automation.Env')
    @patch('couchpotato.core.plugins.automation.fireEvent')
    def test_skips_already_added_movies(self, mock_fire, mock_env):
        from couchpotato.core.plugins.automation import Automation as AutomationPlugin

        inst = object.__new__(AutomationPlugin)
        inst.shuttingDown = lambda: False
        inst.conf = lambda key, **kw: kw.get('default', 12)

        mock_fire.side_effect = [
            ['tt1234567'],  # automation.get_movies
        ]
        mock_env.setting.return_value = 'en'
        mock_env.prop.return_value = True  # already added

        result = inst.addMovies()

        assert result is True
        assert mock_fire.call_count == 1  # only automation.get_movies

    @patch('couchpotato.core.plugins.automation.Env')
    @patch('couchpotato.core.plugins.automation.fireEvent')
    def test_allows_movie_when_info_has_no_language(self, mock_fire, mock_env):
        """If TMDB doesn't return original_language, allow it (can't verify)."""
        from couchpotato.core.plugins.automation import Automation as AutomationPlugin

        inst = object.__new__(AutomationPlugin)
        inst.shuttingDown = lambda: False
        inst.conf = lambda key, **kw: kw.get('default', 12)

        mock_fire.side_effect = [
            ['tt1234567'],  # automation.get_movies
            {'original_title': 'Test'},  # movie.info — no original_language key
            {'_id': 'abc123'},  # movie.add
            {'title': 'Test'},  # media.get
            None,  # movie.searcher.single
        ]
        mock_env.setting.return_value = 'en'
        mock_env.prop.return_value = False

        result = inst.addMovies()

        assert result is True
        add_calls = [c for c in mock_fire.call_args_list if c[0][0] == 'movie.add']
        assert len(add_calls) == 1
