"""Playwright E2E test configuration for CouchPotato.

Tests run against a live CP instance. Set CP_BASE_URL to override
the default (http://couchpotato.inside.lan).
"""
import os

import pytest


CP_BASE_URL = os.environ.get('CP_BASE_URL', 'http://couchpotato.inside.lan')
CP_API_KEY = os.environ.get('CP_API_KEY', '8f8da066c00c43d7b3ec01da5619d5fc')


@pytest.fixture(scope='session')
def cp_url():
    return CP_BASE_URL


@pytest.fixture(scope='session')
def cp_api_key():
    return CP_API_KEY


@pytest.fixture(scope='session')
def browser_context_args(browser_context_args):
    return {
        **browser_context_args,
        'ignore_https_errors': True,
    }


def _navigate(page, cp_url, cp_api_key, spa_path):
    """Navigate within the CP SPA.

    CP uses MooTools History for client-side routing.  A full page.goto()
    to the settings URL results in an empty page because the JS router
    hasn't initialised yet.  Instead we load the root once, wait for the
    app to boot, then push the desired path via History.push().
    """
    # Load root if not already there
    current = page.url
    root = f'{cp_url}/{cp_api_key}/'
    if not current.startswith(root):
        page.goto(root)
        page.wait_for_load_state('networkidle')
        page.wait_for_timeout(2000)
    page.evaluate(f'History.push("/{spa_path}")')
    page.wait_for_timeout(2000)


@pytest.fixture()
def navigate(page, cp_url, cp_api_key):
    """Return a helper that navigates within the CP SPA."""
    def _nav(spa_path):
        _navigate(page, cp_url, cp_api_key, spa_path)
    return _nav
