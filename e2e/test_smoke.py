"""Smoke tests — verify CP is reachable and basic pages load."""
from playwright.sync_api import expect


def test_homepage_loads(page, cp_url, cp_api_key):
    """Homepage loads and shows the CouchPotato title."""
    page.goto(f'{cp_url}/{cp_api_key}/')
    expect(page).to_have_title('CouchPotato')


def test_settings_page_loads(page, navigate):
    """Settings page loads and shows tabs."""
    navigate('settings/')
    settings = page.locator('.page.settings.active')
    expect(settings).to_be_visible()
    # Should have the settings tab list
    expect(settings.locator('ul.tabs')).to_be_visible()
    # Verify key tabs exist
    expect(settings.locator('li.t_general')).to_be_visible()
    expect(settings.locator('li.t_manage')).to_be_visible()


def test_audit_tab_loads(page, navigate):
    """Audit subtab loads and shows audit content."""
    navigate('settings/manage/audit/')
    expect(page.locator('.audit_wrap')).to_be_visible(timeout=10000)
    # Should have scan buttons
    expect(page.locator('.scan_btn.audit_quick')).to_be_visible()


def test_movies_page_loads(page, navigate):
    """Movies page loads."""
    navigate('movies/')
    expect(page.locator('.page.movies.active').first).to_be_visible(timeout=5000)
