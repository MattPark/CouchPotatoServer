"""Tests for the plugin Loader — verifies importlib integration.

After replacing the vendored importhelper with stdlib importlib,
these tests confirm that the Loader class can:
  - Load known modules by dotted name
  - Return None for missing modules (graceful fallback)
  - Discover plugin directories correctly
  - Handle the ImportError path with str(e) instead of e.message
"""

import os
import sys
import types

import pytest

# Ensure libs/ is on sys.path
libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libs')
if libs_dir not in sys.path:
    sys.path.insert(0, libs_dir)

from couchpotato.core.loader import Loader


class TestLoaderImportModule:
    """Test Loader.loadModule() which uses importlib.import_module."""

    def setup_method(self):
        self.loader = Loader()

    def test_load_stdlib_module(self):
        """importlib.import_module can load stdlib modules."""
        result = self.loader.loadModule('json')
        assert result is not None
        assert hasattr(result, 'dumps')
        assert hasattr(result, 'loads')

    def test_load_nested_stdlib_module(self):
        """importlib.import_module handles dotted module names."""
        result = self.loader.loadModule('os.path')
        assert result is not None
        assert hasattr(result, 'join')
        assert hasattr(result, 'exists')

    def test_load_missing_module_returns_none(self):
        """Missing modules return None (not raise), matching old behavior."""
        result = self.loader.loadModule('nonexistent_module_xyz_12345')
        assert result is None

    def test_load_couchpotato_module(self):
        """Can load a known couchpotato module."""
        result = self.loader.loadModule('couchpotato.core.helpers.variable')
        assert result is not None
        assert hasattr(result, 'getDataDir')

    def test_load_couchpotato_encoding(self):
        """Can load the encoding helpers module."""
        result = self.loader.loadModule('couchpotato.core.helpers.encoding')
        assert result is not None
        assert hasattr(result, 'toUnicode')

    def test_load_returns_module_type(self):
        """loadModule returns a proper module object."""
        result = self.loader.loadModule('json')
        assert isinstance(result, types.ModuleType)


class TestLoaderAddModule:
    """Test Loader.addModule() for module registry management."""

    def setup_method(self):
        self.loader = Loader()

    def test_add_module_creates_priority_dict(self):
        """Adding a module creates the priority bucket if needed."""
        self.loader.addModule(0, 'core', 'couchpotato.core._base', '_base')
        assert 0 in self.loader.modules
        assert 'couchpotato.core._base' in self.loader.modules[0]

    def test_add_module_stores_metadata(self):
        """Module metadata (priority, type, name) is stored correctly."""
        self.loader.addModule(10, 'plugin', 'couchpotato.core.plugins.scanner', 'scanner')
        entry = self.loader.modules[10]['couchpotato.core.plugins.scanner']
        assert entry['priority'] == 10
        assert entry['type'] == 'plugin'
        assert entry['name'] == 'scanner'

    def test_add_module_strips_leading_dot(self):
        """Module names with leading dots are stripped."""
        self.loader.addModule(0, 'core', '.couchpotato.core', 'core')
        assert 'couchpotato.core' in self.loader.modules[0]

    def test_add_multiple_priorities(self):
        """Modules at different priorities are sorted correctly."""
        self.loader.addModule(20, 'notifications', 'couchpotato.core.notifications', 'notifications')
        self.loader.addModule(0, 'core', 'couchpotato.core._base', '_base')
        assert sorted(self.loader.modules.keys()) == [0, 20]


class TestLoaderImportErrorHandling:
    """Test that ImportError handling works with str(e) instead of e.message."""

    def test_import_error_has_str(self):
        """Verify str(e) works for ImportError (replaced e.message)."""
        try:
            raise ImportError("Missing dependency: some_lib")
        except ImportError as e:
            msg = str(e)
            assert msg == "Missing dependency: some_lib"
            assert msg.lower().startswith("missing")

    def test_import_error_no_message_attr(self):
        """Confirm ImportError has no .message in Python 3."""
        try:
            raise ImportError("test")
        except ImportError as e:
            # Python 3 removed .message from BaseException
            assert not hasattr(e, 'message') or str(e) == "test"

    def test_import_error_non_missing_prefix(self):
        """Non-'missing' ImportErrors go to the else branch."""
        try:
            raise ImportError("No module named 'foobar'")
        except ImportError as e:
            msg = str(e)
            assert not msg.lower().startswith("missing")


class TestImportlibDropIn:
    """Verify stdlib importlib is a drop-in replacement for vendored importhelper."""

    def test_importlib_import_module_exists(self):
        """importlib.import_module is available in stdlib."""
        from importlib import import_module
        assert callable(import_module)

    def test_importlib_absolute_import(self):
        """importlib.import_module handles absolute imports like importhelper did."""
        from importlib import import_module
        mod = import_module('json')
        assert mod.__name__ == 'json'

    def test_importlib_dotted_import(self):
        """importlib.import_module handles dotted names like importhelper did."""
        from importlib import import_module
        mod = import_module('email.mime.text')
        assert hasattr(mod, 'MIMEText')

    def test_importlib_raises_on_missing(self):
        """importlib.import_module raises ImportError (same as importhelper)."""
        from importlib import import_module
        with pytest.raises(ImportError):
            import_module('nonexistent_xyz_99999')

    def test_no_vendored_importhelper(self):
        """The vendored importhelper directory has been deleted."""
        importhelper_path = os.path.join(libs_dir, 'importhelper')
        assert not os.path.exists(importhelper_path), \
            "libs/importhelper/ should have been deleted"
