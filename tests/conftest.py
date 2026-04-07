"""Test configuration for CouchPotato test suite.

Adds the libs/ directory to sys.path so that vendored dependencies
(importhelper, etc.) are available during test imports.
"""
import os
import sys

# Add libs/ to sys.path before any couchpotato imports
libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libs')
if libs_dir not in sys.path:
    sys.path.insert(0, libs_dir)
