"""Tests for the log parser (toList) in the Logging plugin.

Verifies that the parser correctly handles:
- Plain-text log lines (current format, no ANSI codes)
- Old ANSI-coded log lines (from rotated log files before color_logs removal)
- Multi-line entries (tracebacks)
- Empty input and edge cases
"""

import os
import sys

# Ensure project root and libs/ are on sys.path
project_root = os.path.dirname(os.path.dirname(__file__))
libs_dir = os.path.join(project_root, 'libs')
for p in (project_root, libs_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

from couchpotato.core.plugins.log.main import Logging  # noqa: E402


class TestToList:
    """Tests for Logging.toList() plain-text parser."""

    def setup_method(self):
        # Instantiate without calling __init__ (which registers API views)
        self.logger = object.__new__(Logging)

    def test_empty_input(self):
        assert self.logger.toList('') == []

    def test_single_info_line(self):
        content = '04-08 10:30:46 INFO [some.module] Hello world\n'
        result = self.logger.toList(content)
        assert len(result) == 1
        assert result[0]['time'] == '04-08 10:30:46'
        assert result[0]['type'] == 'INFO'
        assert result[0]['message'] == '[some.module] Hello world'

    def test_multiple_lines(self):
        content = (
            '04-08 10:30:46 INFO [mod.a] First message\n'
            '04-08 10:30:47 WARNING [mod.b] Second message\n'
            '04-08 10:30:48 ERROR [mod.c] Third message\n'
        )
        result = self.logger.toList(content)
        assert len(result) == 3
        assert result[0]['type'] == 'INFO'
        assert result[1]['type'] == 'WARNING'
        assert result[2]['type'] == 'ERROR'

    def test_debug_and_critical(self):
        content = (
            '01-15 00:00:00 DEBUG [d] debug msg\n'
            '01-15 00:00:01 CRITICAL [c] critical msg\n'
        )
        result = self.logger.toList(content)
        assert len(result) == 2
        assert result[0]['type'] == 'DEBUG'
        assert result[1]['type'] == 'CRITICAL'

    def test_multiline_traceback(self):
        content = (
            '04-08 10:30:46 ERROR [mod] Something failed\n'
            'Traceback (most recent call last):\n'
            '  File "foo.py", line 42, in bar\n'
            '    raise ValueError("oops")\n'
            'ValueError: oops\n'
            '04-08 10:30:47 INFO [mod] Recovery\n'
        )
        result = self.logger.toList(content)
        assert len(result) == 2
        assert result[0]['type'] == 'ERROR'
        assert 'Traceback' in result[0]['message']
        assert 'ValueError: oops' in result[0]['message']
        assert result[1]['type'] == 'INFO'
        assert result[1]['message'] == '[mod] Recovery'

    def test_old_ansi_codes_stripped(self):
        """Old rotated log files may still have ANSI escape codes."""
        content = (
            '\x1b[36m04-08 10:30:46 INFO \x1b[0m\x1b[37m[mod] msg\x1b[0m\n'
        )
        result = self.logger.toList(content)
        assert len(result) == 1
        assert result[0]['time'] == '04-08 10:30:46'
        assert result[0]['type'] == 'INFO'
        assert '[mod] msg' in result[0]['message']

    def test_no_trailing_newline(self):
        content = '04-08 10:30:46 INFO [mod] no newline at end'
        result = self.logger.toList(content)
        assert len(result) == 1

    def test_blank_lines_ignored(self):
        content = '\n\n04-08 10:30:46 INFO [mod] msg\n\n\n'
        result = self.logger.toList(content)
        assert len(result) == 1

    def test_continuation_without_prior_entry_ignored(self):
        """Lines before the first valid log entry are silently dropped."""
        content = (
            'some garbage line\n'
            '04-08 10:30:46 INFO [mod] real entry\n'
        )
        result = self.logger.toList(content)
        assert len(result) == 1
        assert result[0]['message'] == '[mod] real entry'
