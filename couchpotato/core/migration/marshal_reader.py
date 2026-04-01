"""
Custom Python 2 marshal reader for CodernityDB documents.

Python 3.14 dropped TYPE_STRINGREF (0x52) from the stdlib marshal module,
which Python 2's marshal used extensively for string deduplication in larger
documents. This reader handles all 17 type codes found in real CodernityDB
databases, including STRINGREF.

Usage:
    from couchpotato.core.migration.marshal_reader import loads
    doc = loads(raw_bytes)
"""

import struct


# Sentinel for TYPE_NULL — must be distinguishable from None (TYPE_NONE)
# because CodernityDB uses NULL as a dict terminator.
_NULL = object()


class MarshalReader:
    """Stateful reader that tracks interned strings for STRINGREF resolution."""

    __slots__ = ('data', 'pos', 'refs')

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.refs = []  # interned string table

    def _read(self, n: int) -> bytes:
        end = self.pos + n
        chunk = self.data[self.pos:end]
        if len(chunk) < n:
            raise ValueError(
                f'Unexpected end of marshal data at offset {self.pos}, '
                f'wanted {n} bytes, got {len(chunk)}'
            )
        self.pos = end
        return chunk

    def _read_byte(self) -> int:
        b = self.data[self.pos]
        self.pos += 1
        return b

    def _read_long(self) -> int:
        """Read a 4-byte signed little-endian int (C long on 32-bit)."""
        return struct.unpack('<i', self._read(4))[0]

    def _read_ulong(self) -> int:
        """Read a 4-byte unsigned little-endian int."""
        return struct.unpack('<I', self._read(4))[0]

    def read_object(self):
        """Read one marshalled object and return a Python 3 value."""
        code = self._read_byte()
        return self._read_typed(code)

    def _read_typed(self, code: int):
        # TYPE_NULL '0' (0x30) — dict terminator sentinel
        if code == 0x30:
            return _NULL

        # TYPE_NONE 'N' (0x4e)
        if code == 0x4e:
            return None

        # TYPE_TRUE 'T' (0x54)
        if code == 0x54:
            return True

        # TYPE_FALSE 'F' (0x46)
        if code == 0x46:
            return False

        # TYPE_INT 'i' (0x69) — 4-byte signed
        if code == 0x69:
            return self._read_long()

        # TYPE_INT64 'I' (0x49) — 8-byte signed
        if code == 0x49:
            lo = self._read_ulong()
            hi = self._read_ulong()
            # Reconstruct as signed 64-bit
            val = lo | (hi << 32)
            if val >= (1 << 63):
                val -= (1 << 64)
            return val

        # TYPE_FLOAT 'f' (0x66) — ASCII float (Py2 marshal v1)
        if code == 0x66:
            n = self._read_byte()
            return float(self._read(n).decode('ascii'))

        # TYPE_BINARY_FLOAT 'g' (0x67) — 8-byte IEEE 754 double (Py2 marshal v2)
        if code == 0x67:
            return struct.unpack('<d', self._read(8))[0]

        # TYPE_LONG 'l' (0x6c) — arbitrary precision int
        if code == 0x6c:
            ndigits = self._read_long()  # may be negative (for negative numbers)
            if ndigits == 0:
                return 0
            sign = -1 if ndigits < 0 else 1
            ndigits = abs(ndigits)
            result = 0
            for i in range(ndigits):
                digit = struct.unpack('<H', self._read(2))[0]
                result |= digit << (i * 15)
            return sign * result

        # TYPE_STRING 's' (0x73) — raw byte string → decode to str
        if code == 0x73:
            n = self._read_long()
            return self._read(n).decode('utf-8', errors='replace')

        # TYPE_INTERNED 't' (0x74) — interned string, added to ref table
        if code == 0x74:
            n = self._read_long()
            s = self._read(n).decode('utf-8', errors='replace')
            self.refs.append(s)
            return s

        # TYPE_STRINGREF 'R' (0x52) — reference to previously interned string
        if code == 0x52:
            idx = self._read_long()
            if idx < 0 or idx >= len(self.refs):
                raise ValueError(
                    f'STRINGREF index {idx} out of range '
                    f'(have {len(self.refs)} refs)'
                )
            return self.refs[idx]

        # TYPE_UNICODE 'u' (0x75) — UTF-8 encoded unicode string
        if code == 0x75:
            n = self._read_long()
            return self._read(n).decode('utf-8', errors='replace')

        # TYPE_TUPLE '(' (0x28) — tuple with 4-byte length
        if code == 0x28:
            n = self._read_long()
            return tuple(self.read_object() for _ in range(n))

        # TYPE_SMALL_TUPLE ')' (0x29) — tuple with 1-byte length
        if code == 0x29:
            n = self._read_byte()
            return tuple(self.read_object() for _ in range(n))

        # TYPE_LIST '[' (0x5b)
        if code == 0x5b:
            n = self._read_long()
            return [self.read_object() for _ in range(n)]

        # TYPE_DICT '{' (0x7b) — key/value pairs terminated by TYPE_NULL
        if code == 0x7b:
            d = {}
            while True:
                key = self.read_object()
                if key is _NULL:
                    break
                val = self.read_object()
                d[key] = val
            return d

        raise ValueError(
            f'Unknown marshal type code 0x{code:02x} '
            f'(char {chr(code)!r}) at offset {self.pos - 1}'
        )


def loads(data: bytes):
    """Deserialize a Python 2 marshal byte string into a Python 3 object.

    Returns the deserialized object (typically a dict for CodernityDB docs).
    Byte strings are decoded to str (UTF-8 with replacement).
    """
    reader = MarshalReader(data)
    return reader.read_object()
