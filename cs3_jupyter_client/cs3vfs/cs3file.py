"""
CS3 abstract file-like object implementation.

Authors: Rasmus Oscar Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

from __future__ import annotations

import base64
from typing import Optional, Union, TYPE_CHECKING
if TYPE_CHECKING:
    from .cs3vfs import CS3VirtualFileSystem


class CS3File:
    """File-like object for CS3 storage with proper context manager support."""

    def __init__(self, cs3_vfs: "CS3VirtualFileSystem", path: str, mode: str = 'r', encoding: Optional[str] = None) -> None:
        self.cs3_vfs = cs3_vfs
        self.path = path
        self.mode = mode
        self.encoding = encoding or 'utf-8'
        self._content: Union[str, bytes, None] = None
        self._position = 0
        self._closed = False
        self._modified = False

    def _init(self) -> None:
        """Initialization after creation."""
        # Load content if reading
        if 'r' in self.mode or 'a' in self.mode:
            self._load_content()
        else:
            self._content = b'' if 'b' in self.mode else ''

    def _load_content(self) -> None:
        """Load file content from CS3."""
        try:
            if 'b' in self.mode:
                result = self.cs3_vfs._read_file(self.path, "byte")
                self._content = result[0]
            else:
                result = self.cs3_vfs._read_file(self.path, "text")
                self._content = result[0]
        except Exception:
            if 'r' in self.mode:
                raise
            self._content = b'' if 'b' in self.mode else ''

    def read(self, size: int = -1) -> Union[str, bytes]:
        """Read from file."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size == -1:
            result = self._content[self._position:]
            self._position = len(self._content)
        else:
            result = self._content[self._position:self._position + size]
            self._position += len(result)

        return result

    def write(self, data: Union[str, bytes]) -> int:
        """Write to file."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if 'r' in self.mode and 'w' not in self.mode and 'a' not in self.mode:
            raise OSError("File not open for writing")

        if isinstance(data, str) and 'b' in self.mode:
            data = data.encode(self.encoding)
        elif isinstance(data, bytes) and 'b' not in self.mode:
            data = data.decode(self.encoding)

        if 'a' in self.mode:
            self._content += data
        else:
            self._content = self._content[:self._position] + data + self._content[self._position + len(data):]
            self._position += len(data)

        self._modified = True
        return len(data)

    def flush(self) -> None:
        """Flush to CS3 storage."""
        if self._closed or not self._modified:
            return

        if 'w' in self.mode or 'a' in self.mode:
            if isinstance(self._content, str):
                format = "text"
                content = self._content
            else:
                format = "base64"
                content = base64.encodebytes(self._content).decode("ascii")

            self.cs3_vfs._save_file(self.path, content, format)
            self._modified = False

    def close(self) -> None:
        """Close file."""
        if not self._closed:
            self.flush()
            self._closed = True

    def __enter__(self) -> 'CS3File':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def fileno(self) -> int:
        """Get file descriptor (not applicable for CS3)."""
        raise NotImplementedError("File descriptors not supported in CS3")
