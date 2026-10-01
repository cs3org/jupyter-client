"""The contents managers to configure as ServerApp.contents_manager_class."""

from jupyter_server.services.contents.largefilemanager import LargeFileManager

from .filemanager import CS3FileContentsManager, CS3HybridFileManager
from .upstreamlargefilemanager import UpstreamLargeFileManager


class CS3LargeFileManager(CS3FileContentsManager, UpstreamLargeFileManager):
    """Pure CS3: every read, write and stat goes through the CS3 APIs."""


class CS3HybridLargeFileManager(CS3HybridFileManager, LargeFileManager):
    """CS3 for all mutations (so they carry and respect locks), the local
    FUSE mount as a fast path for metadata."""
