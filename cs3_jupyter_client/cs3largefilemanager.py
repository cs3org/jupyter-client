from .upstreamlargefilemanager import UpstreamLargeFileManager
from .filemanager import CS3FileContentsManager
class CS3LargeFileManager(CS3FileContentsManager, UpstreamLargeFileManager):
    pass
