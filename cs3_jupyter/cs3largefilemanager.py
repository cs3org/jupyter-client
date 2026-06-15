from .upstreamlargefilemanager import UpstreamLargeFileManager
from .filemanager import CS3FileContentsManager, CS3HybridFileManager
from jupyter_server.services.contents.largefilemanager import LargeFileManager
class CS3LargeFileManager(CS3FileContentsManager, UpstreamLargeFileManager):
    pass
# Override open via CS3HybridFileManager, to introduce locking when opening a file.
class CS3HybridLargeFileManager(CS3HybridFileManager, LargeFileManager):
    pass
