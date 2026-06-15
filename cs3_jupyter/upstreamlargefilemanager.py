import base64

from anyio.to_thread import run_sync
from tornado import web
from .upstreamfilemanager import UpstreamFileManager

'''
These are the methods with modifications to the upstream Jupyter LargeFileManager to handle large file uploads
with the os functionality replaced by "self.open" and "self.write".

See comments with ## in the class below for lines that have been changed from upstream.
'''
class UpstreamLargeFileManager(UpstreamFileManager):
    """Handle large file upload asynchronously"""

    # Upstream uses os.open and f.write, we need to use cs3_fs
    # upstream also takes into account if it's a symlink, we skip that here
    async def _save_large_file(self, os_path, content, format):
        """Save content of a generic file."""
        if format not in {"text", "base64"}:
            raise web.HTTPError(
                400,
                "Must specify format of file contents as 'text' or 'base64'",
            )
        try:
            if format == "text":
                bcontent = content.encode("utf8")
            else:
                b64_bytes = content.encode("ascii")
                bcontent = base64.b64decode(b64_bytes)
        except Exception as e:
            raise web.HTTPError(400, f"Encoding error saving {os_path}: {e}") from e

        with self.perm_to_403(os_path):
            ## This part is replaced to use cs3mixin open and write
            with self.open(os_path, "ab") as f:  # noqa: ASYNC101
                await run_sync(f.write, bcontent)
        """
        Original upstream code:

        with self.perm_to_403(os_path):
            if os.path.islink(os_path):
                os_path = os.path.join(os.path.dirname(os_path), os.readlink(os_path))
            with open(os_path, "ab") as f:
                f.write(bcontent)
        """
