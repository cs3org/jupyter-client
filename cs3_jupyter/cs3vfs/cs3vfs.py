"""
CS3 Operating System Interface

This module provides a CS3-based implementation of common file system operations
to replace standard library functions like os, shutil, etc. with CS3 storage operations.

Authors: Rasmus Welander.
Emails: rasmus.oscar.welander@cern.ch.
"""

import base64
import os
import stat

from .cs3file import CS3File
from .utils import resource_from_path, retry_on_auth_failure, StatResult
from cs3client.cs3resource import Resource
from contextlib import contextmanager
from typing import Generator, List, Optional, Tuple, Union
from tornado import web


import cs3.storage.provider.v1beta1.resources_pb2 as cs3spr


class CS3VirtualFileSystem:
    """
    CS3-based file system operations that can replace standard library functions.

    This class provides implementations for file operations using CS3 storage
    instead of the local file system.
    """
    @contextmanager
    def cs3open(self, path: str, mode: str = 'r', encoding: Optional[str] = None, **kwargs) -> Generator['CS3File', None, None]:
        """Context manager for opening CS3 files."""
        cs3_file = CS3File(self, path, mode, encoding)
        cs3_file._init()
        try:
            yield cs3_file
        finally:
            cs3_file.close()

    @retry_on_auth_failure
    def vfs_exists(self, path: str) -> bool:
        """Check if path exists."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            return result is not None
        except Exception:
            return False

    @retry_on_auth_failure
    def is_file(self, path: str) -> bool:
        """Check if path is a file."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            if result is None:
                return False

            is_file = hasattr(result, 'type') and result.type == cs3spr.ResourceType.RESOURCE_TYPE_FILE
            return is_file
        except Exception:
            return False

    @retry_on_auth_failure
    def is_dir(self, path: str) -> bool:
        """Check if path is a directory."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            if result is None:
                return False
            return result.type == cs3spr.ResourceType.RESOURCE_TYPE_CONTAINER
        except Exception:
            return False

    def is_abs(self, path: str) -> bool:
        """Check if path is absolute."""
        return path.startswith(self.root_path)

    def abs_path(self, path: str) -> str:
        return path

    @retry_on_auth_failure
    def list_dir(self, path: str) -> List[Tuple[str, 'StatResult']]:
        """List directory contents with stat information in one call."""
        resource = resource_from_path(path)
        try:
            result = self.client.file.list_dir(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)
        items = []
        for item in result:
            stat_result = StatResult(item)
            # item.path should be the full path
            # but jupyter expects just the name
            # so we extract the name from the path
            name = item.path.split("/")[-1]
            items.append((name, stat_result))

        return items

    @retry_on_auth_failure
    def mkdir(self, path: str) -> None:
        """Create directory."""
        try:
            resource = resource_from_path(path)
            self.client.file.make_dir(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    # Alias for rmdir, because jupyter uses it
    def rmdir(self, path: str) -> None:
        """Remove directory."""
        return self.unlink(path)

    @retry_on_auth_failure
    def unlink(self, path: str) -> None:
        """Remove file."""
        try:
            resource = resource_from_path(path)
            self.client.file.remove_file(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    def move(self, src: str, dst: str) -> None:
        """Move file or directory."""
        self.vfs_rename(src, dst)

    @retry_on_auth_failure
    def vfs_rename(self, src: str, dst: str) -> None:
        """Rename file or directory."""
        try:
            src_resource = resource_from_path(src)
            dst_resource = resource_from_path(dst)
            self.client.file.rename_file(
                self.auth.get_token(),
                src_resource,
                dst_resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def lstat(self, path: str) -> 'StatResult':
        """Get file stats."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

        return StatResult(result)

    @retry_on_auth_failure
    def access(self, path: str, mode: int) -> bool:
        """Check file access permissions."""
        try:
            resource = resource_from_path(path)
            result = self.client.file.stat(
                self.auth.get_token(),
                resource
            )
            return result is not None
        except PermissionError:
            return False
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def read_file(self, path: str, format: Optional[str] = None, raw: bool = False) -> Union[Tuple[Union[str, bytes], str], Tuple[Union[str, bytes], str, bytes]]:
        """Read a file with CS3."""

        try:
            resource = resource_from_path(path)
            result = self.client.file.read_file(
                self.auth.get_token(),
                resource
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

        # Collect all chunks
        bcontent = b''
        for chunk in result:
            if isinstance(chunk, Exception):
                raise chunk
            bcontent += chunk

        if format == "byte":
            return (bcontent, "byte", bcontent) if raw else (bcontent, "byte")

        if format is None or format == "text":
            try:
                text_content = bcontent.decode("utf8")
                return (text_content, "text", bcontent) if raw else (text_content, "text")
            except UnicodeError as e:
                if format == "text":
                    raise web.HTTPError(400, "Cannot decode file, file type may not be supported: %s" % path) from e
        # Fall back to base64
        b64_content = base64.encodebytes(bcontent).decode("ascii")
        return (b64_content, "base64", bcontent) if raw else (b64_content, "base64")

    @retry_on_auth_failure
    def vfs_save_file(self, path: str, content: Union[str, bytes], format: str) -> None:
        """Save a file with CS3."""
        try:
            if format == "text":
                bcontent = content.encode("utf8")
            else:
                b64_bytes = content.encode("ascii")
                bcontent = base64.decodebytes(b64_bytes)
        except Exception as e:
            return self.status_handler.handle_errors(e)
        resource = resource_from_path(path)
        try:
            self.client.file.write_file(
                self.auth.get_token(),
                resource,
                bcontent,
                len(bcontent),
                self.lock_app_name,
                self.lock_value
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    def get_dir_size(self, path: str) -> int:
        """Calculate total size of directory and subdirectories using CS3 stat."""
        try:
            resource = resource_from_path(path)
            try:
                result = self.client.file.stat(
                    self.auth.get_token(),
                    resource
                )
            except Exception as e:
                self.status_handler.handle_errors(e)

            # Get stat info which includes tree_size in opaque metadata
            stat_result = StatResult(result)

            # If it's a file, return its size
            if stat_result.st_mode & stat.S_IFREG:
                return stat_result.st_size

            # For directories, try to get tree_size from opaque metadata
            if result and hasattr(result, 'opaque') and result.opaque:
                # Look for EOS metadata with tree_size
                for key, value in result.opaque.map.items():
                    if key == "eos" and value.decoder == "json":
                        import json
                        try:
                            eos_data = json.loads(value.value.decode('utf-8'))
                            if 'tree_size' in eos_data:
                                return int(eos_data['tree_size'])
                        except (json.JSONDecodeError, KeyError, ValueError):
                            pass

            # Fallback to directory size (not including subdirectories)
            return stat_result.st_size

        except Exception as e:
            self.log.warning(f"Error calculating directory size for {path}: {e}")
            return 0

    @retry_on_auth_failure
    async def copyfile(self, src: str, dst: str) -> None:
        """Copy file contents using streaming to avoid loading entire file in memory."""
        src_resource = resource_from_path(src)
        dst_resource = resource_from_path(dst)

        try:
            # Get the source file size first
            stat = self.client.file.stat(
                self.auth.get_token(),
                src_resource
            )

            file_size = stat.size

            # Get the content generator
            content_generator = self.client.file.read_file(
                self.auth.get_token(),
                src_resource
            )

            # Stream write
            self._write_file_streamed(dst_resource, content_generator, file_size)

        except Exception as e:
            self.status_handler.handle_errors(e)

    def _write_file_streamed(self, resource: Resource, content_generator: Generator[bytes, None, None], size: int) -> None:
        """Write a file using streaming to avoid loading entire content in memory."""
        try:
            self.client.file.write_file(
                self.auth.get_token(),
                resource,
                content_generator,  # Pass generator directly
                size
            )
        except Exception as e:
            self.status_handler.handle_errors(e)

    @retry_on_auth_failure
    async def copy_tree(self, src: str, dst: str) -> None:
        """Copy directory tree."""
        self.mkdir(dst)
        dir_contents = self.list_dir(src)
        for dir_name, _ in dir_contents:
            src_path = os.path.join(src, dir_name)
            dst_path = os.path.join(dst, dir_name)

            if self.is_dir(src_path):
                await self.copy_tree(src_path, dst_path)
            else:
                await self.copyfile(src_path, dst_path)

    def rm_tree(self, path: str) -> None:
        """Remove directory tree."""
        if self.is_dir(path):
            self.unlink(path)

    # Jupyter core utils
    def ensure_dir_exists(self, path: str) -> None:
        """Ensure directory exists."""
        if not self.vfs_exists(path):
            parent = os.path.dirname(path)
            # Ensure parent directory exists
            if parent and not self.vfs_exists(parent):
                self.ensure_dir_exists(parent)
            self.mkdir(path)


# Convenience function to create a global CS3 file system instance
def create_cs3_filesystem(config, root_path, client_id = None, client_secret = None) -> CS3VirtualFileSystem:
    """Create a CS3FileSystem instance."""
    cs3_fs = CS3VirtualFileSystem(config, root_path, client_id=client_id, client_secret=client_secret)
    return cs3_fs
