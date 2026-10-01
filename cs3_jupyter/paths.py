"""Path checks reimplemented from jupyter_core.paths and os.path.

The originals stat the local filesystem; these only look at the path, which
is all a remote storage path offers.
"""

from pathlib import Path


# replaces "import jupyter.core.paths is_file_hidden"
def is_file_hidden(os_path, stat_res=None):
    """Return whether a file is hidden based on its name."""
    p = Path(os_path)

    for part in p.parts:
        if part.startswith('.') and part not in ('.', '..'):
            return True

    return False


# replaces "import jupyter.core.paths is_hidden"
def is_hidden(os_path, root_dir=None):
    return is_file_hidden(os_path)


# replaces "import os.path.samefile"
def naive_same_file(path1, path2):
    """Check if two paths are the same file"""
    return path1 == path2
