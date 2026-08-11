# Jupyter client

A Jupyter Server Contents Manager implementation that provides seamless integration with CS3 (Cloud Storage Synchronization and Sharing) storage systems. This allows Jupyter environments to directly interact with distributed storage backends that implement the CS3 API, such as CERNBox.

The package also includes a JupyterLab extension that adds CERNBox sidebar panels (Spaces, Shares) and a storage quota indicator to the file browser.

## Overview

The CS3 Contents Manager extends Jupyter Server's file management capabilities to work with CS3-compatible storage systems. It provides a complete replacement for the default file-based contents manager, enabling users to open, edit, save, and manage notebooks and files stored in remote CS3 storage.

## Architecture

The CS3 Contents Manager consists of several key components:

### Core Components

1. **CS3FileContentsManager** (`cs3_contents_manager/filemanager.py`)
   - Implements Jupyter's AsyncContentsManager interface
   - Handles file and directory operations
   - Contains methods that are significantly different then its upstream countpart.

2. **UpstreamFileContentsManager** (`cs3_contents_manager/upstreamlargefilemanager.py`)
   - Main contents manager class
   - Implements Jupyter's AsyncContentsManager interface
   - Handles file and directory operations
   - Contains methods where only the OS functionality is replaced and can be pushed upstream.

3. **CS3FileSystem** (`cs3_contents_manager/cs3fs/cs3fs.py`)
   - CS3 storage abstraction layer
   - Provides filesystem-like interface over CS3 APIs
   - Handles low-level CS3 client operations

4. **CS3FileManagerMixin** (`cs3_contents_manager/fileio.py`)
   - Base mixin providing common file operations
   - Authentication and configuration management
   - File I/O utilities

5. **CS3FileCheckpoints** (`cs3_contents_manager/filecheckpoints.py`)
   - Checkpoint management for notebooks
   - Backup and restore functionality

6. **UpstreamLargeFileManager** (`cs3_contents_manager/largefilemanager.py`)
   - Specialized handling for large file uploads
   - Chunked transfer support

### JupyterLab Extension

The bundled labextension (`@cs3org/cs3-jupyter-client`) provides four plugins:

- **Spaces** - sidebar panel listing CERNBox Spaces (projects) the user has access to
- **Shares** - sidebar panel showing incoming and outgoing CERNBox shared folders
- **Storage Quota** - progress bar at the bottom of the file browser showing storage usage
- **Locking** - notifies the server when documents are opened and closed, so the
  CS3 lock is held while a document is open and released when the last session
  closes it. Without the labextension installed, locks are only taken on save
  and released when they expire (`lock_expiration`).

## Installation

### Install from Source

```bash
git clone <repository-url>
cd jupyter-client
pip install -e .
```

### Labextension Development

```bash
# Create fake EOS directories for testing
./setup-fake-eos.sh ./fake-eos

# Install the extension (pip install -e . also builds the labextension)
pip install -e .

# Alternative manual install
# jlpm install
# jlpm build
# jupyter labextension develop . --overwrite

# Verify the extension is loaded
jupyter labextension list

# Start JupyterLab
jupyter lab \
    --ServerApp.root_dir='./fake-eos' \
    --FileContentsManager.preferred_dir='user/<u>/<user>' \
    --ServerApp.token=''
```

## Configuration

### Jupyter Server Configuration

Add the following to your `jupyter_server_config.py`:

```python
from cs3_jupyter.cs3largefilemanager import CS3LargeFileManager

c.ServerApp.contents_manager_class = CS3LargeFileManager
c.CS3Mixin.host = '<host>'
# Keep TUS disabled while locking is enabled: cs3-python-client sends a
# misspelled X-Lock_Holder header on the TUS branch (cs3client/file.py), so
# locked writes would be rejected by EOS holder matching.
c.CS3Mixin.tus_enabled = False
c.CS3Mixin.ssl_enabled = False
c.CS3Mixin.token_path = '/path/to/oauth.token'
c.CS3Mixin.auth_login_type = 'bearer'
c.CS3Mixin.authtokenvalidity = 3600
c.CS3Mixin.lock_not_impl = False
c.CS3Mixin.lock_by_setting_attr = False
c.CS3Mixin.root_path = '/eos/user/r/rwelande'
c.CS3Mixin.client_id = 'rwelande'
c.CS3LargeFileManager.max_copy_folder_size_mb = 500
```

Configure the traits on `CS3Mixin`: it is the only class in the MRO of both
contents managers, so the same section applies to `CS3LargeFileManager` and
`CS3HybridLargeFileManager`. (`c.CS3FileManagerMixin.*` is silently ignored by
the hybrid manager, which does not inherit from it.)

### Locking

Files being edited are locked in the storage through the CS3 APIs, so other
applications (sync clients, web office, ...) cannot write to them concurrently.

- `lock_app_name` (default `jupyter-rtc`): the CS3 lock holder ("app name").
  EOS enforces write locks by app name, and lowercases it - keep it lowercase.
- `lock_holder_suffix_client_id` (default `True`): appends `-<client_id>` to the
  holder, making locks per-user (`jupyter-rtc-rwelande`). Set it to `False` on
  all servers to share one holder (`jupyter-rtc`) so multiple users can
  collaborate on the same locked files.
- `lock_value` (default `jupyter_rtc_lock`): the shared lock id, required by
  reva to refresh or release a lock.
- `lock_expiration` (default `300` seconds): locks not refreshed within this
  window expire in the storage; it is also the session-tracker heartbeat timeout.

The `/lock` API tracks how many sessions have a document open (`POST
/lock?path=...&session_id=...` on open and as heartbeat, `DELETE` on close) and
releases the reva lock when the last session leaves. A background task refreshes
locks for tracked sessions and unlocks documents whose sessions went stale.

### Authentication

The CS3 Contents Manager supports OAuth token-based authentication. Set up your authentication:

1. **Token File**: Place your OAuth token in a file (default: `/tmp/cernbox_oauth.token`)
2. **Configure Token Path**: Set `token_path` in your configuration
3. **Refresh**: If authentication fails at some point the client will attempt to read in case of an update.
