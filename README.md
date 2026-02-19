# Jupyter client

A Jupyter Server Contents Manager implementation that provides seamless integration with CS3 (Cloud Storage Synchronization and Sharing) storage systems. This allows Jupyter environments to directly interact with distributed storage backends that implement the CS3 API, such as CERNBox.

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

## Installation

### Install from Source

```bash
git clone <repository-url>
cd cs3-contents-manager
pip install -e .
```

## Configuration

### Jupyter Server Configuration

Add the following to your `jupyter_server_config.py`:

```python
from cs3_jupyter_client.cs3largefilemanager import CS3LargeFileManager

c.ServerApp.contents_manager_class = CS3LargeFileManager
c.CS3FileManagerMixin.host = '<host>'
c.CS3FileManagerMixin.tus_enabled = False
c.CS3FileManagerMixin.ssl_enabled = False
c.CS3FileManagerMixin.token_path = '/path/to/oauth.token'
c.CS3FileManagerMixin.auth_login_type = 'bearer'
c.CS3FileManagerMixin.authtokenvalidity = 3600
c.CS3FileManagerMixin.lock_not_impl = False
c.CS3FileManagerMixin.lock_as_attr = False
c.CS3FileManagerMixin.root_path = '/eos/user/r/rwelande'
c.CS3LargeFileManager.max_copy_folder_size_mb = 500
```

### Authentication

The CS3 Contents Manager supports OAuth token-based authentication. Set up your authentication:

1. **Token File**: Place your OAuth token in a file (default: `/tmp/cernbox_oauth.token`)
2. **Configure Token Path**: Set `token_path` in your configuration
3. **Refresh**: If authentication fails at some point the client will attempt to read in case of an update.
