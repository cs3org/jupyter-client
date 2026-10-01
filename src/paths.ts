/**
 * CERNBox storage paths start with /eos, the contents manager's root_dir, so
 * a file's API path is its storage path without that prefix.
 *
 * TODO: Ask the server for its root instead of assuming /eos.
 */
const STORAGE_ROOT = '/eos';

/** The API path (relative to root_dir) of a storage path. */
export function toApiPath(storagePath: string): string {
  return storagePath.slice(STORAGE_ROOT.length);
}

/** The storage path of an API path. */
export function toStoragePath(apiPath: string): string {
  return `${STORAGE_ROOT}/${apiPath}`;
}
