import { ServerConnection } from '@jupyterlab/services';

interface IQuotaResponse {
  quota: {
    status: {
      code: 'CODE_OK' | string;
    };
    total_bytes: number;
    used_bytes: number;
  };
}

interface IQuota {
  totalBytes: number;
  usedBytes: number;
}

/**
 * Fetch the user's storage quota.
 */
export async function fetchQuota(): Promise<IQuota | null> {
  const settings = ServerConnection.makeSettings();
  let resp;
  try {
    resp = await ServerConnection.makeRequest(`${settings.baseUrl}cs3/quota/me`, {}, settings);
  } catch (err) {
    console.warn('[cs3org/cs3-jupyter-client] Could not fetch quota', err);
    return null;
  }

  if (!resp.ok) {
    console.warn('[cs3org/cs3-jupyter-client] Could not fetch quota', resp.statusText);
    return null;
  }

  const data: IQuotaResponse = await resp.json();
  if (data.quota.status.code !== 'CODE_OK') {
    return null;
  }

  return {
    totalBytes: data.quota.total_bytes,
    usedBytes: data.quota.used_bytes
  };
}

/**
 * Format bytes into a human-readable string.
 */
export function formatBytes(bytes: number): string {
  if (bytes < 1024) {
    return `${bytes} B`;
  }
  const units = ['KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unitIndex = -1;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex++;
  }
  return `${value.toFixed(value >= 100 ? 0 : 1)} ${units[unitIndex]}`;
}
