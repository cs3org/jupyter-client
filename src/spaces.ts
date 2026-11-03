import { ServerConnection } from '@jupyterlab/services';

interface SpaceResponse {
  id: {
    opaque_id: string;
  };
  name: string;
  description?: string;
  quota: {
    quota_max_bytes: number;
    remaining_bytes: number;
  };
  root_info: {
    relative_path: string;
  };
}

interface SpacesResponse {
  spaces: SpaceResponse[];
}

export interface Space {
  id: string;
  name: string;
  description?: string;
  quota: {
    maxBytes: number;
    remainingBytes: number;
  };
  /**
   * Path relative to the ContentsManager root_dir (e.g. /eos) including the slash
   * Example: "/project/a/atlas-analysis"
   */
  path: string;
}

/**
 * Fetch the list of spaces the current user has access to.
 */
export async function fetchSpaces(): Promise<Space[] | null> {
  const settings = ServerConnection.makeSettings();
  let resp;
  try {
    resp = await ServerConnection.makeRequest(`${settings.baseUrl}cs3/spaces`, {}, settings);
  } catch (err) {
    console.warn('[cs3org/cs3-jupyter-client] Could not fetch spaces', err);
    return null;
  }

  if (!resp.ok) {
    console.warn('[cs3org/cs3-jupyter-client] Could not fetch spaces', resp.statusText);
    return null;
  }

  let data: SpacesResponse;
  try {
    data = await resp.json();
  } catch {
    console.warn('[cs3org/cs3-jupyter-client] Could not fetch spaces', resp.statusText);
    return null;
  }

  return data.spaces.map(space => ({
    id: space.id.opaque_id,
    name: space.name,
    description: space.description,
    quota: {
      maxBytes: space.quota.quota_max_bytes,
      remainingBytes: space.quota.remaining_bytes
    },
    path: `/${space.root_info.relative_path}`
  }));
}
