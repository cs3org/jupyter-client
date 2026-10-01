import { ServerConnection } from '@jupyterlab/services';

/**
 * Call an endpoint of the cs3_jupyter server extension.
 *
 * Resolves to the parsed JSON body, or undefined for an empty one (204).
 * A non-2xx response throws a ServerConnection.ResponseError carrying the
 * server's error message.
 */
export async function requestAPI<T = unknown>(
  endpoint: string,
  init: RequestInit = {},
  params?: Record<string, string>
): Promise<T> {
  const settings = ServerConnection.makeSettings();
  const query = params ? `?${new URLSearchParams(params)}` : '';
  const resp = await ServerConnection.makeRequest(settings.baseUrl + endpoint + query, init, settings);
  const body = await resp.text();
  if (!resp.ok) {
    throw new ServerConnection.ResponseError(resp, errorMessage(body) ?? resp.statusText);
  }
  return (body ? JSON.parse(body) : undefined) as T;
}

/** The message of an error body: {"error"} from the CS3 handlers, {"message"} from Jupyter's. */
function errorMessage(body: string): string | undefined {
  try {
    const data = JSON.parse(body);
    return data.error ?? data.message;
  } catch {
    return undefined;
  }
}
