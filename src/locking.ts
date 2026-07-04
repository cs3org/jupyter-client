import { ServerConnection } from '@jupyterlab/services';
import type { ISignal } from '@lumino/signaling';

/**
 * Structural subset of DocumentRegistry.IDocumentWidget that the tracker
 * needs. Typing structurally keeps us independent of the (potentially
 * duplicated) @jupyterlab/docregistry version the opener signal uses.
 */
export interface ITrackedDocumentWidget {
  isDisposed: boolean;
  disposed: ISignal<any, void>;
  context: {
    path: string;
    pathChanged: ISignal<any, string>;
  };
}

/**
 * Client for the server extension's /lock endpoint.
 *
 * Every open document holds a CS3 lock in the storage, refreshed by a
 * heartbeat while the document stays open. The server counts sessions per
 * document and releases the lock when the last one closes; sessions whose
 * heartbeat stops (crashed tab) are swept server-side after the lock
 * expiration, so closing cleanly is an optimization, not a requirement.
 */

const DEFAULT_EXPIRATION_SECONDS = 300;

interface ILockResponse {
  locked: boolean;
  read_only: boolean;
  path: string;
  count?: number;
  holder?: string;
}

function lockRequest(method: 'POST' | 'DELETE', path: string, sessionId: string): Promise<Response> {
  const settings = ServerConnection.makeSettings();
  const query = `?path=${encodeURIComponent(path)}&session_id=${encodeURIComponent(sessionId)}`;
  return ServerConnection.makeRequest(settings.baseUrl + 'lock' + query, { method }, settings);
}

function newSessionId(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `s-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

export class DocumentLockTracker {
  private expirationSeconds = DEFAULT_EXPIRATION_SECONDS;
  private tracked = new WeakSet<ITrackedDocumentWidget>();

  /** Read the server's lock expiration to derive the heartbeat period. */
  async initialize(): Promise<void> {
    try {
      const settings = ServerConnection.makeSettings();
      const resp = await ServerConnection.makeRequest(settings.baseUrl + 'lock', {}, settings);
      if (resp.ok) {
        const data = await resp.json();
        if (typeof data.expiration === 'number' && data.expiration > 0) {
          this.expirationSeconds = data.expiration;
        }
      }
    } catch (err) {
      console.warn('[cs3org/cs3-jupyter-client] Could not read lock expiration, using default', err);
    }
  }

  /** Heartbeat faster than the expiration, mirroring the server refresher. */
  private get heartbeatMs(): number {
    return Math.max(Math.floor(this.expirationSeconds / 3), 10) * 1000;
  }

  track(widget: ITrackedDocumentWidget): void {
    if (this.tracked.has(widget) || widget.isDisposed) {
      return;
    }
    this.tracked.add(widget);

    const sessionId = newSessionId();
    let path = widget.context.path;

    const open = async (p: string): Promise<void> => {
      try {
        const resp = await lockRequest('POST', p, sessionId);
        if (!resp.ok) {
          console.warn(`[cs3org/cs3-jupyter-client] Could not lock ${p}: HTTP ${resp.status}`);
          return;
        }
        const data = (await resp.json()) as ILockResponse;
        if (data.read_only) {
          console.info(
            `[cs3org/cs3-jupyter-client] ${p} is locked by ${data.holder ?? 'another application'}, opening read-only`
          );
        }
      } catch (err) {
        console.warn(`[cs3org/cs3-jupyter-client] Lock request for ${p} failed`, err);
      }
    };

    const close = (p: string): void => {
      // Fire and forget: a missed close is cleaned up by the server sweep.
      lockRequest('DELETE', p, sessionId).catch(() => undefined);
    };

    const heartbeat = window.setInterval(() => void open(path), this.heartbeatMs);

    widget.context.pathChanged.connect((_, newPath) => {
      // Rename moves the lock with the file server-side; retrack the new
      // path so the session count and heartbeat follow it.
      close(path);
      path = newPath;
      void open(path);
    });

    widget.disposed.connect(() => {
      window.clearInterval(heartbeat);
      close(path);
    });

    void open(path);
  }
}
