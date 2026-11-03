import React, { useState, useEffect, useCallback } from 'react';
import { FileBrowser } from '@jupyterlab/filebrowser';
import { ReactWidget } from '@jupyterlab/ui-components';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { Space, fetchSpaces } from './spaces';
import { CERNBOX_LOGO_SVG } from './icons';

function SpaceItem({ space, onNavigate }: { space: Space; onNavigate: (space: Space) => void }) {
  return (
    <div className="swan-spaces-item" title={`Open ${space.name}\n${space.path}`} onClick={() => onNavigate(space)}>
      <div className="swan-spaces-item-name">{space.name}</div>
      {space.description && <div className="swan-spaces-item-description">{space.description}</div>}
      <div className="swan-spaces-item-path">{space.path}</div>
    </div>
  );
}

function SpacesPanel({
  fileBrowser,
  shell,
  refreshSignal
}: {
  fileBrowser: FileBrowser;
  shell: JupyterFrontEnd.IShell;
  refreshSignal: number;
}) {
  const [spaces, setSpaces] = useState<Space[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState('');

  const loadSpaces = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const spaces = await fetchSpaces();
      if (spaces === null) {
        setError('Failed to load spaces');
      } else {
        setSpaces(spaces);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load spaces');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSpaces();
  }, [loadSpaces, refreshSignal]);

  const navigateToSpace = async (space: Space) => {
    try {
      await fileBrowser.model.cd(space.path);
      // Switch to the file browser tab in the sidebar
      shell.activateById(fileBrowser.id);
    } catch (err) {
      console.error(`[cs3org/cs3-jupyter:spaces] Failed to navigate to ${space.path}:`, err);
    }
  };

  const filterQuery = filter.trim().toLowerCase();
  const filtered = spaces
    .filter(s => !filterQuery || s.name.toLowerCase().includes(filterQuery))
    .sort((a, b) => a.name.localeCompare(b.name));

  return (
    <>
      <div className="swan-spaces-header">
        <span className="swan-spaces-header-logo" dangerouslySetInnerHTML={{ __html: CERNBOX_LOGO_SVG }} />
        <h2 className="swan-spaces-header-title">CERNBox Spaces</h2>
        <button className="swan-spaces-refresh-btn" title="Refresh shares" onClick={loadSpaces}>
          ↻
        </button>
      </div>

      <div className="swan-spaces-filter-container">
        <input
          className="swan-spaces-filter-input"
          type="text"
          placeholder="Filter by name..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
        />
      </div>

      <div className="swan-spaces-list">
        {loading && <div className="swan-spaces-loading">Loading spaces…</div>}
        {error && <div className="swan-spaces-error">{error}</div>}
        {!loading && !error && filtered.length === 0 && <div className="swan-spaces-empty">No spaces available</div>}
        {!loading &&
          !error &&
          filtered.map(space => <SpaceItem key={`${space.id}`} space={space} onNavigate={navigateToSpace} />)}
      </div>
    </>
  );
}

/**
 * A sidebar widget that displays CERNBox Spaces and navigates
 * the file browser when a space is clicked.
 */
export class SpacesWidget extends ReactWidget {
  private _fileBrowser: FileBrowser;
  private _shell: JupyterFrontEnd.IShell;
  private _refreshSignal = 0;

  constructor(fileBrowser: FileBrowser, shell: JupyterFrontEnd.IShell) {
    super();
    this._fileBrowser = fileBrowser;
    this._shell = shell;
    this.id = 'cernbox-spaces';
    this.title.caption = 'CERNBox Spaces';
    this.addClass('swan-spaces-panel');
  }

  refresh(): void {
    this._refreshSignal++;
    this.update();
  }

  render(): React.ReactElement {
    return <SpacesPanel fileBrowser={this._fileBrowser} shell={this._shell} refreshSignal={this._refreshSignal} />;
  }
}
