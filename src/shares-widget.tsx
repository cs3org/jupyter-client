import React, { useState, useEffect, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { ReactWidget } from '@jupyterlab/ui-components';
import { FileBrowser } from '@jupyterlab/filebrowser';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { fileIcon, folderIcon } from '@jupyterlab/ui-components';
import { Share, ByMeRegularShare, fetchShares } from './shares';
import { openEditShareModal } from './share-edit-modal';
import { CERNBOX_LOGO_SVG, SharedWithMeIcon, SharedByMeIcon, SharedPubliclyIcon } from './icons';

interface FileTypeRegistry {
  getFileTypesForPath(
    path: string
  ): ReadonlyArray<{ icon?: { react: React.ForwardRefExoticComponent<React.RefAttributes<SVGElement>> } }>;
}

type TabId = 'WITH_ME' | 'BY_ME' | 'PUBLIC';

const TABS: { id: TabId; label: string; icon: React.ReactElement }[] = [
  { id: 'WITH_ME', label: 'Shared with me', icon: SharedWithMeIcon },
  { id: 'BY_ME', label: 'Shared with others', icon: SharedByMeIcon },
  { id: 'PUBLIC', label: 'Shared publicly', icon: SharedPubliclyIcon }
];

function FileTypeIcon({ share, registry }: { share: Share; registry: FileTypeRegistry }) {
  if (share.resourceType !== 'RESOURCE_TYPE_FILE') {
    return <span className="swan-shares-item-icon"><folderIcon.react stylesheet="listing" /></span>;
  }
  const fileTypes = registry.getFileTypesForPath(share.name);
  const Icon = fileTypes.length > 0 && fileTypes[0].icon ? fileTypes[0].icon.react : fileIcon.react;
  return <span className="swan-shares-item-icon"><Icon stylesheet="listing" /></span>;
}

function ShareItem({
  share,
  registry,
  onNavigate,
  onContextMenu
}: {
  share: Share;
  registry: FileTypeRegistry;
  onNavigate: (share: Share) => void;
  onContextMenu: (share: ByMeRegularShare, x: number, y: number) => void;
}) {
  let meta = '';
  if (share.shareDirection === 'WITH_ME' && share.sharedBy) {
    meta = `from ${share.sharedBy}`;
  } else if (share.shareDirection === 'BY_ME' && share.shareType === 'REGULAR' && share.sharedWith.length > 0) {
    meta = `with ${share.sharedWith.map(g => g.opaqueId).join(', ')}`;
  }

  const handleContext = (e: React.MouseEvent) => {
    if (share.shareDirection === 'BY_ME' && share.shareType === 'REGULAR') {
      e.preventDefault();
      e.stopPropagation();
      onContextMenu(share, e.clientX, e.clientY);
    }
  };

  return (
    <div
      className="swan-shares-item"
      title={share.path}
      onClick={() => onNavigate(share)}
      onContextMenu={handleContext}
    >
      <FileTypeIcon share={share} registry={registry} />
      <div style={{ minWidth: 0 }}>
        <div className="swan-shares-item-name">{share.name}</div>
        {meta && <div className="swan-shares-item-meta">{meta}</div>}
      </div>
    </div>
  );
}

function SharesPanel({
  fileBrowser,
  shell,
  commands,
  registry,
  refreshSignal
}: {
  fileBrowser: FileBrowser;
  shell: JupyterFrontEnd.IShell;
  commands: JupyterFrontEnd['commands'];
  registry: FileTypeRegistry;
  refreshSignal: number;
}) {
  const [shares, setShares] = useState<Share[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabId>('WITH_ME');
  const [filter, setFilter] = useState('');
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; share: ByMeRegularShare } | null>(null);

  const loadShares = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setShares(await fetchShares());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load shares');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadShares();
  }, [loadShares, refreshSignal]);

  useEffect(() => {
    const dismiss = () => setContextMenu(null);
    document.addEventListener('click', dismiss);
    document.addEventListener('contextmenu', dismiss);
    return () => {
      document.removeEventListener('click', dismiss);
      document.removeEventListener('contextmenu', dismiss);
    };
  }, []);

  const switchTab = (tab: TabId) => {
    setActiveTab(tab);
    setFilter('');
  };

  const navigateToShare = async (share: Share) => {
    try {
      if (share.resourceType === 'RESOURCE_TYPE_FILE') {
        await commands.execute('docmanager:open', { path: share.path });
      } else {
        await fileBrowser.model.cd(share.path);
        shell.activateById(fileBrowser.id);
      }
    } catch (err) {
      console.error(`[cs3org/cs3-jupyter:shares] Failed to open ${share.path}:`, err);
    }
  };

  const handleEdit = (share: ByMeRegularShare) => {
    openEditShareModal(share, loadShares);
  };

  const filterQuery = filter.trim().toLowerCase();
  const filtered = shares
    .filter(s => {
      if (activeTab === 'WITH_ME') return s.shareDirection === 'WITH_ME';
      if (activeTab === 'BY_ME') return s.shareDirection === 'BY_ME' && s.shareType === 'REGULAR';
      return s.shareDirection === 'BY_ME' && s.shareType === 'PUBLIC';
    })
    .filter(s => !filterQuery || s.name.toLowerCase().includes(filterQuery))
    .sort((a, b) => a.name.localeCompare(b.name));

  return (
    <>
      <div className="swan-shares-header">
        <span className="swan-shares-header-logo" dangerouslySetInnerHTML={{ __html: CERNBOX_LOGO_SVG }} />
        <h2 className="swan-shares-header-title">CERNBox Shares</h2>
        <button className="swan-shares-refresh-btn" title="Refresh shares" onClick={loadShares}>
          ↻
        </button>
      </div>

      <div className="swan-shares-tabs">
        {TABS.map(tab => (
          <button
            key={tab.id}
            className={`swan-shares-tab${activeTab === tab.id ? ' swan-shares-tab-active' : ''}`}
            onClick={() => switchTab(tab.id)}
          >
            {tab.icon} {tab.label}
          </button>
        ))}
      </div>

      <div className="swan-shares-filter-container">
        <input
          className="swan-shares-filter-input"
          type="text"
          placeholder="Filter by name..."
          value={filter}
          onChange={e => setFilter(e.target.value)}
        />
      </div>

      <div className="swan-shares-list">
        {loading && <div className="swan-shares-loading">Loading shares…</div>}
        {error && <div className="swan-shares-error">{error}</div>}
        {!loading && !error && filtered.length === 0 && <div className="swan-shares-empty">No shares available</div>}
        {!loading &&
          !error &&
          filtered.map(share => (
            <ShareItem
              key={`${share.shareDirection}-${share.shareType}-${share.resourceOpaueId}`}
              share={share}
              registry={registry}
              onNavigate={navigateToShare}
              onContextMenu={(s, x, y) => setContextMenu({ x, y, share: s })}
            />
          ))}
      </div>

      {contextMenu &&
        createPortal(
          <div className="swan-shares-context-menu" style={{ left: contextMenu.x, top: contextMenu.y }}>
            <div
              className="swan-shares-context-menu-item"
              onClick={e => {
                e.stopPropagation();
                setContextMenu(null);
                handleEdit(contextMenu.share);
              }}
            >
              Edit
            </div>
          </div>,
          document.body
        )}
    </>
  );
}

export class SharesWidget extends ReactWidget {
  private _fileBrowser: FileBrowser;
  private _shell: JupyterFrontEnd.IShell;
  private _commands: JupyterFrontEnd['commands'];
  private _registry: FileTypeRegistry;
  private _refreshSignal = 0;

  constructor(
    fileBrowser: FileBrowser,
    shell: JupyterFrontEnd.IShell,
    commands: JupyterFrontEnd['commands'],
    registry: FileTypeRegistry
  ) {
    super();
    this._fileBrowser = fileBrowser;
    this._shell = shell;
    this._commands = commands;
    this._registry = registry;
    this.id = 'cernbox-shares';
    this.title.caption = 'CERNBox Shares';
    this.addClass('swan-shares-panel');
  }

  refresh(): void {
    this._refreshSignal++;
    this.update();
  }

  render(): React.ReactElement {
    return (
      <SharesPanel
        fileBrowser={this._fileBrowser}
        shell={this._shell}
        commands={this._commands}
        registry={this._registry}
        refreshSignal={this._refreshSignal}
      />
    );
  }
}
