import { Widget } from '@lumino/widgets';
import { FileBrowser } from '@jupyterlab/filebrowser';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { Share, fetchShares } from './shares';

const CSS = {
  panel: 'swan-shares-panel',
  header: 'swan-shares-header',
  headerTitle: 'swan-shares-header-title',
  refreshBtn: 'swan-shares-refresh-btn',
  list: 'swan-shares-list',
  section: 'swan-shares-section',
  sectionTitle: 'swan-shares-section-title',
  item: 'swan-shares-item',
  itemActive: 'swan-shares-item-active',
  itemName: 'swan-shares-item-name',
  itemMeta: 'swan-shares-item-meta',
  itemPath: 'swan-shares-item-path',
  loading: 'swan-shares-loading',
  error: 'swan-shares-error',
  empty: 'swan-shares-empty'
} as const;

/**
 * A sidebar widget that displays CERNBox shares and navigates
 * the file browser when a share is clicked.
 */
export class SharesWidget extends Widget {
  private _fileBrowser: FileBrowser;
  private _shell: JupyterFrontEnd.IShell;
  private _listNode: HTMLElement;
  private _shares: Share[] = [];

  constructor(fileBrowser: FileBrowser, shell: JupyterFrontEnd.IShell) {
    super();
    this._fileBrowser = fileBrowser;
    this._shell = shell;
    this.id = 'cernbox-shares';
    this.title.caption = 'CERNBox Shares';
    this.addClass(CSS.panel);

    const header = this._createHeader();
    this._listNode = document.createElement('div');
    this._listNode.className = CSS.list;

    this.node.appendChild(header);
    this.node.appendChild(this._listNode);

    this._loadShares();
  }

  private _createHeader(): HTMLElement {
    const header = document.createElement('div');
    header.className = CSS.header;

    const title = document.createElement('h2');
    title.className = CSS.headerTitle;
    title.textContent = 'CERNBox Shares';

    const refreshBtn = document.createElement('button');
    refreshBtn.className = CSS.refreshBtn;
    refreshBtn.title = 'Refresh shares';
    refreshBtn.textContent = '↻';
    refreshBtn.addEventListener('click', () => {
      this._loadShares();
    });

    header.appendChild(title);
    header.appendChild(refreshBtn);
    return header;
  }

  private async _loadShares(): Promise<void> {
    this._listNode.innerHTML = '';
    this._showLoading();

    try {
      this._shares = await fetchShares();
      this._renderShares();
    } catch (err) {
      this._showError(err instanceof Error ? err.message : 'Failed to load shares');
    }
  }

  private _renderShares() {
    this._listNode.innerHTML = '';

    if (this._shares.length === 0) {
      const empty = document.createElement('div');
      empty.className = CSS.empty;
      empty.textContent = 'No shares available';
      this._listNode.appendChild(empty);
      return;
    }

    const incoming = this._shares.filter(s => s.shareDirection === 'WITH_ME');
    const outgoing = this._shares.filter(s => s.shareDirection === 'BY_ME');

    if (incoming.length > 0) {
      this._renderSection('Shared with me', incoming);
    }
    if (outgoing.length > 0) {
      this._renderSection('Shared by me', outgoing);
    }
  }

  private _renderSection(title: string, shares: Share[]) {
    const section = document.createElement('div');
    section.className = CSS.section;

    const heading = document.createElement('div');
    heading.className = CSS.sectionTitle;
    heading.textContent = title;
    section.appendChild(heading);

    for (const share of shares) {
      section.appendChild(this._createShareItem(share));
    }

    this._listNode.appendChild(section);
  }

  private _createShareItem(share: Share): HTMLElement {
    const item = document.createElement('div');
    item.className = CSS.item;
    item.dataset.path = share.path;
    item.title = `Open ${share.name}\n${share.path}`;

    const name = document.createElement('div');
    name.className = CSS.itemName;
    name.textContent = share.name;
    item.appendChild(name);

    const meta = document.createElement('div');
    meta.className = CSS.itemMeta;
    if (share.shareDirection === 'WITH_ME' && share.sharedBy) {
      meta.textContent = `from ${share.sharedBy}`;
    } else if (share.shareDirection === 'BY_ME' && share.shareType === 'REGULAR' && share.sharedWith.length > 0) {
      meta.textContent = `with ${share.sharedWith.map(g => g.opaqueId).join(', ')}`;
    }
    item.appendChild(meta);

    const path = document.createElement('div');
    path.className = CSS.itemPath;
    path.textContent = share.path;
    item.appendChild(path);

    item.addEventListener('click', () => {
      this._navigateToShare(share);
    });

    return item;
  }

  /**
   * Navigate the file browser to the given share.
   */
  private async _navigateToShare(share: Share): Promise<void> {
    try {
      await this._fileBrowser.model.cd(share.path);
      this._shell.activateById(this._fileBrowser.id);
    } catch (err) {
      console.error(`[cs3org/cs3-jupyter-client:shares] Failed to navigate to ${share.path}:`, err);
    }
  }

  private _showLoading() {
    const el = document.createElement('div');
    el.className = CSS.loading;
    el.textContent = 'Loading shares…';
    this._listNode.appendChild(el);
  }

  private _showError(message: string) {
    this._listNode.innerHTML = '';
    const el = document.createElement('div');
    el.className = CSS.error;
    el.textContent = message;
    this._listNode.appendChild(el);
  }
}
