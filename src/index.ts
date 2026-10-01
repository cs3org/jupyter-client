import { ILabShell, JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { IDocumentManager, IDocumentWidgetOpener } from '@jupyterlab/docmanager';
import { IDefaultFileBrowser } from '@jupyterlab/filebrowser';
import { shareIcon, cernboxIcon } from './icons';
import { SharesWidget } from './shares-widget';
import { openEditShareModal } from './share-edit-modal';
import { DocumentLockTracker } from './locking';
import { LOG_PREFIX, PACKAGE_NAME } from './constants';

/**
 * The Shares plugin.
 *
 * Adds a sidebar panel showing CERNBox folders shared with and by
 * the user. Clicking a share navigates the default file browser
 * to the corresponding EOS path.
 */
const sharesPlugin: JupyterFrontEndPlugin<void> = {
  id: `${PACKAGE_NAME}:shares`,
  description: 'Browse CERNBox shared folders from the JupyterLab sidebar',
  autoStart: true,
  requires: [IDefaultFileBrowser, IDocumentManager],
  optional: [ILabShell],
  activate: (
    app: JupyterFrontEnd,
    fileBrowser: IDefaultFileBrowser,
    docManager: IDocumentManager,
    labShell: ILabShell | null
  ) => {
    console.log(`${LOG_PREFIX} Activating shares plugin`);

    const widget = new SharesWidget(fileBrowser, app.shell, app.commands, docManager.registry);
    widget.title.icon = shareIcon;

    if (labShell) {
      labShell.add(widget, 'left', { rank: 120 });
    } else {
      app.shell.add(widget, 'left');
    }

    const SHARE_COMMAND = `${PACKAGE_NAME}:share-file`;

    app.commands.addCommand(SHARE_COMMAND, {
      label: 'Share',
      icon: cernboxIcon,
      execute: () => {
        const item = fileBrowser.selectedItems().next();
        if (item.done || !item.value) return;
        const selected = item.value;
        const rawPath = '/eos/' + selected.path; // TODO: Don't hardcode this prefix
        openEditShareModal({ name: selected.name, rawPath }, () => widget.refresh());
      }
    });

    app.contextMenu.addItem({
      command: SHARE_COMMAND,
      selector: '.jp-DirListing-item',
      rank: 5
    });
  }
};

/**
 * The Document Locking plugin.
 *
 * Notifies the server extension when documents are opened and closed
 * (POST/DELETE /lock with a per-widget session id) and heartbeats while
 * they stay open, so the CS3 lock on the file is held for exactly as long
 * as someone has it open and released when the last session leaves.
 */
const lockingPlugin: JupyterFrontEndPlugin<void> = {
  id: `${PACKAGE_NAME}:locking`,
  description: 'Hold CS3 locks on open documents via the /lock endpoint',
  autoStart: true,
  requires: [IDocumentWidgetOpener],
  activate: (app: JupyterFrontEnd, opener: IDocumentWidgetOpener) => {
    console.log(`${LOG_PREFIX} Activating locking plugin`);

    const tracker = new DocumentLockTracker();
    void tracker.initialize();
    opener.opened.connect((_, widget) => tracker.track(widget));
  }
};

export default [sharesPlugin, lockingPlugin];
