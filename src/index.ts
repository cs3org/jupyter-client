import { ILabShell, JupyterFrontEnd, JupyterFrontEndPlugin } from '@jupyterlab/application';
import { IDocumentManager } from '@jupyterlab/docmanager';
import { IDefaultFileBrowser } from '@jupyterlab/filebrowser';
import { shareIcon, cernboxIcon } from './icons';
import { SharesWidget } from './shares-widget';
import { openEditShareModal } from './share-edit-modal';

/**
 * The Shares plugin.
 *
 * Adds a sidebar panel showing CERNBox folders shared with and by
 * the user. Clicking a share navigates the default file browser
 * to the corresponding EOS path.
 */
const sharesPlugin: JupyterFrontEndPlugin<void> = {
  id: '@cs3org/cs3-jupyter:shares',
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
    console.log('[cs3org/cs3-jupyter] Activating shares plugin');

    const widget = new SharesWidget(fileBrowser, app.shell, app.commands, docManager.registry);
    widget.title.icon = shareIcon;

    if (labShell) {
      labShell.add(widget, 'left', { rank: 120 });
    } else {
      app.shell.add(widget, 'left');
    }

    const SHARE_COMMAND = '@cs3org/cs3-jupyter:share-file';

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

export default [sharesPlugin];
