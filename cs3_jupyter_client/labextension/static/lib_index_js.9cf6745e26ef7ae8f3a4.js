"use strict";
(self["webpackChunk_cs3org_cs3_jupyter_client"] = self["webpackChunk_cs3org_cs3_jupyter_client"] || []).push([["lib_index_js"],{

/***/ "./lib/debounce.js"
/*!*************************!*\
  !*** ./lib/debounce.js ***!
  \*************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   createDebouncedFetcher: () => (/* binding */ createDebouncedFetcher)
/* harmony export */ });
/**
 * Creates a debounced async caller that delays invocation and aborts
 * any in-flight request when a new call arrives.
 *
 * Returns a `run` function and a `cancel` function (for cleanup).
 * The callback receives an `AbortSignal` it should forward to fetch calls.
 */
function createDebouncedFetcher(delay) {
    let timer;
    let controller;
    function run(fn) {
        if (timer)
            clearTimeout(timer);
        timer = setTimeout(() => {
            if (controller)
                controller.abort();
            controller = new AbortController();
            fn(controller.signal);
        }, delay);
    }
    function cancel() {
        if (timer)
            clearTimeout(timer);
        if (controller)
            controller.abort();
    }
    return { run, cancel };
}


/***/ },

/***/ "./lib/icons.js"
/*!**********************!*\
  !*** ./lib/icons.js ***!
  \**********************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   CERNBOX_LOGO_SVG: () => (/* binding */ CERNBOX_LOGO_SVG),
/* harmony export */   EditIcon: () => (/* binding */ EditIcon),
/* harmony export */   GroupEmoji: () => (/* binding */ GroupEmoji),
/* harmony export */   SharedByMeIcon: () => (/* binding */ SharedByMeIcon),
/* harmony export */   SharedPubliclyIcon: () => (/* binding */ SharedPubliclyIcon),
/* harmony export */   SharedWithMeIcon: () => (/* binding */ SharedWithMeIcon),
/* harmony export */   TrashIcon: () => (/* binding */ TrashIcon),
/* harmony export */   UserEmoji: () => (/* binding */ UserEmoji),
/* harmony export */   ViewIcon: () => (/* binding */ ViewIcon),
/* harmony export */   cernboxIcon: () => (/* binding */ cernboxIcon),
/* harmony export */   shareIcon: () => (/* binding */ shareIcon),
/* harmony export */   spacesIcon: () => (/* binding */ spacesIcon)
/* harmony export */ });
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! react */ "webpack/sharing/consume/default/react");
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(react__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! @jupyterlab/ui-components */ "webpack/sharing/consume/default/@jupyterlab/ui-components");
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1__);


const CERNBOX_LOGO_SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="-2 -1 70 79"><polygon fill="#78b6e4" points="33.847,38.36 33.847,55.915 49.767,49.075 49.767,30.13 33.729,37.444"/><path fill="currentColor" d="M34.289 76.691c-9.242.0-16.483-16.843-16.483-38.346C17.806 16.845 25.047.0 34.289.0c9.24.0 16.482 16.845 16.482 38.346.0 21.503-7.242 38.345-16.482 38.345zm0-74.725c-7.868.0-14.517 16.657-14.517 36.38s6.648 36.38 14.517 36.38c7.866.0 14.515-16.657 14.515-36.38S42.155 1.966 34.289 1.966z"/><path fill="currentColor" d="M12.516 62.298c-5.683.0-9.647-1.669-11.467-4.834-4.607-8.014 6.382-22.688 25.024-33.407 10.521-6.051 21.732-9.666 29.989-9.666 5.681.0 9.646 1.669 11.467 4.835 2.272 3.956.822 9.654-4.098 16.045-4.733 6.159-12.165 12.322-20.925 17.36-10.525 6.053-21.734 9.667-29.99 9.667zM56.062 16.356c-7.927.0-18.771 3.516-29.015 9.406C9.958 35.593-1.167 49.661 2.753 56.487c1.831 3.18 6.32 3.846 9.763 3.846 7.925.0 18.771-3.516 29.011-9.409 8.541-4.91 15.768-10.896 20.347-16.85 4.338-5.639 5.774-10.693 3.95-13.873-1.829-3.176-6.32-3.845-9.762-3.845z"/><path fill="currentColor" d="M56.063 62.298c-8.258.0-19.471-3.614-29.99-9.667C17.31 47.593 9.88 41.43 5.145 35.271.229 28.88-1.227 23.182 1.049 19.226c1.819-3.166 5.784-4.835 11.467-4.835 8.257.0 19.466 3.615 29.991 9.666 18.637 10.72 29.629 25.394 25.022 33.407C65.709 60.629 61.743 62.298 56.063 62.298zM12.516 16.356c-3.442.0-7.932.669-9.763 3.845-1.824 3.18-.386 8.234 3.951 13.873 4.582 5.953 11.804 11.939 20.344 16.85 10.24 5.894 21.085 9.409 29.015 9.409h.001c3.443.0 7.932-.666 9.76-3.846 3.924-6.826-7.203-20.895-24.297-30.725C31.287 19.872 20.44 16.356 12.516 16.356z"/><polygon fill="#27aae1" points="34.816,20.646 19.294,29.13 33.599,37.566 48.683,29.104"/><path fill="currentColor" d="M33.599 38.549c-.167.0-.333-.044-.484-.132l-14.308-8.434c-.303-.18-.486-.512-.481-.867.006-.357.201-.681.512-.852l15.521-8.485c.297-.163.664-.156.955.026l13.865 8.457c.295.181.478.507.471.86-.007.351-.195.674-.503.841l-15.078 8.464C33.92 38.509 33.762 38.549 33.599 38.549zM21.275 29.163l12.336 7.274 13.134-7.369L34.789 21.78 21.275 29.163z"/><rect fill="currentColor" x="32.746" y="37.566" width="1.965" height="16.848"/><polygon fill="currentColor" points="33.847,38.36 19.294,30.13 19.294,49.075 33.847,55.915"/></svg>';
const spacesIcon = new _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1__.LabIcon({
    name: '@cs3org/cs3-jupyter-client:spaces',
    svgstr: `<svg xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path xmlns="http://www.w3.org/2000/svg" d="M22 12.999V20C22 20.5523 21.5523 21 21 21H13V12.999H22ZM11 12.999V21H3C2.44772 21 2 20.5523 2 20V12.999H11ZM11 3V10.999H2V4C2 3.44772 2.44772 3 3 3H11ZM21 3C21.5523 3 22 3.44772 22 4V10.999H13V3H21Z"></path></svg>`
});
const cernboxIcon = new _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1__.LabIcon({
    name: '@cs3org/cs3-jupyter-client:cernbox',
    svgstr: CERNBOX_LOGO_SVG
});
const shareIcon = new _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_1__.LabIcon({
    name: '@cs3org/cs3-jupyter-client:shares',
    svgstr: `<svg xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true" focusable="false"><path xmlns="http://www.w3.org/2000/svg" d="M13 14H11C7.54202 14 4.53953 15.9502 3.03239 18.8107C3.01093 18.5433 3 18.2729 3 18C3 12.4772 7.47715 8 13 8V3L23 11L13 19V14Z"></path></svg>`
});
const SharedWithMeIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "16", height: "16", fill: "currentColor", viewBox: "0 0 24 24" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M13 14H11C7.54202 14 4.53953 15.9502 3.03239 18.8107C3.01093 18.5433 3 18.2729 3 18C3 12.4772 7.47715 8 13 8V3L23 11L13 19V14Z" })));
const SharedByMeIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "16", height: "16", fill: "currentColor", viewBox: "0 0 24 24" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M11 14H13C16.458 14 19.4605 15.9502 20.9676 18.8107C20.9891 18.5433 21 18.2729 21 18C21 12.4772 16.5228 8 11 8V3L1 11L11 19V14Z" })));
const SharedPubliclyIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "16", height: "16", viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: "2", strokeLinecap: "round", strokeLinejoin: "round" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71" })));
const ViewIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "14", height: "14", viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: "2", strokeLinecap: "round", strokeLinejoin: "round" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("circle", { cx: "12", cy: "12", r: "3" })));
const EditIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "14", height: "14", viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: "2", strokeLinecap: "round", strokeLinejoin: "round" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M17 3a2.83 2.83 0 1 1 4 4L7.5 20.5 2 22l1.5-5.5L17 3z" })));
const UserEmoji = '\ud83d\udc64';
const GroupEmoji = '\ud83d\udc65';
const TrashIcon = (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("svg", { xmlns: "http://www.w3.org/2000/svg", width: "14", height: "14", viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: "2", strokeLinecap: "round", strokeLinejoin: "round" },
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("polyline", { points: "3 6 5 6 21 6" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M10 11v6" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M14 11v6" }),
    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("path", { d: "M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2" })));


/***/ },

/***/ "./lib/index.js"
/*!**********************!*\
  !*** ./lib/index.js ***!
  \**********************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "default": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony import */ var _jupyterlab_application__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @jupyterlab/application */ "webpack/sharing/consume/default/@jupyterlab/application");
/* harmony import */ var _jupyterlab_application__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_application__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _jupyterlab_docmanager__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! @jupyterlab/docmanager */ "webpack/sharing/consume/default/@jupyterlab/docmanager");
/* harmony import */ var _jupyterlab_docmanager__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_docmanager__WEBPACK_IMPORTED_MODULE_1__);
/* harmony import */ var _jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! @jupyterlab/filebrowser */ "webpack/sharing/consume/default/@jupyterlab/filebrowser");
/* harmony import */ var _jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var _icons__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ./icons */ "./lib/icons.js");
/* harmony import */ var _spaces_widget__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ./spaces-widget */ "./lib/spaces-widget.js");
/* harmony import */ var _shares_widget__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ./shares-widget */ "./lib/shares-widget.js");
/* harmony import */ var _share_edit_modal__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! ./share-edit-modal */ "./lib/share-edit-modal.js");
/* harmony import */ var _quota_widget__WEBPACK_IMPORTED_MODULE_7__ = __webpack_require__(/*! ./quota-widget */ "./lib/quota-widget.js");
/* harmony import */ var _locking__WEBPACK_IMPORTED_MODULE_8__ = __webpack_require__(/*! ./locking */ "./lib/locking.js");









/**
 * The Shares plugin.
 *
 * Adds a sidebar panel showing CERNBox folders shared with and by
 * the user. Clicking a share navigates the default file browser
 * to the corresponding EOS path.
 */
const sharesPlugin = {
    id: '@cs3org/cs3-jupyter-client:shares',
    description: 'Browse CERNBox shared folders from the JupyterLab sidebar',
    autoStart: true,
    requires: [_jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2__.IDefaultFileBrowser, _jupyterlab_docmanager__WEBPACK_IMPORTED_MODULE_1__.IDocumentManager],
    optional: [_jupyterlab_application__WEBPACK_IMPORTED_MODULE_0__.ILabShell],
    activate: (app, fileBrowser, docManager, labShell) => {
        console.log('[cs3org/cs3-jupyter-client] Activating shares plugin');
        const widget = new _shares_widget__WEBPACK_IMPORTED_MODULE_5__.SharesWidget(fileBrowser, app.shell, app.commands, docManager.registry);
        widget.title.icon = _icons__WEBPACK_IMPORTED_MODULE_3__.shareIcon;
        if (labShell) {
            labShell.add(widget, 'left', { rank: 120 });
        }
        else {
            app.shell.add(widget, 'left');
        }
        const SHARE_COMMAND = '@cs3org/cs3-jupyter-client:share-file';
        app.commands.addCommand(SHARE_COMMAND, {
            label: 'Share',
            icon: _icons__WEBPACK_IMPORTED_MODULE_3__.cernboxIcon,
            execute: () => {
                const item = fileBrowser.selectedItems().next();
                if (item.done || !item.value)
                    return;
                const selected = item.value;
                const rawPath = '/eos/' + selected.path; // TODO: Don't hardcode this prefix
                (0,_share_edit_modal__WEBPACK_IMPORTED_MODULE_6__.openEditShareModal)({ name: selected.name, rawPath }, () => widget.refresh());
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
 * The Spaces plugin.
 *
 * Adds a sidebar panel that lists CERNBox Spaces (projects) the user
 * has access to. Clicking a space navigates the default file browser
 * to the corresponding EOS path.
 */
const spacesPlugin = {
    id: '@cs3org/cs3-jupyter-client:spaces',
    description: 'Navigate CERNBox Spaces from the JupyterLab sidebar',
    autoStart: true,
    requires: [_jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2__.IDefaultFileBrowser],
    optional: [_jupyterlab_application__WEBPACK_IMPORTED_MODULE_0__.ILabShell],
    activate: (app, fileBrowser, labShell) => {
        console.log('[cs3org/cs3-jupyter-client] Activating spaces plugin');
        const widget = new _spaces_widget__WEBPACK_IMPORTED_MODULE_4__.SpacesWidget(fileBrowser, app.shell);
        widget.title.icon = _icons__WEBPACK_IMPORTED_MODULE_3__.spacesIcon;
        if (labShell) {
            labShell.add(widget, 'left', { rank: 121 });
        }
        else {
            app.shell.add(widget, 'left');
        }
    }
};
/**
 * The Storage Quota plugin.
 *
 * Attaches a progress bar to the bottom of the default file browser
 * showing the user's CERNBox storage usage.
 */
const quotaPlugin = {
    id: '@cs3org/cs3-jupyter-client:quota',
    description: 'CERNBox storage quota indicator in the file browser',
    autoStart: true,
    requires: [_jupyterlab_filebrowser__WEBPACK_IMPORTED_MODULE_2__.IDefaultFileBrowser],
    activate: (app, fileBrowser) => {
        console.log('[cs3org/cs3-jupyter-client] Activating quota plugin');
        (0,_quota_widget__WEBPACK_IMPORTED_MODULE_7__.attachQuotaIndicator)(fileBrowser);
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
const lockingPlugin = {
    id: '@cs3org/cs3-jupyter-client:locking',
    description: 'Hold CS3 locks on open documents via the /lock endpoint',
    autoStart: true,
    requires: [_jupyterlab_docmanager__WEBPACK_IMPORTED_MODULE_1__.IDocumentWidgetOpener],
    activate: (app, opener) => {
        console.log('[cs3org/cs3-jupyter-client] Activating locking plugin');
        const tracker = new _locking__WEBPACK_IMPORTED_MODULE_8__.DocumentLockTracker();
        void tracker.initialize();
        opener.opened.connect((_, widget) => tracker.track(widget));
    }
};
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = ([sharesPlugin, spacesPlugin, quotaPlugin, lockingPlugin]);


/***/ },

/***/ "./lib/locking.js"
/*!************************!*\
  !*** ./lib/locking.js ***!
  \************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   DocumentLockTracker: () => (/* binding */ DocumentLockTracker)
/* harmony export */ });
/* harmony import */ var _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @jupyterlab/services */ "webpack/sharing/consume/default/@jupyterlab/services");
/* harmony import */ var _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__);

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
function lockRequest(method, path, sessionId) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const query = `?path=${encodeURIComponent(path)}&session_id=${encodeURIComponent(sessionId)}`;
    return _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(settings.baseUrl + 'lock' + query, { method }, settings);
}
function newSessionId() {
    if (typeof crypto !== 'undefined' && crypto.randomUUID) {
        return crypto.randomUUID();
    }
    return `s-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
class DocumentLockTracker {
    expirationSeconds = DEFAULT_EXPIRATION_SECONDS;
    tracked = new WeakSet();
    /** Read the server's lock expiration to derive the heartbeat period. */
    async initialize() {
        try {
            const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
            const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(settings.baseUrl + 'lock', {}, settings);
            if (resp.ok) {
                const data = await resp.json();
                if (typeof data.expiration === 'number' && data.expiration > 0) {
                    this.expirationSeconds = data.expiration;
                }
            }
        }
        catch (err) {
            console.warn('[cs3org/cs3-jupyter-client] Could not read lock expiration, using default', err);
        }
    }
    /** Heartbeat faster than the expiration, mirroring the server refresher. */
    get heartbeatMs() {
        return Math.max(Math.floor(this.expirationSeconds / 3), 10) * 1000;
    }
    track(widget) {
        if (this.tracked.has(widget) || widget.isDisposed) {
            return;
        }
        this.tracked.add(widget);
        const sessionId = newSessionId();
        let path = widget.context.path;
        const open = async (p) => {
            try {
                const resp = await lockRequest('POST', p, sessionId);
                if (!resp.ok) {
                    console.warn(`[cs3org/cs3-jupyter-client] Could not lock ${p}: HTTP ${resp.status}`);
                    return;
                }
                const data = (await resp.json());
                if (data.read_only) {
                    console.info(`[cs3org/cs3-jupyter-client] ${p} is locked by ${data.holder ?? 'another application'}, opening read-only`);
                }
            }
            catch (err) {
                console.warn(`[cs3org/cs3-jupyter-client] Lock request for ${p} failed`, err);
            }
        };
        const close = (p) => {
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


/***/ },

/***/ "./lib/quota-widget.js"
/*!*****************************!*\
  !*** ./lib/quota-widget.js ***!
  \*****************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   attachQuotaIndicator: () => (/* binding */ attachQuotaIndicator)
/* harmony export */ });
/* harmony import */ var _quota__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ./quota */ "./lib/quota.js");

const CSS = {
    container: 'swan-quota-container',
    label: 'swan-quota-label',
    barOuter: 'swan-quota-bar-outer',
    barInner: 'swan-quota-bar-inner',
    barWarning: 'swan-quota-bar-warning',
    barCritical: 'swan-quota-bar-critical'
};
/** Usage thresholds for visual warnings */
const WARN_THRESHOLD = 0.8;
const CRITICAL_THRESHOLD = 0.95;
/**
 * Create and attach a storage quota indicator to the bottom
 * of the default file browser panel.
 */
async function attachQuotaIndicator(fileBrowser) {
    const container = document.createElement('div');
    container.className = CSS.container;
    const label = document.createElement('div');
    label.className = CSS.label;
    label.textContent = 'Loading…';
    const barOuter = document.createElement('div');
    barOuter.className = CSS.barOuter;
    const barInner = document.createElement('div');
    barInner.className = CSS.barInner;
    barInner.style.width = '0%';
    barOuter.appendChild(barInner);
    container.appendChild(label);
    container.appendChild(barOuter);
    fileBrowser.node.appendChild(container);
    // Initial load
    await updateQuota(label, barInner);
    // Refresh periodically (every 5 minutes)
    setInterval(() => {
        updateQuota(label, barInner);
    }, 5 * 60 * 1000);
}
async function updateQuota(label, barInner) {
    try {
        const quota = await (0,_quota__WEBPACK_IMPORTED_MODULE_0__.fetchQuota)();
        const fraction = quota.total > 0 ? quota.used / quota.total : 0;
        const percent = Math.min(fraction * 100, 100);
        label.textContent = `${(0,_quota__WEBPACK_IMPORTED_MODULE_0__.formatBytes)(quota.used)} / ${(0,_quota__WEBPACK_IMPORTED_MODULE_0__.formatBytes)(quota.total)}`;
        barInner.style.width = `${percent}%`;
        // Remove old threshold classes
        barInner.classList.remove(CSS.barWarning, CSS.barCritical);
        if (fraction >= CRITICAL_THRESHOLD) {
            barInner.classList.add(CSS.barCritical);
        }
        else if (fraction >= WARN_THRESHOLD) {
            barInner.classList.add(CSS.barWarning);
        }
    }
    catch {
        label.textContent = 'Quota unavailable';
        barInner.style.width = '0%';
    }
}


/***/ },

/***/ "./lib/quota.js"
/*!**********************!*\
  !*** ./lib/quota.js ***!
  \**********************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   fetchQuota: () => (/* binding */ fetchQuota),
/* harmony export */   formatBytes: () => (/* binding */ formatBytes)
/* harmony export */ });
/**
 * Fetch the user's storage quota.
 *
 * TODO: Replace with a real CS3/CERNBox API call.
 */
async function fetchQuota() {
    // Simulate network latency
    await new Promise(resolve => setTimeout(resolve, 200));
    return {
        used: 6.8 * 1024 * 1024 * 1024, // 6.8 GB
        total: 10 * 1024 * 1024 * 1024 // 10 GB
    };
}
/**
 * Format bytes into a human-readable string.
 */
function formatBytes(bytes) {
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


/***/ },

/***/ "./lib/share-edit-modal.js"
/*!*********************************!*\
  !*** ./lib/share-edit-modal.js ***!
  \*********************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   openEditShareModal: () => (/* binding */ openEditShareModal)
/* harmony export */ });
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! react */ "webpack/sharing/consume/default/react");
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(react__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var react_dom__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! react-dom */ "webpack/sharing/consume/default/react-dom");
/* harmony import */ var react_dom__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(react_dom__WEBPACK_IMPORTED_MODULE_1__);
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! @jupyterlab/ui-components */ "webpack/sharing/consume/default/@jupyterlab/ui-components");
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var _lumino_widgets__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! @lumino/widgets */ "webpack/sharing/consume/default/@lumino/widgets");
/* harmony import */ var _lumino_widgets__WEBPACK_IMPORTED_MODULE_3___default = /*#__PURE__*/__webpack_require__.n(_lumino_widgets__WEBPACK_IMPORTED_MODULE_3__);
/* harmony import */ var _shares__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ./shares */ "./lib/shares.js");
/* harmony import */ var _debounce__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ./debounce */ "./lib/debounce.js");
/* harmony import */ var _icons__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! ./icons */ "./lib/icons.js");







const ROLE_OPTIONS = [
    { value: 'VIEWER', label: 'Can view', icon: _icons__WEBPACK_IMPORTED_MODULE_6__.ViewIcon },
    { value: 'EDITOR', label: 'Can edit', icon: _icons__WEBPACK_IMPORTED_MODULE_6__.EditIcon }
];
function RoleDropdown({ value, onChange, disabled }) {
    const [open, setOpen] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(false);
    const [menuPos, setMenuPos] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const ref = (0,react__WEBPACK_IMPORTED_MODULE_0__.useRef)(null);
    const opt = ROLE_OPTIONS.find(o => o.value === value) ?? ROLE_OPTIONS[0];
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        const close = (e) => {
            if (ref.current && !ref.current.contains(e.target))
                setOpen(false);
        };
        document.addEventListener('click', close);
        return () => document.removeEventListener('click', close);
    }, []);
    const handleToggle = (e) => {
        e.stopPropagation();
        if (disabled)
            return;
        if (!open && ref.current) {
            const rect = ref.current.getBoundingClientRect();
            setMenuPos({ top: rect.bottom + 2, left: rect.left });
        }
        setOpen(!open);
    };
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-role-dropdown", ref: ref },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { type: "button", className: "swan-shares-role-dropdown-selected", onClick: handleToggle, style: disabled ? { opacity: 0.5, cursor: 'not-allowed' } : undefined },
            opt.icon,
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", null, opt.label)),
        open &&
            menuPos &&
            (0,react_dom__WEBPACK_IMPORTED_MODULE_1__.createPortal)(react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-role-dropdown-menu", style: { position: 'fixed', top: menuPos.top, left: menuPos.left } }, ROLE_OPTIONS.map(o => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { key: o.value, className: `swan-shares-role-dropdown-option${o.value === value ? ' swan-shares-role-dropdown-option-active' : ''}`, onClick: e => {
                    e.stopPropagation();
                    onChange(o.value);
                    setOpen(false);
                } },
                o.icon,
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", null, o.label))))), document.body)));
}
function GranteeItem({ grantee, onRoleChange, onRemove }) {
    const [busy, setBusy] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(false);
    const [role, setRole] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(grantee.role);
    const handleRole = async (newRole) => {
        setBusy(true);
        try {
            await onRoleChange(grantee.shareId, newRole);
            setRole(newRole);
        }
        catch {
            setRole(role);
        }
        finally {
            setBusy(false);
        }
    };
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-grantee-item" },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-grantee-info" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-grantee-icon", title: grantee.type === 'GRANTEE_TYPE_USER' ? 'User' : 'Group' }, grantee.type === 'GRANTEE_TYPE_USER' ? _icons__WEBPACK_IMPORTED_MODULE_6__.UserEmoji : _icons__WEBPACK_IMPORTED_MODULE_6__.GroupEmoji),
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-grantee-name" }, grantee.opaqueId)),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement(RoleDropdown, { value: role, onChange: handleRole, disabled: busy }),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { className: "swan-shares-grantee-remove", title: "Remove", onClick: () => onRemove(grantee.shareId) }, _icons__WEBPACK_IMPORTED_MODULE_6__.TrashIcon)));
}
function SearchResultItem({ icon, name, detail, alreadyAdded, onAdd }) {
    const [state, setState] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(alreadyAdded ? 'added' : 'idle');
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        setState(prev => (prev === 'adding' ? prev : alreadyAdded ? 'added' : 'idle'));
    }, [alreadyAdded]);
    const handleAdd = async () => {
        setState('adding');
        try {
            await onAdd();
            setState('added');
        }
        catch {
            setState('idle');
        }
    };
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-result-item" },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-grantee-icon" }, icon),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-result-info" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-result-name" }, name),
            detail && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-result-detail" }, detail)),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { className: "swan-shares-search-result-add", disabled: state !== 'idle', onClick: handleAdd }, state === 'adding' ? 'Adding...' : state === 'added' ? 'Added' : 'Add')));
}
function EditShareModalContent({ share, onClose }) {
    const [grantees, setGrantees] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)([]);
    const [granteesLoading, setGranteesLoading] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(true);
    const [granteesError, setGranteesError] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const [addRole, setAddRole] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)('VIEWER');
    const [searchQuery, setSearchQuery] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)('');
    const [searchResults, setSearchResults] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const [searchLoading, setSearchLoading] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(false);
    const [searchError, setSearchError] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const [status, setStatus] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const fetcher = (0,react__WEBPACK_IMPORTED_MODULE_0__.useMemo)(() => (0,_debounce__WEBPACK_IMPORTED_MODULE_5__.createDebouncedFetcher)(300), []);
    const showStatus = (0,react__WEBPACK_IMPORTED_MODULE_0__.useCallback)((msg, isError = false) => {
        setStatus({ msg, isError });
        if (!isError) {
            setTimeout(() => setStatus(prev => (prev?.msg === msg ? null : prev)), 3000);
        }
    }, []);
    const loadGrantees = (0,react__WEBPACK_IMPORTED_MODULE_0__.useCallback)(async (silent = false) => {
        if (!silent) {
            setGranteesLoading(true);
            setGranteesError(null);
        }
        try {
            const data = await (0,_shares__WEBPACK_IMPORTED_MODULE_4__.fetchSharesForResource)(share.rawPath);
            setGrantees(data);
        }
        catch (err) {
            if (!silent) {
                setGranteesError(err instanceof Error ? err.message : 'Failed to load');
            }
        }
        finally {
            if (!silent) {
                setGranteesLoading(false);
            }
        }
    }, [share.rawPath]);
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        loadGrantees();
    }, [loadGrantees]);
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        const query = searchQuery.trim();
        if (query.length < 2) {
            setSearchResults(null);
            setSearchLoading(false);
            setSearchError(null);
            return fetcher.cancel;
        }
        fetcher.run(async (signal) => {
            setSearchLoading(true);
            try {
                const [users, groups] = await Promise.all([(0,_shares__WEBPACK_IMPORTED_MODULE_4__.findUsers)(query, signal), (0,_shares__WEBPACK_IMPORTED_MODULE_4__.findGroups)(query, signal)]);
                setSearchResults({ users, groups });
                setSearchError(null);
            }
            catch (err) {
                if (signal.aborted)
                    return;
                setSearchError(err instanceof Error ? err.message : 'Search failed');
                setSearchResults(null);
            }
            finally {
                if (!signal.aborted)
                    setSearchLoading(false);
            }
        });
        return fetcher.cancel;
    }, [searchQuery, fetcher]);
    const handleRoleChange = async (shareId, newRole) => {
        try {
            await (0,_shares__WEBPACK_IMPORTED_MODULE_4__.updateShareRole)(shareId, newRole);
            setGrantees(prev => prev.map(g => (g.shareId === shareId ? { ...g, role: newRole } : g)));
        }
        catch (err) {
            showStatus(`Failed to update role: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
            throw err;
        }
    };
    const handleRemove = async (shareId) => {
        try {
            await (0,_shares__WEBPACK_IMPORTED_MODULE_4__.removeShare)(shareId);
            setGrantees(prev => prev.filter(g => g.shareId !== shareId));
        }
        catch (err) {
            showStatus(`Failed to remove: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
        }
    };
    const handleAddUser = async (user) => {
        try {
            await (0,_shares__WEBPACK_IMPORTED_MODULE_4__.createShare)(share.rawPath, user.opaqueId, user.idp, addRole, 'GRANTEE_TYPE_USER');
            setGrantees(prev => [
                ...prev,
                { shareId: `pending-${user.opaqueId}`, type: 'GRANTEE_TYPE_USER', opaqueId: user.opaqueId, role: addRole }
            ]);
            loadGrantees(true);
        }
        catch (err) {
            showStatus(`Failed to add: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
            throw err;
        }
    };
    const handleAddGroup = async (group) => {
        try {
            await (0,_shares__WEBPACK_IMPORTED_MODULE_4__.createShare)(share.rawPath, group.opaqueId, '', addRole, 'GRANTEE_TYPE_GROUP');
            setGrantees(prev => [
                ...prev,
                { shareId: `pending-${group.opaqueId}`, type: 'GRANTEE_TYPE_GROUP', opaqueId: group.opaqueId, role: addRole }
            ]);
            loadGrantees(true);
        }
        catch (err) {
            showStatus(`Failed to add: ${err instanceof Error ? err.message : 'Unknown error'}`, true);
            throw err;
        }
    };
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-overlay", onMouseDown: e => {
            if (e.target === e.currentTarget)
                onClose();
        } },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-header" },
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("h3", { className: "swan-shares-modal-title" },
                    "Share: ",
                    share.name),
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { className: "swan-shares-modal-close-btn", onClick: onClose }, '\u00d7')),
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-body" },
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-section" },
                    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-section-title" }, "Share with people"),
                    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-container" },
                        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { style: { display: 'flex', alignItems: 'center', gap: '8px', marginTop: '6px' } },
                            react__WEBPACK_IMPORTED_MODULE_0___default().createElement(RoleDropdown, { value: addRole, onChange: setAddRole })),
                        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("input", { className: "swan-shares-search-input", type: "text", placeholder: "Search users or groups...", value: searchQuery, onChange: e => setSearchQuery(e.target.value) }),
                        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-search-results" },
                            searchLoading && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-empty" }, "Searching..."),
                            searchError && (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-empty", style: { color: 'var(--jp-error-color1)' } }, searchError)),
                            searchResults &&
                                !searchLoading &&
                                searchResults.users.length === 0 &&
                                searchResults.groups.length === 0 && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-empty" }, "No results found"),
                            searchResults && !searchLoading && (react__WEBPACK_IMPORTED_MODULE_0___default().createElement((react__WEBPACK_IMPORTED_MODULE_0___default().Fragment), null,
                                searchResults.users.map(user => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement(SearchResultItem, { key: `user-${user.opaqueId}`, icon: _icons__WEBPACK_IMPORTED_MODULE_6__.UserEmoji, iconTitle: "User", name: user.displayName || user.opaqueId, detail: user.mail || undefined, alreadyAdded: grantees.some(g => g.type === 'GRANTEE_TYPE_USER' && g.opaqueId === user.opaqueId), onAdd: () => handleAddUser(user) }))),
                                searchResults.groups.map(group => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement(SearchResultItem, { key: `group-${group.opaqueId}`, icon: _icons__WEBPACK_IMPORTED_MODULE_6__.GroupEmoji, iconTitle: "Group", name: group.displayName, alreadyAdded: grantees.some(g => g.type === 'GRANTEE_TYPE_GROUP' && g.opaqueId === group.opaqueId), onAdd: () => handleAddGroup(group) })))))))),
                !granteesLoading && !granteesError && grantees.length > 0 && (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-section" },
                    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-section-title" }, "Shared with"),
                    react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-grantee-list" }, grantees.map(g => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement(GranteeItem, { key: g.shareId, grantee: g, onRoleChange: handleRoleChange, onRemove: handleRemove })))))),
                granteesLoading && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-empty" }, "Loading..."),
                granteesError && (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-empty", style: { color: 'var(--jp-error-color1)' } }, granteesError)),
                status && (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-status", style: { color: status.isError ? 'var(--jp-error-color1)' : 'var(--jp-ui-font-color2)' } }, status.msg))),
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-modal-footer" },
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { className: "swan-shares-modal-footer-btn", onClick: onClose }, "Close")))));
}
let activeModal = null;
function openEditShareModal(share, onClose) {
    if (activeModal) {
        activeModal.dispose();
        activeModal = null;
    }
    const widget = _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__.ReactWidget.create(react__WEBPACK_IMPORTED_MODULE_0___default().createElement(EditShareModalContent, { share: share, onClose: () => {
            widget.dispose();
            activeModal = null;
            onClose();
        } }));
    activeModal = widget;
    _lumino_widgets__WEBPACK_IMPORTED_MODULE_3__.Widget.attach(widget, document.body);
}


/***/ },

/***/ "./lib/shares-widget.js"
/*!******************************!*\
  !*** ./lib/shares-widget.js ***!
  \******************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   SharesWidget: () => (/* binding */ SharesWidget)
/* harmony export */ });
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! react */ "webpack/sharing/consume/default/react");
/* harmony import */ var react__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(react__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var react_dom__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! react-dom */ "webpack/sharing/consume/default/react-dom");
/* harmony import */ var react_dom__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(react_dom__WEBPACK_IMPORTED_MODULE_1__);
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! @jupyterlab/ui-components */ "webpack/sharing/consume/default/@jupyterlab/ui-components");
/* harmony import */ var _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var _shares__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! ./shares */ "./lib/shares.js");
/* harmony import */ var _share_edit_modal__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! ./share-edit-modal */ "./lib/share-edit-modal.js");
/* harmony import */ var _icons__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! ./icons */ "./lib/icons.js");







const TABS = [
    { id: 'WITH_ME', label: 'Shared with me', icon: _icons__WEBPACK_IMPORTED_MODULE_5__.SharedWithMeIcon },
    { id: 'BY_ME', label: 'Shared with others', icon: _icons__WEBPACK_IMPORTED_MODULE_5__.SharedByMeIcon },
    { id: 'PUBLIC', label: 'Shared publicly', icon: _icons__WEBPACK_IMPORTED_MODULE_5__.SharedPubliclyIcon }
];
function FileTypeIcon({ share, registry }) {
    if (share.resourceType !== 'RESOURCE_TYPE_FILE') {
        return react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-item-icon" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement(_jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__.folderIcon.react, { stylesheet: "listing" }));
    }
    const fileTypes = registry.getFileTypesForPath(share.name);
    const Icon = fileTypes.length > 0 && fileTypes[0].icon ? fileTypes[0].icon.react : _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__.fileIcon.react;
    return react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-item-icon" },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement(Icon, { stylesheet: "listing" }));
}
function ShareItem({ share, registry, onNavigate, onContextMenu }) {
    let meta = '';
    if (share.shareDirection === 'WITH_ME' && share.sharedBy) {
        meta = `from ${share.sharedBy}`;
    }
    else if (share.shareDirection === 'BY_ME' && share.shareType === 'REGULAR' && share.sharedWith.length > 0) {
        meta = `with ${share.sharedWith.map(g => g.opaqueId).join(', ')}`;
    }
    const handleContext = (e) => {
        if (share.shareDirection === 'BY_ME' && share.shareType === 'REGULAR') {
            e.preventDefault();
            e.stopPropagation();
            onContextMenu(share, e.clientX, e.clientY);
        }
    };
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-item", title: share.path, onClick: () => onNavigate(share), onContextMenu: handleContext },
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement(FileTypeIcon, { share: share, registry: registry }),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { style: { minWidth: 0 } },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-item-name" }, share.name),
            meta && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-item-meta" }, meta))));
}
function SharesPanel({ fileBrowser, shell, commands, registry, refreshSignal }) {
    const [shares, setShares] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)([]);
    const [loading, setLoading] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(true);
    const [error, setError] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const [activeTab, setActiveTab] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)('WITH_ME');
    const [filter, setFilter] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)('');
    const [contextMenu, setContextMenu] = (0,react__WEBPACK_IMPORTED_MODULE_0__.useState)(null);
    const loadShares = (0,react__WEBPACK_IMPORTED_MODULE_0__.useCallback)(async () => {
        setLoading(true);
        setError(null);
        try {
            setShares(await (0,_shares__WEBPACK_IMPORTED_MODULE_3__.fetchShares)());
        }
        catch (err) {
            setError(err instanceof Error ? err.message : 'Failed to load shares');
        }
        finally {
            setLoading(false);
        }
    }, []);
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        loadShares();
    }, [loadShares, refreshSignal]);
    (0,react__WEBPACK_IMPORTED_MODULE_0__.useEffect)(() => {
        const dismiss = () => setContextMenu(null);
        document.addEventListener('click', dismiss);
        document.addEventListener('contextmenu', dismiss);
        return () => {
            document.removeEventListener('click', dismiss);
            document.removeEventListener('contextmenu', dismiss);
        };
    }, []);
    const switchTab = (tab) => {
        setActiveTab(tab);
        setFilter('');
    };
    const navigateToShare = async (share) => {
        try {
            if (share.resourceType === 'RESOURCE_TYPE_FILE') {
                await commands.execute('docmanager:open', { path: share.path });
            }
            else {
                await fileBrowser.model.cd(share.path);
                shell.activateById(fileBrowser.id);
            }
        }
        catch (err) {
            console.error(`[cs3org/cs3-jupyter-client:shares] Failed to open ${share.path}:`, err);
        }
    };
    const handleEdit = (share) => {
        (0,_share_edit_modal__WEBPACK_IMPORTED_MODULE_4__.openEditShareModal)(share, loadShares);
    };
    const filterQuery = filter.trim().toLowerCase();
    const filtered = shares
        .filter(s => {
        if (activeTab === 'WITH_ME')
            return s.shareDirection === 'WITH_ME';
        if (activeTab === 'BY_ME')
            return s.shareDirection === 'BY_ME' && s.shareType === 'REGULAR';
        return s.shareDirection === 'BY_ME' && s.shareType === 'PUBLIC';
    })
        .filter(s => !filterQuery || s.name.toLowerCase().includes(filterQuery))
        .sort((a, b) => a.name.localeCompare(b.name));
    return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement((react__WEBPACK_IMPORTED_MODULE_0___default().Fragment), null,
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-header" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("span", { className: "swan-shares-header-logo", dangerouslySetInnerHTML: { __html: _icons__WEBPACK_IMPORTED_MODULE_5__.CERNBOX_LOGO_SVG } }),
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("h2", { className: "swan-shares-header-title" }, "CERNBox Shares"),
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { className: "swan-shares-refresh-btn", title: "Refresh shares", onClick: loadShares }, "\u21BB")),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-tabs" }, TABS.map(tab => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement("button", { key: tab.id, className: `swan-shares-tab${activeTab === tab.id ? ' swan-shares-tab-active' : ''}`, onClick: () => switchTab(tab.id) },
            tab.icon,
            " ",
            tab.label)))),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-filter-container" },
            react__WEBPACK_IMPORTED_MODULE_0___default().createElement("input", { className: "swan-shares-filter-input", type: "text", placeholder: "Filter by name...", value: filter, onChange: e => setFilter(e.target.value) })),
        react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-list" },
            loading && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-loading" }, "Loading shares\u2026"),
            error && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-error" }, error),
            !loading && !error && filtered.length === 0 && react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-empty" }, "No shares available"),
            !loading &&
                !error &&
                filtered.map(share => (react__WEBPACK_IMPORTED_MODULE_0___default().createElement(ShareItem, { key: `${share.shareDirection}-${share.shareType}-${share.resourceOpaueId}`, share: share, registry: registry, onNavigate: navigateToShare, onContextMenu: (s, x, y) => setContextMenu({ x, y, share: s }) })))),
        contextMenu &&
            (0,react_dom__WEBPACK_IMPORTED_MODULE_1__.createPortal)(react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-context-menu", style: { left: contextMenu.x, top: contextMenu.y } },
                react__WEBPACK_IMPORTED_MODULE_0___default().createElement("div", { className: "swan-shares-context-menu-item", onClick: e => {
                        e.stopPropagation();
                        setContextMenu(null);
                        handleEdit(contextMenu.share);
                    } }, "Edit")), document.body)));
}
class SharesWidget extends _jupyterlab_ui_components__WEBPACK_IMPORTED_MODULE_2__.ReactWidget {
    _fileBrowser;
    _shell;
    _commands;
    _registry;
    _refreshSignal = 0;
    constructor(fileBrowser, shell, commands, registry) {
        super();
        this._fileBrowser = fileBrowser;
        this._shell = shell;
        this._commands = commands;
        this._registry = registry;
        this.id = 'cernbox-shares';
        this.title.caption = 'CERNBox Shares';
        this.addClass('swan-shares-panel');
    }
    refresh() {
        this._refreshSignal++;
        this.update();
    }
    render() {
        return (react__WEBPACK_IMPORTED_MODULE_0___default().createElement(SharesPanel, { fileBrowser: this._fileBrowser, shell: this._shell, commands: this._commands, registry: this._registry, refreshSignal: this._refreshSignal }));
    }
}


/***/ },

/***/ "./lib/shares.js"
/*!***********************!*\
  !*** ./lib/shares.js ***!
  \***********************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   createShare: () => (/* binding */ createShare),
/* harmony export */   fetchShares: () => (/* binding */ fetchShares),
/* harmony export */   fetchSharesForResource: () => (/* binding */ fetchSharesForResource),
/* harmony export */   findGroups: () => (/* binding */ findGroups),
/* harmony export */   findUsers: () => (/* binding */ findUsers),
/* harmony export */   removeShare: () => (/* binding */ removeShare),
/* harmony export */   updateShareRole: () => (/* binding */ updateShareRole)
/* harmony export */ });
/* harmony import */ var _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @jupyterlab/services */ "webpack/sharing/consume/default/@jupyterlab/services");
/* harmony import */ var _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__);

async function fetchShares() {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const [byMeResp, withMeResp] = await Promise.all([
        _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(settings.baseUrl + 'share/getSharedByMe', {}, settings),
        _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(settings.baseUrl + 'share/getSharedWithMe', {}, settings)
    ]);
    if (!byMeResp.ok) {
        const data = await byMeResp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(byMeResp, data.error ?? byMeResp.statusText);
    }
    if (!withMeResp.ok) {
        const data = await withMeResp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(withMeResp, data.error ?? withMeResp.statusText);
    }
    const byMeData = await byMeResp.json();
    const withMeData = await withMeResp.json();
    const byMeMerged = new Map();
    for (const share of byMeData.shares) {
        const resourceId = share.share.resource_id.opaque_id;
        const sharedWithOpaqueId = share.share.grantee.type === 'GRANTEE_TYPE_USER'
            ? share.share.grantee.user_id.opaque_id
            : share.share.grantee.group_id.opaque_id;
        if (!byMeMerged.has(resourceId)) {
            byMeMerged.set(resourceId, {
                shareDirection: 'BY_ME',
                shareType: 'REGULAR',
                resourceOpaueId: share.share.resource_id.opaque_id,
                resourceType: share.resource_info.type ?? 'RESOURCE_TYPE_CONTAINER',
                name: share.resource_info.name,
                path: share.resource_info.path.slice('/eos'.length), // TODO: Don't hardcode this prefix
                rawPath: share.resource_info.path,
                sharedWith: [
                    {
                        type: share.share.grantee.type,
                        opaqueId: sharedWithOpaqueId
                    }
                ]
            });
        }
        else {
            byMeMerged.get(resourceId)?.sharedWith.push({
                type: share.share.grantee.type,
                opaqueId: sharedWithOpaqueId
            });
        }
    }
    const byMePublicMerged = new Map();
    for (const share of byMeData.public_shares) {
        const resourceId = share.public_share.resource_id.opaque_id;
        byMePublicMerged.set(resourceId, {
            shareDirection: 'BY_ME',
            shareType: 'PUBLIC',
            resourceOpaueId: share.public_share.resource_id.opaque_id,
            resourceType: share.resource_info.type ?? 'RESOURCE_TYPE_CONTAINER',
            name: share.resource_info.name,
            path: share.resource_info.path.slice('/eos'.length), // TODO: Don't hardcode this prefix
            rawPath: share.resource_info.path
        });
    }
    const withMeMerged = new Map();
    for (const share of withMeData.shares) {
        if (share.received_share.state !== 'SHARE_STATE_ACCEPTED') {
            continue; // Skip non-accepted shares
        }
        const resourceId = share.received_share.share.resource_id.opaque_id;
        withMeMerged.set(resourceId, {
            shareDirection: 'WITH_ME',
            shareType: 'REGULAR',
            resourceOpaueId: share.received_share.share.resource_id.opaque_id,
            resourceType: share.resource_info.type ?? 'RESOURCE_TYPE_CONTAINER',
            name: share.resource_info.name,
            path: share.resource_info.path.slice('/eos'.length), // TODO: Don't hardcode this prefix
            rawPath: share.resource_info.path,
            sharedBy: share.received_share.share.creator.opaque_id
        });
    }
    return [...byMeMerged.values(), ...byMePublicMerged.values(), ...withMeMerged.values()];
}
function roleFromRawPermissions(permissions) {
    const perms = permissions?.permissions;
    return perms?.initiate_file_upload ? 'EDITOR' : 'VIEWER';
}
async function fetchSharesForResource(rawPath) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'share/getSharedByResource?' + new URLSearchParams({ path: rawPath });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, {}, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
    const data = await resp.json();
    const results = [];
    for (const item of data.shares) {
        const grantee = item.share.grantee;
        const opaqueId = grantee.type === 'GRANTEE_TYPE_USER'
            ? grantee.user_id.opaque_id
            : grantee.group_id.opaque_id;
        results.push({
            shareId: item.share.id.opaque_id,
            type: grantee.type,
            opaqueId,
            role: roleFromRawPermissions(item.share.permissions)
        });
    }
    return results;
}
async function removeShare(shareId) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'share/share?' + new URLSearchParams({ share_id: shareId });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, { method: 'DELETE' }, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
}
async function updateShareRole(shareId, role) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'share/share?' + new URLSearchParams({ share_id: shareId });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, {
        method: 'PUT',
        body: JSON.stringify({ role })
    }, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
}
async function createShare(rawPath, opaqueId, idp, role, granteeType) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'share/share?' + new URLSearchParams({ path: rawPath });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, {
        method: 'POST',
        body: JSON.stringify({
            opaque_id: opaqueId,
            idp,
            role,
            grantee_type: granteeType === 'GRANTEE_TYPE_USER' ? 'USER' : 'GROUP'
        })
    }, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
}
async function findUsers(query, signal) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'find/users?' + new URLSearchParams({ search: query });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, { signal }, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
    const data = await resp.json();
    return data.items.map((u) => ({
        opaqueId: u.id.opaque_id,
        idp: u.id.idp,
        displayName: u.display_name || u.username || '',
        mail: u.mail || ''
    }));
}
async function findGroups(query, signal) {
    const settings = _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeSettings();
    const url = settings.baseUrl + 'find/groups?' + new URLSearchParams({ search: query });
    const resp = await _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.makeRequest(url, { signal }, settings);
    if (!resp.ok) {
        const data = await resp.json();
        throw new _jupyterlab_services__WEBPACK_IMPORTED_MODULE_0__.ServerConnection.ResponseError(resp, data.error ?? resp.statusText);
    }
    const data = await resp.json();
    return data.items.map((g) => ({
        opaqueId: g.id.opaque_id,
        displayName: g.group_name || g.id.opaque_id
    }));
}


/***/ },

/***/ "./lib/spaces-widget.js"
/*!******************************!*\
  !*** ./lib/spaces-widget.js ***!
  \******************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   SpacesWidget: () => (/* binding */ SpacesWidget)
/* harmony export */ });
/* harmony import */ var _lumino_widgets__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! @lumino/widgets */ "webpack/sharing/consume/default/@lumino/widgets");
/* harmony import */ var _lumino_widgets__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_lumino_widgets__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _spaces__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ./spaces */ "./lib/spaces.js");


/**
 * CSS class names used by the widget.
 * All prefixed with 'swan-spaces-' to avoid collisions.
 */
const CSS = {
    panel: 'swan-spaces-panel',
    header: 'swan-spaces-header',
    headerTitle: 'swan-spaces-header-title',
    refreshBtn: 'swan-spaces-refresh-btn',
    list: 'swan-spaces-list',
    item: 'swan-spaces-item',
    itemActive: 'swan-spaces-item-active',
    itemName: 'swan-spaces-item-name',
    itemDescription: 'swan-spaces-item-description',
    itemPath: 'swan-spaces-item-path',
    loading: 'swan-spaces-loading',
    error: 'swan-spaces-error',
    empty: 'swan-spaces-empty'
};
/**
 * A sidebar widget that displays CERNBox Spaces and navigates
 * the file browser when a space is clicked.
 */
class SpacesWidget extends _lumino_widgets__WEBPACK_IMPORTED_MODULE_0__.Widget {
    _fileBrowser;
    _shell;
    _listNode;
    _spaces = [];
    constructor(fileBrowser, shell) {
        super();
        this._fileBrowser = fileBrowser;
        this._shell = shell;
        this.id = 'cernbox-spaces';
        this.title.caption = 'CERNBox Spaces';
        this.addClass(CSS.panel);
        // Build DOM structure
        const header = this._createHeader();
        this._listNode = document.createElement('div');
        this._listNode.className = CSS.list;
        this.node.appendChild(header);
        this.node.appendChild(this._listNode);
        // Load spaces on creation
        this._loadSpaces();
    }
    /**
     * Create the header with title and refresh button.
     */
    _createHeader() {
        const header = document.createElement('div');
        header.className = CSS.header;
        const title = document.createElement('h2');
        title.className = CSS.headerTitle;
        title.textContent = 'CERNBox Spaces';
        const refreshBtn = document.createElement('button');
        refreshBtn.className = CSS.refreshBtn;
        refreshBtn.title = 'Refresh spaces';
        refreshBtn.textContent = '↻';
        refreshBtn.addEventListener('click', () => {
            this._loadSpaces();
        });
        header.appendChild(title);
        header.appendChild(refreshBtn);
        return header;
    }
    /**
     * Fetch spaces and render the list.
     */
    async _loadSpaces() {
        this._listNode.innerHTML = '';
        this._showLoading();
        try {
            this._spaces = await (0,_spaces__WEBPACK_IMPORTED_MODULE_1__.fetchSpaces)();
            this._renderSpaces();
        }
        catch (err) {
            this._showError(err instanceof Error ? err.message : 'Failed to load spaces');
        }
    }
    /**
     * Render the list of spaces.
     */
    _renderSpaces() {
        this._listNode.innerHTML = '';
        if (this._spaces.length === 0) {
            const empty = document.createElement('div');
            empty.className = CSS.empty;
            empty.textContent = 'No spaces available';
            this._listNode.appendChild(empty);
            return;
        }
        for (const space of this._spaces) {
            const item = this._createSpaceItem(space);
            this._listNode.appendChild(item);
        }
    }
    /**
     * Create a DOM element for a single space.
     */
    _createSpaceItem(space) {
        const item = document.createElement('div');
        item.className = CSS.item;
        item.dataset.path = space.path;
        item.title = `Open ${space.name}\n${space.path}`;
        const name = document.createElement('div');
        name.className = CSS.itemName;
        name.textContent = space.name;
        item.appendChild(name);
        if (space.description) {
            const desc = document.createElement('div');
            desc.className = CSS.itemDescription;
            desc.textContent = space.description;
            item.appendChild(desc);
        }
        const path = document.createElement('div');
        path.className = CSS.itemPath;
        path.textContent = space.path;
        item.appendChild(path);
        item.addEventListener('click', () => {
            this._navigateToSpace(space);
        });
        return item;
    }
    /**
     * Navigate the file browser to the given space.
     */
    async _navigateToSpace(space) {
        try {
            await this._fileBrowser.model.cd(space.path);
            // Switch to the file browser tab in the sidebar
            this._shell.activateById(this._fileBrowser.id);
        }
        catch (err) {
            console.error(`[cs3org/cs3-jupyter-client:spaces] Failed to navigate to ${space.path}:`, err);
        }
    }
    _showLoading() {
        const el = document.createElement('div');
        el.className = CSS.loading;
        el.textContent = 'Loading spaces…';
        this._listNode.appendChild(el);
    }
    _showError(message) {
        this._listNode.innerHTML = '';
        const el = document.createElement('div');
        el.className = CSS.error;
        el.textContent = message;
        this._listNode.appendChild(el);
    }
}


/***/ },

/***/ "./lib/spaces.js"
/*!***********************!*\
  !*** ./lib/spaces.js ***!
  \***********************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   fetchSpaces: () => (/* binding */ fetchSpaces)
/* harmony export */ });
/**
 * Fetch the list of spaces the current user has access to.
 *
 * TODO: Replace with a real CS3/CERNBox API call.
 */
async function fetchSpaces() {
    // Simulate network latency
    await new Promise(resolve => setTimeout(resolve, 300));
    return [
        {
            id: 'atlas-analysis',
            name: 'ATLAS Analysis',
            description: 'Shared analysis workspace for the ATLAS experiment',
            path: '/project/a/atlas-analysis'
        },
        {
            id: 'cms-opendata',
            name: 'CMS Open Data',
            description: 'Public datasets and analysis scripts for CMS open data',
            path: '/project/c/cms-opendata'
        },
        {
            id: 'it-swan-dev',
            name: 'SWAN Development',
            description: 'Internal development and testing for the SWAN team',
            path: '/project/s/swan-dev'
        },
        {
            id: 'theory-lattice',
            name: 'Lattice QCD',
            path: '/project/l/lattice'
        },
        {
            id: 'alice-qgp',
            name: 'ALICE QGP Studies',
            description: 'Quark-gluon plasma analysis notebooks and shared results',
            path: '/project/a/alice-qgp'
        }
    ];
}


/***/ }

}]);
//# sourceMappingURL=lib_index_js.9cf6745e26ef7ae8f3a4.js.map