"use strict";
(self["webpackChunk_cs3org_cs3_jupyter_client"] = self["webpackChunk_cs3org_cs3_jupyter_client"] || []).push([["style_base_css"],{

/***/ "./node_modules/css-loader/dist/cjs.js!./style/base.css"
/*!**************************************************************!*\
  !*** ./node_modules/css-loader/dist/cjs.js!./style/base.css ***!
  \**************************************************************/
(module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "default": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony import */ var _node_modules_css_loader_dist_runtime_sourceMaps_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! ../node_modules/css-loader/dist/runtime/sourceMaps.js */ "./node_modules/css-loader/dist/runtime/sourceMaps.js");
/* harmony import */ var _node_modules_css_loader_dist_runtime_sourceMaps_js__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_node_modules_css_loader_dist_runtime_sourceMaps_js__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _node_modules_css_loader_dist_runtime_api_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! ../node_modules/css-loader/dist/runtime/api.js */ "./node_modules/css-loader/dist/runtime/api.js");
/* harmony import */ var _node_modules_css_loader_dist_runtime_api_js__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(_node_modules_css_loader_dist_runtime_api_js__WEBPACK_IMPORTED_MODULE_1__);
// Imports


var ___CSS_LOADER_EXPORT___ = _node_modules_css_loader_dist_runtime_api_js__WEBPACK_IMPORTED_MODULE_1___default()((_node_modules_css_loader_dist_runtime_sourceMaps_js__WEBPACK_IMPORTED_MODULE_0___default()));
// Module
___CSS_LOADER_EXPORT___.push([module.id, `/* ============================================================
 * CERNBox integration
 *
 * Uses JupyterLab CSS variables for theme compatibility.
 * ============================================================ */

.swan-spaces-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  background: var(--jp-layout-color1);
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
}

/* ── Header ────────────────────────────────────────────────── */

.swan-spaces-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  border-bottom: 1px solid var(--jp-border-color2);
  flex-shrink: 0;
}

.swan-spaces-header-title {
  margin: 0;
  font-size: var(--jp-ui-font-size2);
  font-weight: 600;
  color: var(--jp-ui-font-color0);
}

.swan-spaces-refresh-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  border: none;
  border-radius: var(--jp-border-radius);
  background: transparent;
  color: var(--jp-ui-font-color2);
  font-size: 16px;
  cursor: pointer;
  transition:
    color 0.15s,
    background 0.15s;
}

.swan-spaces-refresh-btn:hover {
  background: var(--jp-layout-color2);
  color: var(--jp-ui-font-color0);
}

/* ── List container ────────────────────────────────────────── */

.swan-spaces-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}

/* ── Space item ────────────────────────────────────────────── */

.swan-spaces-item {
  padding: 8px 12px;
  cursor: pointer;
  border-left: 3px solid transparent;
  transition:
    background 0.15s,
    border-color 0.15s;
}

.swan-spaces-item:hover {
  background: var(--jp-layout-color2);
}

.swan-spaces-item-active {
  background: var(--jp-layout-color2);
  border-left-color: var(--jp-brand-color1);
}

.swan-spaces-item-active:hover {
  background: var(--jp-layout-color3);
}

.swan-spaces-item-name {
  font-weight: 500;
  color: var(--jp-ui-font-color0);
  line-height: 1.4;
}

.swan-spaces-item-description {
  margin-top: 2px;
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color2);
  line-height: 1.4;

  /* Clamp to 2 lines */
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.swan-spaces-item-path {
  margin-top: 2px;
  font-size: 11px;
  font-family: var(--jp-code-font-family);
  color: var(--jp-ui-font-color3);
  line-height: 1.3;
}

/* ── Error state on individual items (navigation failure) ── */

.swan-spaces-item.swan-spaces-error {
  background: var(--jp-error-color3, #fdd);
}

/* ── Status messages ───────────────────────────────────────── */

.swan-spaces-loading,
.swan-spaces-empty {
  padding: 16px 12px;
  text-align: center;
  color: var(--jp-ui-font-color2);
  font-style: italic;
}

.swan-spaces-error {
  padding: 16px 12px;
  text-align: center;
  color: var(--jp-error-color1);
}

/* ============================================================
 * Shares — sidebar widget styles
 *
 * All classes prefixed with 'swan-shares-' to avoid collisions.
 * ============================================================ */

.swan-shares-panel {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
  background: var(--jp-layout-color1);
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
}

/* ── Header ────────────────────────────────────────────────── */

.swan-shares-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  border-bottom: 1px solid var(--jp-border-color2);
  flex-shrink: 0;
}

.swan-shares-header-logo {
  display: flex;
  align-items: center;
  margin-right: 6px;
  color: var(--jp-ui-font-color1);
}

.swan-shares-header-logo svg {
  height: 22px;
  width: auto;
}

.swan-shares-header-title {
  margin: 0;
  font-size: var(--jp-ui-font-size2);
  font-weight: 600;
  color: var(--jp-ui-font-color0);
  flex: 1;
}

.swan-shares-refresh-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  border: none;
  border-radius: var(--jp-border-radius);
  background: transparent;
  color: var(--jp-ui-font-color2);
  font-size: 16px;
  cursor: pointer;
  transition:
    color 0.15s,
    background 0.15s;
}

.swan-shares-refresh-btn:hover {
  background: var(--jp-layout-color2);
  color: var(--jp-ui-font-color0);
}

/* ── List container ────────────────────────────────────────── */

.swan-shares-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}

/* ── Tabs ──────────────────────────────────────────────────── */

.swan-shares-tabs {
  display: flex;
  flex-shrink: 0;
  border-bottom: 1px solid var(--jp-border-color2);
}

.swan-shares-tab {
  flex: 1;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  padding: 6px 8px;
  border: none;
  border-bottom: 2px solid transparent;
  background: transparent;
  color: var(--jp-ui-font-color2);
  font-size: var(--jp-ui-font-size0);
  font-weight: 500;
  cursor: pointer;
  transition:
    color 0.15s,
    border-color 0.15s;
}

.swan-shares-tab:hover {
  color: var(--jp-ui-font-color1);
}

.swan-shares-tab-active {
  color: var(--jp-ui-font-color0);
  border-bottom-color: var(--jp-brand-color1);
}

/* ── Filter ────────────────────────────────────────────────── */

.swan-shares-filter-container {
  flex-shrink: 0;
  padding: 6px 8px;
}

.swan-shares-filter-input {
  width: 100%;
  box-sizing: border-box;
  padding: 4px 8px;
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  background: var(--jp-layout-color0, var(--jp-layout-color1));
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
  outline: none;
  transition: border-color 0.15s;
}

.swan-shares-filter-input:focus {
  border-color: var(--jp-brand-color1);
}

/* ── Share item ────────────────────────────────────────────── */

.swan-shares-item-icon {
  display: flex;
  align-items: center;
  flex-shrink: 0;
}

.swan-shares-item {
  color: red !important;
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 2px 12px;
  cursor: pointer;
  border-left: 3px solid transparent;
  transition:
    background 0.15s,
    border-color 0.15s;
}

.swan-shares-item:hover {
  background: var(--jp-layout-color2);
}

.swan-shares-item-name {
  font-weight: 500;
  color: var(--jp-ui-font-color0);
  line-height: 1.4;
  overflow-x: hidden;
  text-overflow: ellipsis;
}

.swan-shares-item-meta {
  margin-top: 0;
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color2);
  font-style: italic;
  line-height: 1.4;
  white-space: nowrap;
  overflow-x: hidden;
  text-overflow: ellipsis;
}

/* ── Error state on individual items (navigation failure) ── */

.swan-shares-item.swan-shares-error {
  background: var(--jp-error-color3, #fdd);
}

/* ── Status messages ───────────────────────────────────────── */

.swan-shares-loading,
.swan-shares-empty {
  padding: 16px 12px;
  text-align: center;
  color: var(--jp-ui-font-color2);
  font-style: italic;
}

.swan-shares-error {
  padding: 16px 12px;
  text-align: center;
  color: var(--jp-error-color1);
}

/* ── Context menu ──────────────────────────────────────────── */

.swan-shares-context-menu {
  position: fixed;
  z-index: 10000;
  min-width: 120px;
  background: var(--jp-layout-color1);
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
  padding: 4px 0;
}

.swan-shares-context-menu-item {
  padding: 6px 16px;
  cursor: pointer;
  font-size: var(--jp-ui-font-size1);
  color: var(--jp-ui-font-color1);
  transition: background 0.1s;
}

.swan-shares-context-menu-item:hover {
  background: var(--jp-layout-color2);
}

/* ── Edit-share modal ─────────────────────────────────────── */

.swan-shares-modal-overlay {
  position: fixed;
  inset: 0;
  z-index: 10001;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.4);
}

.swan-shares-modal {
  display: flex;
  flex-direction: column;
  width: 480px;
  max-width: 90vw;
  max-height: 80vh;
  background: var(--jp-layout-color1);
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.25);
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
}

.swan-shares-modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  border-bottom: 1px solid var(--jp-border-color2);
  flex-shrink: 0;
}

.swan-shares-modal-title {
  margin: 0;
  font-size: var(--jp-ui-font-size2);
  font-weight: 600;
  color: var(--jp-ui-font-color0);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.swan-shares-modal-close-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  padding: 0;
  border: none;
  border-radius: var(--jp-border-radius);
  background: transparent;
  color: var(--jp-ui-font-color2);
  font-size: 20px;
  cursor: pointer;
  transition:
    color 0.15s,
    background 0.15s;
  flex-shrink: 0;
}

.swan-shares-modal-close-btn:hover {
  background: var(--jp-layout-color2);
  color: var(--jp-ui-font-color0);
}

.swan-shares-modal-body {
  flex: 1;
  overflow-y: auto;
  padding: 12px 16px;
}

.swan-shares-modal-section {
  margin-bottom: 16px;
}

.swan-shares-modal-section:last-child {
  margin-bottom: 0;
}

.swan-shares-modal-section-title {
  font-size: var(--jp-ui-font-size0);
  font-weight: 600;
  color: var(--jp-ui-font-color2);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 8px;
}

/* ── Grantee list ─────────────────────────────────────────── */

.swan-shares-grantee-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.swan-shares-grantee-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 8px;
  border-radius: var(--jp-border-radius);
  background: var(--jp-layout-color2);
}

.swan-shares-grantee-info {
  display: flex;
  align-items: center;
  gap: 6px;
  flex: 1;
  min-width: 0;
}

.swan-shares-grantee-icon {
  flex-shrink: 0;
  font-size: 14px;
}

.swan-shares-grantee-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* ── Custom role dropdown ──────────────────────────────────── */

.swan-shares-role-dropdown {
  position: relative;
  flex-shrink: 0;
}

.swan-shares-role-dropdown-selected {
  min-width: 76px;
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 2px 6px;
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  background: var(--jp-layout-color1);
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size0);
  cursor: pointer;
  white-space: nowrap;
  transition: border-color 0.15s;
}

.swan-shares-role-dropdown-selected:hover {
  border-color: var(--jp-brand-color1);
}

.swan-shares-role-dropdown-selected svg {
  flex-shrink: 0;
}

.swan-shares-role-dropdown-menu {
  z-index: 10001;
  min-width: 120px;
  background: var(--jp-layout-color1);
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
  overflow: hidden;
}

.swan-shares-role-dropdown-option {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 4px 8px;
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color1);
  cursor: pointer;
  white-space: nowrap;
  transition: background 0.1s;
}

.swan-shares-role-dropdown-option:hover {
  background: var(--jp-layout-color2);
}

.swan-shares-role-dropdown-option-active {
  color: var(--jp-brand-color1);
}

.swan-shares-role-dropdown-option svg {
  flex-shrink: 0;
}

.swan-shares-grantee-remove {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 24px;
  height: 24px;
  padding: 0;
  border: none;
  border-radius: var(--jp-border-radius);
  background: transparent;
  color: var(--jp-ui-font-color2);
  font-size: 16px;
  cursor: pointer;
  flex-shrink: 0;
  transition:
    color 0.15s,
    background 0.15s;
}

.swan-shares-grantee-remove:hover {
  background: var(--jp-error-color3, #fdd);
  color: var(--jp-error-color1);
}

/* ── Search ───────────────────────────────────────────────── */

.swan-shares-search-container {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.swan-shares-search-input {
  padding: 6px 10px;
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  background: var(--jp-layout-color0, var(--jp-layout-color1));
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
  outline: none;
  transition: border-color 0.15s;
}

.swan-shares-search-input:focus {
  border-color: var(--jp-brand-color1);
}

.swan-shares-search-results {
  display: flex;
  flex-direction: column;
  gap: 2px;
  max-height: 200px;
  overflow-y: auto;
}

.swan-shares-search-result-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 6px 8px;
  border-radius: var(--jp-border-radius);
  transition: background 0.1s;
}

.swan-shares-search-result-item:hover {
  background: var(--jp-layout-color2);
}

.swan-shares-search-result-info {
  flex: 1;
  min-width: 0;
}

.swan-shares-search-result-name {
  font-weight: 500;
  color: var(--jp-ui-font-color0);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.swan-shares-search-result-detail {
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.swan-shares-search-result-add {
  padding: 3px 10px;
  border: 1px solid var(--jp-brand-color1);
  border-radius: var(--jp-border-radius);
  background: transparent;
  color: var(--jp-brand-color1);
  font-size: var(--jp-ui-font-size0);
  cursor: pointer;
  flex-shrink: 0;
  transition:
    background 0.15s,
    color 0.15s;
}

.swan-shares-search-result-add:hover:not(:disabled) {
  background: var(--jp-brand-color1);
  color: var(--jp-ui-inverse-font-color0, #fff);
}

.swan-shares-search-result-add:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* ── Modal status & footer ────────────────────────────────── */

.swan-shares-modal-status {
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color2);
  min-height: 18px;
  padding: 4px 0;
}

.swan-shares-modal-empty {
  padding: 8px;
  text-align: center;
  color: var(--jp-ui-font-color2);
  font-style: italic;
  font-size: var(--jp-ui-font-size0);
}

.swan-shares-modal-footer {
  display: flex;
  justify-content: flex-end;
  padding: 10px 16px;
  border-top: 1px solid var(--jp-border-color2);
  flex-shrink: 0;
}

.swan-shares-modal-footer-btn {
  padding: 6px 20px;
  border: 1px solid var(--jp-border-color1);
  border-radius: var(--jp-border-radius);
  background: var(--jp-layout-color1);
  color: var(--jp-ui-font-color1);
  font-size: var(--jp-ui-font-size1);
  cursor: pointer;
  transition:
    background 0.15s,
    color 0.15s;
}

.swan-shares-modal-footer-btn:hover {
  background: var(--jp-layout-color2);
}

/* ============================================================
 * Storage Quota — progress bar at bottom of file browser
 * ============================================================ */

.swan-quota-container {
  flex-shrink: 0;
  padding: 8px 12px;
  border-top: 1px solid var(--jp-border-color2);
  background: var(--jp-layout-color1);
}

.swan-quota-label {
  font-size: var(--jp-ui-font-size0);
  color: var(--jp-ui-font-color2);
  margin-bottom: 4px;
}

.swan-quota-bar-outer {
  height: 6px;
  border-radius: 3px;
  background: var(--jp-layout-color3);
  overflow: hidden;
}

.swan-quota-bar-inner {
  height: 100%;
  border-radius: 3px;
  background: var(--jp-brand-color1);
  transition:
    width 0.4s ease,
    background-color 0.3s ease;
}

.swan-quota-bar-warning {
  background: var(--jp-warn-color1, #e6a117);
}

.swan-quota-bar-critical {
  background: var(--jp-error-color1, #d32f2f);
}
`, "",{"version":3,"sources":["webpack://./style/base.css"],"names":[],"mappings":"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","sourcesContent":["/* ============================================================\n * CERNBox integration\n *\n * Uses JupyterLab CSS variables for theme compatibility.\n * ============================================================ */\n\n.swan-spaces-panel {\n  display: flex;\n  flex-direction: column;\n  height: 100%;\n  overflow: hidden;\n  background: var(--jp-layout-color1);\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n}\n\n/* ── Header ────────────────────────────────────────────────── */\n\n.swan-spaces-header {\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  padding: 8px 12px;\n  border-bottom: 1px solid var(--jp-border-color2);\n  flex-shrink: 0;\n}\n\n.swan-spaces-header-title {\n  margin: 0;\n  font-size: var(--jp-ui-font-size2);\n  font-weight: 600;\n  color: var(--jp-ui-font-color0);\n}\n\n.swan-spaces-refresh-btn {\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  width: 24px;\n  height: 24px;\n  padding: 0;\n  border: none;\n  border-radius: var(--jp-border-radius);\n  background: transparent;\n  color: var(--jp-ui-font-color2);\n  font-size: 16px;\n  cursor: pointer;\n  transition:\n    color 0.15s,\n    background 0.15s;\n}\n\n.swan-spaces-refresh-btn:hover {\n  background: var(--jp-layout-color2);\n  color: var(--jp-ui-font-color0);\n}\n\n/* ── List container ────────────────────────────────────────── */\n\n.swan-spaces-list {\n  flex: 1;\n  overflow-y: auto;\n  padding: 4px 0;\n}\n\n/* ── Space item ────────────────────────────────────────────── */\n\n.swan-spaces-item {\n  padding: 8px 12px;\n  cursor: pointer;\n  border-left: 3px solid transparent;\n  transition:\n    background 0.15s,\n    border-color 0.15s;\n}\n\n.swan-spaces-item:hover {\n  background: var(--jp-layout-color2);\n}\n\n.swan-spaces-item-active {\n  background: var(--jp-layout-color2);\n  border-left-color: var(--jp-brand-color1);\n}\n\n.swan-spaces-item-active:hover {\n  background: var(--jp-layout-color3);\n}\n\n.swan-spaces-item-name {\n  font-weight: 500;\n  color: var(--jp-ui-font-color0);\n  line-height: 1.4;\n}\n\n.swan-spaces-item-description {\n  margin-top: 2px;\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color2);\n  line-height: 1.4;\n\n  /* Clamp to 2 lines */\n  display: -webkit-box;\n  -webkit-line-clamp: 2;\n  -webkit-box-orient: vertical;\n  overflow: hidden;\n}\n\n.swan-spaces-item-path {\n  margin-top: 2px;\n  font-size: 11px;\n  font-family: var(--jp-code-font-family);\n  color: var(--jp-ui-font-color3);\n  line-height: 1.3;\n}\n\n/* ── Error state on individual items (navigation failure) ── */\n\n.swan-spaces-item.swan-spaces-error {\n  background: var(--jp-error-color3, #fdd);\n}\n\n/* ── Status messages ───────────────────────────────────────── */\n\n.swan-spaces-loading,\n.swan-spaces-empty {\n  padding: 16px 12px;\n  text-align: center;\n  color: var(--jp-ui-font-color2);\n  font-style: italic;\n}\n\n.swan-spaces-error {\n  padding: 16px 12px;\n  text-align: center;\n  color: var(--jp-error-color1);\n}\n\n/* ============================================================\n * Shares — sidebar widget styles\n *\n * All classes prefixed with 'swan-shares-' to avoid collisions.\n * ============================================================ */\n\n.swan-shares-panel {\n  display: flex;\n  flex-direction: column;\n  height: 100%;\n  overflow: hidden;\n  background: var(--jp-layout-color1);\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n}\n\n/* ── Header ────────────────────────────────────────────────── */\n\n.swan-shares-header {\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  padding: 8px 12px;\n  border-bottom: 1px solid var(--jp-border-color2);\n  flex-shrink: 0;\n}\n\n.swan-shares-header-logo {\n  display: flex;\n  align-items: center;\n  margin-right: 6px;\n  color: var(--jp-ui-font-color1);\n}\n\n.swan-shares-header-logo svg {\n  height: 22px;\n  width: auto;\n}\n\n.swan-shares-header-title {\n  margin: 0;\n  font-size: var(--jp-ui-font-size2);\n  font-weight: 600;\n  color: var(--jp-ui-font-color0);\n  flex: 1;\n}\n\n.swan-shares-refresh-btn {\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  width: 24px;\n  height: 24px;\n  padding: 0;\n  border: none;\n  border-radius: var(--jp-border-radius);\n  background: transparent;\n  color: var(--jp-ui-font-color2);\n  font-size: 16px;\n  cursor: pointer;\n  transition:\n    color 0.15s,\n    background 0.15s;\n}\n\n.swan-shares-refresh-btn:hover {\n  background: var(--jp-layout-color2);\n  color: var(--jp-ui-font-color0);\n}\n\n/* ── List container ────────────────────────────────────────── */\n\n.swan-shares-list {\n  flex: 1;\n  overflow-y: auto;\n  padding: 4px 0;\n}\n\n/* ── Tabs ──────────────────────────────────────────────────── */\n\n.swan-shares-tabs {\n  display: flex;\n  flex-shrink: 0;\n  border-bottom: 1px solid var(--jp-border-color2);\n}\n\n.swan-shares-tab {\n  flex: 1;\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  gap: 12px;\n  padding: 6px 8px;\n  border: none;\n  border-bottom: 2px solid transparent;\n  background: transparent;\n  color: var(--jp-ui-font-color2);\n  font-size: var(--jp-ui-font-size0);\n  font-weight: 500;\n  cursor: pointer;\n  transition:\n    color 0.15s,\n    border-color 0.15s;\n}\n\n.swan-shares-tab:hover {\n  color: var(--jp-ui-font-color1);\n}\n\n.swan-shares-tab-active {\n  color: var(--jp-ui-font-color0);\n  border-bottom-color: var(--jp-brand-color1);\n}\n\n/* ── Filter ────────────────────────────────────────────────── */\n\n.swan-shares-filter-container {\n  flex-shrink: 0;\n  padding: 6px 8px;\n}\n\n.swan-shares-filter-input {\n  width: 100%;\n  box-sizing: border-box;\n  padding: 4px 8px;\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  background: var(--jp-layout-color0, var(--jp-layout-color1));\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n  outline: none;\n  transition: border-color 0.15s;\n}\n\n.swan-shares-filter-input:focus {\n  border-color: var(--jp-brand-color1);\n}\n\n/* ── Share item ────────────────────────────────────────────── */\n\n.swan-shares-item-icon {\n  display: flex;\n  align-items: center;\n  flex-shrink: 0;\n}\n\n.swan-shares-item {\n  color: red !important;\n  display: flex;\n  align-items: center;\n  gap: 4px;\n  padding: 2px 12px;\n  cursor: pointer;\n  border-left: 3px solid transparent;\n  transition:\n    background 0.15s,\n    border-color 0.15s;\n}\n\n.swan-shares-item:hover {\n  background: var(--jp-layout-color2);\n}\n\n.swan-shares-item-name {\n  font-weight: 500;\n  color: var(--jp-ui-font-color0);\n  line-height: 1.4;\n  overflow-x: hidden;\n  text-overflow: ellipsis;\n}\n\n.swan-shares-item-meta {\n  margin-top: 0;\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color2);\n  font-style: italic;\n  line-height: 1.4;\n  white-space: nowrap;\n  overflow-x: hidden;\n  text-overflow: ellipsis;\n}\n\n/* ── Error state on individual items (navigation failure) ── */\n\n.swan-shares-item.swan-shares-error {\n  background: var(--jp-error-color3, #fdd);\n}\n\n/* ── Status messages ───────────────────────────────────────── */\n\n.swan-shares-loading,\n.swan-shares-empty {\n  padding: 16px 12px;\n  text-align: center;\n  color: var(--jp-ui-font-color2);\n  font-style: italic;\n}\n\n.swan-shares-error {\n  padding: 16px 12px;\n  text-align: center;\n  color: var(--jp-error-color1);\n}\n\n/* ── Context menu ──────────────────────────────────────────── */\n\n.swan-shares-context-menu {\n  position: fixed;\n  z-index: 10000;\n  min-width: 120px;\n  background: var(--jp-layout-color1);\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);\n  padding: 4px 0;\n}\n\n.swan-shares-context-menu-item {\n  padding: 6px 16px;\n  cursor: pointer;\n  font-size: var(--jp-ui-font-size1);\n  color: var(--jp-ui-font-color1);\n  transition: background 0.1s;\n}\n\n.swan-shares-context-menu-item:hover {\n  background: var(--jp-layout-color2);\n}\n\n/* ── Edit-share modal ─────────────────────────────────────── */\n\n.swan-shares-modal-overlay {\n  position: fixed;\n  inset: 0;\n  z-index: 10001;\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  background: rgba(0, 0, 0, 0.4);\n}\n\n.swan-shares-modal {\n  display: flex;\n  flex-direction: column;\n  width: 480px;\n  max-width: 90vw;\n  max-height: 80vh;\n  background: var(--jp-layout-color1);\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.25);\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n}\n\n.swan-shares-modal-header {\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  padding: 12px 16px;\n  border-bottom: 1px solid var(--jp-border-color2);\n  flex-shrink: 0;\n}\n\n.swan-shares-modal-title {\n  margin: 0;\n  font-size: var(--jp-ui-font-size2);\n  font-weight: 600;\n  color: var(--jp-ui-font-color0);\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n}\n\n.swan-shares-modal-close-btn {\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  width: 28px;\n  height: 28px;\n  padding: 0;\n  border: none;\n  border-radius: var(--jp-border-radius);\n  background: transparent;\n  color: var(--jp-ui-font-color2);\n  font-size: 20px;\n  cursor: pointer;\n  transition:\n    color 0.15s,\n    background 0.15s;\n  flex-shrink: 0;\n}\n\n.swan-shares-modal-close-btn:hover {\n  background: var(--jp-layout-color2);\n  color: var(--jp-ui-font-color0);\n}\n\n.swan-shares-modal-body {\n  flex: 1;\n  overflow-y: auto;\n  padding: 12px 16px;\n}\n\n.swan-shares-modal-section {\n  margin-bottom: 16px;\n}\n\n.swan-shares-modal-section:last-child {\n  margin-bottom: 0;\n}\n\n.swan-shares-modal-section-title {\n  font-size: var(--jp-ui-font-size0);\n  font-weight: 600;\n  color: var(--jp-ui-font-color2);\n  text-transform: uppercase;\n  letter-spacing: 0.5px;\n  margin-bottom: 8px;\n}\n\n/* ── Grantee list ─────────────────────────────────────────── */\n\n.swan-shares-grantee-list {\n  display: flex;\n  flex-direction: column;\n  gap: 4px;\n}\n\n.swan-shares-grantee-item {\n  display: flex;\n  align-items: center;\n  gap: 8px;\n  padding: 6px 8px;\n  border-radius: var(--jp-border-radius);\n  background: var(--jp-layout-color2);\n}\n\n.swan-shares-grantee-info {\n  display: flex;\n  align-items: center;\n  gap: 6px;\n  flex: 1;\n  min-width: 0;\n}\n\n.swan-shares-grantee-icon {\n  flex-shrink: 0;\n  font-size: 14px;\n}\n\n.swan-shares-grantee-name {\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n}\n\n/* ── Custom role dropdown ──────────────────────────────────── */\n\n.swan-shares-role-dropdown {\n  position: relative;\n  flex-shrink: 0;\n}\n\n.swan-shares-role-dropdown-selected {\n  min-width: 76px;\n  display: flex;\n  align-items: center;\n  gap: 4px;\n  padding: 2px 6px;\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  background: var(--jp-layout-color1);\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size0);\n  cursor: pointer;\n  white-space: nowrap;\n  transition: border-color 0.15s;\n}\n\n.swan-shares-role-dropdown-selected:hover {\n  border-color: var(--jp-brand-color1);\n}\n\n.swan-shares-role-dropdown-selected svg {\n  flex-shrink: 0;\n}\n\n.swan-shares-role-dropdown-menu {\n  z-index: 10001;\n  min-width: 120px;\n  background: var(--jp-layout-color1);\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);\n  overflow: hidden;\n}\n\n.swan-shares-role-dropdown-option {\n  display: flex;\n  align-items: center;\n  gap: 4px;\n  padding: 4px 8px;\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color1);\n  cursor: pointer;\n  white-space: nowrap;\n  transition: background 0.1s;\n}\n\n.swan-shares-role-dropdown-option:hover {\n  background: var(--jp-layout-color2);\n}\n\n.swan-shares-role-dropdown-option-active {\n  color: var(--jp-brand-color1);\n}\n\n.swan-shares-role-dropdown-option svg {\n  flex-shrink: 0;\n}\n\n.swan-shares-grantee-remove {\n  display: flex;\n  align-items: center;\n  justify-content: center;\n  width: 24px;\n  height: 24px;\n  padding: 0;\n  border: none;\n  border-radius: var(--jp-border-radius);\n  background: transparent;\n  color: var(--jp-ui-font-color2);\n  font-size: 16px;\n  cursor: pointer;\n  flex-shrink: 0;\n  transition:\n    color 0.15s,\n    background 0.15s;\n}\n\n.swan-shares-grantee-remove:hover {\n  background: var(--jp-error-color3, #fdd);\n  color: var(--jp-error-color1);\n}\n\n/* ── Search ───────────────────────────────────────────────── */\n\n.swan-shares-search-container {\n  display: flex;\n  flex-direction: column;\n  gap: 6px;\n}\n\n.swan-shares-search-input {\n  padding: 6px 10px;\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  background: var(--jp-layout-color0, var(--jp-layout-color1));\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n  outline: none;\n  transition: border-color 0.15s;\n}\n\n.swan-shares-search-input:focus {\n  border-color: var(--jp-brand-color1);\n}\n\n.swan-shares-search-results {\n  display: flex;\n  flex-direction: column;\n  gap: 2px;\n  max-height: 200px;\n  overflow-y: auto;\n}\n\n.swan-shares-search-result-item {\n  display: flex;\n  align-items: center;\n  justify-content: space-between;\n  gap: 8px;\n  padding: 6px 8px;\n  border-radius: var(--jp-border-radius);\n  transition: background 0.1s;\n}\n\n.swan-shares-search-result-item:hover {\n  background: var(--jp-layout-color2);\n}\n\n.swan-shares-search-result-info {\n  flex: 1;\n  min-width: 0;\n}\n\n.swan-shares-search-result-name {\n  font-weight: 500;\n  color: var(--jp-ui-font-color0);\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n}\n\n.swan-shares-search-result-detail {\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color2);\n  overflow: hidden;\n  text-overflow: ellipsis;\n  white-space: nowrap;\n}\n\n.swan-shares-search-result-add {\n  padding: 3px 10px;\n  border: 1px solid var(--jp-brand-color1);\n  border-radius: var(--jp-border-radius);\n  background: transparent;\n  color: var(--jp-brand-color1);\n  font-size: var(--jp-ui-font-size0);\n  cursor: pointer;\n  flex-shrink: 0;\n  transition:\n    background 0.15s,\n    color 0.15s;\n}\n\n.swan-shares-search-result-add:hover:not(:disabled) {\n  background: var(--jp-brand-color1);\n  color: var(--jp-ui-inverse-font-color0, #fff);\n}\n\n.swan-shares-search-result-add:disabled {\n  opacity: 0.5;\n  cursor: not-allowed;\n}\n\n/* ── Modal status & footer ────────────────────────────────── */\n\n.swan-shares-modal-status {\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color2);\n  min-height: 18px;\n  padding: 4px 0;\n}\n\n.swan-shares-modal-empty {\n  padding: 8px;\n  text-align: center;\n  color: var(--jp-ui-font-color2);\n  font-style: italic;\n  font-size: var(--jp-ui-font-size0);\n}\n\n.swan-shares-modal-footer {\n  display: flex;\n  justify-content: flex-end;\n  padding: 10px 16px;\n  border-top: 1px solid var(--jp-border-color2);\n  flex-shrink: 0;\n}\n\n.swan-shares-modal-footer-btn {\n  padding: 6px 20px;\n  border: 1px solid var(--jp-border-color1);\n  border-radius: var(--jp-border-radius);\n  background: var(--jp-layout-color1);\n  color: var(--jp-ui-font-color1);\n  font-size: var(--jp-ui-font-size1);\n  cursor: pointer;\n  transition:\n    background 0.15s,\n    color 0.15s;\n}\n\n.swan-shares-modal-footer-btn:hover {\n  background: var(--jp-layout-color2);\n}\n\n/* ============================================================\n * Storage Quota — progress bar at bottom of file browser\n * ============================================================ */\n\n.swan-quota-container {\n  flex-shrink: 0;\n  padding: 8px 12px;\n  border-top: 1px solid var(--jp-border-color2);\n  background: var(--jp-layout-color1);\n}\n\n.swan-quota-label {\n  font-size: var(--jp-ui-font-size0);\n  color: var(--jp-ui-font-color2);\n  margin-bottom: 4px;\n}\n\n.swan-quota-bar-outer {\n  height: 6px;\n  border-radius: 3px;\n  background: var(--jp-layout-color3);\n  overflow: hidden;\n}\n\n.swan-quota-bar-inner {\n  height: 100%;\n  border-radius: 3px;\n  background: var(--jp-brand-color1);\n  transition:\n    width 0.4s ease,\n    background-color 0.3s ease;\n}\n\n.swan-quota-bar-warning {\n  background: var(--jp-warn-color1, #e6a117);\n}\n\n.swan-quota-bar-critical {\n  background: var(--jp-error-color1, #d32f2f);\n}\n"],"sourceRoot":""}]);
// Exports
/* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (___CSS_LOADER_EXPORT___);


/***/ },

/***/ "./node_modules/css-loader/dist/runtime/api.js"
/*!*****************************************************!*\
  !*** ./node_modules/css-loader/dist/runtime/api.js ***!
  \*****************************************************/
(module) {



/*
  MIT License http://www.opensource.org/licenses/mit-license.php
  Author Tobias Koppers @sokra
*/
module.exports = function (cssWithMappingToString) {
  var list = [];

  // return the list of modules as css string
  list.toString = function toString() {
    return this.map(function (item) {
      var content = "";
      var needLayer = typeof item[5] !== "undefined";
      if (item[4]) {
        content += "@supports (".concat(item[4], ") {");
      }
      if (item[2]) {
        content += "@media ".concat(item[2], " {");
      }
      if (needLayer) {
        content += "@layer".concat(item[5].length > 0 ? " ".concat(item[5]) : "", " {");
      }
      content += cssWithMappingToString(item);
      if (needLayer) {
        content += "}";
      }
      if (item[2]) {
        content += "}";
      }
      if (item[4]) {
        content += "}";
      }
      return content;
    }).join("");
  };

  // import a list of modules into the list
  list.i = function i(modules, media, dedupe, supports, layer) {
    if (typeof modules === "string") {
      modules = [[null, modules, undefined]];
    }
    var alreadyImportedModules = {};
    if (dedupe) {
      for (var k = 0; k < this.length; k++) {
        var id = this[k][0];
        if (id != null) {
          alreadyImportedModules[id] = true;
        }
      }
    }
    for (var _k = 0; _k < modules.length; _k++) {
      var item = [].concat(modules[_k]);
      if (dedupe && alreadyImportedModules[item[0]]) {
        continue;
      }
      if (typeof layer !== "undefined") {
        if (typeof item[5] === "undefined") {
          item[5] = layer;
        } else {
          item[1] = "@layer".concat(item[5].length > 0 ? " ".concat(item[5]) : "", " {").concat(item[1], "}");
          item[5] = layer;
        }
      }
      if (media) {
        if (!item[2]) {
          item[2] = media;
        } else {
          item[1] = "@media ".concat(item[2], " {").concat(item[1], "}");
          item[2] = media;
        }
      }
      if (supports) {
        if (!item[4]) {
          item[4] = "".concat(supports);
        } else {
          item[1] = "@supports (".concat(item[4], ") {").concat(item[1], "}");
          item[4] = supports;
        }
      }
      list.push(item);
    }
  };
  return list;
};

/***/ },

/***/ "./node_modules/css-loader/dist/runtime/sourceMaps.js"
/*!************************************************************!*\
  !*** ./node_modules/css-loader/dist/runtime/sourceMaps.js ***!
  \************************************************************/
(module) {



module.exports = function (item) {
  var content = item[1];
  var cssMapping = item[3];
  if (!cssMapping) {
    return content;
  }
  if (typeof btoa === "function") {
    var base64 = btoa(unescape(encodeURIComponent(JSON.stringify(cssMapping))));
    var data = "sourceMappingURL=data:application/json;charset=utf-8;base64,".concat(base64);
    var sourceMapping = "/*# ".concat(data, " */");
    return [content].concat([sourceMapping]).join("\n");
  }
  return [content].join("\n");
};

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/injectStylesIntoStyleTag.js"
/*!****************************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/injectStylesIntoStyleTag.js ***!
  \****************************************************************************/
(module) {



var stylesInDOM = [];
function getIndexByIdentifier(identifier) {
  var result = -1;
  for (var i = 0; i < stylesInDOM.length; i++) {
    if (stylesInDOM[i].identifier === identifier) {
      result = i;
      break;
    }
  }
  return result;
}
function modulesToDom(list, options) {
  var idCountMap = {};
  var identifiers = [];
  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    var id = options.base ? item[0] + options.base : item[0];
    var count = idCountMap[id] || 0;
    var identifier = "".concat(id, " ").concat(count);
    idCountMap[id] = count + 1;
    var indexByIdentifier = getIndexByIdentifier(identifier);
    var obj = {
      css: item[1],
      media: item[2],
      sourceMap: item[3],
      supports: item[4],
      layer: item[5]
    };
    if (indexByIdentifier !== -1) {
      stylesInDOM[indexByIdentifier].references++;
      stylesInDOM[indexByIdentifier].updater(obj);
    } else {
      var updater = addElementStyle(obj, options);
      options.byIndex = i;
      stylesInDOM.splice(i, 0, {
        identifier: identifier,
        updater: updater,
        references: 1
      });
    }
    identifiers.push(identifier);
  }
  return identifiers;
}
function addElementStyle(obj, options) {
  var api = options.domAPI(options);
  api.update(obj);
  var updater = function updater(newObj) {
    if (newObj) {
      if (newObj.css === obj.css && newObj.media === obj.media && newObj.sourceMap === obj.sourceMap && newObj.supports === obj.supports && newObj.layer === obj.layer) {
        return;
      }
      api.update(obj = newObj);
    } else {
      api.remove();
    }
  };
  return updater;
}
module.exports = function (list, options) {
  options = options || {};
  list = list || [];
  var lastIdentifiers = modulesToDom(list, options);
  return function update(newList) {
    newList = newList || [];
    for (var i = 0; i < lastIdentifiers.length; i++) {
      var identifier = lastIdentifiers[i];
      var index = getIndexByIdentifier(identifier);
      stylesInDOM[index].references--;
    }
    var newLastIdentifiers = modulesToDom(newList, options);
    for (var _i = 0; _i < lastIdentifiers.length; _i++) {
      var _identifier = lastIdentifiers[_i];
      var _index = getIndexByIdentifier(_identifier);
      if (stylesInDOM[_index].references === 0) {
        stylesInDOM[_index].updater();
        stylesInDOM.splice(_index, 1);
      }
    }
    lastIdentifiers = newLastIdentifiers;
  };
};

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/insertBySelector.js"
/*!********************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/insertBySelector.js ***!
  \********************************************************************/
(module) {



var memo = {};

/* istanbul ignore next  */
function getTarget(target) {
  if (typeof memo[target] === "undefined") {
    var styleTarget = document.querySelector(target);

    // Special case to return head of iframe instead of iframe itself
    if (window.HTMLIFrameElement && styleTarget instanceof window.HTMLIFrameElement) {
      try {
        // This will throw an exception if access to iframe is blocked
        // due to cross-origin restrictions
        styleTarget = styleTarget.contentDocument.head;
      } catch (e) {
        // istanbul ignore next
        styleTarget = null;
      }
    }
    memo[target] = styleTarget;
  }
  return memo[target];
}

/* istanbul ignore next  */
function insertBySelector(insert, style) {
  var target = getTarget(insert);
  if (!target) {
    throw new Error("Couldn't find a style target. This probably means that the value for the 'insert' parameter is invalid.");
  }
  target.appendChild(style);
}
module.exports = insertBySelector;

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/insertStyleElement.js"
/*!**********************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/insertStyleElement.js ***!
  \**********************************************************************/
(module) {



/* istanbul ignore next  */
function insertStyleElement(options) {
  var element = document.createElement("style");
  options.setAttributes(element, options.attributes);
  options.insert(element, options.options);
  return element;
}
module.exports = insertStyleElement;

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/setAttributesWithoutAttributes.js"
/*!**********************************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/setAttributesWithoutAttributes.js ***!
  \**********************************************************************************/
(module, __unused_webpack_exports, __webpack_require__) {



/* istanbul ignore next  */
function setAttributesWithoutAttributes(styleElement) {
  var nonce =  true ? __webpack_require__.nc : 0;
  if (nonce) {
    styleElement.setAttribute("nonce", nonce);
  }
}
module.exports = setAttributesWithoutAttributes;

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/styleDomAPI.js"
/*!***************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/styleDomAPI.js ***!
  \***************************************************************/
(module) {



/* istanbul ignore next  */
function apply(styleElement, options, obj) {
  var css = "";
  if (obj.supports) {
    css += "@supports (".concat(obj.supports, ") {");
  }
  if (obj.media) {
    css += "@media ".concat(obj.media, " {");
  }
  var needLayer = typeof obj.layer !== "undefined";
  if (needLayer) {
    css += "@layer".concat(obj.layer.length > 0 ? " ".concat(obj.layer) : "", " {");
  }
  css += obj.css;
  if (needLayer) {
    css += "}";
  }
  if (obj.media) {
    css += "}";
  }
  if (obj.supports) {
    css += "}";
  }
  var sourceMap = obj.sourceMap;
  if (sourceMap && typeof btoa !== "undefined") {
    css += "\n/*# sourceMappingURL=data:application/json;base64,".concat(btoa(unescape(encodeURIComponent(JSON.stringify(sourceMap)))), " */");
  }

  // For old IE
  /* istanbul ignore if  */
  options.styleTagTransform(css, styleElement, options.options);
}
function removeStyleElement(styleElement) {
  // istanbul ignore if
  if (styleElement.parentNode === null) {
    return false;
  }
  styleElement.parentNode.removeChild(styleElement);
}

/* istanbul ignore next  */
function domAPI(options) {
  if (typeof document === "undefined") {
    return {
      update: function update() {},
      remove: function remove() {}
    };
  }
  var styleElement = options.insertStyleElement(options);
  return {
    update: function update(obj) {
      apply(styleElement, options, obj);
    },
    remove: function remove() {
      removeStyleElement(styleElement);
    }
  };
}
module.exports = domAPI;

/***/ },

/***/ "./node_modules/style-loader/dist/runtime/styleTagTransform.js"
/*!*********************************************************************!*\
  !*** ./node_modules/style-loader/dist/runtime/styleTagTransform.js ***!
  \*********************************************************************/
(module) {



/* istanbul ignore next  */
function styleTagTransform(css, styleElement) {
  if (styleElement.styleSheet) {
    styleElement.styleSheet.cssText = css;
  } else {
    while (styleElement.firstChild) {
      styleElement.removeChild(styleElement.firstChild);
    }
    styleElement.appendChild(document.createTextNode(css));
  }
}
module.exports = styleTagTransform;

/***/ },

/***/ "./style/base.css"
/*!************************!*\
  !*** ./style/base.css ***!
  \************************/
(__unused_webpack_module, __webpack_exports__, __webpack_require__) {

__webpack_require__.r(__webpack_exports__);
/* harmony export */ __webpack_require__.d(__webpack_exports__, {
/* harmony export */   "default": () => (__WEBPACK_DEFAULT_EXPORT__)
/* harmony export */ });
/* harmony import */ var _node_modules_style_loader_dist_runtime_injectStylesIntoStyleTag_js__WEBPACK_IMPORTED_MODULE_0__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/injectStylesIntoStyleTag.js */ "./node_modules/style-loader/dist/runtime/injectStylesIntoStyleTag.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_injectStylesIntoStyleTag_js__WEBPACK_IMPORTED_MODULE_0___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_injectStylesIntoStyleTag_js__WEBPACK_IMPORTED_MODULE_0__);
/* harmony import */ var _node_modules_style_loader_dist_runtime_styleDomAPI_js__WEBPACK_IMPORTED_MODULE_1__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/styleDomAPI.js */ "./node_modules/style-loader/dist/runtime/styleDomAPI.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_styleDomAPI_js__WEBPACK_IMPORTED_MODULE_1___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_styleDomAPI_js__WEBPACK_IMPORTED_MODULE_1__);
/* harmony import */ var _node_modules_style_loader_dist_runtime_insertBySelector_js__WEBPACK_IMPORTED_MODULE_2__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/insertBySelector.js */ "./node_modules/style-loader/dist/runtime/insertBySelector.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_insertBySelector_js__WEBPACK_IMPORTED_MODULE_2___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_insertBySelector_js__WEBPACK_IMPORTED_MODULE_2__);
/* harmony import */ var _node_modules_style_loader_dist_runtime_setAttributesWithoutAttributes_js__WEBPACK_IMPORTED_MODULE_3__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/setAttributesWithoutAttributes.js */ "./node_modules/style-loader/dist/runtime/setAttributesWithoutAttributes.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_setAttributesWithoutAttributes_js__WEBPACK_IMPORTED_MODULE_3___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_setAttributesWithoutAttributes_js__WEBPACK_IMPORTED_MODULE_3__);
/* harmony import */ var _node_modules_style_loader_dist_runtime_insertStyleElement_js__WEBPACK_IMPORTED_MODULE_4__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/insertStyleElement.js */ "./node_modules/style-loader/dist/runtime/insertStyleElement.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_insertStyleElement_js__WEBPACK_IMPORTED_MODULE_4___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_insertStyleElement_js__WEBPACK_IMPORTED_MODULE_4__);
/* harmony import */ var _node_modules_style_loader_dist_runtime_styleTagTransform_js__WEBPACK_IMPORTED_MODULE_5__ = __webpack_require__(/*! !../node_modules/style-loader/dist/runtime/styleTagTransform.js */ "./node_modules/style-loader/dist/runtime/styleTagTransform.js");
/* harmony import */ var _node_modules_style_loader_dist_runtime_styleTagTransform_js__WEBPACK_IMPORTED_MODULE_5___default = /*#__PURE__*/__webpack_require__.n(_node_modules_style_loader_dist_runtime_styleTagTransform_js__WEBPACK_IMPORTED_MODULE_5__);
/* harmony import */ var _node_modules_css_loader_dist_cjs_js_base_css__WEBPACK_IMPORTED_MODULE_6__ = __webpack_require__(/*! !!../node_modules/css-loader/dist/cjs.js!./base.css */ "./node_modules/css-loader/dist/cjs.js!./style/base.css");

      
      
      
      
      
      
      
      
      

var options = {};

options.styleTagTransform = (_node_modules_style_loader_dist_runtime_styleTagTransform_js__WEBPACK_IMPORTED_MODULE_5___default());
options.setAttributes = (_node_modules_style_loader_dist_runtime_setAttributesWithoutAttributes_js__WEBPACK_IMPORTED_MODULE_3___default());

      options.insert = _node_modules_style_loader_dist_runtime_insertBySelector_js__WEBPACK_IMPORTED_MODULE_2___default().bind(null, "head");
    
options.domAPI = (_node_modules_style_loader_dist_runtime_styleDomAPI_js__WEBPACK_IMPORTED_MODULE_1___default());
options.insertStyleElement = (_node_modules_style_loader_dist_runtime_insertStyleElement_js__WEBPACK_IMPORTED_MODULE_4___default());

var update = _node_modules_style_loader_dist_runtime_injectStylesIntoStyleTag_js__WEBPACK_IMPORTED_MODULE_0___default()(_node_modules_css_loader_dist_cjs_js_base_css__WEBPACK_IMPORTED_MODULE_6__["default"], options);




       /* harmony default export */ const __WEBPACK_DEFAULT_EXPORT__ = (_node_modules_css_loader_dist_cjs_js_base_css__WEBPACK_IMPORTED_MODULE_6__["default"] && _node_modules_css_loader_dist_cjs_js_base_css__WEBPACK_IMPORTED_MODULE_6__["default"].locals ? _node_modules_css_loader_dist_cjs_js_base_css__WEBPACK_IMPORTED_MODULE_6__["default"].locals : undefined);


/***/ }

}]);
//# sourceMappingURL=style_base_css.58dca47475775697bd47.js.map