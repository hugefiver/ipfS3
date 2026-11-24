import { deploymentDefaults } from "./config.js";
import { decryptEnvelope, encryptEnvelope, estimateEncryptedSize } from "./crypto.js";
import { publish } from "./publish.mjs";
import { persistSettings, readSettings, validateSettings } from "./settings.js";
import {
  decodeShare,
  directoryListUrl,
  encodeShare,
  formatBytes,
  ipfsUrl,
  objectUrlFromList,
  probePublicUrl,
} from "./share.js";

const $ = (id) => document.getElementById(id);
const utf8Encoder = new TextEncoder();
const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });
const MAX_TEXT_PREVIEW_BYTES = 2 * 1024 * 1024;
const SAFE_IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/webp", "image/gif"]);
const TEXT_MIME_TYPES = new Set([
  "application/json",
  "application/xml",
  "application/javascript",
  "application/ecmascript",
  "application/x-yaml",
  "application/yaml",
  "application/toml",
]);

const publishSection = $("publish");
const publishForm = $("publish-form");
const publishButton = $("publish-button");
const publishStatus = $("publish-status");
const settingsForm = $("settings-form");
const settingsStatus = $("settings-status");
const resultSection = $("result");
const workbench = document.querySelector(".workbench");
const credentialsDetails = $("credentials-details");
const authBadge = $("auth-badge");
const fileDropzone = $("file-dropzone");
const fileInput = $("file-content");
const encryptToggle = $("encrypt-toggle");
const encryptBadge = $("encrypt-badge");

let settings;
let currentResult;
let currentSelection;
let activeShareButton;
let activeSharedSelection;
let droppedFile = null;
let nameAuto = true;
let cachedCiphertext = null;
let decryptedItem = null;
let decryptRunId = 0;

function setStatus(element, message, state = "") {
  element.textContent = message;
  if (state) element.dataset.state = state;
  else delete element.dataset.state;
}

function setSecretVisibility(input, button, visible) {
  input.type = visible ? "text" : "password";
  button.textContent = visible ? "Hide" : "Show";
  button.setAttribute("aria-pressed", visible ? "true" : "false");
}

function bindSecretVisibility(input, button) {
  button.addEventListener("click", () => {
    setSecretVisibility(input, button, input.type === "password");
  });
}

bindSecretVisibility($("encrypt-passphrase"), $("toggle-encrypt-passphrase"));
bindSecretVisibility($("result-secret"), $("toggle-result-secret"));
bindSecretVisibility($("decrypt-secret"), $("toggle-decrypt-secret"));

function isEncryptionEnabled() {
  return publishForm.elements.mode.value !== "directory" && encryptToggle.checked;
}

function selectedKdf() {
  return publishForm.elements.keySource.value === "passphrase" ? "passphrase" : "raw";
}

function updateEncryptionUi() {
  const isZip = publishForm.elements.mode.value === "directory";
  encryptToggle.disabled = isZip;
  const enabled = !isZip && encryptToggle.checked;
  $("public-warning").hidden = enabled;
  $("encrypt-zip-note").hidden = !isZip;
  $("encryption-options").hidden = !enabled;
  $("passphrase-field").hidden = !enabled || selectedKdf() !== "passphrase";
  if (isZip) {
    encryptBadge.textContent = "Unavailable for ZIP";
    delete encryptBadge.dataset.state;
  } else if (enabled) {
    encryptBadge.textContent = "Encrypted";
    encryptBadge.dataset.state = "signed";
  } else {
    encryptBadge.textContent = "Plaintext";
    delete encryptBadge.dataset.state;
  }
  updateSizeMeter();
}

function clearCredentials() {
  $("access-key").value = "";
  $("secret-key").value = "";
  $("session-token").value = "";
  updateAuthBadge();
}

function isDefaultSettings(current) {
  return (
    current.endpoint === deploymentDefaults.endpoint &&
    current.bucket === deploymentDefaults.bucket &&
    current.region === deploymentDefaults.region &&
    current.maxUploadBytes === deploymentDefaults.maxUploadBytes &&
    current.ipfsGateway === deploymentDefaults.ipfsGateway
  );
}

function updateMaxUploadReadable() {
  const value = Number($("max-upload").value);
  $("max-upload-readable").textContent =
    Number.isSafeInteger(value) && value > 0 ? `(${formatBytes(value)})` : "";
}

function fillSettings() {
  $("endpoint").value = settings.endpoint;
  $("bucket").value = settings.bucket;
  $("region").value = settings.region;
  $("max-upload").value = settings.maxUploadBytes;
  $("max-upload").max = deploymentDefaults.maxUploadBytes;
  $("ipfs-gateway").value = settings.ipfsGateway;
  const host = new URL(settings.endpoint).host;
  $("instance-summary").textContent = `${host} / ${settings.bucket}`;
  const custom = !isDefaultSettings(settings);
  const badge = $("instance-badge");
  badge.textContent = custom ? "Custom" : "Default";
  if (custom) badge.dataset.state = "custom";
  else delete badge.dataset.state;
  updateMaxUploadReadable();
  updateSizeMeter();
}

function readSettingsForm() {
  return {
    endpoint: $("endpoint").value,
    bucket: $("bucket").value,
    region: $("region").value,
    maxUploadBytes: Number($("max-upload").value),
    ipfsGateway: $("ipfs-gateway").value,
    inbrowserGateway: settings.inbrowserGateway,
  };
}

function saveSettings(next) {
  const valid = persistSettings(window.localStorage, next, deploymentDefaults);
  if (valid.endpoint !== settings.endpoint) clearCredentials();
  settings = valid;
  fillSettings();
  setStatus(settingsStatus, "Saved.", "success");
  if (currentResult) renderResult(currentResult);
}

try {
  settings = readSettings(window.localStorage, deploymentDefaults);
} catch (error) {
  settings = validateSettings(deploymentDefaults, deploymentDefaults.maxUploadBytes);
  $("page-alert").hidden = false;
  $("page-alert").textContent = `Saved settings were not loaded: ${error.message}`;
}
fillSettings();

$("max-upload").addEventListener("input", updateMaxUploadReadable);

settingsForm.addEventListener("submit", (event) => {
  event.preventDefault();
  try {
    saveSettings(readSettingsForm());
  } catch (error) {
    setStatus(settingsStatus, error.message, "error");
  }
});

$("reset-settings").addEventListener("click", () => {
  try {
    clearCredentials();
    saveSettings(deploymentDefaults);
    setStatus(settingsStatus, "Defaults restored.", "success");
  } catch (error) {
    setStatus(settingsStatus, error.message, "error");
  }
});

function currentFile() {
  return droppedFile ?? fileInput.files[0] ?? null;
}

function updateAuthBadge() {
  const mode = publishForm.elements.mode.value;
  const ak = $("access-key").value;
  const sk = $("secret-key").value;
  const st = $("session-token").value;
  const hasAny = Boolean(ak || sk || st);
  const hasPair = Boolean(ak && sk);
  if (hasPair) {
    authBadge.textContent = "Signed";
    authBadge.dataset.state = "signed";
  } else if (hasAny) {
    authBadge.textContent = "Incomplete";
    authBadge.dataset.state = "required";
  } else if (mode === "directory") {
    authBadge.textContent = "Required for ZIP";
    authBadge.dataset.state = "required";
  } else {
    authBadge.textContent = "Anonymous";
    delete authBadge.dataset.state;
  }
}

for (const id of ["access-key", "secret-key", "session-token"]) {
  $(id).addEventListener("input", updateAuthBadge);
}

function updateSizeMeter() {
  if (!settings) return;
  const mode = publishForm.elements.mode.value;
  let bytes = 0;
  let type = "application/octet-stream";
  let kind = "file";
  if (mode === "text") {
    bytes = utf8Encoder.encode($("text-content").value).byteLength;
    type = "text/plain;charset=utf-8";
    kind = "text";
  } else {
    const file = currentFile();
    bytes = file?.size ?? 0;
    type = file?.type || "application/octet-stream";
  }
  const uploadBytes = isEncryptionEnabled()
    ? estimateEncryptedSize(bytes, {
        name: $("content-name").value.trim() || "paste.txt",
        type,
        kind,
        kdf: selectedKdf(),
      })
    : bytes;
  const over = uploadBytes > settings.maxUploadBytes;
  setStatus(
    $("size-meter"),
    `${formatBytes(uploadBytes)} / ${formatBytes(settings.maxUploadBytes)}`,
    over ? "error" : "",
  );
}

function setSelectedFile(file) {
  droppedFile = file;
  if (file) {
    $("file-selected").textContent = `${file.name} (${formatBytes(file.size)})`;
    if (nameAuto || !$("content-name").value.trim()) {
      $("content-name").value = file.name;
      nameAuto = true;
    }
  } else {
    $("file-selected").textContent = "Drop a file here or click to browse.";
  }
  updateSizeMeter();
}

function updateMode() {
  const selected = publishForm.elements.mode.value;
  const isText = selected === "text";
  $("text-panel").hidden = !isText;
  $("file-panel").hidden = isText;
  fileInput.accept = selected === "directory" ? ".zip,application/zip" : "";
  if (isText && nameAuto) {
    $("content-name").value = "paste.txt";
  } else if (!isText && currentFile() && nameAuto) {
    $("content-name").value = currentFile().name;
  }
  if (selected === "directory" && !($("access-key").value && $("secret-key").value)) {
    credentialsDetails.open = true;
  }
  updateAuthBadge();
  updateEncryptionUi();
}

for (const input of publishForm.elements.mode) {
  input.addEventListener("change", updateMode);
}

encryptToggle.addEventListener("change", updateEncryptionUi);
for (const input of publishForm.elements.keySource) {
  input.addEventListener("change", () => {
    updateEncryptionUi();
    if (selectedKdf() === "passphrase" && isEncryptionEnabled()) {
      $("encrypt-passphrase").focus();
    }
  });
}

$("content-name").addEventListener("input", () => {
  nameAuto = false;
  updateSizeMeter();
});

$("text-content").addEventListener("input", updateSizeMeter);
$("text-content").addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
    event.preventDefault();
    publishForm.requestSubmit();
  }
});

fileInput.addEventListener("change", () => {
  const file = fileInput.files[0] ?? null;
  droppedFile = null;
  if (file) {
    $("file-selected").textContent = `${file.name} (${formatBytes(file.size)})`;
    if (nameAuto || !$("content-name").value.trim()) {
      $("content-name").value = file.name;
      nameAuto = true;
    }
  } else {
    $("file-selected").textContent = "Drop a file here or click to browse.";
  }
  updateSizeMeter();
});

function setupDragDrop() {
  let depth = 0;
  const activeTarget = () => ($("file-panel").hidden ? publishSection : fileDropzone);
  const clearHighlight = () => {
    publishSection.classList.remove("drag-over");
    fileDropzone.classList.remove("drag-over");
  };
  publishSection.addEventListener("dragenter", (event) => {
    if (!event.dataTransfer?.types?.includes("Files")) return;
    event.preventDefault();
    depth += 1;
    clearHighlight();
    activeTarget().classList.add("drag-over");
  });
  publishSection.addEventListener("dragover", (event) => {
    if (!event.dataTransfer?.types?.includes("Files")) return;
    event.preventDefault();
    event.dataTransfer.dropEffect = "copy";
  });
  publishSection.addEventListener("dragleave", () => {
    depth = Math.max(0, depth - 1);
    if (depth === 0) clearHighlight();
  });
  publishSection.addEventListener("drop", (event) => {
    event.preventDefault();
    depth = 0;
    clearHighlight();
    const file = event.dataTransfer?.files?.[0];
    if (!file) return;
    if (publishForm.elements.mode.value === "text") {
      publishForm.elements.mode.value = "file";
    }
    fileInput.value = "";
    nameAuto = true;
    setSelectedFile(file);
    updateMode();
  });
}

setupDragDrop();
updateMode();

function selectedBody() {
  const mode = publishForm.elements.mode.value;
  if (mode === "text") {
    const value = $("text-content").value;
    return { mode: "file", body: new Blob([value], { type: "text/plain;charset=utf-8" }) };
  }
  const file = currentFile();
  if (!file) throw new Error(mode === "directory" ? "Choose a ZIP file." : "Choose a file.");
  return { mode, body: file };
}

function credentials() {
  const accessKeyId = $("access-key").value;
  const secretAccessKey = $("secret-key").value;
  const sessionToken = $("session-token").value;
  if (!accessKeyId && !secretAccessKey && !sessionToken) return undefined;
  const value = { accessKeyId, secretAccessKey };
  if (sessionToken) value.sessionToken = sessionToken;
  return value;
}

function buildEncryptedShareUrl(selection) {
  const url = new URL(window.location.origin + window.location.pathname);
  url.hash = encodeShare(selection);
  return url.href;
}

function encryptedSelectionFor(result, backend, url) {
  const selection = {
    kind: "encrypted",
    backend,
    url,
    kdf: result.encrypted.kdf,
  };
  if (result.encrypted.includeSecretInUrl) {
    selection.secret = result.encrypted.secret;
  }
  return selection;
}

function showShare(selection, triggerButton = null) {
  if (activeShareButton) activeShareButton.setAttribute("aria-pressed", "false");
  activeShareButton = triggerButton;
  if (activeShareButton) activeShareButton.setAttribute("aria-pressed", "true");

  currentSelection = selection;
  if (selection.kind === "encrypted") {
    $("share-url").value = buildEncryptedShareUrl(selection);
    $("share-selection-label").textContent = `${selection.backend} · ${selection.secret ? "encrypted (key in URL)" : "encrypted (manual key)"}`;
    $("copy-share").disabled = false;
    setStatus(
      $("copy-status"),
      selection.secret
        ? "Encrypted share link ready (key stays in #fragment only, never sent to the server)."
        : "Encrypted share link ready (key omitted from URL; send the key separately).",
      "success",
    );
    return;
  }

  const direct = selection.backend !== "s3" || (selection.kind === "file" && selection.mode === "preview");
  const url = new URL(window.location.origin + window.location.pathname);
  url.hash = direct ? "" : encodeShare(selection);
  $("share-url").value = direct ? selection.url : url.href;
  $("share-selection-label").textContent = `${selection.backend} · ${selection.mode}`;
  $("copy-share").disabled = false;
  setStatus(
    $("copy-status"),
    direct ? "Direct link ready to copy." : "Share fragment ready; contains only this selected target.",
    "success",
  );
}

function addEncryptedShareAction(container, result, backend, url, label, { selectDefault = false } = {}) {
  const wrapper = document.createElement("div");
  wrapper.className = "link-actions";
  const open = document.createElement("a");
  open.dataset.encryptedBackend = backend;
  open.dataset.encryptedTarget = url;
  open.href = buildEncryptedShareUrl(encryptedSelectionFor(result, backend, url));
  open.target = "_blank";
  open.rel = "noopener noreferrer";
  open.textContent = label;
  const share = document.createElement("button");
  share.type = "button";
  share.className = "button secondary";
  share.textContent = "Share";
  share.setAttribute("aria-pressed", "false");
  share.addEventListener("click", () => {
    showShare(encryptedSelectionFor(result, backend, url), share);
  });
  wrapper.append(open, share);
  container.append(wrapper);
  if (selectDefault) showShare(encryptedSelectionFor(result, backend, url), share);
}

async function downloadObject(url, name, status) {
  try {
    const response = await fetch(url, {
      credentials: "omit",
      redirect: "error",
      cache: "no-store",
      referrerPolicy: "no-referrer",
      headers: { "X-IPFS3-Download-Gateway": "proxy" },
    });
    if (!response.ok) throw new Error(`Raw download returned HTTP ${response.status}.`);
    const objectUrl = URL.createObjectURL(await response.blob());
    const download = document.createElement("a");
    download.href = objectUrl;
    download.download = name;
    download.click();
    setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
    setStatus(status, "Download started.", "success");
  } catch (error) {
    setStatus(status, `Raw download failed: ${error.message}`, "error");
  }
}

function addLink(container, { url, label, name, backend, mode, kind, rootStatus }, { selectDefault = false } = {}) {
  if (!(mode === "raw" && backend === "s3") && new URL(url).origin === window.location.origin) {
    const warning = document.createElement("p");
    warning.className = "hint";
    warning.textContent = "Navigation is disabled on the publisher origin; use an isolated IPFS gateway.";
    container.append(warning);
    return;
  }
  const wrapper = document.createElement("div");
  wrapper.className = "link-actions";
  const blobDownload = mode === "raw" && backend === "s3";
  const action = document.createElement(blobDownload ? "button" : "a");
  action.textContent = label;
  const status = document.createElement("span");
  status.role = "status";
  if (blobDownload) {
    action.type = "button";
    action.className = "button secondary";
    action.addEventListener("click", async () => {
      if (action.getAttribute("aria-busy") === "true") return;
      action.setAttribute("aria-busy", "true");
      setStatus(status, "Fetching raw object…");
      await downloadObject(url, name, status);
      action.removeAttribute("aria-busy");
    });
  } else {
    action.href = url;
    action.target = "_blank";
    action.rel = "noopener noreferrer";
  }
  wrapper.append(action);
  const share = document.createElement("button");
  share.type = "button";
  share.className = "button secondary";
  share.textContent = "Share";
  share.setAttribute("aria-pressed", "false");
  const selection = { kind, name, backend, mode, url, rootStatus };
  share.addEventListener("click", () => showShare(selection, share));
  wrapper.append(share, status);
  container.append(wrapper);
  if (selectDefault) showShare(selection, share);
}

function resultLinkOptions(result, backend, mode, url, label, name = result.name) {
  return { url, label, name, backend, mode, kind: result.kind, rootStatus: result.rootStatus };
}

function renderFailures(failures) {
  const rows = $("failure-rows");
  rows.replaceChildren();
  for (const failure of failures ?? []) {
    const row = document.createElement("tr");
    for (const value of [failure.name, failure.code, failure.message]) {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.append(cell);
    }
    rows.append(row);
  }
  $("failures-section").hidden = !failures?.length;
}

function directoryPrefixFor(key) {
  return key.slice(0, key.lastIndexOf("/") + 1) + "files/";
}

function renderEntries(result) {
  const rows = $("entry-rows");
  rows.replaceChildren();
  for (const entry of result.entries ?? []) {
    const row = document.createElement("tr");
    row.dataset.entryKey = entry.key;
    const name = entry.key.slice(directoryPrefixFor(result.key).length) || entry.key;
    const nameCell = document.createElement("td");
    nameCell.textContent = name;
    const sizeCell = document.createElement("td");
    sizeCell.textContent = formatBytes(entry.size);
    const ipfsCell = document.createElement("td");
    const gateway = settings.ipfsGateway;
    const preview = ipfsUrl(gateway, entry.cid, { name });
    const raw = ipfsUrl(gateway, entry.cid, { name, download: true });
    addLink(ipfsCell, resultLinkOptions({ ...result, kind: "file" }, "ipfs", "preview", preview, "Preview", name));
    addLink(ipfsCell, resultLinkOptions({ ...result, kind: "file" }, "ipfs", "raw", raw, "Raw", name));
    const s3Cell = document.createElement("td");
    const check = document.createElement("button");
    check.type = "button";
    check.className = "button secondary compact";
    check.textContent = "Check";
    check.addEventListener("click", () => checkPublicEntry(result, entry, s3Cell, check));
    s3Cell.append(check);
    row.append(nameCell, sizeCell, ipfsCell, s3Cell);
    rows.append(row);
  }
  $("entries-section").hidden = !result.entries?.length;
}

function renderResult(result) {
  currentResult = result;
  currentSelection = undefined;
  activeShareButton = null;
  resultSection.hidden = false;
  const isEncrypted = Boolean(result.encrypted);
  const kindBadge = $("result-kind");
  if (isEncrypted) {
    kindBadge.textContent = "Encrypted";
    kindBadge.dataset.state = "signed";
  } else {
    kindBadge.textContent = result.kind === "directory" ? "ZIP directory" : "File";
    delete kindBadge.dataset.state;
  }
  $("result-name").textContent = isEncrypted ? result.encrypted.originalName : result.name;
  $("result-key").textContent = result.key;
  $("result-cid").textContent = result.cid;
  $("copy-cid").textContent = "Copy CID";
  $("partial-notice").hidden = result.rootStatus !== "partial";
  $("partial-notice").textContent = "This directory is partial. Failed entries are listed below; the root still contains only successful outputs.";
  $("share-url").value = "";
  $("share-selection-label").textContent = "";
  $("copy-share").disabled = true;
  setStatus($("copy-status"), "");
  setStatus($("public-status"), "Not checked. No anonymous request has been sent.");
  $("s3-links").replaceChildren();
  $("check-public").disabled = false;
  $("gateway-label").textContent = settings.ipfsGateway;

  const secretPanel = $("result-secret-panel");
  if (isEncrypted) {
    secretPanel.hidden = false;
    const isPassphrase = result.encrypted.kdf === "passphrase";
    $("result-secret-label").textContent = isPassphrase ? "Passphrase" : "Decryption key";
    $("copy-secret").textContent = isPassphrase ? "Copy passphrase" : "Copy key";
    $("result-secret").value = result.encrypted.secret;
    setSecretVisibility($("result-secret"), $("toggle-result-secret"), false);
    $("share-include-secret").checked = result.encrypted.includeSecretInUrl;
    setStatus($("secret-status"), "");
  } else {
    secretPanel.hidden = true;
    $("result-secret").value = "";
    setStatus($("secret-status"), "");
  }

  const inbrowserLinks = $("inbrowser-links");
  const gatewayLinks = $("gateway-links");
  inbrowserLinks.replaceChildren();
  gatewayLinks.replaceChildren();

  if (isEncrypted) {
    const gatewayFetchUrl = ipfsUrl(settings.ipfsGateway, result.cid);
    addEncryptedShareAction(
      gatewayLinks,
      result,
      "ipfs",
      gatewayFetchUrl,
      "Open decoder",
      { selectDefault: true },
    );
    const gatewayRaw = ipfsUrl(settings.ipfsGateway, result.cid, { name: "encrypted.bin", download: true });
    addLink(
      gatewayLinks,
      resultLinkOptions(result, "ipfs", "raw", gatewayRaw, "Download ciphertext (.bin)", "encrypted.bin"),
    );
    const inbrowserRaw = ipfsUrl(settings.inbrowserGateway, result.cid, { name: "encrypted.bin", download: true });
    addLink(
      inbrowserLinks,
      resultLinkOptions(result, "inbrowser", "raw", inbrowserRaw, "Download ciphertext (.bin)", "encrypted.bin"),
    );
    const note = document.createElement("p");
    note.className = "hint";
    note.textContent = "inbrowser.link serves raw ciphertext only.";
    inbrowserLinks.append(note);
    renderEntries({ entries: [] });
    renderFailures([]);
    return;
  }

  const directory = result.kind === "directory";
  const inbrowser = ipfsUrl(settings.inbrowserGateway, result.cid, { name: result.name, directory });
  addLink(inbrowserLinks, resultLinkOptions(result, "inbrowser", "preview", inbrowser, directory ? "Open directory" : "Open preview"));
  const preview = ipfsUrl(settings.ipfsGateway, result.cid, { name: result.name, directory });
  addLink(
    gatewayLinks,
    resultLinkOptions(result, "ipfs", "preview", preview, directory ? "Preview directory" : "Preview file"),
    { selectDefault: true },
  );
  if (!directory || result.archiveCid) {
    const rawCid = directory ? result.archiveCid : result.cid;
    const label = directory ? "Download original ZIP" : "Download raw file";
    const raw = ipfsUrl(settings.ipfsGateway, rawCid, { name: result.name, download: true });
    addLink(gatewayLinks, resultLinkOptions(result, "ipfs", "raw", raw, label));
    const inbrowserRaw = ipfsUrl(settings.inbrowserGateway, rawCid, { name: result.name, download: true });
    addLink(inbrowserLinks, resultLinkOptions(result, "inbrowser", "raw", inbrowserRaw, label));
  }
  renderEntries(result);
  renderFailures(result.failures);
}

$("share-include-secret").addEventListener("change", () => {
  if (!currentResult?.encrypted) return;
  currentResult.encrypted.includeSecretInUrl = $("share-include-secret").checked;
  for (const link of resultSection.querySelectorAll("a[data-encrypted-target]")) {
    const backend = link.dataset.encryptedBackend;
    const target = link.dataset.encryptedTarget;
    link.href = buildEncryptedShareUrl(encryptedSelectionFor(currentResult, backend, target));
  }
  if (currentSelection?.kind === "encrypted") {
    showShare(
      encryptedSelectionFor(currentResult, currentSelection.backend, currentSelection.url),
      activeShareButton,
    );
  }
});

$("copy-secret").addEventListener("click", async () => {
  const secret = currentResult?.encrypted?.secret;
  if (!secret) return;
  const label = currentResult.encrypted.kdf === "passphrase" ? "Passphrase" : "Decryption key";
  try {
    await navigator.clipboard.writeText(secret);
    setStatus($("secret-status"), `${label} copied to clipboard.`, "success");
  } catch {
    setSecretVisibility($("result-secret"), $("toggle-result-secret"), true);
    $("result-secret").focus();
    $("result-secret").select();
    setStatus($("secret-status"), "Clipboard access was denied; the secret is selected for manual copying.", "error");
  }
});

function validatePlainName(name) {
  if (!name || /^\.{1,2}$/.test(name) || /[\/\\\x00-\x1f\x7f\uD800-\uDFFF]/u.test(name)) {
    throw new Error("Use a file name without path separators or control characters.");
  }
  return name;
}

publishForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  publishButton.disabled = true;
  publishForm.setAttribute("aria-busy", "true");
  setStatus(publishStatus, "Publishing…");
  try {
    const uiMode = publishForm.elements.mode.value;
    const body = selectedBody();
    const creds = credentials();
    const name = validatePlainName($("content-name").value.trim());

    let result;
    if (isEncryptionEnabled()) {
      const kdf = selectedKdf();
      const passphrase = $("encrypt-passphrase").value;
      if (kdf === "passphrase" && !passphrase) {
        $("encrypt-passphrase").focus();
        throw new Error("Enter a passphrase to encrypt this item.");
      }
      const originalKind = uiMode === "text" ? "text" : "file";
      const originalType = body.body.type || "application/octet-stream";
      const estimated = estimateEncryptedSize(body.body.size, {
        name,
        type: originalType,
        kind: originalKind,
        kdf,
      });
      if (estimated > settings.maxUploadBytes) {
        throw new Error("This encrypted upload exceeds the site size limit.");
      }
      setStatus(publishStatus, kdf === "passphrase" ? "Deriving key and encrypting…" : "Encrypting…");
      const plainBytes = await body.body.arrayBuffer();
      const encrypted = await encryptEnvelope(plainBytes, {
        name,
        type: originalType,
        kind: originalKind,
        kdf,
        passphrase,
      });
      setStatus(publishStatus, "Publishing encrypted payload…");
      result = await publish(
        {
          mode: "file",
          body: new Blob([encrypted.payload], { type: "application/octet-stream" }),
          name: "encrypted.bin",
          credentials: creds,
        },
        settings,
      );
      result.encrypted = {
        kdf: encrypted.kdf,
        secret: encrypted.secret,
        includeSecretInUrl: publishForm.elements.keyDelivery.value === "url",
        originalName: name,
        originalType,
        originalKind,
        originalSize: plainBytes.byteLength,
      };
    } else {
      result = await publish(
        { ...body, name, credentials: creds },
        settings,
      );
    }

    renderResult(result);
    setStatus(
      publishStatus,
      result.encrypted
        ? (creds ? "Encrypted and published with signed credentials." : "Encrypted and published anonymously.")
        : (creds ? "Published with signed credentials." : "Published anonymously."),
      "success",
    );
    resultSection.scrollIntoView({ behavior: "smooth", block: "nearest" });
  } catch (error) {
    setStatus(publishStatus, error.message, "error");
    if (/credentials|Access key/i.test(error.message)) {
      credentialsDetails.open = true;
      ($("access-key").value ? $("secret-key") : $("access-key")).focus();
    }
  } finally {
    publishButton.disabled = false;
    publishForm.removeAttribute("aria-busy");
  }
});

$("copy-share").addEventListener("click", async () => {
  if (!currentSelection) return;
  const field = $("share-url");
  try {
    await navigator.clipboard.writeText(field.value);
    setStatus($("copy-status"), "Share link copied to clipboard.", "success");
  } catch {
    field.focus();
    field.select();
    setStatus($("copy-status"), "Clipboard access was denied; the link is selected for manual copying.", "error");
  }
});

$("copy-cid").addEventListener("click", async () => {
  const cid = currentResult?.cid;
  if (!cid) return;
  const button = $("copy-cid");
  try {
    await navigator.clipboard.writeText(cid);
    button.textContent = "Copied";
    setTimeout(() => {
      if (currentResult?.cid === cid) button.textContent = "Copy CID";
    }, 1500);
  } catch {
    setStatus($("copy-status"), "Clipboard access was denied; copy the CID directly from the metadata field.", "error");
  }
});

async function listPublicDirectory(listUrl, singlePage = false) {
  const entries = [];
  const seenTokens = new Set();
  let pageUrl = listUrl;
  do {
    const response = await fetch(pageUrl, {
      credentials: "omit",
      redirect: "error",
      cache: "no-store",
      referrerPolicy: "no-referrer",
    });
    if (response.status === 403) {
      throw new Error("Directory listing is disabled or private (HTTP 403); use IPFS or inbrowser.link for directory preview.");
    }
    if (!response.ok) throw new Error(`Anonymous ListObjectsV2 returned HTTP ${response.status}.`);
    const documentXml = new DOMParser().parseFromString(await response.text(), "application/xml");
    if (documentXml.querySelector("parsererror")) throw new Error("The S3 listing was not valid XML.");
    for (const content of documentXml.getElementsByTagName("Contents")) {
      const key = content.getElementsByTagName("Key")[0]?.textContent;
      const sizeText = content.getElementsByTagName("Size")[0]?.textContent;
      if (key && sizeText !== undefined) entries.push({ key, size: Number(sizeText) });
    }
    const truncated = !singlePage && documentXml.getElementsByTagName("IsTruncated")[0]?.textContent === "true";
    const token = documentXml.getElementsByTagName("NextContinuationToken")[0]?.textContent;
    if (truncated) {
      if (!token || seenTokens.has(token)) throw new Error("The S3 listing pagination token did not advance.");
      seenTokens.add(token);
      const next = new URL(listUrl);
      next.searchParams.set("continuation-token", token);
      pageUrl = next.href;
    } else {
      pageUrl = undefined;
    }
  } while (pageUrl);
  return entries;
}

function addPublicS3Links(result) {
  const container = $("s3-links");
  if (result.encrypted) {
    addEncryptedShareAction(container, result, "s3", result.url, "Open decoder");
    addLink(
      container,
      resultLinkOptions(result, "s3", "raw", result.url, "Download ciphertext (.bin)", "encrypted.bin"),
    );
    return;
  }
  if (result.kind === "file") {
    addLink(container, resultLinkOptions(result, "s3", "preview", result.url, "Preview object"));
  }
  addLink(
    container,
    resultLinkOptions(result, "s3", "raw", result.url, result.kind === "directory" ? "Download original ZIP" : "Download raw object"),
  );
}

async function checkPublicEntry(result, entry, cell, button) {
  button.disabled = true;
  const url = objectUrlFromList(directoryListUrl(result), entry.key);
  const probe = await probePublicUrl(url);
  if (currentResult !== result) return;
  cell.replaceChildren();
  if (probe.state === "public") {
    const name = entry.key.slice(directoryPrefixFor(result.key).length) || entry.key;
    addLink(cell, resultLinkOptions({ ...result, kind: "file" }, "s3", "preview", url, "Preview", name));
    addLink(cell, resultLinkOptions({ ...result, kind: "file" }, "s3", "raw", url, "Raw", name));
  } else {
    cell.textContent = probe.message;
  }
}

async function checkPublicAccess() {
  const result = currentResult;
  if (!result) return;
  $("check-public").disabled = true;
  $("s3-links").replaceChildren();
  setStatus(
    $("public-status"),
    result.kind === "directory"
      ? "Checking the original archive and directory listing…"
      : "Checking anonymous object access…",
  );
  try {
    const source = await probePublicUrl(result.url);
    if (currentResult !== result) return;
    if (source.state === "public") addPublicS3Links(result);
    let listError;
    if (result.kind === "directory") {
      try {
        const listUrl = directoryListUrl(result);
        const probeUrl = new URL(listUrl);
        probeUrl.searchParams.set("max-keys", "1");
        await listPublicDirectory(probeUrl.href, true);
        if (currentResult !== result) return;
        const share = document.createElement("button");
        share.type = "button";
        share.className = "button secondary";
        share.textContent = "Share public directory listing";
        share.setAttribute("aria-pressed", "false");
        share.addEventListener("click", () =>
          showShare(
            { kind: "directory", name: result.name, backend: "s3", mode: "preview", url: listUrl, rootStatus: result.rootStatus },
            share,
          ),
        );
        $("s3-links").append(share);
      } catch (error) {
        listError = error.message;
      }
    }
    const message = [
      `Object: ${source.message}`,
      result.kind === "directory"
        ? (listError ?? "Directory listing is anonymously readable. Check individual entries only when needed.")
        : "",
    ].filter(Boolean).join(" ");
    setStatus($("public-status"), message, source.state === "public" && !listError ? "public" : "unknown");
  } finally {
    if (currentResult === result) $("check-public").disabled = false;
  }
}
$("check-public").addEventListener("click", checkPublicAccess);

function renderSharedEntries(entries, listUrl) {
  const rows = $("shared-entry-rows");
  rows.replaceChildren();
  for (const entry of entries) {
    const row = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = entry.key;
    const size = document.createElement("td");
    size.textContent = formatBytes(entry.size);
    const links = document.createElement("td");
    const url = objectUrlFromList(listUrl, entry.key);
    const preview = document.createElement("a");
    preview.href = url;
    preview.target = "_blank";
    preview.rel = "noopener noreferrer";
    preview.textContent = "Preview";
    if (new URL(url).origin === window.location.origin) {
      preview.removeAttribute("href");
      preview.textContent = "Preview unavailable on this origin";
    }
    const raw = document.createElement("button");
    raw.type = "button";
    raw.className = "button secondary compact";
    raw.textContent = "Raw";
    raw.addEventListener("click", async () => {
      await downloadObject(url, entry.key.slice(entry.key.lastIndexOf("/") + 1), $("shared-status"));
    });
    links.append(preview, raw);
    row.append(name, size, links);
    rows.append(row);
  }
  $("shared-entries").hidden = false;
}

$("shared-download").addEventListener("click", async () => {
  if (!activeSharedSelection) return;
  const download = $("shared-download");
  if (download.getAttribute("aria-busy") === "true") return;
  download.setAttribute("aria-busy", "true");
  setStatus($("shared-status"), "Fetching raw object…");
  await downloadObject(activeSharedSelection.url, activeSharedSelection.name, $("shared-status"));
  download.removeAttribute("aria-busy");
});

$("shared-list").addEventListener("click", async () => {
  if (!activeSharedSelection) return;
  const button = $("shared-list");
  button.disabled = true;
  setStatus($("shared-status"), "Listing public entries…");
  try {
    const entries = await listPublicDirectory(activeSharedSelection.url);
    renderSharedEntries(entries, activeSharedSelection.url);
    setStatus($("shared-status"), `${entries.length} public entries.`, "public");
  } catch (error) {
    setStatus($("shared-status"), error.message, "error");
  } finally {
    button.disabled = false;
  }
});

function clearDecryptedState() {
  if (decryptedItem?.imageUrl) {
    URL.revokeObjectURL(decryptedItem.imageUrl);
  }
  decryptedItem = null;
  $("decrypted-output").hidden = true;
  $("decrypted-meta").textContent = "";
  $("decrypted-text").hidden = true;
  $("decrypted-text").textContent = "";
  $("decrypted-copy").hidden = true;
  $("decrypted-image-wrap").hidden = true;
  $("decrypted-image").removeAttribute("src");
  $("decrypted-binary-note").hidden = true;
  $("decrypted-binary-note").textContent = "";
}

function isPreviewableTextType(type) {
  const normalized = (type || "").split(";")[0].trim().toLowerCase();
  return normalized.startsWith("text/") || TEXT_MIME_TYPES.has(normalized);
}

function renderDecryptedItem(plain) {
  clearDecryptedState();
  const mime = (plain.type || "application/octet-stream").split(";")[0].trim().toLowerCase();
  const state = {
    bytes: plain.bytes,
    name: plain.name,
    type: plain.type,
    kind: plain.kind,
    text: null,
    imageUrl: null,
  };

  $("shared-title").textContent = plain.name;
  $("decrypted-meta").textContent = `${plain.name} · ${plain.type} · ${formatBytes(plain.bytes.byteLength)}`;

  let previewRendered = false;
  if ((plain.kind === "text" || isPreviewableTextType(plain.type)) && plain.bytes.byteLength <= MAX_TEXT_PREVIEW_BYTES) {
    try {
      const decodedText = utf8FatalDecoder.decode(plain.bytes);
      state.text = decodedText;
      $("decrypted-text").textContent = decodedText;
      $("decrypted-text").hidden = false;
      $("decrypted-copy").hidden = false;
      previewRendered = true;
    } catch {
      // Fall through to binary download if declared text is not valid UTF-8.
    }
  } else if (SAFE_IMAGE_TYPES.has(mime)) {
    const imageUrl = URL.createObjectURL(new Blob([plain.bytes], { type: mime }));
    state.imageUrl = imageUrl;
    $("decrypted-image").src = imageUrl;
    $("decrypted-image-wrap").hidden = false;
    previewRendered = true;
  }

  if (!previewRendered) {
    $("decrypted-binary-note").textContent =
      "Inline preview is disabled for this file type or size. Use Download to save the decrypted file.";
    $("decrypted-binary-note").hidden = false;
  }

  decryptedItem = state;
  $("decrypted-output").hidden = false;
}

async function fetchEncryptedPayload(selection) {
  if (cachedCiphertext?.url === selection.url) {
    return cachedCiphertext.bytes;
  }
  const options = {
    credentials: "omit",
    cache: "no-store",
    referrerPolicy: "no-referrer",
    redirect: selection.backend === "s3" ? "error" : "follow",
  };
  if (selection.backend === "s3") {
    options.headers = { "X-IPFS3-Download-Gateway": "proxy" };
  }
  let response;
  try {
    response = await fetch(selection.url, options);
  } catch {
    throw new Error("Could not fetch the encrypted payload. Check gateway availability or CORS settings.");
  }
  if (!response.ok) {
    throw new Error(`Fetching encrypted payload returned HTTP ${response.status}.`);
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  cachedCiphertext = { url: selection.url, bytes };
  return bytes;
}

async function decryptActiveSelection() {
  const selection = activeSharedSelection;
  if (!selection || selection.kind !== "encrypted") return;
  const secret = $("decrypt-secret").value;
  if (!secret) {
    setStatus(
      $("shared-status"),
      selection.kdf === "passphrase"
        ? "Enter the passphrase to decrypt this item."
        : "Enter the decryption key to decrypt this item.",
      "error",
    );
    $("decrypt-secret").focus();
    return;
  }

  const runId = ++decryptRunId;
  const button = $("decrypt-button");
  button.disabled = true;
  button.setAttribute("aria-busy", "true");
  clearDecryptedState();
  try {
    if (cachedCiphertext?.url !== selection.url) {
      setStatus($("shared-status"), "Fetching encrypted payload…");
    }
    const bytes = await fetchEncryptedPayload(selection);
    if (runId !== decryptRunId) return;
    setStatus(
      $("shared-status"),
      selection.kdf === "passphrase" ? "Deriving key and decrypting…" : "Decrypting…",
    );
    const plain = await decryptEnvelope(bytes, { secret });
    if (runId !== decryptRunId) return;
    renderDecryptedItem(plain);
    setStatus($("shared-status"), "Decrypted locally in your browser.", "success");
  } catch (error) {
    if (runId !== decryptRunId) return;
    setStatus($("shared-status"), error.message, "error");
  } finally {
    if (runId === decryptRunId) {
      button.disabled = false;
      button.removeAttribute("aria-busy");
    }
  }
}

$("decrypt-form").addEventListener("submit", (event) => {
  event.preventDefault();
  void decryptActiveSelection();
});

$("decrypted-download").addEventListener("click", () => {
  if (!decryptedItem) return;
  const objectUrl = URL.createObjectURL(
    new Blob([decryptedItem.bytes], { type: "application/octet-stream" }),
  );
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = decryptedItem.name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
});

$("decrypted-copy").addEventListener("click", async () => {
  if (decryptedItem?.text == null) return;
  try {
    await navigator.clipboard.writeText(decryptedItem.text);
    setStatus($("shared-status"), "Decrypted text copied to clipboard.", "success");
  } catch {
    setStatus($("shared-status"), "Clipboard access was denied; copy text directly from the preview.", "error");
  }
});

function showWorkbench() {
  decryptRunId += 1;
  activeSharedSelection = null;
  cachedCiphertext = null;
  clearDecryptedState();
  $("decrypt-form").hidden = true;
  $("decrypt-secret").value = "";
  $("shared-selection").hidden = true;
  $("shared-entries").hidden = true;
  $("shared-entry-rows").replaceChildren();
  setStatus($("shared-status"), "");
  workbench.hidden = false;
  resultSection.hidden = !currentResult;
}

$("shared-exit").addEventListener("click", () => {
  history.replaceState(null, "", window.location.pathname + window.location.search);
  showWorkbench();
});

function renderSharedSelection(selection) {
  decryptRunId += 1;
  activeSharedSelection = selection;
  cachedCiphertext = null;
  clearDecryptedState();
  workbench.hidden = true;
  resultSection.hidden = true;
  $("shared-selection").hidden = false;
  $("shared-entries").hidden = true;
  $("shared-entry-rows").replaceChildren();
  setStatus($("shared-status"), "");

  const open = $("shared-open");
  const download = $("shared-download");
  const listButton = $("shared-list");
  const decryptForm = $("decrypt-form");

  if (selection.kind === "encrypted") {
    const isPassphrase = selection.kdf === "passphrase";
    $("shared-title").textContent = "Encrypted item";
    $("shared-description").textContent = `Encrypted · ${selection.backend} · AES-256-GCM (${isPassphrase ? "passphrase" : "random key"})`;
    $("shared-partial").hidden = true;
    open.hidden = true;
    download.hidden = true;
    listButton.hidden = true;
    decryptForm.hidden = false;
    $("decrypt-secret-label").textContent = isPassphrase ? "Passphrase" : "Decryption key";
    $("decrypt-secret").placeholder = isPassphrase ? "Enter passphrase" : "Enter decryption key";
    $("decrypt-secret").value = selection.secret ?? "";
    setSecretVisibility($("decrypt-secret"), $("toggle-decrypt-secret"), false);
    if (selection.secret) {
      void decryptActiveSelection();
    } else {
      setStatus(
        $("shared-status"),
        isPassphrase
          ? "Enter the passphrase to decrypt this item."
          : "Enter the decryption key to decrypt this item.",
      );
      $("decrypt-secret").focus();
    }
    return;
  }

  decryptForm.hidden = true;
  $("decrypt-secret").value = "";
  $("shared-title").textContent = selection.name;
  $("shared-description").textContent = `${selection.kind === "directory" ? "ZIP directory" : "File"} · ${selection.backend} · ${selection.mode}`;
  $("shared-partial").hidden = selection.rootStatus !== "partial";
  const isDirectoryListing = selection.kind === "directory" && selection.backend === "s3" && selection.mode === "preview";
  const isS3Raw = selection.backend === "s3" && selection.mode === "raw";
  open.hidden = isDirectoryListing || isS3Raw;
  download.hidden = !isS3Raw;
  listButton.hidden = !isDirectoryListing;
  if (!isS3Raw && !isDirectoryListing) {
    if (new URL(selection.url).origin === window.location.origin) {
      open.removeAttribute("href");
      open.hidden = true;
      setStatus($("shared-status"), "Navigation is blocked because the target uses this site’s origin.", "error");
    } else {
      open.href = selection.url;
      open.textContent = selection.mode === "raw" ? "Download raw object" : "Open preview";
    }
  }
}

function syncViewFromHash() {
  $("page-alert").hidden = true;
  if (window.location.hash.startsWith("#share?")) {
    try {
      renderSharedSelection(decodeShare(window.location.hash));
    } catch (error) {
      showWorkbench();
      $("page-alert").hidden = false;
      $("page-alert").textContent = `The share link could not be opened: ${error.message}`;
    }
  } else {
    showWorkbench();
  }
}

window.addEventListener("hashchange", syncViewFromHash);
syncViewFromHash();
