import { deploymentDefaults } from "./config.js";
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
const publishForm = $("publish-form");
const publishButton = $("publish-button");
const publishStatus = $("publish-status");
const settingsForm = $("settings-form");
const settingsStatus = $("settings-status");
const resultSection = $("result");
const workbench = document.querySelector(".workbench");
let settings;
let currentResult;
let currentSelection;

function setStatus(element, message, state = "") {
  element.textContent = message;
  if (state) element.dataset.state = state;
  else delete element.dataset.state;
}

function clearCredentials() {
  $("access-key").value = "";
  $("secret-key").value = "";
  $("session-token").value = "";
}

function fillSettings() {
  $("endpoint").value = settings.endpoint;
  $("bucket").value = settings.bucket;
  $("region").value = settings.region;
  $("max-upload").value = settings.maxUploadBytes;
  $("max-upload").max = deploymentDefaults.maxUploadBytes;
  $("ipfs-gateway").value = settings.ipfsGateway;
  $("limit-label").textContent = `Up to ${formatBytes(settings.maxUploadBytes)}`;
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
  setStatus(settingsStatus, "Instance saved. Credentials were not persisted.", "success");
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
    setStatus(settingsStatus, "Deployment defaults restored; credentials cleared.", "success");
  } catch (error) {
    setStatus(settingsStatus, error.message, "error");
  }
});

for (const input of publishForm.elements.mode) {
  input.addEventListener("change", updateMode);
}

function updateMode() {
  const selected = publishForm.elements.mode.value;
  const isText = selected === "text";
  $("text-panel").hidden = !isText;
  $("file-panel").hidden = isText;
  $("file-content").accept = selected === "directory" ? ".zip,application/zip" : "";
  $("file-hint").textContent = selected === "directory"
    ? "The ZIP is extracted on the server. Partial results identify every failed entry."
    : "The selected file stays local until you publish.";
}

$("file-content").addEventListener("change", () => {
  const file = $("file-content").files[0];
  if (file) $("content-name").value = file.name;
});

function selectedBody() {
  const mode = publishForm.elements.mode.value;
  if (mode === "text") {
    const value = $("text-content").value;
    return { mode: "file", body: new Blob([value], { type: "text/plain;charset=utf-8" }) };
  }
  const file = $("file-content").files[0];
  if (!file) throw new Error(mode === "directory" ? "Choose a ZIP file." : "Choose a file.");
  return { mode, body: file };
}

function credentials() {
  const value = {
    accessKeyId: $("access-key").value,
    secretAccessKey: $("secret-key").value,
  };
  const sessionToken = $("session-token").value;
  if (sessionToken) value.sessionToken = sessionToken;
  return value;
}

function showShare(selection) {
  currentSelection = selection;
  const direct = selection.backend !== "s3" || (selection.kind === "file" && selection.mode === "preview");
  const url = new URL(window.location.origin + window.location.pathname);
  url.hash = direct ? "" : encodeShare(selection);
  $("share-url").value = direct ? selection.url : url.href;
  $("share-selection-label").textContent = `${selection.backend} · ${selection.mode}`;
  $("copy-share").disabled = false;
  setStatus($("copy-status"), direct ? "Direct object link ready." : "Download/listing link ready; it contains only this selected target.", "success");
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
    URL.revokeObjectURL(objectUrl);
    setStatus(status, "Download ready.", "success");
  } catch (error) {
    setStatus(status, `Raw download failed: ${error.message}`, "error");
  }
}

function addLink(container, { url, label, name, backend, mode, kind, rootStatus }) {
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
  share.addEventListener("click", () => showShare({ kind, name, backend, mode, url, rootStatus }));
  wrapper.append(share, status);
  container.append(wrapper);
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
    check.className = "button secondary";
    check.textContent = "Check";
    check.addEventListener("click", () => checkPublicEntry(result, entry, s3Cell, check));
    s3Cell.append(check);
    row.append(nameCell, sizeCell, ipfsCell, s3Cell);
    rows.append(row);
  }
  $("entries-section").hidden = !result.entries?.length;
}

function directoryPrefixFor(key) {
  return key.slice(0, key.lastIndexOf("/") + 1) + "files/";
}

function renderResult(result) {
  currentResult = result;
  currentSelection = undefined;
  resultSection.hidden = false;
  $("result-kind").textContent = result.kind === "directory" ? "ZIP directory" : "File";
  $("result-name").textContent = result.name;
  $("result-key").textContent = result.key;
  $("result-cid").textContent = result.cid;
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

  const inbrowserLinks = $("inbrowser-links");
  const gatewayLinks = $("gateway-links");
  inbrowserLinks.replaceChildren();
  gatewayLinks.replaceChildren();
  const directory = result.kind === "directory";
  const inbrowser = ipfsUrl(settings.inbrowserGateway, result.cid, { name: result.name, directory });
  addLink(inbrowserLinks, resultLinkOptions(result, "inbrowser", "preview", inbrowser, directory ? "Open directory" : "Open preview"));
  const preview = ipfsUrl(settings.ipfsGateway, result.cid, { name: result.name, directory });
  addLink(gatewayLinks, resultLinkOptions(result, "ipfs", "preview", preview, directory ? "Preview directory" : "Preview file"));
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

publishForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  publishButton.disabled = true;
  publishForm.setAttribute("aria-busy", "true");
  setStatus(publishStatus, "Publishing…");
  try {
    const body = selectedBody();
    const result = await publish(
      { ...body, name: $("content-name").value, credentials: credentials() },
      settings,
    );
    renderResult(result);
    setStatus(publishStatus, "Published. Choose Share beside the exact link you want to send.", "success");
  } catch (error) {
    setStatus(publishStatus, error.message, "error");
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
    setStatus($("copy-status"), "Share link copied.", "success");
  } catch {
    field.focus();
    field.select();
    setStatus($("copy-status"), "Clipboard access was denied; the link is selected for manual copying.", "error");
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
  if (result.kind === "file") {
    addLink(container, resultLinkOptions(result, "s3", "preview", result.url, "Preview object"));
  }
  addLink(container, resultLinkOptions(result, "s3", "raw", result.url, result.kind === "directory" ? "Download original ZIP" : "Download raw object"));
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
  setStatus($("public-status"), "Checking the original object and directory listing…");
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
        share.addEventListener("click", () => showShare({ kind: "directory", name: result.name, backend: "s3", mode: "preview", url: listUrl, rootStatus: result.rootStatus }));
        $("s3-links").append(share);
      } catch (error) {
        listError = error.message;
      }
    }
    const message = [
      `Original object: ${source.message}`,
      result.kind === "directory" ? (listError ?? "Directory listing is anonymously readable. Check individual entries only when needed.") : "",
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
    raw.className = "button secondary";
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

function renderSharedSelection(selection) {
  workbench.hidden = true;
  resultSection.hidden = true;
  $("shared-selection").hidden = false;
  $("shared-title").textContent = selection.name;
  $("shared-description").textContent = `${selection.kind === "directory" ? "ZIP directory" : "File"} · ${selection.backend} · ${selection.mode}`;
  $("shared-partial").hidden = selection.rootStatus !== "partial";
  const open = $("shared-open");
  const download = $("shared-download");
  const isDirectoryListing = selection.kind === "directory" && selection.backend === "s3" && selection.mode === "preview";
  const isS3Raw = selection.backend === "s3" && selection.mode === "raw";
  open.hidden = isDirectoryListing || isS3Raw;
  download.hidden = !isS3Raw;
  $("shared-list").hidden = !isDirectoryListing;
  if (isS3Raw) {
    download.addEventListener("click", async () => {
      download.setAttribute("aria-busy", "true");
      await downloadObject(selection.url, selection.name, $("shared-status"));
      download.removeAttribute("aria-busy");
    });
  } else if (!isDirectoryListing) {
    if (new URL(selection.url).origin === window.location.origin) {
      open.removeAttribute("href");
      open.hidden = true;
      setStatus($("shared-status"), "Navigation is blocked because the target uses this site’s origin.", "error");
    } else {
      open.href = selection.url;
      open.textContent = selection.mode === "raw" ? "Download raw object" : "Open preview";
    }
  }
  $("shared-list").addEventListener("click", async () => {
    const button = $("shared-list");
    button.disabled = true;
    setStatus($("shared-status"), "Listing public entries…");
    try {
      const entries = await listPublicDirectory(selection.url);
      renderSharedEntries(entries, selection.url);
      setStatus($("shared-status"), `${entries.length} public entries.`, "public");
    } catch (error) {
      setStatus($("shared-status"), error.message, "error");
    } finally {
      button.disabled = false;
    }
  });
}

if (window.location.hash.startsWith("#share?")) {
  try {
    renderSharedSelection(decodeShare(window.location.hash));
  } catch (error) {
    $("page-alert").hidden = false;
    $("page-alert").textContent = `The share link could not be opened: ${error.message}`;
  }
}
