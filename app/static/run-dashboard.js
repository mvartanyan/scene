(function () {
  "use strict";

  if (window.__sceneRunsDashboardController) {
    window.__sceneRunsDashboardController.init();
    return;
  }

  var CUSTOM_BASELINE_OPTION = "__custom__";
  var RUN_LOG_INTERVAL_MS = 5000;
  var RUN_DETAIL_INTERVAL_MS = 5000;
  var EXECUTION_LOG_INTERVAL_MS = 2000;
  var EXECUTION_LOG_TIMEOUT_SECONDS = 3;
  var defaultRunDetailHtml = [
    '<div id="run-detail-shell-inner" data-run-id="">',
    '  <div class="modal-header">',
    '    <h5 class="modal-title">Run Detail</h5>',
    '    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>',
    '  </div>',
    '  <div class="modal-body d-flex justify-content-center align-items-center" style="min-height: 12rem;">',
    '    <div class="text-muted d-flex align-items-center gap-2">',
    '      <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>',
    '      <span>Loading run...</span>',
    '    </div>',
    '  </div>',
    '  <div class="modal-footer">',
    '    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>',
    '  </div>',
    '</div>'
  ].join("");

  var state = {
    initialized: false,
    selectedRunId: "",
    pendingRunId: "",
    runModalOpen: false,
    viewerOpen: false,
    logOpen: false,
    runLogTimer: 0,
    runLogInFlight: false,
    detailTimer: 0,
    detailInFlight: false,
    logPollTimer: 0,
    logPollInFlight: false,
    logPollKey: "",
    preservedScrollTop: null,
    counters: {
      globalListeners: 0,
      runModalListeners: 0,
      viewerModalListeners: 0,
      logModalListeners: 0,
      runLogPollStarts: 0,
      detailPollStarts: 0,
      executionLogPollStarts: 0
    }
  };

  function getRunLog() {
    return document.getElementById("run-log");
  }

  function getRunDetailShell() {
    return document.getElementById("run-detail-shell");
  }

  function getRunModal() {
    return document.getElementById("runDetailModal");
  }

  function getViewerModal() {
    return document.getElementById("executionViewerModal");
  }

  function getLogModal() {
    return document.getElementById("executionLogModal");
  }

  function bootstrapModal(element) {
    if (!element || !window.bootstrap || typeof window.bootstrap.Modal !== "function") {
      return null;
    }
    return window.bootstrap.Modal.getOrCreateInstance(element);
  }

  function processHtmx(element) {
    if (window.htmx && typeof window.htmx.process === "function" && element) {
      window.htmx.process(element);
    }
  }

  function loadFragmentInto(trigger, fallbackTarget) {
    if (!window.htmx || typeof window.htmx.ajax !== "function") {
      return;
    }
    var url = trigger.getAttribute("hx-get");
    var target = trigger.getAttribute("hx-target") || fallbackTarget;
    if (url && target) {
      window.htmx.ajax("GET", url, target);
    }
  }

  function updateDebugState() {
    window.__sceneRunDashboardDebug = {
      state: {
        selectedRunId: state.selectedRunId,
        pendingRunId: state.pendingRunId,
        runModalOpen: state.runModalOpen,
        viewerOpen: state.viewerOpen,
        logOpen: state.logOpen,
        runLogPolling: Boolean(state.runLogTimer),
        detailPolling: Boolean(state.detailTimer),
        logPolling: Boolean(state.logPollTimer || state.logPollInFlight)
      },
      counters: Object.assign({}, state.counters)
    };
  }

  function selectedFromDom() {
    var runLog = getRunLog();
    var bodySelected = document.body ? document.body.dataset.sceneSelectedRun || "" : "";
    return state.selectedRunId || bodySelected || (runLog ? runLog.dataset.selectedRunId || "" : "");
  }

  function setSelectedRun(runId) {
    state.selectedRunId = runId || "";
    if (document.body) {
      document.body.dataset.sceneSelectedRun = state.selectedRunId;
    }
    highlightRunSelection();
    syncFilterSelected();
    updateDebugState();
  }

  function highlightRunSelection() {
    var runLog = getRunLog();
    if (!runLog) {
      return;
    }
    var selected = state.selectedRunId || runLog.dataset.selectedRunId || "";
    if (selected && !runLog.querySelector('.run-log-entry[data-run-id="' + selected + '"]')) {
      selected = "";
      state.selectedRunId = "";
      if (document.body) {
        document.body.dataset.sceneSelectedRun = "";
      }
    }
    runLog.dataset.selectedRunId = selected;
    Array.prototype.forEach.call(runLog.querySelectorAll(".run-log-entry"), function (entry) {
      entry.classList.toggle("selected-run", Boolean(selected) && entry.dataset.runId === selected);
    });
    Array.prototype.forEach.call(runLog.querySelectorAll('[data-run-filter="selected_run_id"]'), function (input) {
      input.value = selected;
    });
  }

  function syncFilterSelected() {
    var input = document.getElementById("run-filter-selected");
    if (input) {
      input.value = state.selectedRunId || "";
    }
  }

  function showRunModal() {
    var instance = bootstrapModal(getRunModal());
    if (instance) {
      instance.show();
    }
  }

  function hideRunModal() {
    var modal = getRunModal();
    if (!modal || !window.bootstrap || typeof window.bootstrap.Modal !== "function") {
      return;
    }
    var instance = window.bootstrap.Modal.getInstance(modal);
    if (instance) {
      instance.hide();
    }
  }

  function resetRunDetail() {
    var shell = getRunDetailShell();
    if (!shell) {
      return;
    }
    shell.dataset.runId = "";
    shell.dataset.sceneHash = "";
    shell.dataset.pendingRunId = "";
    shell.innerHTML = defaultRunDetailHtml;
    processHtmx(shell);
  }

  function preserveRunDetailScroll() {
    var shell = getRunDetailShell();
    var body = shell ? shell.querySelector(".modal-body-scroll") : null;
    state.preservedScrollTop = body ? body.scrollTop : null;
  }

  function restoreRunDetailScroll() {
    if (typeof state.preservedScrollTop !== "number") {
      return;
    }
    var shell = getRunDetailShell();
    var body = shell ? shell.querySelector(".modal-body-scroll") : null;
    if (body) {
      body.scrollTop = state.preservedScrollTop;
    }
    state.preservedScrollTop = null;
  }

  function applyRunDetailHtml(html, response) {
    var shell = getRunDetailShell();
    if (!shell) {
      return;
    }
    preserveRunDetailScroll();
    shell.innerHTML = html;
    var inner = shell.querySelector("#run-detail-shell-inner");
    var responseRunId = response ? response.headers.get("X-Scene-Run-Id") || "" : "";
    var responseHash = response ? response.headers.get("X-Scene-Run-Hash") || "" : "";
    var runId = responseRunId || (inner && inner.dataset ? inner.dataset.runId || "" : "");
    shell.dataset.runId = runId;
    shell.dataset.sceneHash = responseHash || shell.dataset.sceneHash || "";
    shell.dataset.pendingRunId = "";
    state.pendingRunId = "";
    if (runId) {
      setSelectedRun(runId);
    }
    processHtmx(shell);
    initExecutionViewer(shell);
    restoreRunDetailScroll();
    updateDetailPolling();
  }

  function sameRunDetail(response) {
    var shell = getRunDetailShell();
    if (!shell || !response) {
      return false;
    }
    var responseHash = response.headers.get("X-Scene-Run-Hash") || "";
    return Boolean(responseHash && shell.dataset.sceneHash === responseHash);
  }

  function canRefreshRunDetail(runId) {
    if (!runId || !state.runModalOpen || state.viewerOpen || state.logOpen) {
      return false;
    }
    var active = document.activeElement;
    var shell = getRunDetailShell();
    if (active && shell && shell.contains(active)) {
      var tagName = active.tagName ? active.tagName.toLowerCase() : "";
      if (["input", "select", "textarea", "button"].indexOf(tagName) !== -1) {
        return false;
      }
    }
    return true;
  }

  async function fetchRunDetail(runId, options) {
    options = options || {};
    if (!runId || state.detailInFlight) {
      return;
    }
    if (!options.force && !canRefreshRunDetail(runId)) {
      return;
    }
    state.detailInFlight = true;
    try {
      var response = await fetch("/runs/" + encodeURIComponent(runId) + "/overlay", {
        cache: "no-store",
        headers: { "X-Scene-Request": "run-detail-refresh" }
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      if (!options.force && sameRunDetail(response)) {
        return;
      }
      var responseRunId = response.headers.get("X-Scene-Run-Id") || runId;
      if (!options.force && responseRunId !== selectedFromDom()) {
        return;
      }
      applyRunDetailHtml(await response.text(), response);
    } catch (error) {
      console.error("Run detail refresh failed", error);
    } finally {
      state.detailInFlight = false;
      updateDebugState();
    }
  }

  function loadRunDetail(runId, force) {
    if (!runId) {
      return;
    }
    setSelectedRun(runId);
    state.pendingRunId = runId;
    var shell = getRunDetailShell();
    if (shell) {
      shell.dataset.pendingRunId = runId;
      shell.dataset.runId = runId;
      shell.innerHTML = defaultRunDetailHtml;
    }
    showRunModal();
    fetchRunDetail(runId, { force: force !== false });
  }

  function updateDetailPolling() {
    var runId = selectedFromDom();
    var shouldPoll = canRefreshRunDetail(runId);
    if (shouldPoll && !state.detailTimer) {
      state.counters.detailPollStarts += 1;
      state.detailTimer = window.setInterval(function () {
        fetchRunDetail(selectedFromDom(), { force: false });
      }, RUN_DETAIL_INTERVAL_MS);
    } else if (!shouldPoll && state.detailTimer) {
      window.clearInterval(state.detailTimer);
      state.detailTimer = 0;
    }
    updateDebugState();
  }

  function runLogUrl() {
    var runLog = getRunLog();
    var params = new URLSearchParams();
    var selected = state.selectedRunId || (runLog ? runLog.dataset.selectedRunId || "" : "");
    if (selected) {
      params.set("selected_run_id", selected);
    }
    if (runLog) {
      Array.prototype.forEach.call(runLog.querySelectorAll("[data-run-filter]"), function (input) {
        if (input.name && input.value && input.name !== "selected_run_id") {
          params.set(input.name, input.value);
        }
      });
    }
    var query = params.toString();
    return "/runs/log" + (query ? "?" + query : "");
  }

  function showToasts(container) {
    if (!window.bootstrap || typeof window.bootstrap.Toast !== "function") {
      return;
    }
    Array.prototype.forEach.call((container || document).querySelectorAll(".toast"), function (toastEl) {
      if (toastEl.dataset.sceneShown === "1") {
        return;
      }
      toastEl.dataset.sceneShown = "1";
      window.bootstrap.Toast.getOrCreateInstance(toastEl).show();
    });
  }

  async function pollRunLog() {
    var current = getRunLog();
    if (!current || state.runLogInFlight) {
      return;
    }
    state.runLogInFlight = true;
    try {
      var response = await fetch(runLogUrl(), { cache: "no-store" });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      var responseHash = response.headers.get("X-Scene-Run-Hash") || "";
      if (responseHash && current.dataset.sceneHash === responseHash) {
        highlightRunSelection();
        return;
      }
      var html = await response.text();
      var wrapper = document.createElement("div");
      wrapper.innerHTML = html.trim();
      var next = wrapper.querySelector("#run-log");
      if (!next) {
        return;
      }
      var toastContainers = Array.prototype.slice.call(wrapper.querySelectorAll(".toast-container"));
      next.dataset.selectedRunId = state.selectedRunId || next.dataset.selectedRunId || "";
      if (responseHash) {
        next.dataset.sceneHash = responseHash;
      }
      current.replaceWith(next);
      toastContainers.forEach(function (toastContainer) {
        next.parentNode.insertBefore(toastContainer, next);
      });
      processHtmx(next);
      highlightRunSelection();
      syncFilterSelected();
      showToasts(document);
    } catch (error) {
      console.error("Run log refresh failed", error);
    } finally {
      state.runLogInFlight = false;
      updateDebugState();
    }
  }

  function startRunLogPolling() {
    if (state.runLogTimer || !getRunLog()) {
      return;
    }
    state.counters.runLogPollStarts += 1;
    state.runLogTimer = window.setInterval(pollRunLog, RUN_LOG_INTERVAL_MS);
    updateDebugState();
  }

  function stopRunLogPolling() {
    if (state.runLogTimer) {
      window.clearInterval(state.runLogTimer);
      state.runLogTimer = 0;
    }
    updateDebugState();
  }

  function stopExecutionLogPolling() {
    if (state.logPollTimer) {
      window.clearTimeout(state.logPollTimer);
      state.logPollTimer = 0;
    }
    state.logPollInFlight = false;
    state.logPollKey = "";
    updateDebugState();
  }

  function scheduleExecutionLogPoll(delay) {
    if (!state.logOpen) {
      return;
    }
    if (state.logPollTimer) {
      window.clearTimeout(state.logPollTimer);
    }
    state.logPollTimer = window.setTimeout(pollExecutionLog, delay);
    updateDebugState();
  }

  async function pollExecutionLog() {
    var container = document.getElementById("execution-log-body");
    if (!state.logOpen || !container || state.logPollInFlight) {
      return;
    }
    var runId = container.dataset.runId || "";
    var executionId = container.dataset.executionId || "";
    if (!runId || !executionId) {
      return;
    }
    state.logPollTimer = 0;
    state.logPollInFlight = true;
    try {
      var length = parseInt(container.dataset.logLength || "0", 10);
      var response = await fetch(
        "/runs/" + encodeURIComponent(runId) +
          "/executions/" + encodeURIComponent(executionId) +
          "/log/stream?since=" + encodeURIComponent(String(length)) +
          "&timeout=" + EXECUTION_LOG_TIMEOUT_SECONDS,
        { cache: "no-store" }
      );
      if (!response.ok) {
        throw new Error(await response.text());
      }
      var data = await response.json();
      if (!document.body.contains(container)) {
        return;
      }
      container.dataset.logLength = String(data.length || 0);
      container.textContent = data.text || "";
      container.scrollTop = container.scrollHeight;
    } catch (error) {
      console.error("Execution log refresh failed", error);
    } finally {
      state.logPollInFlight = false;
      if (state.logOpen && document.getElementById("execution-log-body")) {
        scheduleExecutionLogPoll(EXECUTION_LOG_INTERVAL_MS);
      }
    }
  }

  function startExecutionLogPolling() {
    var container = document.getElementById("execution-log-body");
    if (!container) {
      return;
    }
    var key = (container.dataset.runId || "") + ":" + (container.dataset.executionId || "");
    if (state.logPollKey === key && (state.logPollTimer || state.logPollInFlight)) {
      return;
    }
    stopExecutionLogPolling();
    state.logPollKey = key;
    state.counters.executionLogPollStarts += 1;
    scheduleExecutionLogPoll(EXECUTION_LOG_INTERVAL_MS);
  }

  function initExecutionViewer(root) {
    var content = root && root.id === "execution-viewer-content"
      ? root
      : document.getElementById("execution-viewer-content");
    if (!content) {
      return;
    }
    var viewerRoot = content.querySelector('[data-role="viewer-root"]');
    if (!viewerRoot || viewerRoot.dataset.sceneViewerInit === "1") {
      return;
    }
    viewerRoot.dataset.sceneViewerInit = "1";
    var initialMode = viewerRoot.dataset.initialMode || "observed";
    var stage = viewerRoot.querySelector('[data-role="viewer-stage"]');
    var buttons = Array.prototype.slice.call(viewerRoot.querySelectorAll('[data-role="viewer-mode"]'));
    var compareElements = Array.prototype.slice.call(viewerRoot.querySelectorAll('[data-role="image-compare"]'));
    var compareInstances = {};
    var metadata = viewerRoot.querySelector('[data-role="viewer-metadata"]');
    var scrollContainer = viewerRoot.querySelector(".viewer-stage-scroll");

    if (metadata && scrollContainer) {
      var updateCompact = function () {
        if (scrollContainer.scrollTop > 120) {
          metadata.setAttribute("data-compact", "true");
        } else {
          metadata.removeAttribute("data-compact");
        }
      };
      scrollContainer.addEventListener("scroll", function () {
        window.requestAnimationFrame(updateCompact);
      }, { passive: true });
      updateCompact();
    }

    function ensureCompare(kind) {
      if (typeof window.ImageCompare !== "function" || compareInstances[kind]) {
        return compareInstances[kind] || null;
      }
      var element = compareElements.find(function (candidate) {
        return candidate.dataset.compareKind === kind;
      });
      if (!element) {
        return null;
      }
      var beforeWidth = parseInt(element.dataset.beforeWidth || "", 10);
      var beforeHeight = parseInt(element.dataset.beforeHeight || "", 10);
      var afterWidth = parseInt(element.dataset.afterWidth || "", 10);
      var afterHeight = parseInt(element.dataset.afterHeight || "", 10);
      var targetWidth = Number.isFinite(afterWidth) && afterWidth > 0 ? afterWidth : beforeWidth;
      var targetHeight = Number.isFinite(afterHeight) && afterHeight > 0 ? afterHeight : beforeHeight;
      if (Number.isFinite(targetWidth) && targetWidth > 0 && Number.isFinite(targetHeight) && targetHeight > 0) {
        element.style.aspectRatio = String(targetWidth) + " / " + String(targetHeight);
      }
      var instance = new window.ImageCompare(element, {
        initial: 50,
        beforeLabel: element.dataset.beforeLabel || "Baseline",
        afterLabel: element.dataset.afterLabel || "Observed"
      });
      instance.mount();
      compareInstances[kind] = instance;
      return instance;
    }

    function activate(mode) {
      if (!stage) {
        return;
      }
      stage.setAttribute("data-active", mode);
      buttons.forEach(function (button) {
        var active = button.dataset.mode === mode;
        button.classList.toggle("btn-primary", active);
        button.classList.toggle("btn-outline-secondary", !active);
        button.setAttribute("aria-pressed", active ? "true" : "false");
      });
      if (mode === "slider" || mode === "reference") {
        ensureCompare(mode);
      }
    }

    buttons.forEach(function (button) {
      button.addEventListener("click", function () {
        if (!button.disabled) {
          activate(button.dataset.mode || "observed");
        }
      });
    });

    if (window.bootstrap && typeof window.bootstrap.Tooltip === "function") {
      Array.prototype.forEach.call(viewerRoot.querySelectorAll('[data-bs-toggle="tooltip"]'), function (element) {
        window.bootstrap.Tooltip.getOrCreateInstance(element);
      });
    }

    ensureCompare("slider");
    ensureCompare("reference");
    activate(initialMode);
  }

  function copyExecutionLog(button) {
    var targetSelector = button.dataset.copyTarget || "#execution-log-body";
    var target = document.querySelector(targetSelector);
    var text = target ? target.innerText || "" : "";
    if (!navigator.clipboard || !navigator.clipboard.writeText) {
      return;
    }
    navigator.clipboard.writeText(text).then(function () {
      button.classList.add("btn-success");
    }).catch(function (error) {
      console.error("Execution log copy failed", error);
    });
  }

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, function (character) {
      return {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;"
      }[character];
    });
  }

  function setupLaunchForm() {
    var form = document.getElementById("run-launch-form");
    var metadataElement = document.getElementById("run-launch-metadata");
    if (!form || !metadataElement || form.dataset.sceneInit === "1") {
      return;
    }

    var metadata = {};
    try {
      metadata = JSON.parse(metadataElement.textContent || "{}");
    } catch (error) {
      console.error("Failed to parse run launch metadata", error);
      return;
    }

    var projectSelect = form.querySelector('[data-role="launch-project"]');
    var batchSelect = form.querySelector('[data-role="launch-batch"]');
    var purposeSelect = form.querySelector('[data-role="launch-purpose"]');
    var baselineField = form.querySelector('[data-role="baseline-field"]');
    var baselineSelect = form.querySelector('[data-role="baseline-select"]');
    var baselineInput = form.querySelector('[data-role="baseline-input"]');
    var baselineHelp = form.querySelector('[data-role="baseline-help"]');
    if (!projectSelect || !batchSelect || !purposeSelect || !baselineField || !baselineSelect || !baselineInput || !baselineHelp) {
      return;
    }

    var projects = Array.isArray(metadata.projects) ? metadata.projects : [];
    var batchMap = metadata.batches || {};
    var defaults = metadata.defaults || {};
    var baselineCache = new Map(Object.entries(metadata.baselines || {}).map(function (entry) {
      return [entry[0], normalizeBaselineEntries(entry[1])];
    }));
    var baselineRequests = new Map();
    var launchState = {
      activeProject: defaults.project_id || "",
      activeBatch: defaults.batch_id || "",
      baselineSelection: defaults.baseline_id || "",
      customBaseline: defaults.baseline_input || ""
    };

    function normalizeBaselineEntries(items) {
      if (!Array.isArray(items)) {
        return [];
      }
      return items.map(function (item) {
        if (!item || typeof item !== "object" || !item.id) {
          return null;
        }
        return {
          id: String(item.id),
          label: String(item.label || item.id)
        };
      }).filter(Boolean);
    }

    function projectBatches(projectId) {
      return Array.isArray(batchMap[projectId]) ? batchMap[projectId] : [];
    }

    async function fetchBaselines(batchId, force) {
      if (!batchId) {
        return [];
      }
      if (!force && baselineCache.has(batchId)) {
        return baselineCache.get(batchId) || [];
      }
      if (baselineRequests.has(batchId)) {
        return baselineRequests.get(batchId);
      }
      var request = fetch("/api/batches/" + encodeURIComponent(batchId) + "/baselines", {
        cache: "no-store"
      }).then(function (response) {
        if (!response.ok) {
          throw new Error("Failed to load baselines");
        }
        return response.json();
      }).then(function (items) {
        var normalized = normalizeBaselineEntries(items);
        baselineCache.set(batchId, normalized);
        return normalized;
      }).catch(function (error) {
        console.error("Unable to fetch baselines", error);
        return baselineCache.get(batchId) || [];
      }).finally(function () {
        baselineRequests.delete(batchId);
      });
      baselineRequests.set(batchId, request);
      return request;
    }

    function renderProjectOptions() {
      if (!projects.length) {
        projectSelect.innerHTML = '<option value="" disabled>No projects available</option>';
        projectSelect.disabled = true;
        batchSelect.innerHTML = '<option value="" disabled>No batches available</option>';
        batchSelect.disabled = true;
        return;
      }
      projectSelect.innerHTML = projects.map(function (project) {
        return '<option value="' + escapeHtml(project.id) + '">' + escapeHtml(project.name) + '</option>';
      }).join("");
      if (!projects.some(function (project) { return project.id === launchState.activeProject; })) {
        launchState.activeProject = projects[0].id;
      }
      projectSelect.value = launchState.activeProject;
    }

    function renderBatchOptions() {
      var batches = projectBatches(projectSelect.value);
      if (!batches.length) {
        batchSelect.innerHTML = '<option value="" disabled>No batches available</option>';
        batchSelect.disabled = true;
        launchState.activeBatch = "";
        renderBaselineControls([], true);
        return;
      }
      batchSelect.disabled = false;
      batchSelect.innerHTML = batches.map(function (batch) {
        return '<option value="' + escapeHtml(batch.id) + '">' + escapeHtml(batch.name) + '</option>';
      }).join("");
      if (!batches.some(function (batch) { return batch.id === launchState.activeBatch; })) {
        launchState.activeBatch = batches[0].id;
      }
      batchSelect.value = launchState.activeBatch;
    }

    function renderBaselineControls(options, inactive) {
      var purpose = purposeSelect.value || "comparison";
      baselineField.classList.toggle("opacity-50", Boolean(inactive) || purpose === "baseline_recording");
      if (inactive || !launchState.activeBatch) {
        baselineSelect.classList.add("d-none");
        baselineSelect.disabled = true;
        baselineInput.classList.add("d-none");
        baselineInput.disabled = true;
        baselineInput.value = "";
        baselineHelp.textContent = "Select a project and batch to choose a baseline.";
        return;
      }
      if (purpose === "baseline_recording") {
        baselineSelect.classList.add("d-none");
        baselineSelect.disabled = true;
        baselineInput.classList.add("d-none");
        baselineInput.disabled = true;
        baselineInput.value = "";
        baselineHelp.textContent = "Baseline selection disabled when recording a new baseline.";
        return;
      }

      baselineInput.disabled = false;
      if (options.length) {
        baselineSelect.classList.remove("d-none");
        baselineSelect.disabled = false;
        baselineSelect.innerHTML = ['<option value="">Latest baseline</option>']
          .concat(options.map(function (option) {
            return '<option value="' + escapeHtml(option.id) + '">' + escapeHtml(option.label) + '</option>';
          }))
          .concat(['<option value="' + CUSTOM_BASELINE_OPTION + '">Enter custom baseline...</option>'])
          .join("");
        var selected = launchState.baselineSelection;
        if (selected && selected !== CUSTOM_BASELINE_OPTION && !options.some(function (option) { return option.id === selected; })) {
          selected = "";
        }
        if (!selected && launchState.customBaseline) {
          selected = CUSTOM_BASELINE_OPTION;
        }
        baselineSelect.value = selected || "";
        if (baselineSelect.value === CUSTOM_BASELINE_OPTION) {
          baselineInput.classList.remove("d-none");
          baselineInput.value = launchState.customBaseline;
          baselineHelp.textContent = "Enter a baseline id or prefix to compare against.";
        } else {
          baselineInput.classList.add("d-none");
          baselineInput.value = baselineSelect.value;
          baselineHelp.textContent = baselineSelect.value
            ? "Comparing against the selected stored baseline."
            : "Using the latest stored baseline for this batch.";
        }
      } else {
        baselineSelect.classList.add("d-none");
        baselineSelect.disabled = true;
        baselineInput.classList.remove("d-none");
        baselineInput.value = launchState.customBaseline;
        baselineHelp.textContent = "No stored baselines yet. Enter an id or leave blank to use the latest when available.";
      }
    }

    async function refreshBaselines(force) {
      var options = await fetchBaselines(launchState.activeBatch, force);
      renderBaselineControls(options, false);
    }

    projectSelect.addEventListener("change", function () {
      launchState.activeProject = projectSelect.value || "";
      launchState.activeBatch = "";
      launchState.baselineSelection = "";
      launchState.customBaseline = "";
      renderBatchOptions();
      refreshBaselines(true);
    });
    batchSelect.addEventListener("change", function () {
      launchState.activeBatch = batchSelect.value || "";
      launchState.baselineSelection = "";
      launchState.customBaseline = "";
      refreshBaselines(true);
    });
    purposeSelect.addEventListener("change", function () {
      if (purposeSelect.value === "baseline_recording") {
        launchState.baselineSelection = "";
        launchState.customBaseline = "";
      }
      refreshBaselines(false);
    });
    baselineSelect.addEventListener("change", function () {
      if (baselineSelect.value === CUSTOM_BASELINE_OPTION) {
        launchState.baselineSelection = CUSTOM_BASELINE_OPTION;
      } else {
        launchState.baselineSelection = baselineSelect.value || "";
        launchState.customBaseline = "";
      }
      refreshBaselines(false);
    });
    baselineInput.addEventListener("input", function () {
      launchState.customBaseline = baselineInput.value;
      launchState.baselineSelection = CUSTOM_BASELINE_OPTION;
    });
    baselineSelect.addEventListener("focus", function () {
      refreshBaselines(true);
    });
    baselineInput.addEventListener("focus", function () {
      refreshBaselines(true);
    });

    renderProjectOptions();
    renderBatchOptions();
    purposeSelect.value = defaults.purpose || purposeSelect.value || "comparison";
    refreshBaselines(false);
    form.dataset.sceneInit = "1";
  }

  function onDocumentClick(event) {
    var copyButton = event.target && event.target.closest ? event.target.closest('[data-role="copy-execution-log"]') : null;
    if (copyButton) {
      copyExecutionLog(copyButton);
      return;
    }

    var viewerTrigger = event.target && event.target.closest
      ? event.target.closest('[data-bs-target="#executionViewerModal"]')
      : null;
    if (viewerTrigger) {
      event.preventDefault();
      event.stopPropagation();
      if (typeof event.stopImmediatePropagation === "function") {
        event.stopImmediatePropagation();
      }
      state.viewerOpen = true;
      var viewerRunId = viewerTrigger.dataset.runId || "";
      if (viewerRunId) {
        setSelectedRun(viewerRunId);
      }
      updateDetailPolling();
      loadFragmentInto(viewerTrigger, "#execution-viewer-content");
      var viewerInstance = bootstrapModal(getViewerModal());
      if (viewerInstance) {
        viewerInstance.show();
      }
      return;
    }

    var logTrigger = event.target && event.target.closest
      ? event.target.closest('[data-bs-target="#executionLogModal"], [data-role="execution-log-trigger"]')
      : null;
    if (logTrigger) {
      event.preventDefault();
      event.stopPropagation();
      if (typeof event.stopImmediatePropagation === "function") {
        event.stopImmediatePropagation();
      }
      state.logOpen = true;
      var logRunId = logTrigger.dataset.runId || "";
      if (logRunId) {
        setSelectedRun(logRunId);
      }
      updateDetailPolling();
      loadFragmentInto(logTrigger, "#execution-log-content");
      var logInstance = bootstrapModal(getLogModal());
      if (logInstance) {
        logInstance.show();
      }
      return;
    }

    var runButton = event.target && event.target.closest ? event.target.closest(".run-select-btn") : null;
    if (runButton) {
      event.preventDefault();
      var runId = runButton.dataset.runId || (runButton.closest(".run-log-entry") || {}).dataset.runId || "";
      loadRunDetail(runId, true);
    }
  }

  function onBeforeSwap(event) {
    if (event.target && event.target.id === "run-detail-shell") {
      preserveRunDetailScroll();
    }
  }

  function onAfterSwap(event) {
    var target = event.target;
    if (!target) {
      return;
    }
    if (target.id === "runs-dashboard") {
      initPageFragments();
    } else if (target.id === "run-detail-shell" || target.id === "run-detail-shell-inner") {
      var shell = getRunDetailShell();
      var inner = shell ? shell.querySelector("#run-detail-shell-inner") : null;
      if (shell && inner) {
        shell.dataset.runId = inner.dataset.runId || shell.dataset.runId || "";
        if (shell.dataset.runId) {
          setSelectedRun(shell.dataset.runId);
        }
      }
      restoreRunDetailScroll();
      initExecutionViewer(shell);
      updateDetailPolling();
    } else if (target.id === "execution-viewer-content") {
      initExecutionViewer(target);
    } else if (target.id === "execution-log-content") {
      startExecutionLogPolling();
    }
  }

  function onResponseError(event) {
    if (!event.target || event.target.id !== "run-detail-shell") {
      return;
    }
    event.target.innerHTML = [
      '<div id="run-detail-shell-inner" data-run-id="">',
      '  <div class="modal-header">',
      '    <h5 class="modal-title">Run Detail</h5>',
      '    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>',
      '  </div>',
      '  <div class="modal-body">',
      '    <div class="alert alert-danger mb-0">Unable to load run details. Please try again.</div>',
      '  </div>',
      '  <div class="modal-footer">',
      '    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>',
      '  </div>',
      '</div>'
    ].join("");
    showRunModal();
  }

  function setupModals() {
    var runModal = getRunModal();
    if (runModal && runModal.dataset.sceneRunModalInit !== "1") {
      runModal.addEventListener("hide.bs.modal", function (event) {
        if (state.viewerOpen || state.logOpen) {
          event.preventDefault();
          state.runModalOpen = true;
          updateDetailPolling();
        }
      });
      runModal.addEventListener("shown.bs.modal", function () {
        state.runModalOpen = true;
        updateDetailPolling();
      });
      runModal.addEventListener("hidden.bs.modal", function () {
        state.runModalOpen = false;
        state.pendingRunId = "";
        state.detailInFlight = false;
        updateDetailPolling();
        if (!state.viewerOpen && !state.logOpen) {
          state.selectedRunId = "";
          if (document.body) {
            document.body.dataset.sceneSelectedRun = "";
          }
          resetRunDetail();
          highlightRunSelection();
        }
      });
      runModal.dataset.sceneRunModalInit = "1";
      state.counters.runModalListeners += 3;
    }

    var viewerModal = getViewerModal();
    if (viewerModal && viewerModal.dataset.sceneViewerModalInit !== "1") {
      viewerModal.addEventListener("show.bs.modal", function (event) {
        state.viewerOpen = true;
        var trigger = event.relatedTarget;
        var runId = trigger ? trigger.dataset.runId || "" : "";
        if (runId) {
          setSelectedRun(runId);
        }
        updateDetailPolling();
      });
      viewerModal.addEventListener("hidden.bs.modal", function () {
        state.viewerOpen = false;
        showRunModal();
        updateDetailPolling();
      });
      viewerModal.dataset.sceneViewerModalInit = "1";
      state.counters.viewerModalListeners += 2;
    }

    var logModal = getLogModal();
    if (logModal && logModal.dataset.sceneLogModalInit !== "1") {
      logModal.addEventListener("show.bs.modal", function (event) {
        state.logOpen = true;
        var trigger = event.relatedTarget;
        var runId = trigger ? trigger.dataset.runId || "" : "";
        if (runId) {
          setSelectedRun(runId);
        }
        updateDetailPolling();
      });
      logModal.addEventListener("shown.bs.modal", function () {
        startExecutionLogPolling();
      });
      logModal.addEventListener("hidden.bs.modal", function () {
        state.logOpen = false;
        stopExecutionLogPolling();
        showRunModal();
        updateDetailPolling();
      });
      logModal.dataset.sceneLogModalInit = "1";
      state.counters.logModalListeners += 3;
    }
  }

  function initPageFragments() {
    state.selectedRunId = selectedFromDom();
    setupModals();
    setupLaunchForm();
    highlightRunSelection();
    syncFilterSelected();
    showToasts(document);
    if (getRunLog()) {
      startRunLogPolling();
    } else {
      stopRunLogPolling();
    }
    updateDetailPolling();
  }

  function init() {
    if (!state.initialized) {
      document.addEventListener("click", onDocumentClick, true);
      document.body.addEventListener("htmx:beforeSwap", onBeforeSwap);
      document.body.addEventListener("htmx:afterSwap", onAfterSwap);
      document.body.addEventListener("htmx:responseError", onResponseError);
      state.counters.globalListeners += 4;
      state.initialized = true;
    }
    initPageFragments();
    updateDebugState();
  }

  window.__sceneRunsDashboardController = {
    init: init,
    loadRunDetail: loadRunDetail,
    stop: function () {
      stopRunLogPolling();
      stopExecutionLogPolling();
      if (state.detailTimer) {
        window.clearInterval(state.detailTimer);
        state.detailTimer = 0;
      }
      updateDebugState();
    },
    debug: function () {
      updateDebugState();
      return window.__sceneRunDashboardDebug;
    }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
