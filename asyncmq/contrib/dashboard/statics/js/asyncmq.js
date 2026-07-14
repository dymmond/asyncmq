(function () {
  function getLoadingOverlay() {
    return document.getElementById("loading-overlay");
  }

  function showLoading() {
    const overlay = getLoadingOverlay();
    if (overlay) {
      overlay.classList.remove("hidden");
    }
  }

  function hideLoading() {
    const overlay = getLoadingOverlay();
    if (overlay) {
      overlay.classList.add("hidden");
    }
  }

  function setupLoadingIndicators() {
    document.querySelectorAll("form").forEach((form) => {
      form.addEventListener("submit", showLoading);
    });

    document.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", () => {
        const href = link.getAttribute("href");
        if (href && !href.startsWith("#")) {
          showLoading();
        }
      });
    });

    window.addEventListener("pageshow", hideLoading);
  }

  function setupAutosubmitControls() {
    document.querySelectorAll("[data-auto-submit]").forEach((control) => {
      control.addEventListener("change", () => {
        if (control.form) {
          control.form.requestSubmit();
        }
      });
    });
  }

  function registerAlpineComponents() {
    document.addEventListener("alpine:init", () => {
      window.Alpine.data("asyncmqDismissible", () => ({
        visible: true,
        dismiss() {
          this.visible = false;
        },
      }));

      window.Alpine.data("asyncmqPassword", () => ({
        visible: false,
        get inputType() {
          return this.visible ? "text" : "password";
        },
        get label() {
          return this.visible ? "Hide password" : "Show password";
        },
        get pressed() {
          return this.visible ? "true" : "false";
        },
        toggle() {
          this.visible = !this.visible;
        },
      }));
    });
  }

  function parseEventJson(event) {
    try {
      return JSON.parse(event.data);
    } catch (error) {
      console.warn("AsyncMQ live update parse error", error);
      return null;
    }
  }

  function connectEvents(url, label) {
    if (!url || typeof EventSource === "undefined") {
      return null;
    }
    const source = new EventSource(url);
    source.onerror = () => console.warn(`${label || "SSE"} error - retrying`);
    return source;
  }

  function setTextById(id, value) {
    const element = document.getElementById(id);
    if (element) {
      element.textContent = value ?? "";
    }
  }

  function appendTextCell(row, value, className) {
    const cell = document.createElement("td");
    if (className) {
      cell.className = className;
    }
    cell.textContent = value ?? "";
    row.appendChild(cell);
    return cell;
  }

  function badgeForState(state) {
    const value = String(state || "unknown");
    const normalized = value.toLowerCase();
    const badge = document.createElement("span");
    badge.className = "amq-badge amq-badge--neutral";
    if (["completed", "success", "running"].includes(normalized)) {
      badge.className = "amq-badge amq-badge--success";
    } else if (["failed", "failure", "error"].includes(normalized)) {
      badge.className = "amq-badge amq-badge--danger";
    } else if (["active", "paused", "delayed", "retrying"].includes(normalized)) {
      badge.className = "amq-badge amq-badge--warning";
    }
    badge.textContent = value.charAt(0).toUpperCase() + value.slice(1);
    return badge;
  }

  function chartCanvas(id) {
    const canvas = document.getElementById(id);
    if (!canvas || typeof Chart === "undefined") {
      return null;
    }
    return canvas.getContext("2d");
  }

  function setupOverviewLive(root) {
    const source = connectEvents(root.dataset.eventsUrl, "Overview SSE");
    if (!source) {
      return;
    }

    source.addEventListener("overview", (event) => {
      const data = parseEventJson(event);
      if (!data) {
        return;
      }
      setTextById("total-queues", data.total_queues);
      setTextById("total-jobs", data.total_jobs);
      setTextById("total-workers", data.total_workers);
    });

    const pieCtx = chartCanvas("jobdist-chart");
    const pieChart = pieCtx
      ? new Chart(pieCtx, {
          type: "doughnut",
          data: {
            labels: ["Waiting", "Active", "Delayed", "Completed", "Failed"],
            datasets: [{ data: [0, 0, 0, 0, 0] }],
          },
          options: { responsive: true, maintainAspectRatio: false, animation: { duration: 0 } },
        })
      : null;

    source.addEventListener("jobdist", (event) => {
      const dist = parseEventJson(event);
      if (!dist || !pieChart) {
        return;
      }
      pieChart.data.datasets[0].data = [
        dist.waiting,
        dist.active,
        dist.delayed,
        dist.completed,
        dist.failed,
      ];
      pieChart.update();
    });

    const lineCtx = chartCanvas("overview-line-chart");
    const lineData = {
      labels: [],
      datasets: [{ label: "Completed", data: [], borderColor: "rgba(59,130,246,0.8)", fill: false }],
    };
    const lineChart = lineCtx
      ? new Chart(lineCtx, {
          type: "line",
          data: lineData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: {}, y: { beginAtZero: true } },
            animation: { duration: 0 },
          },
        })
      : null;

    source.addEventListener("metrics", (event) => {
      const metrics = parseEventJson(event);
      if (!metrics || !lineChart) {
        return;
      }
      lineData.labels.push(new Date().toLocaleTimeString());
      lineData.datasets[0].data.push(metrics.throughput);
      if (lineData.labels.length > 20) {
        lineData.labels.shift();
        lineData.datasets[0].data.shift();
      }
      lineChart.update();
    });

    const barCtx = chartCanvas("queue-bar-chart");
    const barData = {
      labels: [],
      datasets: [{ label: "Waiting", data: [], backgroundColor: "rgba(59,130,246,0.7)" }],
    };
    const barChart = barCtx
      ? new Chart(barCtx, {
          type: "bar",
          data: barData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: {}, y: { beginAtZero: true } },
            animation: { duration: 0 },
          },
        })
      : null;

    source.addEventListener("queues", (event) => {
      const rows = parseEventJson(event);
      if (!Array.isArray(rows) || !barChart) {
        return;
      }
      barData.labels = rows.map((row) => row.name);
      barData.datasets[0].data = rows.map((row) => row.waiting);
      barChart.update();
    });

    source.addEventListener("latest_jobs", (event) => {
      const jobs = parseEventJson(event);
      const tbody = document.getElementById("latest-jobs-body");
      if (!Array.isArray(jobs) || !tbody) {
        return;
      }
      tbody.textContent = "";
      jobs.forEach((job) => {
        const row = document.createElement("tr");
        appendTextCell(row, job.id, "amq-mono");
        appendTextCell(row, job.queue);
        const stateCell = document.createElement("td");
        stateCell.appendChild(badgeForState(job.state));
        row.appendChild(stateCell);
        appendTextCell(row, job.time);
        tbody.appendChild(row);
      });
    });

    source.addEventListener("latest_queues", (event) => {
      const queues = parseEventJson(event);
      const tbody = document.getElementById("latest-queues-body");
      if (!Array.isArray(queues) || !tbody) {
        return;
      }
      tbody.textContent = "";
      queues.forEach((queue) => {
        const row = document.createElement("tr");
        appendTextCell(row, queue.name, "amq-mono");
        appendTextCell(row, queue.time);
        tbody.appendChild(row);
      });
    });
  }

  function findQueueCard(queueName) {
    return Array.from(document.querySelectorAll("[data-queue]")).find(
      (card) => card.dataset.queue === queueName
    );
  }

  function setupQueueListLive(root) {
    const source = connectEvents(root.dataset.eventsUrl, "Queue SSE");
    if (!source) {
      return;
    }

    source.addEventListener("overview", (event) => {
      const data = parseEventJson(event);
      if (!data) {
        return;
      }
      setTextById("total-queues", data.total_queues);
      setTextById("total-workers", data.total_workers);
    });

    source.addEventListener("queue_stats", (event) => {
      const stats = parseEventJson(event);
      if (!Array.isArray(stats)) {
        return;
      }
      stats.forEach((queue) => {
        const card = findQueueCard(queue.queue);
        if (!card) {
          return;
        }
        ["waiting", "active", "delayed", "failed", "completed"].forEach((state) => {
          const value = card.querySelector(`.${state}`);
          if (value) {
            value.textContent = queue[state] ?? 0;
          }
        });
        const badge = card.querySelector(".amq-badge");
        if (badge) {
          badge.textContent = queue.paused ? "Paused" : "Running";
          badge.className = queue.paused
            ? "amq-badge amq-badge--warning"
            : "amq-badge amq-badge--success";
        }
      });
    });
  }

  function setupQueueDetailLive(root) {
    const source = connectEvents(root.dataset.eventsUrl, "Queue detail SSE");
    const queueName = root.dataset.queueName;
    const ctx = chartCanvas("queue-chart");
    if (!source || !queueName || !ctx) {
      return;
    }

    const maxPoints = 20;
    const data = {
      labels: [],
      datasets: [
        { label: "Waiting", data: [], borderColor: "rgba(59,130,246,0.8)", fill: false },
        { label: "Active", data: [], borderColor: "rgba(234,179,8,0.8)", fill: false },
        { label: "Delayed", data: [], borderColor: "rgba(14,165,233,0.8)", fill: false },
        { label: "Failed", data: [], borderColor: "rgba(239,68,68,0.8)", fill: false },
        { label: "Completed", data: [], borderColor: "rgba(34,197,94,0.8)", fill: false },
      ],
    };
    const queueChart = new Chart(ctx, {
      type: "line",
      data,
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: { x: {}, y: { beginAtZero: true } },
        animation: { duration: 0 },
      },
    });

    source.addEventListener("queues", (event) => {
      const rows = parseEventJson(event);
      if (!Array.isArray(rows)) {
        return;
      }
      const queue = rows.find((row) => row.name === queueName);
      if (!queue) {
        return;
      }
      ["waiting", "active", "delayed", "failed", "completed"].forEach((state) => {
        const value = root.querySelector(`.${state}`);
        if (value) {
          value.textContent = queue[state] ?? 0;
        }
      });
      const badge = root.querySelector("[data-queue-status] .amq-badge");
      if (badge) {
        badge.textContent = queue.paused ? "Paused" : "Running";
        badge.className = queue.paused
          ? "amq-badge amq-badge--warning"
          : "amq-badge amq-badge--success";
      }
      data.labels.push(new Date().toLocaleTimeString());
      data.datasets[0].data.push(queue.waiting);
      data.datasets[1].data.push(queue.active);
      data.datasets[2].data.push(queue.delayed);
      data.datasets[3].data.push(queue.failed);
      data.datasets[4].data.push(queue.completed);

      if (data.labels.length > maxPoints) {
        data.labels.shift();
        data.datasets.forEach((dataset) => dataset.data.shift());
      }
      queueChart.update();
    });
  }

  function readJsonTemplate(id, fallback) {
    const template = document.getElementById(id);
    if (!template) {
      return fallback;
    }
    try {
      return JSON.parse(template.textContent || "");
    } catch (error) {
      console.warn("AsyncMQ JSON data parse error", error);
      return fallback;
    }
  }

  function normalizeMetricTime(row) {
    if (row.time) {
      return new Date(row.time).toLocaleTimeString();
    }
    if (row.timestamp) {
      return new Date(row.timestamp * 1000).toLocaleTimeString();
    }
    return "N/A";
  }

  function setupMetricsLive(root) {
    const maxPoints = 60;
    const metricsCtx = chartCanvas("metrics-chart");
    const metricsData = {
      labels: [],
      datasets: [
        { label: "Throughput", data: [], borderColor: "rgba(59,130,246,0.8)", fill: false },
        { label: "Retries", data: [], borderColor: "rgba(234,179,8,0.8)", fill: false },
        { label: "Failures", data: [], borderColor: "rgba(239,68,68,0.8)", fill: false },
      ],
    };
    const metricsChart = metricsCtx
      ? new Chart(metricsCtx, {
          type: "line",
          data: metricsData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { display: true }, y: { beginAtZero: true } },
            animation: { duration: 0 },
          },
        })
      : null;

    const stateCtx = chartCanvas("state-chart");
    const stateData = {
      labels: ["Waiting", "Active", "Delayed", "Completed", "Failed"],
      datasets: [
        {
          label: "Jobs",
          data: [0, 0, 0, 0, 0],
          backgroundColor: [
            "rgba(59,130,246,0.7)",
            "rgba(16,185,129,0.7)",
            "rgba(245,158,11,0.7)",
            "rgba(99,102,241,0.7)",
            "rgba(239,68,68,0.7)",
          ],
        },
      ],
    };
    const stateChart = stateCtx
      ? new Chart(stateCtx, {
          type: "bar",
          data: stateData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } },
            animation: { duration: 0 },
          },
        })
      : null;

    function setCards(row) {
      if (!row) {
        return;
      }
      setTextById("throughput", row.throughput ?? 0);
      setTextById("avg-duration", row.avg_duration ?? "-");
      setTextById("retries", row.retries ?? 0);
      setTextById("failures", row.failures ?? 0);
      setTextById("waiting-count", row.waiting ?? 0);
      setTextById("active-count", row.active ?? 0);
      setTextById("total-queues", row.total_queues ?? 0);
      setTextById("total-workers", row.total_workers ?? 0);
    }

    function setStateChart(row) {
      if (!row || !stateChart) {
        return;
      }
      stateData.datasets[0].data = [
        row.waiting ?? 0,
        row.active ?? 0,
        row.delayed ?? 0,
        row.completed ?? 0,
        row.failed ?? 0,
      ];
      stateChart.update();
    }

    function setMetricsChart(rows) {
      if (!metricsChart) {
        return;
      }
      const ordered = rows.slice().reverse();
      metricsData.labels = ordered.map(normalizeMetricTime).slice(-maxPoints);
      metricsData.datasets[0].data = ordered.map((row) => row.throughput ?? 0).slice(-maxPoints);
      metricsData.datasets[1].data = ordered.map((row) => row.retries ?? 0).slice(-maxPoints);
      metricsData.datasets[2].data = ordered.map((row) => row.failures ?? 0).slice(-maxPoints);
      metricsChart.update();
    }

    function setHistoryTable(rows) {
      const tbody = document.getElementById("metrics-history-body");
      if (!tbody) {
        return;
      }
      tbody.textContent = "";
      const recent = rows.slice(0, 20);
      if (recent.length === 0) {
        const row = document.createElement("tr");
        const cell = appendTextCell(row, "No history yet.", "amq-table-empty");
        cell.colSpan = 10;
        tbody.appendChild(row);
        return;
      }

      recent.forEach((metric) => {
        const row = document.createElement("tr");
        appendTextCell(row, normalizeMetricTime(metric), "amq-mono");
        appendTextCell(row, metric.throughput ?? 0);
        appendTextCell(row, metric.retries ?? 0);
        appendTextCell(row, metric.failures ?? 0);
        appendTextCell(row, metric.waiting ?? 0);
        appendTextCell(row, metric.active ?? 0);
        appendTextCell(row, metric.delayed ?? 0);
        appendTextCell(row, metric.completed ?? 0);
        appendTextCell(row, metric.failed ?? 0);
        appendTextCell(row, metric.total_workers ?? 0);
        tbody.appendChild(row);
      });
    }

    function renderHistory(rows) {
      setMetricsChart(rows);
      setHistoryTable(rows);
      setCards(rows[0]);
      setStateChart(rows[0]);
    }

    async function refreshHistory() {
      try {
        const response = await fetch(root.dataset.historyUrl, {
          headers: { Accept: "application/json" },
        });
        if (!response.ok) {
          return;
        }
        const payload = await response.json();
        renderHistory(Array.isArray(payload.history) ? payload.history : []);
      } catch (error) {
        console.warn("Metrics history fetch failed", error);
      }
    }

    function appendLiveMetric(metric) {
      if (metricsChart) {
        metricsData.labels.push(new Date().toLocaleTimeString());
        metricsData.datasets[0].data.push(metric.throughput ?? 0);
        metricsData.datasets[1].data.push(metric.retries ?? 0);
        metricsData.datasets[2].data.push(metric.failures ?? 0);

        while (metricsData.labels.length > maxPoints) {
          metricsData.labels.shift();
          metricsData.datasets.forEach((dataset) => dataset.data.shift());
        }
        metricsChart.update();
      }

      setTextById("throughput", metric.throughput ?? 0);
      setTextById("avg-duration", metric.avg_duration ?? "-");
      setTextById("retries", metric.retries ?? 0);
      setTextById("failures", metric.failures ?? 0);
    }

    renderHistory(readJsonTemplate("metrics-history-data", []));
    refreshHistory();
    window.setInterval(refreshHistory, 15000);

    const source = connectEvents(root.dataset.eventsUrl, "Metrics SSE");
    if (!source) {
      return;
    }

    source.addEventListener("metrics", (event) => {
      const metric = parseEventJson(event);
      if (metric) {
        appendLiveMetric(metric);
      }
    });

    source.addEventListener("jobdist", (event) => {
      const dist = parseEventJson(event);
      if (!dist) {
        return;
      }
      setTextById("waiting-count", dist.waiting ?? 0);
      setTextById("active-count", dist.active ?? 0);
      if (stateChart) {
        stateData.datasets[0].data = [
          dist.waiting ?? 0,
          dist.active ?? 0,
          dist.delayed ?? 0,
          dist.completed ?? 0,
          dist.failed ?? 0,
        ];
        stateChart.update();
      }
    });

    source.addEventListener("overview", (event) => {
      const data = parseEventJson(event);
      if (!data) {
        return;
      }
      setTextById("total-queues", data.total_queues ?? 0);
      setTextById("total-workers", data.total_workers ?? 0);
    });
  }

  function setupDlqControls(root) {
    const selectAll = root.querySelector("[data-dlq-select-all]");
    if (!selectAll) {
      return;
    }
    selectAll.addEventListener("change", () => {
      root.querySelectorAll("input.select-dlq").forEach((checkbox) => {
        checkbox.checked = selectAll.checked;
      });
    });
  }

  function setupLiveDashboardPages() {
    document.querySelectorAll("[data-asyncmq-overview]").forEach(setupOverviewLive);
    document.querySelectorAll("[data-asyncmq-queues]").forEach(setupQueueListLive);
    document.querySelectorAll("[data-asyncmq-queue-detail]").forEach(setupQueueDetailLive);
    document.querySelectorAll("[data-asyncmq-metrics]").forEach(setupMetricsLive);
    document.querySelectorAll("[data-asyncmq-dlq]").forEach(setupDlqControls);
  }

  window.showLoading = showLoading;
  window.hideLoading = hideLoading;

  document.addEventListener("DOMContentLoaded", () => {
    setupLoadingIndicators();
    setupAutosubmitControls();
    setupLiveDashboardPages();
  });

  registerAlpineComponents();
})();
