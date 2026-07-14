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

  function setupLiveDashboardPages() {
    document.querySelectorAll("[data-asyncmq-overview]").forEach(setupOverviewLive);
    document.querySelectorAll("[data-asyncmq-queues]").forEach(setupQueueListLive);
    document.querySelectorAll("[data-asyncmq-queue-detail]").forEach(setupQueueDetailLive);
  }

  window.showLoading = showLoading;
  window.hideLoading = hideLoading;

  window.showToast = function showToast(message, type, duration, position) {
    if (typeof Toastify === "undefined") {
      return;
    }
    Toastify({
      text: message,
      duration: duration || 3000,
      close: true,
      className: type || "info",
      gravity: "top",
      position: position || "right",
      stopOnFocus: true,
      style: {
        background: type,
      },
      onClick: function () {},
    }).showToast();
  };

  document.addEventListener("DOMContentLoaded", () => {
    setupLoadingIndicators();
    setupAutosubmitControls();
    setupLiveDashboardPages();
  });

  registerAlpineComponents();
})();
