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

  function setupPasswordToggles() {
    document.querySelectorAll("[data-password-toggle]").forEach((button) => {
      button.addEventListener("click", () => {
        const target = document.querySelector(button.getAttribute("data-password-target"));
        if (!target) {
          return;
        }
        target.type = target.type === "password" ? "text" : "password";
      });
    });
  }

  function setupDismissButtons() {
    document.querySelectorAll("[data-dismiss-target]").forEach((button) => {
      button.addEventListener("click", () => {
        const target = document.querySelector(button.getAttribute("data-dismiss-target"));
        if (!target) {
          return;
        }
        target.classList.add("opacity-0");
        window.setTimeout(() => target.remove(), 200);
      });
    });
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
    setupPasswordToggles();
    setupDismissButtons();
    setupAutosubmitControls();
  });
})();
