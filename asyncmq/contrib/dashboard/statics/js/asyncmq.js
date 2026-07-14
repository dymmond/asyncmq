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
  });

  registerAlpineComponents();
})();
