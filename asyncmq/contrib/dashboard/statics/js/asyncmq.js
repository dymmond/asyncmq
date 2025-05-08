function showToast(message, type, duration, position) {
    Toastify({
        text: message,
        duration: duration || 3000,
        close: true,
        className: type || "info",
        gravity: "top", // `top` or `bottom`
        position: position || "right", // `left`, `center` or `right`
        stopOnFocus: true, // Prevents dismissing of toast on hover
        style: {
          background: type,
        },
        onClick: function(){} // Callback after click
      }).showToast();
}
