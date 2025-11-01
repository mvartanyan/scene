(function (global) {
  "use strict";

  function clamp(value, min, max) {
    return Math.min(Math.max(value, min), max);
  }

  class ImageCompare {
    constructor(element, options) {
      if (!element) {
        throw new Error("ImageCompare requires a root element");
      }
      this.element = element;
      this.options = Object.assign(
        {
          initial: 50,
          beforeLabel: "Baseline",
          afterLabel: "Observed",
        },
        options || {}
      );
      this._mounted = false;
      this._value = clamp(Number(this.options.initial) || 50, 0, 100);
      this._onPointerMove = this._handlePointerMove.bind(this);
      this._onPointerUp = this._handlePointerUp.bind(this);
    }

    mount() {
      if (this._mounted) {
        return this;
      }
      const images = Array.from(this.element.querySelectorAll("img"));
      if (images.length < 2) {
        throw new Error("ImageCompare expects at least two <img> children");
      }
      const beforeImg = images[0];
      const afterImg = images[1];

      this.element.classList.add("image-compare");

      const canvas = document.createElement("div");
      canvas.className = "image-compare__canvas";

      const beforeWrapper = document.createElement("div");
      beforeWrapper.className = "image-compare__before";
      beforeWrapper.appendChild(beforeImg.cloneNode(true));

      const afterWrapper = document.createElement("div");
      afterWrapper.className = "image-compare__after";
      afterWrapper.appendChild(afterImg.cloneNode(true));

      const handle = document.createElement("div");
      handle.className = "image-compare__handle";

      const dragSurface = document.createElement("div");
      dragSurface.className = "image-compare__drag-surface";

      const beforeLabel = document.createElement("div");
      beforeLabel.className = "image-compare__overlay-label image-compare__overlay-label--before";
      beforeLabel.textContent = this.options.beforeLabel;

      const afterLabel = document.createElement("div");
      afterLabel.className = "image-compare__overlay-label image-compare__overlay-label--after";
      afterLabel.textContent = this.options.afterLabel;

      canvas.appendChild(beforeWrapper);
      canvas.appendChild(afterWrapper);
      canvas.appendChild(handle);
      canvas.appendChild(dragSurface);
      canvas.appendChild(beforeLabel);
      canvas.appendChild(afterLabel);

      this.element.innerHTML = "";
      this.element.appendChild(canvas);

      this._canvas = canvas;
      this._afterWrapper = afterWrapper;
      this._handle = handle;
      this._dragSurface = dragSurface;

      this._dragSurface.addEventListener("pointerdown", (event) => this._handlePointerDown(event));

      this._applyValue(this._value);
      this._mounted = true;
      return this;
    }

    destroy() {
      if (!this._mounted) {
        return;
      }
      this._dragSurface.removeEventListener("pointerdown", this._handlePointerDown);
      document.removeEventListener("pointermove", this._onPointerMove);
      document.removeEventListener("pointerup", this._onPointerUp);
      this._mounted = false;
    }

    value(newValue) {
      if (typeof newValue === "undefined") {
        return this._value;
      }
      this._value = clamp(Number(newValue) || 0, 0, 100);
      this._applyValue(this._value);
      return this;
    }

    _handlePointerDown(event) {
      event.preventDefault();
      this._dragSurface.setPointerCapture(event.pointerId);
      this._activePointerId = event.pointerId;
      this._updateFromEvent(event);
      document.addEventListener("pointermove", this._onPointerMove);
      document.addEventListener("pointerup", this._onPointerUp);
      document.addEventListener("pointercancel", this._onPointerUp);
      document.addEventListener("lostpointercapture", this._onPointerUp);
    }

    _handlePointerMove(event) {
      if (event.pointerId !== this._activePointerId) {
        return;
      }
      this._updateFromEvent(event);
    }

    _handlePointerUp(event) {
      if (event.pointerId !== this._activePointerId) {
        return;
      }
      document.removeEventListener("pointermove", this._onPointerMove);
      document.removeEventListener("pointerup", this._onPointerUp);
      document.removeEventListener("pointercancel", this._onPointerUp);
      document.removeEventListener("lostpointercapture", this._onPointerUp);
      this._activePointerId = null;
    }

    _updateFromEvent(event) {
      const rect = this._canvas.getBoundingClientRect();
      const percentage = ((event.clientX - rect.left) / rect.width) * 100;
      this.value(percentage);
    }

    _applyValue(value) {
      if (!this._afterWrapper || !this._handle) {
        return;
      }
      const clamped = clamp(value, 0, 100);
      this._afterWrapper.style.clipPath = `inset(0 0 0 ${clamped}%)`;
      this._handle.style.left = clamped + "%";
    }
  }

  global.ImageCompare = ImageCompare;
})(window);
