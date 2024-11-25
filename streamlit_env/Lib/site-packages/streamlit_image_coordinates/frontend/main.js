// The `Streamlit` object exists because our html file includes
// `streamlit-component-lib.js`.
// If you get an error about "Streamlit" not being defined, that
// means you're missing that file.

function sendValue(value) {
  Streamlit.setComponentValue(value)
}

/**
 * The component's render function. This will be called immediately after
 * the component is initially loaded, and then again every time the
 * component gets new data from Python.
 */

function clickListener(event) {
  const {offsetX, offsetY} = event;
  const img = document.getElementById("image");

  sendValue({x: offsetX, y: offsetY, width: img.width, height: img.height});
}

function mouseDownListener(downEvent) {
  const [x1, y1] = [downEvent.offsetX, downEvent.offsetY];

  window.addEventListener("mouseup", (upEvent) => {
    const [x2, y2] = [upEvent.clientX, upEvent.clientY];
    const img = document.getElementById("image");
    const rect = img.getBoundingClientRect();

    sendValue({x1: x1, y1: y1, x2: x2 - rect.left, y2: y2 - rect.top, width: img.width, height: img.height});
  }, {once: true})
}

function onRender(event) {
  let {src, height, width, use_column_width, click_and_drag} = event.detail.args;

  const img = document.getElementById("image");

  if (img.src !== src) {
    img.src = src;
  }

  function resizeImage() {
    img.classList.remove("auto", "fullWidth");
    img.removeAttribute("width");
    img.removeAttribute("height");

    if (use_column_width === "always" || use_column_width === true) {
      img.classList.add("fullWidth");
    } else if (use_column_width === "auto") {
      img.classList.add("auto");
    } else {
      if (!width && !height) {
        width = img.naturalWidth;
        height = img.naturalHeight;
      } else if (!height) {
        height = width * img.naturalHeight / img.naturalWidth;
      } else if (!width) {
        width = height * img.naturalWidth / img.naturalHeight;
      }

      img.width = width;
      img.height = height;
    }

    Streamlit.setFrameHeight(img.height);
  }

  img.onload = resizeImage;
  window.addEventListener("resize", resizeImage);

  // When image is clicked, send the coordinates to Python through sendValue
  if (click_and_drag) {
    img.onclick = null;
    img.onmousedown = mouseDownListener;
  } else {
    img.onmousedown = null;
    img.onclick = clickListener;
  }
}

// Render the component whenever python send a "render event"
Streamlit.events.addEventListener(Streamlit.RENDER_EVENT, onRender)
// Tell Streamlit that the component is ready to receive events
Streamlit.setComponentReady()
