(function () {
  "use strict";

  var KEY = "qm-theme";
  var ATTR = "data-qm-theme";
  var VALUES = ["ink", "classic"];
  var root = document.documentElement;

  function ok(v) { return VALUES.indexOf(v) !== -1; }

  function stored() {
    try { return localStorage.getItem(KEY); } catch (e) { return null; }
  }

  function save(v) {
    try { localStorage.setItem(KEY, v); } catch (e) {}
  }

  // Synchronous apply before first paint; storage failure must not block.
  var v0 = stored();
  root.setAttribute(ATTR, ok(v0) ? v0 : VALUES[0]);

  function get() {
    return root.getAttribute(ATTR) || VALUES[0];
  }

  function set(v) {
    if (!ok(v)) return false;
    root.setAttribute(ATTR, v);
    save(v);
    return true;
  }

  function sync() {
    var cur = get();
    var list = document.getElementsByTagName("input");
    for (var i = 0; i < list.length; i++) {
      if (list[i].hasAttribute(ATTR + "-option")) {
        list[i].checked = list[i].value === cur;
      }
    }
  }

  function bind() {
    if (root.__qmThemeBound) return;
    root.__qmThemeBound = true;

    document.addEventListener("change", function (e) {
      var t = e.target;
      if (t && t.tagName === "INPUT" && t.hasAttribute(ATTR + "-option") && t.checked) {
        if (!set(t.value)) sync();
      }
    });

    new MutationObserver(sync).observe(root, {
      attributes: true,
      attributeFilter: [ATTR]
    });

    window.addEventListener("storage", function (e) {
      if (e.key === KEY) root.setAttribute(ATTR, ok(e.newValue) ? e.newValue : VALUES[0]);
    });

    sync();
  }

  window.QuantTheme = Object.freeze({
    get: get,
    set: set,
    values: VALUES.slice()
  });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
