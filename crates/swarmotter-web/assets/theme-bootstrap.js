// SPDX-License-Identifier: Apache-2.0
(function() {
  const storageKey = "swarmotter.theme";
  const fallbackTheme = "dark";
  let theme = fallbackTheme;
  try {
    const saved = window.localStorage.getItem(storageKey);
    if (saved === "light" || saved === "dark") theme = saved;
  } catch {
    theme = fallbackTheme;
  }
  document.documentElement.dataset.theme = theme;
})();
