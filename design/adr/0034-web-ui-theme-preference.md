# ADR-0034: Web UI Theme Preference

## Status

Accepted

## Context

The Web UI originally used a dark-only palette. That is usable for many
operators, but all-black or mostly dark interfaces can be difficult to read for
some users and environments. The UI needs a low-friction light/dark choice
without adding a frontend framework, build step, server-side user profile, or
new daemon configuration surface.

## Decision

Add a header icon button that toggles the Web UI between dark and light themes.
Dark remains the default for first load. The browser stores the selected theme
in `localStorage` under `swarmotter.theme`, and a small inline bootstrap script
sets `data-theme` on the root element before stylesheet loading so the selected
theme applies early.

Themes are implemented with CSS custom properties in the embedded Web UI
stylesheet. Tabulator keeps using the vendored Midnight CSS asset, while
SwarmOtter overrides its visible table colors through the same theme variables.
No server API, daemon configuration, or torrent data-plane behavior changes.

## Consequences

Users can choose a lighter interface when dark mode is uncomfortable or
impractical, and that preference follows the browser profile without creating
new server-side state. The Web UI remains static and framework-light.

Future UI color changes should update the shared theme variables instead of
adding hard-coded dark colors. Any new browser storage keys should be documented
and kept unrelated to credentials or torrent data.

## Related Documents

- [Function-over-form Web UI ADR](0006-function-over-form-web-ui.md)
- [Web UI guide](../../docs/web-ui.md)
