# Swarmotter — Master-Aligned UI Mockups (revision 2)

These five high-fidelity visual mockups are aligned to the **selected Torrents command-center concept**, included here as `torrents_MASTER_reference.png`. They supersede the prior five-screen mockup collection.

## What was corrected

- The brand, global header, current status/telemetry strip, icons, visual spacing, and strip background are reused from the original selected Torrents design, rather than reinterpreted per page. Only the active navigation tab changes.
- Left navigation, central workspace, and right-side inspector use the same geometry across all five views.
- Body surfaces were brought closer to the darker and subtler original reference, with consistent shared controls, buttons, borders, typography and component patterns.
- There are five full-size 1672×941 PNGs, an overview sheet, a source-reference comparison, and static HTML/CSS references.

## Files

- `swarmotter_add_MASTER_aligned.png`
- `swarmotter_network_MASTER_aligned.png`
- `swarmotter_settings_MASTER_aligned.png`
- `swarmotter_watch_MASTER_aligned.png`
- `swarmotter_logs_MASTER_aligned.png`
- `torrents_MASTER_reference.png`
- `swarmotter_six_screen_master_comparison.png`
- `torrents_vs_add_MASTER_alignment.png`
- `add.html`, `network.html`, `settings.html`, `watch.html`, `logs.html`, `style.css` and `master-shell-*.png`

## Important design-to-code note

To guarantee image-to-image identity in these static visual references, the top 145 pixels use a reference-derived shell image. That is **not** the proposed production implementation: implement a *single shared DOM component* for global navigation + telemetry, backed by design tokens, shared SVG icon components and one common layout. For a code implementation, test consistent screenshots of the same shell at identical viewport dimensions on every route. Avoid copying or separately recreating the header for each view.

All torrent counts, names, sample settings, and operational telemetry are illustrative UI data, not verified runtime state.
