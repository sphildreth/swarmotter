# SwarmOtter workspace visual specification

The six individual PNGs define their respective views. The original Torrents image controls shared header, navigation and telemetry. The two comparison sheets illustrate the relationship between the approved views. They have different scales. All source filenames, SHA-256 hashes and PNG dimensions are recorded in [the reference manifest](reference-manifest.json).

## Geometry

Coordinates are CSS pixels from the viewport origin at 1672 × 941, device scale one. Right and bottom edges are exclusive. The original raster has soft antialiased edges. Pane bounds correspond to the outer border.

| Region | Reference construction and measured browser bounds |
|---|---|
| Header | Full width, y 0–59. Brand group starts at x 20. Otter box 53 × 53, wordmark starts at x 85. First active tab x 230, width 84, height 33. Theme button and Doctor badge at the right edge. |
| Telemetry | x 9–1663, y 69–135. Single 66 px surface, 9 px radius, one-pixel border. Metric content is vertically centered. Healthy dot 13 px with 10 px halo. Rate glyphs 21 px, statistic glyphs 23 px, sparkline area up to 100 × 26. |
| Torrents panes | x 9–260, 271–1262, 1273–1663. Top 147, bottom 928. All gutters 11. Center header 50 px. Grid body rows approximately 42 px including divider. Center footer y 867–927. |
| Companion panes | x 10–257, 268–1241, 1252–1662. Top 145, bottom 917. Widths 247, 973, 410. Center padding 14 px horizontally, 15 px at top. Heading begins at x 283. |
| Desktop forms | Secondary buttons 35 px. Method tabs 42 px inside a bordered container. Magnet input 54 px. Metric cards 93 px. Inspector footers are pinned for Watch and Logs. |
| Compact layouts | Desktop rails and inspectors narrow before switching to **Sections** and **Inspector** below 1081 px. The grid and long diagnostic tables have their own scroll areas. |

## Surfaces and type

| Use | Design values |
|---|---|
| Header | Nearly black gradient through #0b0d10, #0d0f12 and #0c0e11. Reference interior at (1000,25) is #0d0f12. |
| Telemetry | Dark gradient through #151a1e, #11161a and #101417. Reference interiors at (1050,85) and (1050,115) are #11161a and #101417. |
| Workspace | Companion pane gradient #12191d to #0d1317, border #27353d. Original Torrents workspace has the darker #111417 to #0f1316 treatment. |
| Recessed controls and panels | Companion inputs #0d1417, border #304048. Panel gradient #12191d to #0f151a. Metric gradient #151d21 to #11171c. |
| Content | Primary #eaf0f3, muted #93a5af, accent #35d6c0, positive #55dd96, upload #a77ae9, warning #edbc61, error #f2777b. |
| Typography | Embedded Inter 4.001 Regular, Medium, SemiBold and Bold. Body 13 px, companion titles 23 px, rail labels 12–13 px, metric values 17 px, logs 11 px monospace. Logs, structured entries and URIs use embedded Noto Sans Mono 2.014. |

The package names Inter, Segoe UI and Arial. The source supplies no font file or browser build proving the raster face. Inter is the closest available declared face. It is served locally. The isolated project otter brandmark is the only bitmap used within the working shell.

## Component and icon ownership

The shared shell and accessible SVG symbol definitions are in `crates/swarmotter-web/assets/index.html`. Shared metrics, badges, charts and panel selection are in `assets/js/workspace.js`. Geometry, colors, typography, responsive behavior, panel treatments and focus are in `assets/workspace.css`. The theme preference and existing detailed operations continue to use their original modules.

| Use | Symbol or treatment | Size / stroke |
|---|---|---|
| Network allowance | Circular dot and translucent concentric halo, with adjacent state text | 13 px dot |
| Download / upload | Straight stem and open arrowhead, teal down and purple up | 21 px / 1.65 |
| Torrent count | Database cylinder | 23 px / 1.65 |
| Downloading, paused, seeding | Down, pause bars, check inside circles | 23 px / 1.65 |
| Size and ratio | Drive with front panel, gauge with needle | 23 px / 1.65 |
| Theme | Sun in dark mode, moon in light mode | Existing theme SVG |
| File type | Clapperboard for video, monitor for episode files, music note, archive box or folder | 17 px / 1.75 |
| Source and inspection | RSS waves, folder, clock, page, copy, external link, shield, warning triangle | 17 px / 1.75 |
| Toolbar | Plus, play, pause, refresh, trash, settings and list | 17 px / 1.75 |
| Feed actions | Horizontal three-dot menu in a native disclosure | 17 px / 1.75 |

At the native desktop width, the library name cells and download-rate text use measured optical width adjustments. The original first-row name occupies about 177 px. The unadjusted embedded face occupied about 193 px at the same cap height.

The companion navigations have rasterized active states. The implementation uses a single live active state and the Torrents navigation geometry on every route. Data-specific decisions and the remaining differences from individual references are recorded in [the validation report](VALIDATION_REPORT.md).
