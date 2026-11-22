#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Compare native-resolution Chromium output with the immutable design targets."""
from pathlib import Path
import argparse
import hashlib
import json

import numpy as np
from PIL import Image, ImageChops

ui = Path(__file__).resolve().parents[4] / "design/ui"
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--live", action="store_true", help="compare direct daemon captures with live API data")
args = parser.parse_args()
ref = ui / "swarmotter_MASTER_aligned_UI_refactor_mockups"
evidence = ui / "swarmotter_UI_refactor_evidence"
out = evidence / "live-daemon" if args.live else evidence
manifest = json.loads((evidence / "reference-manifest.json").read_text())
for name, record in manifest.items():
    assert hashlib.sha256((ref / name).read_bytes()).hexdigest() == record["sha256"], name
metrics = {}
shell = None
nav = None
previous_route = None
layout = json.loads((evidence / "layout-results.json").read_text())["canonical"]
for route in ["torrents", "add", "network", "settings", "watch", "logs"]:
    filename = "torrents_MASTER_reference.png" if route == "torrents" else f"swarmotter_{route}_MASTER_aligned.png"
    reference = Image.open(ref / filename).convert("RGB")
    actual = Image.open(out / "actual" / f"{route}.png").convert("RGB")
    assert actual.size == reference.size == (1672, 941)
    pair = Image.new("RGB", (3344, 941))
    pair.paste(reference)
    pair.paste(actual, (1672, 0))
    diff = ImageChops.difference(reference, actual)
    for directory, image in [("comparisons", pair), ("overlays", Image.blend(reference, actual, .5)), ("diffs", diff)]:
        (out / directory).mkdir(exist_ok=True)
        image.save(out / directory / f"{route}.png", compress_level=6)
    telemetry = actual.crop((0, 59, 1672, 145))
    if shell and not args.live:
        assert not ImageChops.difference(shell, telemetry).getbbox(), f"Telemetry changed on {route}"
    shell = telemetry
    header = actual.crop((0, 0, 1672, 59))
    if nav and not args.live:
        changed = np.any(np.asarray(nav) != np.asarray(header), axis=2)
        # Only the outgoing and incoming active buttons may differ. The native
        # visual comparisons below retain all pixels, including these buttons.
        for active in [previous_route, route]:
            x, y, w, h = layout[active]["nav active"]
            x1, y1 = int(x) - 8, max(0, int(y) - 8)
            changed[y1:int(y+h)+9,x1:int(x+w)+9] = False
        assert not changed.any(), f"Header changed outside the active controls on {route}"
    previous_route = route
    nav = header
    t = route == "torrents"
    left, x1, x2, x3, top, bottom = (9,271,1262,1273,147,928) if t else (10,268,1241,1252,145,917)
    regions = {
        "navigation": (0,0,1672,59), "telemetry": (9,69,1663,135),
        "rail": (left,top,x1-11,bottom), "toolbar": (x1,top,x2,266 if t else 222),
        "main": (x1,266 if t else 222,x2,bottom-60),
        "inspector": (x3,top,1663,bottom), "bottom": (x1,bottom-60,x2,bottom),
    }
    metrics[route] = {}
    for region, box in regions.items():
        pixels = np.asarray(diff.crop(box))
        metrics[route][region] = {"box":box,"mean_absolute_rgb_error":round(float(pixels.mean()),3),
                                  "different_pixels":int(np.any(pixels,axis=2).sum()),"pixels":pixels.shape[0]*pixels.shape[1]}
    samples = [(1000,25),(1050,85),(1050,115),(810,180),(3,750),(1350,900)]
    metrics[route]["samples"] = [{"xy":xy,"reference":reference.getpixel(xy),"actual":actual.getpixel(xy)} for xy in samples]
(out / "region-results.json").write_text(json.dumps(metrics,indent=2)+"\n")
print("Compared six 1672 × 941 images with the supplied references.")
if not args.live:
    print("All six controlled telemetry strips are pixel-identical.")

# Native-size registration strips. The three rows share the (0,0)-(1672,145)
# coordinate system of the source. Full unmasked images remain in comparisons/.
stack = Image.new("RGB", (1672, 435))
for i, path in enumerate([ref / "torrents_MASTER_reference.png", out / "actual/torrents.png", out / "actual/add.png"]):
    stack.paste(Image.open(path).convert("RGB").crop((0,0,1672,145)), (0,145*i))
stack.save(out / "comparisons/shared-shell.png")
