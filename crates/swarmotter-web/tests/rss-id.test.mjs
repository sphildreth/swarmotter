// SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { rssIdBase, uniqueRssId } from "../assets/js/rss-id.js";

assert.equal(rssIdBase("My Feed"), "my-feed");
assert.equal(rssIdBase("Café & Release"), "cafe-release");
assert.equal(rssIdBase("Ünïcode Féed"), "unicode-feed");
assert.equal(rssIdBase("  ...  "), "feed");
assert.equal(rssIdBase(""), "feed");
assert.equal(rssIdBase(null), "feed");
assert.equal(rssIdBase("Dots.and_underscores-v2"), "dots.and_underscores-v2");
assert.equal(rssIdBase("a".repeat(100)).length, 64);

assert.equal(uniqueRssId("My Feed", []), "my-feed");
assert.equal(uniqueRssId("My Feed", ["my-feed"]), "my-feed-2");
assert.equal(uniqueRssId("My Feed", ["my-feed", "my-feed-2"]), "my-feed-3");
assert.equal(uniqueRssId("My Feed", ["MY-FEED"]), "my-feed-2");
assert.equal(uniqueRssId("Café & Release", ["cafe-release"]), "cafe-release-2");
