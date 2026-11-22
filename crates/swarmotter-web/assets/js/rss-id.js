// SPDX-License-Identifier: Apache-2.0

const RSS_ID_MAX_LENGTH = 64;

/** Build the stable, human-readable identifier used by RSS config, API paths,
 * and durable history rows. The Web UI derives this automatically; config and
 * API callers may still supply an explicit id. */
export function rssIdBase(name) {
  const ascii = String(name || "")
    .normalize("NFKD")
    .replace(/\p{M}+/gu, "")
    .toLowerCase();
  const slug = ascii
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^[-_.]+|[-_.]+$/g, "")
    .slice(0, RSS_ID_MAX_LENGTH)
    .replace(/[-_.]+$/g, "");
  return slug || "feed";
}

/** Return an id that does not collide with an existing subscription. Matching
 * is case-insensitive for a predictable UI even though stored ids are not. */
export function uniqueRssId(name, existingIds = []) {
  const base = rssIdBase(name);
  const used = new Set(
    existingIds.map(id => String(id || "").toLowerCase()).filter(Boolean),
  );
  if (!used.has(base.toLowerCase())) return base;
  let suffix = 2;
  while (used.has(`${base}-${suffix}`.toLowerCase())) suffix += 1;
  return `${base}-${suffix}`;
}
