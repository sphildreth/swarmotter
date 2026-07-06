# Configuration Design Notes

This document records SwarmOtter's configuration architecture and compatibility
contract. User-facing examples and option reference belong in the published
mdBook page: `../docs/configuration.md`.

The implementation lives in `swarmotter-core::config`.

## Sources

SwarmOtter is configured through a TOML configuration file plus environment
variable overrides. Environment overrides use the `SWARMOTTER_` prefix with
nested fields separated by `__`.

Invalid required configuration must produce clear startup errors. Runtime
updates use two API paths:

- `PATCH /api/v1/settings` for live-safe partial updates.
- `PUT /api/v1/settings` for full config replacement after validation.

## Design constraints

- Defaults should be safe and operator-friendly.
- Strict network containment must require an enforceable data-plane path.
- API auth must require a non-empty token when enabled.
- `GET /api/v1/settings` must redact `api.auth_token`.
- Full config replacement must preserve the existing auth token when the
  request omits it.
- Runtime updates must report fields that require restart.
- Environment overrides must pass through the same validation as file config.

## Compatibility boundaries

Configuration table names, field names, environment override names, defaults,
and validation behavior are release-facing. Breaking changes should follow
`VERSIONING_GUIDE.md`.
- Autopilot control is compatible through `[autopilot].mode`, with exactly
  three values: `disabled`, `observe`, and `act`. Default is `observe`.

Compatibility adapter settings belong under `[compatibility.*]` so optional
adapter surfaces remain isolated from native daemon configuration.

## Maintenance

When configuration behavior changes:

1. Update `swarmotter-core::config` and validation tests.
2. Update any affected API settings handlers.
3. Update `../docs/configuration.md` for user-facing examples and option
   reference.
4. Update this document only when the configuration model or compatibility
   contract changes.
