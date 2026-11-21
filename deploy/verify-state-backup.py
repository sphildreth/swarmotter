#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Locate the active library and verify its archived database and sidecars."""

import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import sys
import tarfile
import tempfile


def active_state_path(spec, from_compose=False):
    """Resolve the daemon's state file against its actual container mounts."""
    import tomllib

    if from_compose:
        env = spec.get("environment", {})
        args = spec.get("command") or ["--config", "/etc/swarmotter/swarmotter.toml"]
        workdir = spec.get("working_dir") or "/"
        mounts = [(v["target"], v.get("source") if v["type"] == "bind" else None)
                  for v in spec.get("volumes", [])]
    else:
        config = spec["Config"]
        env = dict(v.split("=", 1) for v in config["Env"] if "=" in v)
        args = config["Cmd"] or []
        workdir = config.get("WorkingDir") or "/"
        mounts = [(v["Destination"], v["Source"]) for v in spec["Mounts"]]
    if isinstance(args, str):
        import shlex
        args = shlex.split(args)

    def host_path(value):
        path = PurePosixPath(os.path.normpath(os.path.join(workdir, value)))
        for target, source in sorted(mounts, key=lambda m: len(PurePosixPath(m[0]).parts), reverse=True):
            if path.is_relative_to(target):
                if source:
                    return Path(source) / path.relative_to(target)
                break
        raise ValueError(f"container path has no inspectable persistent mount: {path}")

    def option(names, default):
        for i, token in enumerate(args):
            if token in names:
                return args[i + 1]
            for name in names:
                if token.startswith(name + "="):
                    return token[len(name) + 1:]
                if name == "-c" and token.startswith(name) and token != name:
                    return token[len(name):]
        return default

    explicit = option(("--state-file",), env.get("SWARMOTTER_STATE_FILE"))
    if explicit:
        return host_path(explicit)
    state_dir = env.get("SWARMOTTER_STORAGE__STATE_DIR")
    config_path = option(("--config", "-c"), env.get("SWARMOTTER_CONFIG"))
    if state_dir is None and config_path:
        with host_path(config_path).open("rb") as source:
            state_dir = tomllib.load(source).get("storage", {}).get("state_dir")
    default_dir = (env.get("STATE_DIRECTORY") or "/var/lib/swarmotter").split(":")[0]
    return host_path(str(PurePosixPath(state_dir or default_dir) / "state.json"))


def active_rss_history_path(spec, from_compose=False):
    """Resolve the RSS history database (custom path or state-file sibling)."""
    import tomllib

    if from_compose:
        env = spec.get("environment", {})
        args = spec.get("command") or ["--config", "/etc/swarmotter/swarmotter.toml"]
        workdir = spec.get("working_dir") or "/"
        mounts = [(v["target"], v.get("source") if v["type"] == "bind" else None)
                  for v in spec.get("volumes", [])]
    else:
        config = spec["Config"]
        env = dict(v.split("=", 1) for v in config["Env"] if "=" in v)
        args = config["Cmd"] or []
        workdir = config.get("WorkingDir") or "/"
        mounts = [(v["Destination"], v["Source"]) for v in spec["Mounts"]]
    if isinstance(args, str):
        import shlex
        args = shlex.split(args)

    def host_path(value):
        path = PurePosixPath(os.path.normpath(os.path.join(workdir, value)))
        for target, source in sorted(mounts, key=lambda m: len(PurePosixPath(m[0]).parts), reverse=True):
            if path.is_relative_to(target):
                if source:
                    return Path(source) / path.relative_to(target)
                break
        raise ValueError(f"container path has no inspectable persistent mount: {path}")

    def option(names, default):
        for i, token in enumerate(args):
            if token in names:
                return args[i + 1]
            for name in names:
                if token.startswith(name + "="):
                    return token[len(name) + 1:]
                if name == "-c" and token.startswith(name) and token != name:
                    return token[len(name):]
        return default

    config_path = option(("--config", "-c"), env.get("SWARMOTTER_CONFIG"))
    if config_path:
        try:
            with host_path(config_path).open("rb") as source:
                explicit = tomllib.load(source).get("rss", {}).get("history_file")
            if explicit:
                return host_path(explicit)
        except (OSError, ValueError, tomllib.TOMLDecodeError):
            pass
    return rss_history_path(active_state_path(spec, from_compose))


def rss_history_path(state_file):
    """Derive the dedicated RSS history database path from the state file."""
    return str(state_file) + ".rss.sqlite"


def _extract_members(archive_path, root_file, temporary):
    expected = PurePosixPath(root_file).relative_to("/")
    names = {str(expected) + suffix for suffix in ("", "-wal", "-shm", "-journal")}
    root = expected.parent
    destination = Path(temporary)
    found = False
    with tarfile.open(archive_path, "r:gz") as archive:
        for entry in archive:
            member = PurePosixPath(entry.name)
            if str(member) not in names:
                continue
            relative = member.relative_to(root)
            if ".." in relative.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError(f"unsupported state backup entry {member}")
            if entry.isdir():
                raise ValueError(f"expected regular state file at {member}")
            found |= member == expected
            path = destination / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(entry) as contents, path.open("xb") as out:
                shutil.copyfileobj(contents, out)
    return expected, destination, found


def verify_rss_history(archive_path, rss_file):
    """Validate an archived RSS history database when one is present.

    Early deployments and installations that never enable RSS have no history
    database, so absence is accepted. A present database must pass SQLite's
    integrity check and expose the schema that stores durable completions.
    """
    with tempfile.TemporaryDirectory() as temporary:
        expected, destination, found = _extract_members(archive_path, rss_file, temporary)
        if not found:
            return False
        path = destination / expected.name
        with path.open("rb") as source:
            header = source.read(16)
        if header != b"SQLite format 3\0":
            raise ValueError("archived RSS history is not a SQLite database")
        with sqlite3.connect(path.as_uri() + "?mode=rw", uri=True) as db:
            if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise ValueError("SQLite integrity check failed for RSS history")
            tables = {row[0] for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
        required = {"feed_state", "listings", "listing_identities",
                    "completions", "completion_aliases", "decisions"}
        missing = required - tables
        if missing:
            raise ValueError(f"archived RSS history is missing tables: {sorted(missing)}")
        return True


def verify(archive_path, state_file, allow_empty=False, rss_file=None):
    expected = PurePosixPath(state_file).relative_to("/")
    names = {str(expected) + suffix for suffix in ("", "-wal", "-shm", "-journal")}
    root = expected.parent
    with tempfile.TemporaryDirectory() as temporary, tarfile.open(archive_path, "r:gz") as archive:
        destination = Path(temporary)
        found = False
        for entry in archive:
            member = PurePosixPath(entry.name)
            if str(member) not in names:
                continue
            relative = member.relative_to(root)
            if ".." in relative.parts or not (entry.isfile() or entry.isdir()):
                raise ValueError(f"unsupported state backup entry {member}")
            if entry.isdir():
                raise ValueError(f"expected regular state file at {member}")
            found |= member == expected
            path = destination / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(entry) as contents, path.open("xb") as out:
                shutil.copyfileobj(contents, out)
        if not found:
            if allow_empty and not any(destination.iterdir()):
                return
            raise ValueError("configured state file is absent from backup")
        path = destination / expected.name
        with path.open("rb") as source:
            header = source.read(16)
        if header == b"SQLite format 3\0":
            with sqlite3.connect(path.as_uri() + "?mode=rw", uri=True) as db:
                if db.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                    raise ValueError(f"SQLite integrity check failed for {path.name}")
                # The authoritative queue row may still be in the WAL. An
                # empty SQLite database also passes integrity_check.
                queue = db.execute("SELECT queue_json FROM queue_state WHERE singleton=1").fetchone()
                if queue is None or not isinstance(json.loads(queue[0]), dict):
                    raise ValueError("archived library has no valid queue state")
                db.execute("SELECT info_hash, torrent_json FROM torrent_records LIMIT 0")
        else:
            with path.open() as source:
                state = json.load(source)
            if not isinstance(state, dict) or not {"version", "queue", "torrents"} <= state.keys():
                raise ValueError("unrecognized legacy state file")
    verify_rss_history(archive_path, rss_file or rss_history_path(state_file))


if __name__ == "__main__":
    if sys.argv[1] == "--inspect-path":
        print(active_state_path(json.load(sys.stdin)[0]))
    elif sys.argv[1] == "--inspect-rss-path":
        print(active_rss_history_path(json.load(sys.stdin)[0]))
    elif sys.argv[1] == "--compose-path":
        print(active_state_path(json.load(sys.stdin)["services"][sys.argv[2]], True))
    elif sys.argv[1] == "--compose-rss-path":
        print(active_rss_history_path(json.load(sys.stdin)["services"][sys.argv[2]], True))
    else:
        options = sys.argv[3:]
        rss_file = None
        if "--rss-file" in options:
            index = options.index("--rss-file")
            rss_file = options[index + 1]
            del options[index:index + 2]
        verify(sys.argv[1], sys.argv[2], "--allow-empty" in options, rss_file)
