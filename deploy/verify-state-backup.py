#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Locate the active library and verify its archived database and sidecars."""

from contextlib import ExitStack, closing
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import tomllib

SIDECARS = ("", "-wal", "-shm", "-journal")


def container_paths(spec, from_compose=False):
    """Resolve the daemon's state file against its actual container mounts."""

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

    return env, option, host_path


def configured_path(env, option, host_path, key, section, setting):
    if key in env:
        return env[key]
    config = option(("--config", "-c"), env.get("SWARMOTTER_CONFIG"))
    if config:
        with host_path(config).open("rb") as source:
            return tomllib.load(source).get(section, {}).get(setting)
    return None


def active_state_path(spec, from_compose=False):
    env, option, host_path = container_paths(spec, from_compose)
    explicit = option(("--state-file",), env.get("SWARMOTTER_STATE_FILE"))
    if explicit:
        return host_path(explicit)
    state_dir = configured_path(env, option, host_path, "SWARMOTTER_STORAGE__STATE_DIR", "storage", "state_dir")
    default_dir = (env.get("STATE_DIRECTORY") or "/var/lib/swarmotter").split(":")[0]
    return host_path(str(PurePosixPath(state_dir or default_dir) / "state.json"))


def recovery_paths(spec, from_compose=False):
    state = active_state_path(spec, from_compose)
    env, option, host_path = container_paths(spec, from_compose)
    rss = configured_path(env, option, host_path, "SWARMOTTER_RSS__HISTORY_FILE", "rss", "history_file")
    if rss is None:
        return state, Path(rss_history_path(state)), False
    if not isinstance(rss, str) or not rss.strip():
        raise ValueError("rss.history_file must be a nonempty path")
    return state, host_path(rss), True


def active_rss_history_path(spec, from_compose=False):
    return recovery_paths(spec, from_compose)[1]


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


def verify_rss_history(archive_path, rss_file, required=False):
    """Validate an archived RSS history database when one is present.

    Early deployments and installations that never enable RSS have no history
    database, so absence is accepted. A present database must pass SQLite's
    integrity check and expose the schema that stores durable completions.
    """
    with tempfile.TemporaryDirectory() as temporary:
        expected, destination, found = _extract_members(archive_path, rss_file, temporary)
        if not found:
            if required or any(destination.iterdir()):
                raise ValueError("configured RSS history file is absent from backup")
            return False
        path = destination / expected.name
        with path.open("rb") as source:
            header = source.read(16)
        if header != b"SQLite format 3\0":
            raise ValueError("archived RSS history is not a SQLite database")
        with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)) as db:
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


def verify(archive_path, state_file, allow_empty=False, rss_file=None, require_rss=False):
    verify_rss_history(archive_path, rss_file or rss_history_path(state_file), require_rss)
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
            with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True)) as db:
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


def restore_databases(archive, state_file, rss_file, failed_archive, require_rss=False):
    """Called with Compose stopped. Stage both DB families before replacing either."""
    verify(archive, state_file, rss_file=rss_file, require_rss=require_rss)
    targets = [Path(str(path) + suffix) for path in (state_file, rss_file) for suffix in SIDECARS]
    if len(set(targets)) != len(targets):
        raise ValueError("library and RSS history backup paths overlap")
    members = {str(path.relative_to("/")): path for path in targets}
    # Keep the exact failed generation, including WAL contents. An unreadable
    # file or a full backup disk must stop recovery before modifying live files.
    with open(failed_archive, "xb") as output:
        os.fchmod(output.fileno(), 0o600)
        with tarfile.open(fileobj=output, mode="w:gz") as failed:
            for name, path in members.items():
                if path.is_symlink() or (path.exists() and not path.is_file()):
                    raise ValueError(f"expected a regular database file at {path}")
                if path.exists():
                    failed.add(path, arcname=name, recursive=False)
        output.flush()
        os.fsync(output.fileno())
    sync_directory(Path(failed_archive).parent)

    with ExitStack() as stack, tarfile.open(archive, "r:gz") as backup:
        staged = {}
        for entry in backup:
            path = members.get(entry.name)
            if path is None:
                continue
            if path in staged or not entry.isfile():
                raise ValueError(f"unexpected database member {entry.name}")
            staging = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=path.parent)))
            temp = staging / path.name
            with backup.extractfile(entry) as src, temp.open("xb") as dst:
                shutil.copyfileobj(src, dst)
                dst.flush()
                os.fchown(dst.fileno(), entry.uid, entry.gid)
                os.fchmod(dst.fileno(), entry.mode & 0o777)
                os.fsync(dst.fileno())
            staged[path] = temp
        for path in targets:
            if path in staged:
                os.replace(staged[path], path)
            else:
                path.unlink(missing_ok=True)
        for parent in {p.parent for p in targets if p.parent.exists()}:
            sync_directory(parent)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


if __name__ == "__main__":
    if sys.argv[1] == "--restore-databases":
        restore_databases(*sys.argv[2:6], require_rss="--require-rss" in sys.argv[6:])
    elif sys.argv[1] in ("--inspect-recovery", "--compose-recovery"):
        composed = sys.argv[1] == "--compose-recovery"
        spec = json.load(sys.stdin)
        state, rss, explicit = recovery_paths(spec["services"][sys.argv[2]] if composed else spec[0], composed)
        if any("\n" in str(p) for p in (state, rss)):
            raise ValueError("database paths containing newlines cannot be used by this updater")
        print(state, rss, int(explicit), sep="\n")
    elif sys.argv[1] == "--inspect-path":
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
        verify(sys.argv[1], sys.argv[2], "--allow-empty" in options, rss_file, "--require-rss" in options)
