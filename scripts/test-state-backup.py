#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise backup recovery with committed WAL pages and malformed archives."""
import io
import json
from pathlib import Path
import runpy
import sqlite3
import subprocess
import tarfile
import tempfile
import unittest

helper = runpy.run_path(str(Path(__file__).resolve().parents[1] / "deploy/verify-state-backup.py"))
verify = helper["verify"]
verify_rss_history = helper["verify_rss_history"]
rss_history_path = helper["rss_history_path"]
active_state_path = helper["active_state_path"]
active_rss_history_path = helper["active_rss_history_path"]


class StateBackupTest(unittest.TestCase):
    def test_active_file_precedence_and_mount_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "daemon.toml").write_text('[storage]\nstate_dir = "/data/custom"\n')
            config = {"Cmd": ["--config", "/etc/daemon.toml"], "Env": []}
            spec = {"Config": config, "Mounts": [
                {"Destination": "/etc", "Source": str(root)},
                {"Destination": "/data", "Source": "/host/data"},
                {"Destination": "/data/custom", "Source": "/fast/state"},
            ]}
            self.assertEqual(active_state_path(spec), Path("/fast/state/state.json"))
            config["Env"] = ["SWARMOTTER_STATE_FILE=/data/env.db"]
            self.assertEqual(active_state_path(spec), Path("/host/data/env.db"))
            config["Cmd"].append("--state-file=/data/cli")
            self.assertEqual(active_state_path(spec), Path("/host/data/cli"))
            config["Cmd"][-1] = "--state-file=/ephemeral/library"
            with self.assertRaisesRegex(ValueError, "persistent mount"):
                active_state_path(spec)
            config["Env"] = []
            config["Cmd"] = ["-c/etc/daemon.toml"]
            self.assertEqual(active_state_path(spec), Path("/fast/state/state.json"))
            (root / "daemon.toml").write_text(
                '[storage]\nstate_dir = "/data/custom"\n[rss]\nhistory_file = "/data/rss/custom.sqlite"\n'
            )
            self.assertEqual(
                active_rss_history_path(spec), Path("/host/data/rss/custom.sqlite")
            )

    def test_updater_stops_writer_and_restarts_it_on_invalid_backup(self):
        updater = Path(__file__).resolve().parents[1] / "deploy/update-swarmotter.sh"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("state", "custom", "config"):
                (root / name).mkdir()
            (root / "compose.yml").touch()
            (root / ".env").write_text("SWARMOTTER_STATE=./custom\nSWARMOTTER_CONFIG_DIR=config\nGLUETUN_STATE_DIR=gluetun\n")
            db = root / "custom" / "library"
            (root / "inspect.json").write_text(json.dumps([{
                "Config": {"Cmd": ["--state-file=/custom/library"], "Env": []},
                "Mounts": [{"Destination": "/custom", "Source": str(root / "custom")}],
            }]))
            script = r"""
                export SWARMOTTER_DEPLOY_DIR="$2" SWARMOTTER_BACKUP_DIR="$2/$3"
                source "$1"
                docker_cmd() {
                    case "$1:$2" in
                        "container:ls") echo swarmotter ;;
                        "inspect:swarmotter") cat "$DEPLOY_DIR/inspect.json" ;;
                        "inspect:-f") cat "$DEPLOY_DIR/running" ;;
                        "stop:--timeout") echo false > "$DEPLOY_DIR/running" ;;
                        "start:swarmotter") echo true > "$DEPLOY_DIR/running" ;;
                        *) return 1 ;;
                    esac
                }
                sudo() {
                    if [[ "$1" == -v ]]; then return; fi
                    if [[ "$1" == tar && $(cat "$DEPLOY_DIR/running") != false ]]; then return 1; fi
                    "$@"
                }
                create_backup
            """
            for backup_dir, valid, vpn_env in (("with-env", True, True),
                                               ("without-env", True, False),
                                               ("invalid", False, False)):
                if vpn_env:
                    (root / "gluetun.env").touch()
                else:
                    (root / "gluetun.env").unlink(missing_ok=True)
                (root / "running").write_text("true")
                db.write_text(json.dumps({"version": 1, "queue": {}, "torrents": []}) if valid else "invalid")
                result = subprocess.run(["bash", "-c", script, "_", updater, root, backup_dir],
                                        capture_output=True, text=True)
                if valid:
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                    self.assertEqual((root / "running").read_text().strip(), "false")
                    archive, = (root / backup_dir).glob("*.tar.gz")
                    verify(archive, db)
                    with tarfile.open(archive, "r:gz") as backup:
                        names = [entry.name for entry in backup]
                    self.assertEqual(sum(name.endswith("/custom/library") for name in names), 1)
                    self.assertEqual(any(name.endswith("/gluetun.env") for name in names), vpn_env)
                else:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual((root / "running").read_text().strip(), "true")
                    self.assertFalse(list((root / backup_dir).iterdir()))

    def test_paired_rss_history_is_verified(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            state = root / "state"
            state.mkdir()
            library = state / "library"
            library.write_text(json.dumps({"version": 1, "queue": {}, "torrents": []}))
            rss = Path(rss_history_path(str(library)))
            db = sqlite3.connect(rss)
            try:
                for table in ("feed_state", "listings", "listing_identities",
                              "completions", "completion_aliases", "decisions"):
                    db.execute(f"CREATE TABLE {table}(x INTEGER)")
                db.commit()
            finally:
                db.close()
            archive = root / "backup.tar.gz"
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(state, arcname="srv/state")
            verify(archive, "/srv/state/library")
            self.assertTrue(verify_rss_history(archive, rss_history_path("/srv/state/library")))
            rss.write_bytes(b"broken")
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(state, arcname="srv/state")
            with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                verify(archive, "/srv/state/library")
            rss.unlink()
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(state, arcname="srv/state")
            verify(archive, "/srv/state/library")
            self.assertFalse(verify_rss_history(archive, rss_history_path("/srv/state/library")))

    def test_wal_and_corruption(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            state = root / "state"
            state.mkdir()
            archive = root / "backup.tar.gz"
            db = sqlite3.connect(state / "state.json")
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("CREATE TABLE torrent_records(info_hash TEXT, torrent_json BLOB)")
                db.execute("CREATE TABLE queue_state(singleton INTEGER PRIMARY KEY, queue_json BLOB)")
                db.execute("INSERT INTO queue_state VALUES(1, '{}')")
                db.commit()
                with tarfile.open(archive, "w:gz") as tar:
                    tar.add(state, arcname="srv/state")
                verify(archive, "/srv/state/state.json")
                with tarfile.open(archive, "w:gz") as tar:
                    tar.add(state / "state.json", arcname="srv/state/state.json")
                with self.assertRaises(sqlite3.DatabaseError):
                    verify(archive, "/srv/state/state.json")
            finally:
                db.close()
            for data in (b"broken", b"SQLite format 3\0" + bytes(256)):
                with tarfile.open(archive, "w:gz") as tar:
                    entry = tarfile.TarInfo("srv/state/state.json")
                    entry.size = len(data)
                    tar.addfile(entry, io.BytesIO(data))
                with self.assertRaises((ValueError, sqlite3.DatabaseError)):
                    verify(archive, "/srv/state/state.json")
            with tarfile.open(archive, "w:gz") as tar:
                directory = tarfile.TarInfo("srv/state")
                directory.type = tarfile.DIRTYPE
                tar.addfile(directory)
            with self.assertRaisesRegex(ValueError, "absent"):
                verify(archive, "/srv/state/state.json")
            with self.assertRaisesRegex(ValueError, "absent"):
                verify(archive, "/srv/missing")


if __name__ == "__main__":
    unittest.main()
