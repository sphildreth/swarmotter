#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Exercise backup recovery with committed WAL pages and malformed archives."""
from contextlib import closing
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

    def test_rss_environment_precedence_and_required_external_mount(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "config.toml").write_text('[rss]\nhistory_file = "/elsewhere/ignored"')
            spec = {"command": ["-c", "/etc/config.toml", "--state-file=/data/library"],
                    "environment": {"SWARMOTTER_RSS__HISTORY_FILE": "/history/rss.db"},
                    "volumes": [{"type": "bind", "target": dst, "source": str(src)}
                                for dst, src in [("/etc", root), ("/data", root / "lib"),
                                                 ("/history", root / "separate")]]}
            state, rss, explicit = helper["recovery_paths"](spec, True)
            self.assertTrue(explicit)
            self.assertEqual(rss, root / "separate/rss.db")
            del spec["environment"]["SWARMOTTER_RSS__HISTORY_FILE"]
            with self.assertRaisesRegex(ValueError, "persistent mount"):
                helper["recovery_paths"](spec, True)
            archive = root / "without-history.tar.gz"
            state.parent.mkdir()
            state.write_text(json.dumps({"version": 1, "torrents": [], "queue": {}}))
            with tarfile.open(archive, "w:gz") as tar:
                tar.add(state, arcname=str(state.relative_to("/")))
            with self.assertRaisesRegex(ValueError, "RSS history file is absent"):
                verify(archive, state, rss_file=rss, require_rss=True)

    def test_candidate_requires_the_expected_local_image_id(self):
        updater = Path(__file__).resolve().parents[1] / "deploy/update-swarmotter.sh"
        script = r"""
            source "$1"
            TARGET_IMAGE=swarmotter:fixture
            LOCAL_IMAGE_ID="$2"
            FIXTURE_IMAGE_ID="$3"
            docker_cmd() {
                [[ "$*" == "image inspect -f {{.Id}} swarmotter:fixture" ]] || return 2
                echo "$FIXTURE_IMAGE_ID"
            }
            verify_local_image
        """
        image = "sha256:" + "1" * 64
        for supplied, accepted in [(image, True), ("sha256:" + "2" * 64, False), ("short", False)]:
            result = subprocess.run(["bash", "-c", script, "_", str(updater), supplied, image],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode == 0, accepted, result.stderr)

    def test_late_upgrade_failure_restores_prior_generation_before_health_check(self):
        updater = Path(__file__).resolve().parents[1] / "deploy/update-swarmotter.sh"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state, rss = root / "library.db", root / "history.db"
            archive = root / "upgrade.tar.gz"
            with closing(sqlite3.connect(state)) as db, db:
                db.executescript("""CREATE TABLE queue_state(singleton INTEGER, queue_json TEXT);
                    INSERT INTO queue_state VALUES (1, '{}');
                    CREATE TABLE torrent_records(info_hash TEXT, torrent_json BLOB);
                    INSERT INTO torrent_records VALUES ('original', '{}'); PRAGMA user_version=2;""")
            with closing(sqlite3.connect(rss)) as db, db:
                for table in ("feed_state", "listings", "listing_identities", "completions",
                              "completion_aliases", "decisions"):
                    db.execute(f"CREATE TABLE {table}(value TEXT)")
                db.execute("INSERT INTO completions VALUES ('before')")
            state.chmod(0o640)
            with tarfile.open(archive, "w:gz") as tar:
                for path in (state, rss):
                    tar.add(path, arcname=str(path.relative_to("/")))
            with open(str(archive) + ".sha256", "wb") as digest:
                subprocess.run(["sha256sum", archive], stdout=digest, check=True)
            with closing(sqlite3.connect(state)) as db, db:
                db.executescript("PRAGMA user_version=3; INSERT INTO torrent_records VALUES ('after', '{}');")
            with closing(sqlite3.connect(rss)) as db, db:
                db.execute("INSERT INTO completions VALUES ('after')")
            # Use real verification, archive and recovery. Simulate only the
            # container boundary and the previous image's schema gate.
            script = r"""
                source "$1"
                updated_service=1
                rollback_tag=old-image
                verified_backup="$2/upgrade.tar.gz"
                recovery_state="$2/library.db"
                recovery_history="$2/history.db"
                require_history=1
                sudo() { "$@"; }
                set_env_image() { echo "image $1" >> "$test_log"; }
                validate_container() { [[ "$TARGET_IMAGE" == old-image ]]; }
                validate_health() {
                    python3 - "$recovery_state" <<'SQL'
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    assert db.execute("PRAGMA user_version").fetchone() == (2,)
    assert db.execute("SELECT info_hash FROM torrent_records").fetchall() == [("original",)]
SQL
                    echo healthy >> "$test_log"
                }
                test_log="$2/operations"
                compose() { echo "$*" >> "$test_log"; }
                rollback 17
            """
            result = subprocess.run(["bash", "-c", script, "_", updater, root],
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 17, result.stdout + result.stderr)
            self.assertEqual((root / "operations").read_text().splitlines(),
                             ["down", "image old-image", "up -d", "healthy"])
            self.assertEqual(state.stat().st_mode & 0o777, 0o640)
            with closing(sqlite3.connect(rss)) as db, db:
                self.assertEqual(db.execute("SELECT value FROM completions").fetchall(), [("before",)])
            failed = root / "upgrade.failed-state.tar.gz"
            with tarfile.open(failed, "r:gz") as tar:
                for path, table in [(state, "torrent_records"), (rss, "completions")]:
                    copy = root / ("saved-" + path.name)
                    copy.write_bytes(tar.extractfile(str(path.relative_to("/"))).read())
                    with closing(sqlite3.connect(copy)) as db, db:
                        self.assertEqual(db.execute(f"SELECT count(*) FROM {table}").fetchone(), (2,))

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
