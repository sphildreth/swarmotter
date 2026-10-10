// SPDX-License-Identifier: Apache-2.0
//! Exercise the real binary with idle SSE and WebSocket clients at SIGTERM.
#![cfg(unix)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn sigterm_checkpoints_and_exits_with_open_event_clients() {
    let root = std::env::temp_dir().join(format!("swarmotter-shutdown-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let config = root.join("config.toml");
    std::fs::write(
        &config,
        format!(
            r#"
[api]
bind_address = "{addr}"
require_auth = false
[network]
mode = "disabled"
[logging]
file = false
[storage]
download_dir = "{0}/downloads"
incomplete_dir = "{0}/incomplete"
resume_dir = "{0}/resume"
"#,
            root.display()
        ),
    )
    .unwrap();
    let state = root.join("state.sqlite");
    let mut child = Process(
        Command::new(env!("CARGO_BIN_EXE_swarmotterd"))
            .args([
                "--config",
                config.to_str().unwrap(),
                "--state-file",
                state.to_str().unwrap(),
            ])
            .env_remove("SWARMOTTER_CONFIG")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let start = Instant::now();
    let mut sse = loop {
        if let Ok(stream) = TcpStream::connect(addr) {
            break stream;
        }
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "daemon exited at startup"
        );
        assert!(start.elapsed() < Duration::from_secs(10));
        std::thread::sleep(Duration::from_millis(20));
    };
    sse.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    sse.write_all(b"GET /api/v1/events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .unwrap();
    let mut response = [0; 1024];
    let size = sse.read(&mut response).unwrap();
    assert!(String::from_utf8_lossy(&response[..size]).contains("200 OK"));
    let mut ws = TcpStream::connect(addr).unwrap();
    ws.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    ws.write_all(b"GET /api/v1/ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").unwrap();
    let size = ws.read(&mut response).unwrap();
    assert!(String::from_utf8_lossy(&response[..size]).contains("101 Switching Protocols"));
    // SAFETY: the PID is our live child and SIGTERM has no pointer arguments.
    assert_eq!(unsafe { libc::kill(child.0.id() as i32, libc::SIGTERM) }, 0);
    let start = Instant::now();
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "open event clients blocked shutdown"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(state.exists(), "shutdown checkpoint missing");
    let connection = rusqlite::Connection::open(&state).unwrap();
    assert_eq!(
        connection
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    drop(connection);
    drop((sse, ws, child));
    std::fs::remove_dir_all(root).unwrap();
}
