#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0

"""Run the local checks that should pass before opening a SwarmOtter PR.

The default command set mirrors the GitHub Actions `build-test` job that runs on
pull requests:

  - cargo fmt --all -- --check
  - cargo check --workspace --all-targets --all-features
  - cargo clippy --workspace --all-targets --all-features -- -D warnings
  - cargo test --all --all-features

The script uses Rich for progress feedback. Install it with:

    python3 -m pip install rich
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

try:
    from rich.console import Console
    from rich.markup import escape
    from rich.panel import Panel
    from rich.progress import BarColumn
    from rich.progress import Progress
    from rich.progress import SpinnerColumn
    from rich.progress import TaskProgressColumn
    from rich.progress import TextColumn
    from rich.progress import TimeElapsedColumn
    from rich.table import Table
except ModuleNotFoundError:
    print(
        "Missing dependency: rich\n"
        "Install it with: python3 -m pip install rich",
        file=sys.stderr,
    )
    sys.exit(2)


PR_CHECKS = (
    "cargo fmt --all -- --check",
    "cargo check --workspace --all-targets --all-features",
    "cargo clippy --workspace --all-targets --all-features -- -D warnings",
    "cargo test --all --all-features",
)


@dataclass(frozen=True)
class CheckStep:
    name: str
    command: list[str]
    note: str = ""


@dataclass
class StepResult:
    step: CheckStep
    returncode: int | None
    duration: float
    output_tail: list[str]
    skipped: bool = False
    skip_reason: str = ""

    @property
    def ok(self) -> bool:
        return self.skipped or self.returncode == 0


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[1]


def command_text(command: Iterable[str]) -> str:
    return " ".join(command)


def cargo_command(toolchain: str, args: list[str]) -> list[str]:
    if toolchain == "current":
        return ["cargo", *args]
    return ["cargo", f"+{toolchain}", *args]


def command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def build_steps(args: argparse.Namespace) -> list[CheckStep]:
    steps: list[CheckStep] = []

    if args.toolchain == "stable" and not args.no_update_stable:
        steps.append(
            CheckStep(
                "Update stable Rust toolchain",
                ["rustup", "update", "stable", "--no-self-update"],
                "GitHub Actions installs the current stable toolchain.",
            )
        )

    if args.toolchain != "current" and not args.no_install_rust_components:
        steps.append(
            CheckStep(
                "Install Rust PR components",
                [
                    "rustup",
                    "component",
                    "add",
                    "--toolchain",
                    args.toolchain,
                    "rustfmt",
                    "clippy",
                ],
                "GitHub Actions installs rustfmt and clippy for PR checks.",
            )
        )

    steps.extend(
        [
            CheckStep(
                "Format check",
                cargo_command(args.toolchain, ["fmt", "--all", "--", "--check"]),
                PR_CHECKS[0],
            ),
            CheckStep(
                "Workspace check",
                cargo_command(
                    args.toolchain,
                    ["check", "--workspace", "--all-targets", "--all-features"],
                ),
                PR_CHECKS[1],
            ),
            CheckStep(
                "Clippy",
                cargo_command(
                    args.toolchain,
                    [
                        "clippy",
                        "--workspace",
                        "--all-targets",
                        "--all-features",
                        "--",
                        "-D",
                        "warnings",
                    ],
                ),
                PR_CHECKS[2],
            ),
            CheckStep(
                "Test",
                cargo_command(args.toolchain, ["test", "--all", "--all-features"]),
                PR_CHECKS[3],
            ),
        ]
    )

    if args.docs:
        mdbook_version = os.environ.get("DOCS_MDBOOK_VERSION", "0.5.0")
        mermaid_version = os.environ.get("DOCS_MDBOOK_MERMAID_VERSION", "0.17.0")
        if args.install_doc_tools:
            steps.extend(
                [
                    CheckStep(
                        "Install mdBook",
                        cargo_command(
                            args.toolchain,
                            ["install", "mdbook", "--version", mdbook_version, "--locked"],
                        ),
                        "Main-branch/docs CI installs mdbook before building docs.",
                    ),
                    CheckStep(
                        "Install mdBook Mermaid",
                        cargo_command(
                            args.toolchain,
                            [
                                "install",
                                "mdbook-mermaid",
                                "--version",
                                mermaid_version,
                                "--locked",
                            ],
                        ),
                        "Main-branch/docs CI installs mdbook-mermaid before building docs.",
                    ),
                ]
            )
        steps.append(
            CheckStep(
                "Build mdBook",
                ["mdbook", "build"],
                "Optional: docs-site is skipped for pull_request but runs on main/workflow_dispatch.",
            )
        )

    if args.docker:
        steps.append(
            CheckStep(
                "Build Docker image",
                [
                    "docker",
                    "build",
                    "-f",
                    "deploy/Dockerfile",
                    "-t",
                    args.docker_tag,
                    ".",
                ],
                "Optional: container-image is skipped for pull_request but runs on main/workflow_dispatch.",
            )
        )

    return steps


def missing_prerequisite(step: CheckStep, args: argparse.Namespace) -> str | None:
    executable = step.command[0]
    if executable == "cargo":
        return None if command_exists("cargo") else "cargo is not installed or not on PATH"
    if executable == "rustup":
        return None if command_exists("rustup") else "rustup is not installed or not on PATH"
    if executable == "mdbook" and not command_exists("mdbook"):
        if args.install_doc_tools:
            return None
        return "mdbook is not installed; rerun with --install-doc-tools or skip --docs"
    if executable == "docker":
        return None if command_exists("docker") else "docker is not installed or not on PATH"
    return None if command_exists(executable) else f"{executable} is not installed or not on PATH"


def run_step(
    step: CheckStep,
    *,
    cwd: Path,
    env: dict[str, str],
    tail_lines: int,
    verbose: bool,
    console: Console,
) -> StepResult:
    started = time.monotonic()
    tail: deque[str] = deque(maxlen=tail_lines)
    process = subprocess.Popen(
        step.command,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        clean = line.rstrip("\n")
        tail.append(clean)
        if verbose:
            console.print(clean, markup=False, highlight=False)
    returncode = process.wait()
    return StepResult(
        step=step,
        returncode=returncode,
        duration=time.monotonic() - started,
        output_tail=list(tail),
    )


def print_failure(console: Console, result: StepResult) -> None:
    command = command_text(result.step.command)
    body = [
        f"[bold]Command:[/bold] {escape(command)}",
        f"[bold]Exit code:[/bold] {result.returncode}",
    ]
    if result.output_tail:
        body.extend(
            ["", "[bold]Output tail:[/bold]", *[escape(line) for line in result.output_tail]]
        )
    console.print(
        Panel.fit(
            "\n".join(body),
            title=f"[red]Failed: {result.step.name}[/red]",
            border_style="red",
        )
    )


def print_summary(console: Console, results: list[StepResult]) -> None:
    table = Table(title="PR Precheck Summary", show_lines=False)
    table.add_column("Step", style="bold")
    table.add_column("Result")
    table.add_column("Duration", justify="right")
    table.add_column("Command", overflow="fold")

    for result in results:
        if result.skipped:
            status = f"[yellow]SKIPPED[/yellow] {result.skip_reason}"
        elif result.returncode == 0:
            status = "[green]PASS[/green]"
        else:
            status = f"[red]FAIL[/red] ({result.returncode})"
        table.add_row(
            result.step.name,
            status,
            f"{result.duration:.1f}s",
            command_text(result.step.command),
        )
    console.print(table)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run SwarmOtter PR prechecks with Rich progress feedback.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_root_from_script(),
        help="Repository root to run checks from.",
    )
    parser.add_argument(
        "--toolchain",
        default="stable",
        help="Rust toolchain for cargo commands. Use 'current' to omit +toolchain.",
    )
    parser.add_argument(
        "--no-update-stable",
        action="store_true",
        help="Do not run 'rustup update stable' before checks.",
    )
    parser.add_argument(
        "--no-install-rust-components",
        action="store_true",
        help="Do not ensure rustfmt/clippy are installed for the selected toolchain.",
    )
    parser.add_argument(
        "--docs",
        action="store_true",
        help="Also run the main/workflow_dispatch docs-site mdBook build.",
    )
    parser.add_argument(
        "--install-doc-tools",
        action="store_true",
        help="Install CI-pinned mdbook/mdbook-mermaid versions before --docs.",
    )
    parser.add_argument(
        "--docker",
        action="store_true",
        help="Also run a local Docker image build.",
    )
    parser.add_argument(
        "--docker-tag",
        default="swarmotter:pr-precheck",
        help="Tag to use for --docker builds.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Continue running later checks after a failure.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Stream command output while checks run.",
    )
    parser.add_argument(
        "--tail-lines",
        type=int,
        default=120,
        help="Number of output lines to keep and print for failed commands.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the checks that would run and exit.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    console = Console()
    repo_root = args.repo_root.resolve()
    if not (repo_root / "Cargo.toml").exists():
        console.print(f"[red]Cargo.toml not found under {repo_root}[/red]")
        return 2

    steps = build_steps(args)
    if args.list:
        for step in steps:
            console.print(f"[bold]{step.name}[/bold]: {command_text(step.command)}")
            if step.note:
                console.print(f"  [dim]{step.note}[/dim]")
        return 0

    env = os.environ.copy()
    env.setdefault("CARGO_TERM_COLOR", "always")
    results: list[StepResult] = []

    console.print(
        Panel.fit(
            "\n".join(
                [
                    "[bold]SwarmOtter PR prechecks[/bold]",
                    f"Repository: {repo_root}",
                    f"Toolchain: {args.toolchain}",
                    "Default checks mirror .github/workflows/ci.yml build-test.",
                ]
            ),
            border_style="cyan",
        )
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        overall = progress.add_task("Running checks", total=len(steps))
        for step in steps:
            missing = missing_prerequisite(step, args)
            if missing:
                result = StepResult(
                    step=step,
                    returncode=None,
                    duration=0.0,
                    output_tail=[],
                    skipped=False,
                    skip_reason=missing,
                )
                results.append(result)
                progress.advance(overall)
                console.print(
                    Panel.fit(
                        missing,
                        title=f"[red]Cannot run: {step.name}[/red]",
                        border_style="red",
                    )
                )
                if not args.keep_going:
                    break
                continue

            progress.update(overall, description=f"Running {step.name}")
            console.print(f"[cyan]▶ {step.name}[/cyan] [dim]{command_text(step.command)}[/dim]")
            result = run_step(
                step,
                cwd=repo_root,
                env=env,
                tail_lines=args.tail_lines,
                verbose=args.verbose,
                console=console,
            )
            results.append(result)
            progress.advance(overall)
            if result.returncode == 0:
                console.print(f"[green]✓ {step.name} passed[/green] ({result.duration:.1f}s)")
            else:
                print_failure(console, result)
                if not args.keep_going:
                    break

    print_summary(console, results)
    failed = [result for result in results if not result.ok]
    if failed:
        console.print(f"[red]PR prechecks failed: {len(failed)} step(s).[/red]")
        return 1
    if len(results) != len(steps):
        console.print("[red]PR prechecks stopped before all steps completed.[/red]")
        return 1
    console.print("[green]All selected PR prechecks passed.[/green]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
