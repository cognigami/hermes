"""Restore the five catalogued, paused Hermes conversations; never submit work.

Run from the Herdr workspace where you want the workers:
    uv run python scripts/resume_hermes_workers.py --list
    uv run python scripts/resume_hermes_workers.py --dry-run
    uv run python scripts/resume_hermes_workers.py

Requires the existing Herdr server and saved Pi session files. Does not close
anything, start a server, change focus, or restore running processes. Run once
at a time. On failure, stop and inspect; some tabs may already have opened.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parent.parent
CATALOGUE = Path(__file__).with_name("hermes-workers.json")


def herdr(*args):
    result = subprocess.run(["herdr", *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"herdr {' '.join(args)} failed: {result.stderr.strip()}")
    return json.loads(result.stdout)["result"]


def validate_sessions(workers):
    for worker in workers:
        with Path(worker["session_file"]).open() as session:
            header = json.loads(session.readline())
        if (header.get("type") != "session"
                or header.get("id") != worker["session_id"]
                or Path(header.get("cwd", "")).resolve() != REPO):
            raise RuntimeError(f"Saved session identity/cwd mismatch: {worker['name']}")


def make_plan(workers, workspace, agents, tabs, panes):
    """Validate all conflicts before creating tabs or starting any process."""
    plan = []
    for worker in workers:
        name = worker["name"]
        matches = [a for a in agents if (a.get("agent_session") or {}).get("value")
                   in (worker["session_file"], worker["session_id"])]
        if matches:
            if len(matches) != 1 or matches[0]["workspace_id"] != workspace:
                raise RuntimeError(f"{name}: session already open elsewhere; not duplicating it")
            plan.append((worker, "skip", matches[0]["pane_id"]))
            continue
        if any(a.get("name") == name for a in agents):
            raise RuntimeError(f"{name}: live name belongs to another session; not replacing it")
        existing = [t for t in tabs
                    if t["workspace_id"] == workspace and t.get("label") == name]
        if len(existing) > 1:
            raise RuntimeError(f"{name}: multiple matching tabs; resolve ambiguity first")
        if existing:
            candidates = [p for p in panes if p["tab_id"] == existing[0]["tab_id"]
                          and p["workspace_id"] == workspace]
            if len(candidates) != 1 or candidates[0].get("agent"):
                raise RuntimeError(f"{name}: existing tab is split or occupied; not changing it")
            # Herdr agent start additionally requires an interactive shell prompt;
            # it refuses a foreground command/editor. We never send raw shell input.
            plan.append((worker, "reuse", candidates[0]["pane_id"]))
        else:
            plan.append((worker, "create", None))
    return plan


def restore(plan, workspace, dry_run=False):
    for worker, action, pane in plan:
        name = worker["name"]
        if action == "skip":
            print(f"Already open: {name} ({pane}); no input sent", flush=True)
            continue
        if dry_run:
            print(f"Would {action} tab and resume {name}: {worker['session_id']}")
            continue
        if action == "create":
            created = herdr("tab", "create", "--workspace", workspace,
                            "--cwd", str(REPO), "--label", name, "--no-focus")
            pane = created["root_pane"]["pane_id"]
        print(f"Opening {name} in {pane} (conversation only)", flush=True)
        herdr("agent", "start", name, "--kind", "pi", "--pane", pane,
              "--", "--session", worker["session_file"], "--presets:preset", "eng")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--list", action="store_true", help="Print catalogue; no Herdr access")
    mode.add_argument("--dry-run", action="store_true", help="Inspect only; no tabs/processes created")
    args = parser.parse_args()
    catalogue = json.loads(CATALOGUE.read_text())
    workers = catalogue["workers"]
    workspace = catalogue["workspace_id"]
    if args.list:
        validate_sessions(workers)
        print(f"Paused workers from workspace {workspace}; captured {catalogue['captured_at']}")
        for w in workers:
            print(f"{w['name']}  {w['session_id']}  tab={w['tab_id']} pane={w['pane_id']}")
            print(f"  {w['role']}\n  {w['session_file']}")
        return
    workspace = os.environ.get("HERDR_WORKSPACE_ID")
    if os.environ.get("HERDR_ENV") != "1" or not workspace:
        raise RuntimeError("Run inside the destination Herdr workspace; nothing changed")
    validate_sessions(workers)
    agents = herdr("agent", "list")["agents"]
    tabs = herdr("tab", "list", "--workspace", workspace)["tabs"]
    panes = herdr("pane", "list", "--workspace", workspace)["panes"]
    plan = make_plan(workers, workspace, agents, tabs, panes)
    restore(plan, workspace, args.dry_run)
    print("Work remains PAUSED. No prompts submitted. Coordinator is not relaunched.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        print(f"Stopped: {error}\nNo automatic retry. Inspect any tabs already opened.", file=sys.stderr)
        sys.exit(1)
