#!/usr/bin/env python3
"""
VENTURE PIPELINE — Executes DAG configs for ANY venture type.

One script, all ventures. Reads DAG configs from auto_ops/dag_plans/,
filters by --venture (or runs all), executes each phase, routes output
to the right tools (engagement_bait_converter, content_repurposer, etc.).

Replaces 10 copy-paste pipeline scripts with ONE parameterized script.

Cron: 0 8 * * * (daily 8 AM, runs all ventures in sequence)

Usage:
    python3 AUTOMATIONS/venture_pipeline.py --run                    # All ventures
    python3 AUTOMATIONS/venture_pipeline.py --run --venture CONTENT   # One venture
    python3 AUTOMATIONS/venture_pipeline.py --run --limit 10          # Cap per venture
    python3 AUTOMATIONS/venture_pipeline.py --status                  # Show counts
    python3 AUTOMATIONS/venture_pipeline.py --dry-run                 # Preview
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import PROJECT, safe_path, recall_skills_for_task, capture_skill_from_result

AUTOMATIONS = PROJECT / "AUTOMATIONS"
DAG_DIR = AUTOMATIONS / "auto_ops" / "dag_plans"
POSTING_QUEUE = PROJECT / "CONTENT" / "social" / "posting_queue"
LOG_FILE = AUTOMATIONS / "logs" / "venture_pipeline_content.log"
PYTHON = sys.executable or "python3"

# Existing content tools to route to
EB_CONVERTER = AUTOMATIONS / "engagement_bait_converter.py"
CONTENT_REPURPOSER = AUTOMATIONS / "content_repurposer.py"


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [CONTENT-PIPE] [{level}] {msg}"
    print(line)
    safe_path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(safe_path(LOG_FILE), "a") as f:
        f.write(line + "\n")


def load_dags(venture: str = "") -> list[dict]:
    """Load DAG configs, optionally filtered by venture type."""
    dags = []
    for f in sorted(DAG_DIR.glob("dag_*.json")):
        try:
            d = json.loads(f.read_text())
            if venture and d.get("venture", "").upper() != venture.upper():
                continue
            d["_file"] = f.name
            dags.append(d)
        except Exception:
            continue
    return dags


def execute_dag(dag: dict, dry_run: bool = False) -> dict:
    """Execute a single content DAG config."""
    method = dag.get("method", "unknown")[:80]
    phases = dag.get("phases", [])
    result = {"method": method, "phases_run": 0, "steps_run": 0, "status": "pending"}

    log(f"Executing: {method} ({len(phases)} phases)")

    for phase in phases:
        phase_name = phase.get("name", "?")
        steps = phase.get("steps", [])
        parallel = phase.get("parallel", False)

        for step in steps:
            if dry_run:
                log(f"  [DRY] {phase_name}: {step[:60]}")
                result["steps_run"] += 1
                continue

            # Execute step — route to existing tools when possible
            step_lower = step.lower()
            if any(kw in step_lower for kw in ["engagement_bait", "convert to post", "generate post"]):
                # Route to engagement bait converter
                if EB_CONVERTER.exists():
                    _run_script(str(EB_CONVERTER), ["--limit", "5"])
                    log(f"  [EB] {step[:60]}")
                else:
                    log(f"  [SKIP] EB converter not found", "WARN")
            elif any(kw in step_lower for kw in ["repurpose", "cross-post", "distribute"]):
                # Route to content repurposer
                if CONTENT_REPURPOSER.exists():
                    _run_script(str(CONTENT_REPURPOSER))
                    log(f"  [REPURPOSE] {step[:60]}")
                else:
                    log(f"  [SKIP] Repurposer not found", "WARN")
            elif any(kw in step_lower for kw in ["scrape", "extract", "crawl", "fetch"]):
                # Use claude -p for scraping intelligence
                out, ok = _claude_step(f"For the PRINTMAXX content system: {step}")
                log(f"  [{'OK' if ok else 'FAIL'}] {phase_name}: {step[:50]}")
            else:
                # Generic step via claude -p
                out, ok = _claude_step(step)
                log(f"  [{'OK' if ok else 'FAIL'}] {phase_name}: {step[:50]}")

            result["steps_run"] += 1

        result["phases_run"] += 1

    result["status"] = "complete" if not dry_run else "dry_run"

    # Capture skill
    if not dry_run:
        capture_skill_from_result(
            task=f"content pipeline: {method[:60]}",
            result=f"Ran {result['phases_run']} phases, {result['steps_run']} steps",
            success=True,
        )

    return result


def _claude_step(step: str) -> tuple[str, bool]:
    """Execute a step via claude -p sonnet in bare mode."""
    try:
        r = subprocess.run(
            [PYTHON, "-c",
             f"import subprocess; r = subprocess.run(['claude', '-p', '--model', 'sonnet', "
             f"'Execute concisely: {step[:200]}'], capture_output=True, text=True, cwd='/tmp', "
             f"stdin=__import__('subprocess').DEVNULL); print(r.stdout[:200])"],
            capture_output=True, text=True, timeout=180, cwd="/tmp",
            stdin=subprocess.DEVNULL,
        )
        return r.stdout.strip()[:200], r.returncode == 0
    except Exception as e:
        return str(e), False


def _run_script(script: str, args: list[str] | None = None) -> bool:
    """Run an existing AUTOMATIONS script."""
    cmd = [PYTHON, script]
    if args:
        cmd.extend(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                          cwd=str(PROJECT), stdin=subprocess.DEVNULL)
        return r.returncode == 0
    except Exception:
        return False


def run(venture: str = "", limit: int = 0, dry_run: bool = False) -> None:
    dags = load_dags(venture=venture)
    if limit > 0:
        dags = dags[:limit]

    label = venture or "ALL"
    log(f"Starting {label} pipeline: {len(dags)} DAGs to execute")

    total_steps = 0
    completed = 0
    for i, dag in enumerate(dags, 1):
        log(f"--- DAG {i}/{len(dags)} ---")
        result = execute_dag(dag, dry_run=dry_run)
        total_steps += result["steps_run"]
        if result["status"] in ("complete", "dry_run"):
            completed += 1

    log(f"Pipeline complete: {completed}/{len(dags)} DAGs, {total_steps} total steps")


def status(venture: str = "") -> None:
    dags = load_dags(venture=venture)
    label = venture or "ALL VENTURES"
    print(f"{label} Pipeline Status")
    print(f"  DAG configs: {len(dags)}")
    print(f"  Total phases: {sum(len(d.get('phases', [])) for d in dags)}")
    print(f"  Total steps: {sum(sum(len(p.get('steps', [])) for p in d.get('phases', [])) for d in dags)}")
    if LOG_FILE.exists():
        lines = LOG_FILE.read_text().strip().splitlines()
        print(f"  Log entries: {len(lines)}")
        if lines:
            print(f"  Last run: {lines[-1][:80]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Venture pipeline — execute DAG configs")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--venture", type=str, default="", help="Filter by venture type (CONTENT, APP, OUTBOUND, etc.)")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    if args.status:
        status(venture=args.venture)
    elif args.run or args.dry_run:
        run(venture=args.venture, limit=args.limit, dry_run=args.dry_run)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
