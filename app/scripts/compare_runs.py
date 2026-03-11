#!/usr/bin/env python3
"""
compare_runs.py
---------------
Compare two dry_run log files and surface what changed between runs.

Buckets:
  NEWLY ADDED    — FILTERED in run 1, WOULD ADD in run 2
  NEWLY FILTERED — WOULD ADD in run 1, FILTERED in run 2
  REASON CHANGED — appeared in both but filter reason changed
  UNCHANGED      — same decision both runs (hidden by default)

Usage:
    python3 compare_runs.py run1.log run2.log
    python3 compare_runs.py run1.log run2.log --show-unchanged
    python3 compare_runs.py run1.log run2.log --output diff.log
"""

import re
import sys
import argparse
from collections import defaultdict
from datetime import datetime


# ── Parsing ───────────────────────────────────────────────────

# Matches lines like:
#   [Company] WOULD ADD role=engineering_manager location='Lehi, UT': 'Title'
#   [Company] FILTERED role=engineer: 'Title'
#   [Company] FILTERED blocked_title: 'Title'
#   [Company] FILTERED location='Remote': 'Title'
#   [Company] FILTERED dept=['Engineering']: 'Title'

LINE_RE = re.compile(
    r"^\[(?P<company>[^\]]+)\]\s+"
    r"(?P<decision>WOULD ADD|FILTERED)\s+"
    r"(?P<reason>[^:]+):\s+"
    r"'(?P<title>.+)'$"
)


def parse_log(path: str) -> dict:
    """
    Parse a dry_run log file.
    Returns dict keyed by (company, title) → {decision, reason, raw_line}
    """
    jobs = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            m = LINE_RE.match(line)
            if not m:
                continue
            key = (m.group("company"), m.group("title"))
            jobs[key] = {
                "decision": m.group("decision"),
                "reason":   m.group("reason").strip(),
                "raw":      line,
            }
    return jobs


# ── Comparison ────────────────────────────────────────────────

def compare(run1: dict, run2: dict):
    all_keys = set(run1) | set(run2)

    newly_added    = []
    newly_filtered = []
    reason_changed = []
    unchanged      = []
    only_in_run1   = []
    only_in_run2   = []

    for key in sorted(all_keys, key=lambda k: (k[0], k[1])):
        company, title = key
        j1 = run1.get(key)
        j2 = run2.get(key)

        if j1 and not j2:
            only_in_run1.append((company, title, j1))
        elif j2 and not j1:
            only_in_run2.append((company, title, j2))
        elif j1["decision"] == "FILTERED" and j2["decision"] == "WOULD ADD":
            newly_added.append((company, title, j1, j2))
        elif j1["decision"] == "WOULD ADD" and j2["decision"] == "FILTERED":
            newly_filtered.append((company, title, j1, j2))
        elif j1["reason"] != j2["reason"]:
            reason_changed.append((company, title, j1, j2))
        else:
            unchanged.append((company, title, j1))

    return {
        "newly_added":    newly_added,
        "newly_filtered": newly_filtered,
        "reason_changed": reason_changed,
        "unchanged":      unchanged,
        "only_in_run1":   only_in_run1,
        "only_in_run2":   only_in_run2,
    }


# ── Output ────────────────────────────────────────────────────

_out_file = None

def out(msg: str = ""):
    print(msg)
    if _out_file:
        _out_file.write(msg + "\n")
        _out_file.flush()


def section(title: str, items: list, formatter):
    out()
    out(f"{'═' * 60}")
    out(f"  {title}  ({len(items)})")
    out(f"{'═' * 60}")
    if not items:
        out("  (none)")
    for item in items:
        formatter(item)


def fmt_newly_added(item):
    company, title, j1, j2 = item
    out(f"  [{company}] {title!r}")
    out(f"    was: {j1['reason']}")
    out(f"    now: {j2['reason']}")


def fmt_newly_filtered(item):
    company, title, j1, j2 = item
    out(f"  [{company}] {title!r}")
    out(f"    was: {j1['reason']}")
    out(f"    now: {j2['reason']}")


def fmt_reason_changed(item):
    company, title, j1, j2 = item
    out(f"  [{company}] {title!r}")
    out(f"    was: {j1['reason']}")
    out(f"    now: {j2['reason']}")


def fmt_only_in(item):
    company, title, j = item
    out(f"  [{company}] ({j['decision']} — {j['reason']}) {title!r}")


def fmt_unchanged(item):
    company, title, j = item
    out(f"  [{company}] ({j['decision']} — {j['reason']}) {title!r}")


# ── Main ──────────────────────────────────────────────────────

def main():
    global _out_file

    parser = argparse.ArgumentParser(description="Compare two dry_run log files")
    parser.add_argument("run1", help="First log file (baseline)")
    parser.add_argument("run2", help="Second log file (new run)")
    parser.add_argument("--show-unchanged", action="store_true", help="Also show unchanged decisions")
    parser.add_argument("--output", dest="output_path", help="Write output to file as well as stdout")
    args = parser.parse_args()

    run1 = parse_log(args.run1)
    run2 = parse_log(args.run2)
    results = compare(run1, run2)

    if args.output_path:
        _out_file = open(args.output_path, "w")

    out(f"DRY RUN COMPARISON")
    out(f"  Run 1 (baseline): {args.run1}  ({len(run1)} jobs)")
    out(f"  Run 2 (new):      {args.run2}  ({len(run2)} jobs)")
    out(f"  Generated:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    section("✓ NEWLY ADDED (were filtered, now passing)", results["newly_added"],    fmt_newly_added)
    section("✗ NEWLY FILTERED (were passing, now filtered)", results["newly_filtered"], fmt_newly_filtered)
    section("~ FILTER REASON CHANGED (same decision, different reason)", results["reason_changed"], fmt_reason_changed)
    section("? ONLY IN RUN 1 (not seen in run 2)", results["only_in_run1"], fmt_only_in)
    section("? ONLY IN RUN 2 (not seen in run 1)", results["only_in_run2"], fmt_only_in)

    if args.show_unchanged:
        section("= UNCHANGED", results["unchanged"], fmt_unchanged)
    else:
        out()
        out(f"  (unchanged: {len(results['unchanged'])} jobs — use --show-unchanged to see them)")

    out()
    out(f"{'─' * 60}")
    out(f"  SUMMARY")
    out(f"  newly added:    {len(results['newly_added'])}")
    out(f"  newly filtered: {len(results['newly_filtered'])}")
    out(f"  reason changed: {len(results['reason_changed'])}")
    out(f"  only in run 1:  {len(results['only_in_run1'])}")
    out(f"  only in run 2:  {len(results['only_in_run2'])}")
    out(f"  unchanged:      {len(results['unchanged'])}")
    out(f"{'─' * 60}")

    if _out_file:
        _out_file.close()
        print(f"\nOutput also written to: {args.output_path}")


if __name__ == "__main__":
    main()