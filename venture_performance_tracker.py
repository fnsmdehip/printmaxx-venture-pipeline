#!/usr/bin/env python3

from __future__ import annotations
"""
PRINTMAXX Venture Performance Tracker - Score methods 0-100, KILL/MAINTAIN/DOUBLE_DOWN.

Usage:
    python3 AUTOMATIONS/venture_performance_tracker.py --recommend     # All ventures scored
    python3 AUTOMATIONS/venture_performance_tracker.py --score METHOD  # Deep score one method
    python3 AUTOMATIONS/venture_performance_tracker.py --log METHOD --revenue X --hours Y
    python3 AUTOMATIONS/venture_performance_tracker.py --history       # Score changes over time
"""

import os
import sys
import csv
import json
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
LEDGER = BASE / "LEDGER"
FINANCIALS = BASE / "FINANCIALS"
PERF_DIR = LEDGER / "VENTURE_PERFORMANCE"


# Default ventures with infrastructure readiness estimates
VENTURES = [
    {"id": "MM001", "name": "Freelance Arbitrage (Fiverr/Upwork)", "category": "freelance",
     "infra_files": ["PRODUCTS/FREELANCE_LISTINGS_READY/FIVERR_GIGS_10.md",
                     "PRODUCTS/FREELANCE_LISTINGS_READY/UPWORK_PROFILES_5.md"],
     "needs_accounts": ["Fiverr", "Upwork"], "scalability": 8, "market_signal": 9},
    {"id": "MM002", "name": "Digital Products (Gumroad)", "category": "digital",
     "infra_files": ["PRODUCTS/GUMROAD_READY_LISTINGS.md"],
     "needs_accounts": ["Gumroad", "Stripe"], "scalability": 10, "market_signal": 8},
    {"id": "MM003", "name": "Local Biz Websites", "category": "services",
     "infra_files": ["MONEY_METHODS/LOCAL_BIZ/templates/"],
     "needs_accounts": ["Stripe"], "scalability": 7, "market_signal": 8},
    {"id": "MM004", "name": "Content Clipping Service", "category": "services",
     "infra_files": ["AUTOMATIONS/clip_automation_pipeline.py",
                     "MONEY_METHODS/CLIPPING_SERVICE/"],
     "needs_accounts": ["Fiverr"], "scalability": 9, "market_signal": 9},
    {"id": "MM005", "name": "Cold Email Outbound", "category": "outbound",
     "infra_files": ["AUTOMATIONS/content_posting/cold_email_sequences_ready.csv",
                     "OPS/COLD_EMAIL_LAUNCH_CHECKLIST.md"],
     "needs_accounts": ["Email provider"], "scalability": 8, "market_signal": 7},
    {"id": "MM006", "name": "POD (Etsy/Redbubble)", "category": "ecom",
     "infra_files": ["PRODUCTS/ECOM_LISTINGS_READY/ETSY_LISTINGS_COMPLETE.md",
                     "PRODUCTS/POD_DESIGNS_50.md"],
     "needs_accounts": ["Etsy", "Redbubble"], "scalability": 7, "market_signal": 6},
    {"id": "MM007", "name": "AI NSFW/Findom", "category": "ai_influencer",
     "infra_files": ["MONEY_METHODS/AI_INFLUENCER/AI_NSFW_EXECUTION_FULL.md"],
     "needs_accounts": ["Fanvue", "Twitter"], "scalability": 9, "market_signal": 8},
    {"id": "MM008", "name": "Content Farm (5 niches)", "category": "content",
     "infra_files": ["AUTOMATIONS/content_posting/"],
     "needs_accounts": ["Twitter", "TikTok", "Instagram"], "scalability": 8, "market_signal": 7},
    {"id": "MM009", "name": "Newsletters (Beehiiv)", "category": "content",
     "infra_files": ["ralph/loops/social_setup/output/T6_newsletter_printmaxxer.md"],
     "needs_accounts": ["Beehiiv"], "scalability": 8, "market_signal": 7},
    {"id": "MM010", "name": "PWA Apps (7 built)", "category": "apps",
     "infra_files": ["ralph/loops/app_factory/output/"],
     "needs_accounts": ["Vercel"], "scalability": 10, "market_signal": 7},
    {"id": "MM011", "name": "Affiliate Marketing", "category": "affiliate",
     "infra_files": ["OPS/AFFILIATE_LAUNCH_CHECKLIST.md"],
     "needs_accounts": ["Various affiliate networks"], "scalability": 9, "market_signal": 8},
    {"id": "MM012", "name": "Programmatic SEO (600 pages)", "category": "seo",
     "infra_files": ["builds/programmatic_seo/"],
     "needs_accounts": ["Cloudflare Pages"], "scalability": 10, "market_signal": 6},
    {"id": "MM013", "name": "AI Agent Services", "category": "services",
     "infra_files": ["MONEY_METHODS/AI_AGENT_SERVICES/AI_AGENT_SERVICES_PLAYBOOK.md"],
     "needs_accounts": ["Fiverr", "Upwork"], "scalability": 8, "market_signal": 9},
    {"id": "MM014", "name": "Prompt Marketplace", "category": "digital",
     "infra_files": ["MONEY_METHODS/PROMPT_MARKETPLACE/PROMPT_MARKETPLACE_PLAYBOOK.md"],
     "needs_accounts": ["PromptBase"], "scalability": 7, "market_signal": 6},
    {"id": "MM015", "name": "Ecom Multi-Platform Distribution", "category": "ecom",
     "infra_files": ["AUTOMATIONS/ecom_distributor.py"],
     "needs_accounts": ["Multiple"], "scalability": 9, "market_signal": 7},
]


def read_csv(path, max_rows=500):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return [row for i, row in enumerate(csv.DictReader(f)) if i < max_rows]
    except Exception:
        return []


def get_accounts():
    """Get account creation status."""
    rows = read_csv(LEDGER / "ACCOUNTS.csv")
    active = {}
    for r in rows:
        platform = r.get("platform", "").strip()
        status = r.get("status", "").strip().upper()
        if platform:
            active[platform.lower()] = status in ("CREATED", "ACTIVE", "WARMED")
    return active


def get_revenue_by_method():
    """Get revenue per method from FINANCIALS."""
    rows = read_csv(FINANCIALS / "REVENUE_TRACKER.csv")
    revenue = {}
    for r in rows:
        method = r.get("method_id", r.get("method", ""))
        try:
            amt = float(str(r.get("amount", r.get("revenue", "0"))).replace("$", "").replace(",", ""))
            revenue[method] = revenue.get(method, 0) + amt
        except (ValueError, TypeError):
            pass
    return revenue


def get_hours_by_method():
    """Get hours invested per method from performance logs."""
    hours = {}
    if PERF_DIR.exists():
        for f in PERF_DIR.glob("*.jsonl"):
            try:
                with open(f) as fh:
                    for line in fh:
                        entry = json.loads(line)
                        mid = entry.get("method_id", "")
                        h = entry.get("hours", 0)
                        hours[mid] = hours.get(mid, 0) + h
            except Exception:
                pass
    return hours


def score_venture(venture, accounts, revenue_map, hours_map):
    """Score a venture 0-100."""
    vid = venture["id"]
    score = 0

    # Revenue (0-30 points)
    rev = revenue_map.get(vid, 0)
    if rev > 500:
        score += 30
    elif rev > 100:
        score += 20
    elif rev > 0:
        score += 10

    # Time ROI (0-20 points)
    hours = hours_map.get(vid, 0)
    if hours > 0 and rev > 0:
        roi = rev / hours
        if roi > 50:
            score += 20
        elif roi > 20:
            score += 15
        elif roi > 5:
            score += 10
    elif hours == 0:
        score += 5  # Hasn't been tried yet - neutral

    # Infrastructure readiness (0-20 points)
    infra_score = 0
    for f in venture.get("infra_files", []):
        path = BASE / f
        if path.exists():
            infra_score += 1
    total_infra = max(len(venture.get("infra_files", [])), 1)
    score += int((infra_score / total_infra) * 15)

    # Account readiness
    needed = venture.get("needs_accounts", [])
    if needed:
        acct_ready = sum(1 for a in needed if accounts.get(a.lower(), False))
        score += int((acct_ready / len(needed)) * 5)

    # Market signal (0-15 points)
    score += venture.get("market_signal", 5)

    # Scalability (0-15 points, scaled to 0-10 from 1-10 input)
    score += venture.get("scalability", 5)

    return min(score, 100)


def get_recommendation(score):
    if score >= 70:
        return "DOUBLE_DOWN"
    elif score >= 20:
        return "MAINTAIN"
    else:
        return "KILL"


def print_recommend():
    """Print all ventures with scores and recommendations."""
    accounts = get_accounts()
    revenue_map = get_revenue_by_method()
    hours_map = get_hours_by_method()

    scored = []
    for v in VENTURES:
        s = score_venture(v, accounts, revenue_map, hours_map)
        r = get_recommendation(s)
        scored.append((v, s, r))

    scored.sort(key=lambda x: -x[1])

    print(f"""
{'='*65}
  VENTURE PERFORMANCE TRACKER — {datetime.now().strftime('%Y-%m-%d')}
{'='*65}
""")

    for rec_type in ["DOUBLE_DOWN", "MAINTAIN", "KILL"]:
        items = [(v, s, r) for v, s, r in scored if r == rec_type]
        marker = {"DOUBLE_DOWN": "🟢", "MAINTAIN": "🟡", "KILL": "🔴"}[rec_type]
        print(f"{marker} {rec_type} (Score {'≥70' if rec_type == 'DOUBLE_DOWN' else '20-70' if rec_type == 'MAINTAIN' else '<20'}):")

        if not items:
            rev_note = "need revenue data" if rec_type == "DOUBLE_DOWN" else "none flagged"
            print(f"  [none yet - {rev_note}]\n")
        else:
            for v, s, r in items:
                rev = revenue_map.get(v["id"], 0)
                rev_str = f"${rev:.0f}" if rev > 0 else "$0"
                infra_count = sum(1 for f in v.get("infra_files", []) if (BASE / f).exists())
                infra_total = len(v.get("infra_files", []))
                acct_note = ", ".join(v.get("needs_accounts", []))
                print(f"  {v['id']} {v['name']:<40} {s:3}/100  rev:{rev_str:>8}  infra:{infra_count}/{infra_total}  needs:{acct_note}")
            print()

    # Top action
    if scored:
        top = scored[0]
        print(f"{'─'*65}")
        print(f"TOP ACTION: Scale {top[0]['name']} (score: {top[1]}/100)")

        # Also show what's blocked
        blocked = [v for v, s, r in scored if any(
            not accounts.get(a.lower(), False) for a in v.get("needs_accounts", [])
        )]
        if blocked:
            print(f"BLOCKED BY ACCOUNTS: {len(blocked)} ventures waiting on account creation")
            print(f"  → Run: OPS/ACCOUNT_CREATION_NOW.md")
    print(f"{'='*65}\n")


def score_one(method_id):
    """Deep score a single method."""
    accounts = get_accounts()
    revenue_map = get_revenue_by_method()
    hours_map = get_hours_by_method()

    venture = next((v for v in VENTURES if v["id"] == method_id), None)
    if not venture:
        print(f"Method {method_id} not found. Available: {', '.join(v['id'] for v in VENTURES)}")
        return

    s = score_venture(venture, accounts, revenue_map, hours_map)
    r = get_recommendation(s)
    rev = revenue_map.get(method_id, 0)
    hours = hours_map.get(method_id, 0)

    print(f"\n{'='*50}")
    print(f"  {venture['name']} ({method_id})")
    print(f"{'='*50}")
    print(f"  Score: {s}/100 → {r}")
    print(f"  Revenue: ${rev:.2f}")
    print(f"  Hours invested: {hours:.1f}h")
    print(f"  ROI: {'${:.2f}/hr'.format(rev/hours) if hours > 0 else 'N/A'}")
    print(f"  Category: {venture['category']}")
    print(f"  Needs accounts: {', '.join(venture.get('needs_accounts', []))}")
    print(f"  Infrastructure files:")
    for f in venture.get("infra_files", []):
        exists = "✓" if (BASE / f).exists() else "✗"
        print(f"    {exists} {f}")
    print(f"{'='*50}\n")


def log_performance(method_id, revenue=0, hours=0, status="active"):
    """Log performance data for a method."""
    os.makedirs(PERF_DIR, exist_ok=True)
    entry = {
        "timestamp": datetime.now().isoformat(),
        "method_id": method_id,
        "revenue": revenue,
        "hours": hours,
        "status": status
    }
    log_file = PERF_DIR / f"{method_id}.jsonl"
    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"✓ Logged: {method_id} revenue=${revenue} hours={hours}")


def print_history():
    """Show score changes over time."""
    if not PERF_DIR.exists():
        print("No performance history yet. Log data with --log first.")
        return

    print(f"\nPerformance History:\n{'─'*50}")
    for f in sorted(PERF_DIR.glob("*.jsonl")):
        method_id = f.stem
        entries = []
        with open(f) as fh:
            for line in fh:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        if entries:
            total_rev = sum(e.get("revenue", 0) for e in entries)
            total_hrs = sum(e.get("hours", 0) for e in entries)
            print(f"  {method_id}: ${total_rev:.2f} revenue, {total_hrs:.1f}h invested, {len(entries)} logs")
    print()


def main():
    args = sys.argv[1:]

    if not args or "--recommend" in args:
        print_recommend()
    elif "--score" in args:
        idx = args.index("--score")
        if idx + 1 < len(args):
            score_one(args[idx + 1])
        else:
            print("Usage: --score METHOD_ID (e.g. MM001)")
    elif "--log" in args:
        idx = args.index("--log")
        method_id = args[idx + 1] if idx + 1 < len(args) else ""
        revenue = 0
        hours = 0
        status = "active"
        for i, a in enumerate(args):
            if a == "--revenue" and i + 1 < len(args):
                revenue = float(args[i + 1])
            if a == "--hours" and i + 1 < len(args):
                hours = float(args[i + 1])
            if a == "--status" and i + 1 < len(args):
                status = args[i + 1]
        if method_id:
            log_performance(method_id, revenue, hours, status)
        else:
            print("Usage: --log METHOD_ID --revenue X --hours Y")
    elif "--history" in args:
        print_history()
    else:
        print("Usage: python3 venture_performance_tracker.py [--recommend|--score ID|--log ID|--history]")


if __name__ == "__main__":
    main()
