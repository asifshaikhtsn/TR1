import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp
import geo_runner as core

ROOT = Path.cwd()
PRIMARY_NEW_BUDGET = 18000
RETRY_BUDGET = 5000
FAST_TIMEOUT = 7
RETRY_TIMEOUT = 12
PRIMARY_CONCURRENCY = 250
RETRY_CONCURRENCY = 180
FAIR_SHARE_RATIO = 0.30


def source_prior(name):
    n = str(name or "").lower()
    if any(k in n for k in ("tested", "checked", "working")):
        return 0.95
    if "dinoz0rg" in n:
        return 0.90
    if any(k in n for k in ("geonode", "proxifly")):
        return 0.84
    if "proxyscrape" in n:
        return 0.80
    if any(k in n for k in ("freeproxylists", "sslproxies", "proxydb", "hidemy", "proxynova")):
        return 0.70
    if any(k in n for k in ("stormsia", "tuanminpay", "kangproxy", "noctiro", "dpangestuw")):
        return 0.58
    return 0.55


def load_previous_quality():
    path = ROOT / "last_run.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        q = data.get("source_quality") or {}
        return q if isinstance(q, dict) else {}
    except Exception:
        return {}


def source_score(name, previous):
    prior = source_prior(name)
    row = previous.get(name) if isinstance(previous, dict) else None
    if not isinstance(row, dict):
        return prior
    tested = int(row.get("tested") or 0)
    healthy = int(row.get("healthy") or 0)
    if tested < 20:
        return prior
    pass_rate = healthy / max(1, tested)
    observed = min(1.0, pass_rate * 4.0)
    return round(0.40 * prior + 0.60 * observed, 5)


def rotate_group(rows, source, slot):
    if len(rows) <= 1:
        return list(rows)
    seed = sum((i + 1) * ord(ch) for i, ch in enumerate(str(source)))
    start = (seed + slot * 9973) % len(rows)
    return rows[start:] + rows[:start]


def smart_select(rows, limit, scores):
    if not rows or limit <= 0:
        return [], {"slot": 0, "sources": {}}
    if len(rows) <= limit:
        return list(rows), {"slot": int(time.time() // 1800), "sources": {}}

    groups = defaultdict(list)
    for row in rows:
        groups[str(row.get("source") or "unknown")].append(row)

    slot = int(time.time() // 1800)
    ordered = {
        source: rotate_group(items, source, slot)
        for source, items in groups.items()
    }
    selected = []
    used = defaultdict(int)
    selected_keys = set()

    # Guarantee broad source coverage first, then spend the rest on historically
    # better sources. This prevents one huge raw collector from consuming a run.
    fair_budget = int(limit * FAIR_SHARE_RATIO)
    per_source_floor = max(25, fair_budget // max(1, len(ordered)))
    for source in sorted(ordered, key=lambda s: (-scores.get(s, 0.5), s)):
        take = min(per_source_floor, len(ordered[source]))
        for row in ordered[source][:take]:
            key = (row["address"], row["protocol"])
            if key not in selected_keys:
                selected.append(row)
                selected_keys.add(key)
                used[source] += 1
                if len(selected) >= limit:
                    break
        if len(selected) >= limit:
            break

    remaining = limit - len(selected)
    ranked = sorted(ordered, key=lambda s: (-scores.get(s, 0.5), s))
    cursor = {s: used[s] for s in ranked}
    chunk = max(100, min(750, limit // max(4, len(ranked))))

    while remaining > 0:
        progressed = False
        for source in ranked:
            items = ordered[source]
            start = cursor[source]
            if start >= len(items):
                continue
            take = min(chunk, remaining, len(items) - start)
            for row in items[start:start + take]:
                key = (row["address"], row["protocol"])
                if key in selected_keys:
                    continue
                selected.append(row)
                selected_keys.add(key)
                used[source] += 1
                remaining -= 1
                progressed = True
                if remaining <= 0:
                    break
            cursor[source] = start + take
            if remaining <= 0:
                break
        if not progressed:
            break

    meta = {
        "slot": slot,
        "sources": dict(sorted(used.items(), key=lambda x: (-x[1], x[0]))),
    }
    return selected, meta


def count_by_source(rows):
    out = defaultdict(int)
    for row in rows:
        out[str(row.get("source") or "unknown")] += 1
    return out


async def validate_tuned(records, health, reachable, timeout_s, concurrency, existing=False):
    old_timeout = core.REQUEST_TIMEOUT
    try:
        core.REQUEST_TIMEOUT = timeout_s
        return await core.validate(
            records,
            health,
            reachable,
            existing=existing,
            concurrency=concurrency,
        )
    finally:
        core.REQUEST_TIMEOUT = old_timeout


async def scrape_all(module, mode):
    scraped = []
    per_source = {}
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if mode == "walla":
            sources = list(module.SOURCES)
            results = await asyncio.gather(*(
                module.scrape_source(session, src) for src in sources
            ), return_exceptions=True)
            for src, items in zip(sources, results):
                if isinstance(items, Exception):
                    items = []
                name = str(src.get("name") or src.get("id") or "source")
                per_source[name] = len(items)
                for row in items:
                    scraped.append({
                        "address": row.get("address"),
                        "protocol": row.get("protocol") or "HTTP",
                        "country": row.get("country") or "",
                        "source": name,
                    })
        else:
            scrapers = list(module.SCRAPERS)
            names = getattr(module, "SOURCE_NAMES", {})
            results = await asyncio.gather(*(
                scraper(session) for scraper in scrapers
            ), return_exceptions=True)
            for scraper, items in zip(scrapers, results):
                if isinstance(items, Exception):
                    items = []
                name = str(names.get(scraper) or getattr(scraper, "__name__", "source"))
                per_source[name] = len(items)
                for row in items:
                    scraped.append({
                        "address": row.get("address"),
                        "protocol": row.get("protocol") or "HTTP",
                        "country": row.get("country") or "",
                        "source": name,
                    })
    return core.dedup_records(scraped), per_source


async def run_source_pool(module, mode):
    print(f"[PoolV2.2-Smart] {mode}: persistent revalidation -> source scoring -> fast pass -> selective slow retry -> merge")
    health = core.HealthStore()
    reachable = await core.preflight_urls()

    existing = core.load_country_tree()
    still, dead_existing, old_inconclusive = await validate_tuned(
        existing,
        health,
        reachable,
        FAST_TIMEOUT,
        PRIMARY_CONCURRENCY,
        existing=True,
    )
    print(
        f"[Persistent] loaded={len(existing)}, healthy={len(still)}, "
        f"dead={len(dead_existing)}, inconclusive={len(old_inconclusive)}"
    )

    scraped, per_source = await scrape_all(module, mode)
    still_keys = {(row["address"], row["protocol"]) for row in still}
    eligible_all = []
    quarantine_skipped = 0
    for row in scraped:
        key = (row["address"], row["protocol"])
        if key in still_keys:
            continue
        if health.quarantined(row["address"], row["protocol"]):
            quarantine_skipped += 1
            continue
        eligible_all.append(row)

    previous_quality = load_previous_quality()
    source_names = set(per_source) | {str(r.get("source") or "unknown") for r in eligible_all}
    scores = {name: source_score(name, previous_quality) for name in source_names}

    candidates, selection_meta = smart_select(
        eligible_all,
        min(PRIMARY_NEW_BUDGET, len(eligible_all)),
        scores,
    )
    print(
        f"[New] scraped_unique={len(scraped)}, eligible={len(eligible_all)}, "
        f"primary_test={len(candidates)}, quarantine_skipped={quarantine_skipped}"
    )

    first_working, first_failed, first_inconclusive = await validate_tuned(
        candidates,
        health,
        reachable,
        FAST_TIMEOUT,
        PRIMARY_CONCURRENCY,
        existing=False,
    )

    retry_candidates, retry_meta = smart_select(
        first_failed,
        min(RETRY_BUDGET, len(first_failed)),
        scores,
    )

    # First-pass failure should count only once. Clear the temporary mark before
    # slow retry; the retry result becomes the final health decision for that row.
    for row in retry_candidates:
        health.success(row["address"], row["protocol"])

    retry_working, retry_failed, retry_inconclusive = await validate_tuned(
        retry_candidates,
        health,
        reachable,
        RETRY_TIMEOUT,
        RETRY_CONCURRENCY,
        existing=False,
    )

    working_new = core.dedup_records(first_working + retry_working)
    recovered_keys = {(r["address"], r["protocol"]) for r in retry_working}
    final_failed = [
        row for row in first_failed
        if (row["address"], row["protocol"]) not in recovered_keys
    ]

    merged = core.dedup_records(still + working_new)
    unresolved = await core.apply_country(merged, only_missing=False)
    country_counts, protocol_counts = core.write_country_tree(merged, root_all_files=False)
    health.save()

    eligible_count = count_by_source(eligible_all)
    tested_count = count_by_source(candidates)
    first_good_count = count_by_source(first_working)
    retry_count = count_by_source(retry_candidates)
    retry_good_count = count_by_source(retry_working)
    final_good_count = count_by_source(working_new)

    quality = {}
    for name in sorted(source_names):
        tested = tested_count.get(name, 0)
        healthy = final_good_count.get(name, 0)
        quality[name] = {
            "scraped": int(per_source.get(name, 0)),
            "eligible": int(eligible_count.get(name, 0)),
            "tested": int(tested),
            "healthy_fast": int(first_good_count.get(name, 0)),
            "retry_tested": int(retry_count.get(name, 0)),
            "retry_recovered": int(retry_good_count.get(name, 0)),
            "healthy": int(healthy),
            "pass_rate": round(healthy / tested, 4) if tested else 0.0,
            "score_used": round(scores.get(name, 0.5), 4),
        }

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "pool-v2.2-smart",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "validation_policy": {
            "primary_budget": PRIMARY_NEW_BUDGET,
            "primary_timeout_seconds": FAST_TIMEOUT,
            "primary_concurrency": PRIMARY_CONCURRENCY,
            "retry_budget": RETRY_BUDGET,
            "retry_timeout_seconds": RETRY_TIMEOUT,
            "retry_concurrency": RETRY_CONCURRENCY,
            "selection": "30% fair-source coverage + 70% quality-ranked rotating fill",
        },
        "sources": sorted(per_source),
        "per_source": dict(sorted(per_source.items(), key=lambda x: -x[1])),
        "source_quality": quality,
        "total_scraped": len(scraped),
        "eligible_new": len(eligible_all),
        "validated": len(candidates),
        "validation_attempts": len(candidates) + len(retry_candidates),
        "retry_tested": len(retry_candidates),
        "retry_recovered": len(retry_working),
        "selection_slot": selection_meta.get("slot"),
        "selected_per_source": selection_meta.get("sources", {}),
        "retry_selected_per_source": retry_meta.get("sources", {}),
        "still_working": len(still),
        "working_new": len(working_new),
        "working": len(merged),
        "dead_existing": len(dead_existing),
        "dead_new": len(final_failed),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "quarantine_skipped": quarantine_skipped,
        "inconclusive_new": len(first_inconclusive) + len(retry_inconclusive),
        "geolocated": len(merged) - unresolved,
        "stored_count": len(merged),
        "no_country_count": unresolved,
        "country_count": len(country_counts),
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python pool_runner_v22.py <aggregator-script>")
    module = core.load_target(sys.argv[1])
    if hasattr(module, "SCRAPERS") and hasattr(module, "SOURCE_NAMES"):
        await run_source_pool(module, "habibi")
    elif hasattr(module, "scrape_source") and hasattr(module, "SOURCES"):
        await run_source_pool(module, "walla")
    else:
        raise SystemExit("Pool v2.2 supports Walla/Habibi source aggregators only")


if __name__ == "__main__":
    asyncio.run(main())
