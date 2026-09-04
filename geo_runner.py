import asyncio
import importlib.util
import json
import re
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

from geo_country import geolocate_ips

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    ProxyConnector = None
    HAS_SOCKS = False

ROOT = Path.cwd()
DATA_DIR = ROOT / "data"
COUNTRY_DIR = ROOT / "country"
DEAD_FILE = DATA_DIR / "dead_proxies.json"
ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
PROTOCOLS = ("HTTP", "HTTPS", "SOCKS4", "SOCKS5")

# Health policy: a failed proxy is quarantined, not permanently banned.
# Repeated failures get a longer cooldown so dead proxies are not tested every run.
FAILURE_COOLDOWNS = (2 * 3600, 6 * 3600, 12 * 3600, 24 * 3600)
FAILURE_RECORD_TTL = 14 * 24 * 3600
CONCURRENCY = 150
REQUEST_TIMEOUT = 12

HTTP_TEST_URLS = ("http://example.com/", "http://httpbin.org/ip")
HTTPS_TEST_URLS = ("https://example.com/", "https://httpbin.org/ip")

DATA_DIR.mkdir(parents=True, exist_ok=True)


def normalize_protocol(value, default="HTTP"):
    value = str(value or default).strip().upper().replace("-", "").replace(" ", "")
    if value in PROTOCOLS:
        return value
    if value.startswith("HTTPS"):
        return "HTTPS"
    if value.startswith("SOCKS5"):
        return "SOCKS5"
    if value.startswith("SOCKS4"):
        return "SOCKS4"
    return "HTTP"


def _address(value):
    m = ADDRESS_RE.search(str(value or ""))
    return m.group(1) if m else ""


def _record_key(address, protocol):
    return f"{normalize_protocol(protocol)}|{address}"


class HealthStore:
    """Protocol-aware temporary failure quarantine with legacy dead-list migration."""

    def __init__(self, path=DEAD_FILE):
        self.path = Path(path)
        self.failures = {}
        self.legacy_migrated = 0
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return

        if data.get("version") == 2 and isinstance(data.get("failures"), dict):
            now = time.time()
            for key, row in data["failures"].items():
                if not isinstance(row, dict):
                    continue
                address = _address(row.get("address"))
                protocol = normalize_protocol(row.get("protocol"))
                last_failed = float(row.get("last_failed") or 0)
                if not address or (last_failed and now - last_failed > FAILURE_RECORD_TTL):
                    continue
                self.failures[_record_key(address, protocol)] = {
                    "address": address,
                    "protocol": protocol,
                    "failures": max(1, int(row.get("failures") or 1)),
                    "last_failed": last_failed,
                    "next_retry": float(row.get("next_retry") or 0),
                }
            return

        # Legacy files stored every failed IP forever. Do NOT carry that permanent
        # blacklist into v2 because many public proxies become live again later.
        legacy = data.get("dead", []) if isinstance(data, dict) else []
        if isinstance(legacy, list):
            self.legacy_migrated = len(legacy)
            if self.legacy_migrated:
                print(
                    f"[Health] Migrating legacy permanent dead list: "
                    f"{self.legacy_migrated} entries released for future retest."
                )

    def quarantined(self, address, protocol, now=None):
        now = now or time.time()
        row = self.failures.get(_record_key(address, protocol))
        return bool(row and float(row.get("next_retry") or 0) > now)

    def success(self, address, protocol):
        self.failures.pop(_record_key(address, protocol), None)

    def failure(self, address, protocol):
        key = _record_key(address, protocol)
        now = time.time()
        old = self.failures.get(key) or {}
        count = max(0, int(old.get("failures") or 0)) + 1
        cooldown = FAILURE_COOLDOWNS[min(count - 1, len(FAILURE_COOLDOWNS) - 1)]
        self.failures[key] = {
            "address": address,
            "protocol": normalize_protocol(protocol),
            "failures": count,
            "last_failed": now,
            "next_retry": now + cooldown,
        }

    def active_count(self):
        now = time.time()
        return sum(1 for row in self.failures.values() if float(row.get("next_retry") or 0) > now)

    def save(self):
        now = time.time()
        clean = {}
        for key, row in self.failures.items():
            if now - float(row.get("last_failed") or 0) <= FAILURE_RECORD_TTL:
                clean[key] = row
        self.failures = clean

        active_rows = [
            row for row in self.failures.values()
            if float(row.get("next_retry") or 0) > now
        ]
        # Keep a plain-address dead field for human/backward visibility, but v2
        # logic uses protocol-aware failures above.
        dead_addresses = sorted({row["address"] for row in active_rows})
        payload = {
            "version": 2,
            "policy": {
                "type": "temporary_quarantine",
                "cooldowns_seconds": list(FAILURE_COOLDOWNS),
                "record_ttl_seconds": FAILURE_RECORD_TTL,
                "key": "protocol|ip:port",
            },
            "failures": dict(sorted(self.failures.items())),
            "dead": dead_addresses,
            "active_count": len(active_rows),
            "tracked_count": len(self.failures),
            "updated": now,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


async def _preflight_urls():
    urls = list(dict.fromkeys(HTTP_TEST_URLS + HTTPS_TEST_URLS))
    good = set()
    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async def one(url):
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if 200 <= resp.status < 500:
                        return url
            except Exception:
                return None
            return None

        rows = await asyncio.gather(*(one(url) for url in urls))
        good.update(url for url in rows if url)
    print(f"[Validate] Endpoint preflight: {len(good)}/{len(urls)} reachable")
    return good


def _urls_for_protocol(protocol, reachable):
    proto = normalize_protocol(protocol)
    desired = HTTP_TEST_URLS if proto == "HTTP" else HTTPS_TEST_URLS
    return [url for url in desired if url in reachable]


async def _test_proxy(address, protocol, semaphore, reachable, shared_http_session):
    urls = _urls_for_protocol(protocol, reachable)
    if not urls:
        return None  # inconclusive: validation infrastructure unavailable

    proto = normalize_protocol(protocol)
    async with semaphore:
        for attempt in range(2):
            for url in urls:
                try:
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    if proto in ("SOCKS4", "SOCKS5"):
                        if not HAS_SOCKS:
                            return None
                        connector = ProxyConnector.from_url(f"{proto.lower()}://{address}")
                        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                            async with session.get(url, allow_redirects=True) as resp:
                                if 200 <= resp.status < 400:
                                    return True
                    else:
                        # HTTPS proxy lists normally mean HTTP CONNECT capability;
                        # aiohttp therefore still receives an http:// proxy URL.
                        async with shared_http_session.get(
                            url,
                            proxy=f"http://{address}",
                            timeout=timeout,
                            allow_redirects=True,
                        ) as resp:
                            if 200 <= resp.status < 400:
                                return True
                except Exception:
                    continue
            if attempt == 0:
                await asyncio.sleep(0.15)
    return False


async def _validate(records, health, reachable, existing=False, concurrency=CONCURRENCY):
    if not records:
        return [], [], []

    semaphore = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 3)
    connector = aiohttp.TCPConnector(limit=max(concurrency * 2, 200), ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as http_session:
        tasks = [
            _test_proxy(row["address"], row["protocol"], semaphore, reachable, http_session)
            for row in records
        ]
        results = await asyncio.gather(*tasks)

    working, failed, inconclusive = [], [], []
    for row, result in zip(records, results):
        if result is True:
            health.success(row["address"], row["protocol"])
            working.append(row)
        elif result is False:
            health.failure(row["address"], row["protocol"])
            failed.append(row)
        else:
            inconclusive.append(row)
            if existing:
                # Never delete previously healthy proxies because our validation
                # endpoints/dependency are unavailable in this run.
                working.append(row)

    # Extra safety: if every previously healthy proxy suddenly fails, treat it as
    # a validation incident rather than wiping the entire healthy pool.
    if existing and len(records) >= 20 and not working and len(failed) == len(records):
        print("[Validate] Safeguard: 100% existing-health failure; preserving previous healthy pool for this run.")
        for row in failed:
            health.success(row["address"], row["protocol"])
        working = list(records)
        inconclusive.extend(failed)
        failed = []

    return working, failed, inconclusive


def _load_country_tree():
    rows = []
    if not COUNTRY_DIR.exists():
        return rows
    for cc_dir in COUNTRY_DIR.iterdir():
        if not cc_dir.is_dir():
            continue
        cc = cc_dir.name.upper()
        for path in cc_dir.iterdir():
            if not path.is_file():
                continue
            proto = normalize_protocol(path.stem)
            if proto not in PROTOCOLS:
                continue
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except Exception:
                continue
            for line in lines:
                address = _address(line)
                if address:
                    rows.append({"address": address, "protocol": proto, "country": cc, "source": "existing"})
    return _dedup_records(rows)


def _load_live_json(default_protocol="HTTP"):
    path = DATA_DIR / "live_proxies.json"
    rows = []
    if not path.exists():
        return rows
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return rows
    for item in data.get("proxies", []):
        if not isinstance(item, dict):
            continue
        address = _address(item.get("proxy") or item.get("address"))
        if not address:
            continue
        rows.append({
            "address": address,
            "protocol": normalize_protocol(item.get("protocol"), default_protocol),
            "country": str(item.get("country") or "XX").upper(),
            "source": str(item.get("source") or "existing"),
        })
    return _dedup_records(rows)


def _dedup_records(rows):
    out = []
    seen = set()
    for row in rows:
        address = _address(row.get("address") or row.get("proxy"))
        if not address:
            continue
        proto = normalize_protocol(row.get("protocol"))
        key = (address, proto)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "address": address,
            "protocol": proto,
            "country": str(row.get("country") or "").upper(),
            "source": str(row.get("source") or ""),
        })
    return out


async def _apply_country(records, only_missing=False):
    if not records:
        return 0, 0
    targets = []
    for row in records:
        if only_missing and row.get("country") not in ("", "XX", None):
            continue
        targets.append(row["address"].rsplit(":", 1)[0])
    ips = list(dict.fromkeys(targets))
    if not ips:
        return 0, 0
    country_map = await geolocate_ips(ips)
    changed = 0
    unresolved = 0
    for row in records:
        ip = row["address"].rsplit(":", 1)[0]
        cc = country_map.get(ip)
        if cc:
            if row.get("country") != cc:
                changed += 1
            row["country"] = cc
        elif row.get("country") in ("", None):
            row["country"] = "XX"
            unresolved += 1
        elif row.get("country") == "XX":
            unresolved += 1
    return changed, unresolved


def _write_country_tree(records, root_all_files=False):
    if COUNTRY_DIR.exists():
        shutil.rmtree(COUNTRY_DIR)
    COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

    grouped = defaultdict(set)
    protocol_sets = defaultdict(set)
    for row in records:
        cc = str(row.get("country") or "XX").upper()
        proto = normalize_protocol(row.get("protocol"))
        address = row["address"]
        grouped[(cc, proto)].add(address)
        protocol_sets[proto].add(address)

    country_counts = defaultdict(int)
    protocol_counts = defaultdict(int)
    for (cc, proto), addresses in grouped.items():
        out_dir = COUNTRY_DIR / cc
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{proto.lower()}.txt").write_text(
            "\n".join(sorted(addresses)) + "\n", encoding="utf-8"
        )
        country_counts[cc] += len(addresses)
        protocol_counts[proto] += len(addresses)

    if root_all_files:
        for proto in PROTOCOLS:
            addresses = protocol_sets.get(proto, set())
            path = COUNTRY_DIR / f"all_{proto.lower()}.txt"
            path.write_text("\n".join(sorted(addresses)) + "\n" if addresses else "", encoding="utf-8")
        all_addresses = sorted({row["address"] for row in records})
        (COUNTRY_DIR / "all.txt").write_text(
            "\n".join(all_addresses) + "\n" if all_addresses else "", encoding="utf-8"
        )

    return dict(country_counts), dict(protocol_counts)


def _write_live_json(records):
    rows = [
        {
            "proxy": row["address"],
            "country": str(row.get("country") or "XX").upper(),
            "protocol": normalize_protocol(row.get("protocol")),
            **({"source": row.get("source")} if row.get("source") else {}),
        }
        for row in records
    ]
    payload = {"proxies": rows, "count": len(rows), "updated": time.time()}
    for name in ("live_proxies.json", "all_proxies.json"):
        (DATA_DIR / name).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_target(path):
    target = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("proxy_target_module", target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {target}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _run_source_pool(module, mode):
    print(f"[PoolV2] Starting {mode} with persistent healthy + temporary dead quarantine")
    health = HealthStore()
    reachable = await _preflight_urls()

    existing = _load_country_tree()
    print(f"[Persistent] Loaded healthy from previous run: {len(existing)}")
    still_working, dead_existing, existing_inconclusive = await _validate(
        existing, health, reachable, existing=True, concurrency=getattr(module, "CONCURRENCY", CONCURRENCY)
    )
    print(
        f"[Persistent] Revalidated -> healthy={len(still_working)}, "
        f"dead={len(dead_existing)}, inconclusive={len(existing_inconclusive)}"
    )

    per_source = {}
    scraped = []
    timeout = aiohttp.ClientTimeout(total=90)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if mode == "walla":
            sources = list(module.SOURCES)
            results = await asyncio.gather(
                *(module.scrape_source(session, src) for src in sources),
                return_exceptions=True,
            )
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
            results = await asyncio.gather(
                *(scraper(session) for scraper in scrapers),
                return_exceptions=True,
            )
            names = getattr(module, "SOURCE_NAMES", {})
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

    scraped = _dedup_records(scraped)
    still_keys = {(row["address"], row["protocol"]) for row in still_working}
    eligible = []
    quarantine_skipped = 0
    for row in scraped:
        key = (row["address"], row["protocol"])
        if key in still_keys:
            continue
        if health.quarantined(row["address"], row["protocol"]):
            quarantine_skipped += 1
            continue
        eligible.append(row)

    print(
        f"[Scrape] unique={len(scraped)}, eligible_new={len(eligible)}, "
        f"quarantine_skipped={quarantine_skipped}"
    )
    working_new, dead_new, new_inconclusive = await _validate(
        eligible, health, reachable, existing=False, concurrency=getattr(module, "CONCURRENCY", CONCURRENCY)
    )
    print(
        f"[Validate] New -> healthy={len(working_new)}, dead={len(dead_new)}, "
        f"inconclusive={len(new_inconclusive)}"
    )

    merged = _dedup_records(still_working + working_new)
    _, unresolved = await _apply_country(merged, only_missing=False)
    country_counts, protocol_counts = _write_country_tree(merged, root_all_files=False)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "pool-v2",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "sources": sorted(per_source),
        "per_source": dict(sorted(per_source.items(), key=lambda x: -x[1])),
        "total_scraped": len(scraped),
        "validated": len(eligible),
        "still_working": len(still_working),
        "working_new": len(working_new),
        "working": len(merged),
        "dead_existing": len(dead_existing),
        "dead_new": len(dead_new),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "quarantine_skipped": quarantine_skipped,
        "geolocated": len(merged) - unresolved,
        "stored_count": len(merged),
        "no_country_count": unresolved,
        "country_count": len(country_counts),
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def _fetch_boost_source(url):
    timeout = aiohttp.ClientTimeout(total=90)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                if resp.status != 200:
                    print(f"[BoostV2] Source fetch HTTP {resp.status}")
                    return ""
                return await resp.text()
    except Exception as exc:
        print(f"[BoostV2] Source fetch error: {exc}")
        return ""


def _boost_config(module):
    if hasattr(module, "PROTO"):
        proto = normalize_protocol(getattr(module, "PROTO"))
        url = str(getattr(module, "PROXRIPPER_URL", ""))
    elif hasattr(module, "PROXRIPPER_HTTPS"):
        proto = "HTTPS"
        url = str(module.PROXRIPPER_HTTPS)
    else:
        proto = "HTTP"
        url = str(getattr(module, "PROXRIPPER_HTTP", ""))
    skip = max(0, int(getattr(module, "SKIP_FIRST", 0) or 0))
    max_count = int(getattr(module, "MAX_PROXIES", 50000) or 50000)
    return proto, url, skip, max_count


async def _run_boost(module):
    protocol, source_url, base_skip, max_count = _boost_config(module)
    print(
        f"[BoostV2] protocol={protocol}, base_skip={base_skip}, max_new={max_count}; "
        "healthy proxies are revalidated and kept across runs"
    )
    health = HealthStore()
    reachable = await _preflight_urls()

    existing = _load_live_json(protocol)
    # Force this protocol for protocol-specific child repos.
    for row in existing:
        row["protocol"] = protocol
    still_working, dead_existing, existing_inconclusive = await _validate(
        existing, health, reachable, existing=True, concurrency=getattr(module, "CONCURRENCY", 100)
    )
    print(
        f"[BoostV2] Existing -> healthy={len(still_working)}, dead={len(dead_existing)}, "
        f"inconclusive={len(existing_inconclusive)}"
    )

    text = await _fetch_boost_source(source_url)
    all_addresses = []
    seen = set()
    for line in text.splitlines():
        address = _address(line)
        if address and address not in seen:
            seen.add(address)
            all_addresses.append(address)

    if not all_addresses:
        print("[BoostV2] Source empty/unavailable; preserving revalidated healthy list.")
        merged = still_working
        await _apply_country(merged, only_missing=True)
        _write_live_json(merged)
        country_counts, protocol_counts = _write_country_tree(merged, root_all_files=True)
        health.save()
        return

    # Rotate the starting point every hour and keep scanning beyond the old fixed
    # 50k slice until max_count eligible candidates are filled. This fixes the
    # old slice-exhaustion problem without losing each repo's base offset.
    step = max(1000, max_count // 10)
    slot = int(time.time() // 3600)
    start = (base_skip + (slot % max(1, (len(all_addresses) // step) or 1)) * step) % len(all_addresses)
    ordered = all_addresses[start:] + all_addresses[:start]

    still_keys = {(row["address"], protocol) for row in still_working}
    candidates = []
    quarantine_skipped = 0
    for address in ordered:
        if (address, protocol) in still_keys:
            continue
        if health.quarantined(address, protocol):
            quarantine_skipped += 1
            continue
        candidates.append({"address": address, "protocol": protocol, "country": "", "source": "ProxRipper"})
        if len(candidates) >= max_count:
            break

    print(
        f"[BoostV2] source_unique={len(all_addresses)}, rotated_start={start}, "
        f"eligible_to_validate={len(candidates)}, quarantine_skipped={quarantine_skipped}"
    )
    working_new, dead_new, new_inconclusive = await _validate(
        candidates, health, reachable, existing=False, concurrency=getattr(module, "CONCURRENCY", 100)
    )

    merged = _dedup_records(still_working + working_new)
    # Child repos do not need MaxMind secrets. Keep known country on persistent
    # healthy rows and geolocate only newly found/XX rows; Shaikh performs the
    # authoritative MaxMind reclassification later.
    _, unresolved = await _apply_country(merged, only_missing=True)
    _write_live_json(merged)
    country_counts, protocol_counts = _write_country_tree(merged, root_all_files=True)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "boost-v2",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "protocol": protocol,
        "source_unique": len(all_addresses),
        "rotated_start": start,
        "validated": len(candidates),
        "still_working": len(still_working),
        "working_new": len(working_new),
        "working": len(merged),
        "dead_existing": len(dead_existing),
        "dead_new": len(dead_new),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "quarantine_skipped": quarantine_skipped,
        "no_country_count": unresolved,
        "country_count": len(country_counts),
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def _run_shaikh(module):
    print("[ShaikhV2] Aggregating 10 boost repos with protocol normalization + MaxMind final country")
    health = HealthStore()
    reachable = await _preflight_urls()

    existing = _load_live_json("HTTP")
    still_working, dead_existing, existing_inconclusive = await _validate(
        existing, health, reachable, existing=True, concurrency=getattr(module, "CONCURRENCY", CONCURRENCY)
    )
    print(
        f"[ShaikhV2] Existing -> healthy={len(still_working)}, dead={len(dead_existing)}, "
        f"inconclusive={len(existing_inconclusive)}"
    )

    sources = list(module.SOURCES)
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        results = await asyncio.gather(
            *(module.fetch_live(session, repo) for repo, _proto in sources),
            return_exceptions=True,
        )

    per_repo = {}
    child_rows = []
    for (repo, default_proto), items in zip(sources, results):
        if isinstance(items, Exception):
            items = []
        per_repo[repo] = len(items)
        for item in items:
            if not isinstance(item, dict):
                continue
            address = _address(item.get("proxy") or item.get("address"))
            if not address:
                continue
            proto = normalize_protocol(item.get("protocol"), default_proto)
            child_rows.append({
                "address": address,
                "protocol": proto,
                "country": str(item.get("country") or "XX").upper(),
                "source": repo,
            })

    child_rows = _dedup_records(child_rows)
    still_map = {(row["address"], row["protocol"]): row for row in still_working}
    new_rows = []
    seen_new = set()
    for row in child_rows:
        key = (row["address"], row["protocol"])
        if key in still_map:
            continue
        if key in seen_new:
            continue
        seen_new.add(key)
        # A child live_proxies entry was validated by that child in the same/most
        # recent run. Trust it as evidence the proxy recovered, even if Shaikh's
        # old quarantine still had it marked failed.
        health.success(row["address"], row["protocol"])
        new_rows.append(row)

    merged = _dedup_records(still_working + new_rows)
    _, unresolved = await _apply_country(merged, only_missing=False)
    _write_live_json(merged)
    country_counts, protocol_counts = _write_country_tree(merged, root_all_files=True)
    health.save()

    summary = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engine": "shaikh-v2",
        "health_policy": "temporary_quarantine_2h_6h_12h_24h",
        "sources": [repo for repo, _proto in sources],
        "per_repo": per_repo,
        "still_working": len(still_working),
        "new_fetched": len(child_rows),
        "new_deduped": len(new_rows),
        "unique": len(merged),
        "dead_existing": len(dead_existing),
        "dead_total": health.active_count(),
        "dead_tracked": len(health.failures),
        "no_country_count": unresolved,
        "protocol_counts": dict(sorted(protocol_counts.items())),
        "country_count": len(country_counts),
        "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
    }
    (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


async def _fallback(module):
    async def patched_geolocate_batch(*args, **kwargs):
        ips = kwargs.get("ips")
        if ips is None and args:
            ips = args[-1]
        return await geolocate_ips(ips or [])

    if hasattr(module, "geolocate_batch"):
        module.geolocate_batch = patched_geolocate_batch
    await module.main()


async def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python geo_runner.py <aggregator-script>")

    target_path = sys.argv[1]
    module = _load_target(target_path)
    target_name = Path(target_path).name.lower()

    if target_name == "aggregate.py" and hasattr(module, "LIVE_JSON_URL") and hasattr(module, "SOURCES"):
        await _run_shaikh(module)
    elif hasattr(module, "SCRAPERS") and hasattr(module, "SOURCE_NAMES"):
        await _run_source_pool(module, "habibi")
    elif hasattr(module, "scrape_source") and hasattr(module, "SOURCES"):
        await _run_source_pool(module, "walla")
    elif (
        hasattr(module, "PROXRIPPER_URL")
        or hasattr(module, "PROXRIPPER_HTTP")
        or hasattr(module, "PROXRIPPER_HTTPS")
    ):
        await _run_boost(module)
    else:
        print("[Runner] Unknown target shape; using compatibility mode.")
        await _fallback(module)


if __name__ == "__main__":
    asyncio.run(main())
