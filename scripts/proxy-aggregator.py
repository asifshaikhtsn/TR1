import asyncio
import json
import re
import shutil
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
COUNTRY_DIR = ROOT / "country"
DATA_DIR = ROOT / "data"
DEAD_FILE = DATA_DIR / "dead_proxies.json"

# Every source the TrafficFlare software uses. Each entry can be:
#   format: "text"      -> ip:port per line (country via ip-api geolocation)
#   format: "json"      -> structured rows with optional country/protocol fields
#   format: "country"   -> per-country files, one fetch per selected country
ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
SCHEME_RE = re.compile(r"(https?|socks[45])://", re.IGNORECASE)
_TEXT_PROTOCOLS = ("HTTP", "HTTPS", "SOCKS4", "SOCKS5")
CONCURRENCY = 100
TIMEOUT = 10
DATA_DIR.mkdir(parents=True, exist_ok=True)

try:
    from aiohttp_socks import ProxyConnector
    HAS_SOCKS = True
except ImportError:
    HAS_SOCKS = False
    ProxyConnector = None


def load_dead_set():
    if DEAD_FILE.exists():
        try:
            return set(json.loads(DEAD_FILE.read_text(encoding="utf-8")).get("dead", []))
        except Exception:
            return set()
    return set()


def save_dead_set(dead_set):
    DEAD_FILE.write_text(json.dumps({"dead": sorted(dead_set), "updated": time.time(), "count": len(dead_set)}, indent=2), encoding="utf-8")


async def test_proxy(address, protocol, semaphore):
    async with semaphore:
        try:
            timeout = aiohttp.ClientTimeout(total=TIMEOUT)
            proto = protocol.upper()
            if proto in ("SOCKS4", "SOCKS5") and HAS_SOCKS:
                connector = ProxyConnector.from_url(f"{proto.lower()}://{address}")
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
            else:
                scheme = "http" if proto in ("HTTP", "HTTPS") else proto.lower()
                proxy_url = f"{scheme}://{address}"
                async with aiohttp.ClientSession(timeout=timeout) as s:
                    async with s.get("http://httpbin.org/ip", proxy=proxy_url, timeout=timeout) as resp:
                        if resp.status == 200:
                            return True
        except Exception:
            pass
    return False


def _pick(row, keys):
    low = {str(k).lower(): v for k, v in row.items()}
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
        lk = str(key).lower()
        if lk in low and low[lk] not in (None, ""):
            return low[lk]
    return ""


def _normalize_protocol(value):
    value = str(value or "").strip().upper().replace("-", "")
    if value in _TEXT_PROTOCOLS:
        return value
    return "HTTP"


SOURCES = [
    # ---- stormsia/proxy-list ----
    {"id": "stormsia-working", "name": "Stormsia Working (mixed)", "format": "text",
     "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/working_proxies.txt"},
    {"id": "stormsia-http", "name": "Stormsia HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/http.txt"},
    {"id": "stormsia-socks4", "name": "Stormsia SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/socks4.txt"},
    {"id": "stormsia-socks5", "name": "Stormsia SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/socks5.txt"},
    # ---- BlacKSnowDot0/Proxy-Pulse ----
    {"id": "proxy-pulse-http", "name": "Proxy Pulse HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/http.txt"},
    {"id": "proxy-pulse-https", "name": "Proxy Pulse HTTPS", "format": "text", "protocol": "HTTPS",
     "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/https.txt"},
    {"id": "proxy-pulse-socks4", "name": "Proxy Pulse SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/socks4.txt"},
    {"id": "proxy-pulse-socks5", "name": "Proxy Pulse SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/socks5.txt"},
    # ---- Thordata/awesome-free-proxy-list ----
    {"id": "thordata-http", "name": "Thordata HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt"},
    {"id": "thordata-https", "name": "Thordata HTTPS", "format": "text", "protocol": "HTTPS",
     "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/https.txt"},
    {"id": "thordata-socks4", "name": "Thordata SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks4.txt"},
    {"id": "thordata-socks5", "name": "Thordata SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/socks5.txt"},
    # ---- VPSLabCloud/VPSLab-Free-Proxy-List ----
    {"id": "vpslab-http", "name": "VPSLab HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/http_all.txt"},
    {"id": "vpslab-socks4", "name": "VPSLab SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks4_all.txt"},
    {"id": "vpslab-socks5", "name": "VPSLab SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/socks5_all.txt"},
    # ---- Argh94/Proxy-List ----
    {"id": "argh94-http", "name": "Argh94 HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/Argh94/Proxy-List/main/HTTP.txt"},
    {"id": "argh94-https", "name": "Argh94 HTTPS", "format": "text", "protocol": "HTTPS",
     "url": "https://raw.githubusercontent.com/Argh94/Proxy-List/main/HTTPS.txt"},
    {"id": "argh94-socks4", "name": "Argh94 SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/Argh94/Proxy-List/main/SOCKS4.txt"},
    {"id": "argh94-socks5", "name": "Argh94 SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/Argh94/Proxy-List/main/SOCKS5.txt"},
    # ---- ProxyScraper/ProxyScraper ----
    {"id": "proxyscraper-http", "name": "ProxyScraper HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt"},
    {"id": "proxyscraper-socks4", "name": "ProxyScraper SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/socks4.txt"},
    {"id": "proxyscraper-socks5", "name": "ProxyScraper SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/socks5.txt"},
    # ---- officialputuid/KangProxy ----
    {"id": "kangproxy-tested", "name": "KangProxy TESTED (mixed)", "format": "text",
     "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/main/xResults/RAW.txt"},
    {"id": "kangproxy-http", "name": "KangProxy HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/main/http/http.txt"},
    {"id": "kangproxy-https", "name": "KangProxy HTTPS", "format": "text", "protocol": "HTTPS",
     "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/main/https/https.txt"},
    {"id": "kangproxy-socks4", "name": "KangProxy SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/main/socks4/socks4.txt"},
    {"id": "kangproxy-socks5", "name": "KangProxy SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/officialputuid/KangProxy/main/socks5/socks5.txt"},
    # ---- tuanminpay/live-proxy ----
    {"id": "tuanminpay-http", "name": "TuanMinPay HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/tuanminpay/live-proxy/master/http.txt"},
    {"id": "tuanminpay-socks4", "name": "TuanMinPay SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/tuanminpay/live-proxy/master/socks4.txt"},
    {"id": "tuanminpay-socks5", "name": "TuanMinPay SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/tuanminpay/live-proxy/master/socks5.txt"},
    # ---- RioMMO/ProxyFree ----
    {"id": "riommo-http", "name": "RioMMO HTTP", "format": "text", "protocol": "HTTP",
     "url": "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/HTTP.txt"},
    {"id": "riommo-socks4", "name": "RioMMO SOCKS4", "format": "text", "protocol": "SOCKS4",
     "url": "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/SOCKS4.txt"},
    {"id": "riommo-socks5", "name": "RioMMO SOCKS5", "format": "text", "protocol": "SOCKS5",
     "url": "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/SOCKS5.txt"},
    # ---- json sources (country/protocol extracted from fields) ----
    {"id": "bes-public-proxy-list", "name": "Bes-js Public Proxy List", "format": "json",
     "url": "https://raw.githubusercontent.com/Bes-js/public-proxy-list/main/proxies_geolocation.json",
     "country_field": "geolocation.countryCode", "protocol_field": "protocol"},
    {"id": "proxifly-free-proxy-list", "name": "Proxifly Free Proxy List", "format": "json",
     "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json",
     "country_field": "country", "protocol_field": "protocol", "latency_field": "latency"},
]


async def fetch_text(session, url):
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return ""
            return await resp.text()
    except Exception:
        return ""


def _nested_get(row, dotted):
    cur = row
    for part in str(dotted).split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return ""
    return cur if cur is not None else ""


def parse_text_proxies(text, default_protocol="HTTP"):
    rows = {}
    for line in text.splitlines():
        m = ADDRESS_RE.search(line)
        if not m:
            continue
        sm = SCHEME_RE.search(line)
        proto = _normalize_protocol(sm.group(1)) if sm else default_protocol
        rows.setdefault(m.group(1), proto)
    return [{"address": a, "protocol": p, "country": ""} for a, p in rows.items()]


def parse_json_proxies(text, src):
    try:
        data = json.loads(text)
    except Exception:
        return []
    if isinstance(data, dict):
        for key in ("proxies", "data", "items", "results"):
            if isinstance(data.get(key), list):
                data = data[key]
                break
        else:
            vals = [v for v in data.values() if isinstance(v, dict)]
            data = vals
    if not isinstance(data, list):
        return []
    rows = []
    for row in data:
        if not isinstance(row, dict):
            text_row = json.dumps(row)
            m = ADDRESS_RE.search(text_row)
            if m:
                rows.append({"address": m.group(1), "protocol": "HTTP", "country": ""})
            continue
        address = _pick(row, ("address", "proxy", "ip_port", "hostport"))
        if not address:
            ip = _pick(row, ("ip", "host", "addr"))
            port = _pick(row, ("port",))
            if ip and port:
                address = "{}:{}".format(ip, port)
        if not address:
            m = ADDRESS_RE.search(json.dumps(row))
            address = m.group(1) if m else ""
        m = ADDRESS_RE.search(str(address))
        if not m:
            continue
        country = str(_nested_get(row, src.get("country_field")) or _pick(row, ("country", "country_code", "countrycode", "cc")) or "").upper()
        protocol = _normalize_protocol(src.get("protocol_field") and _pick(row, (src.get("protocol_field"), "protocol", "type", "scheme")) or _pick(row, ("protocol", "type", "scheme")))
        rows.append({"address": m.group(1), "protocol": protocol, "country": country})
    return rows


async def scrape_source(session, src):
    if src["format"] == "json":
        text = await fetch_text(session, src["url"])
        if not text:
            return []
        return parse_json_proxies(text, src)
    text = await fetch_text(session, src["url"])
    if not text:
        return []
    return parse_text_proxies(text, src.get("protocol", "HTTP"))


async def geolocate_batch(session, ips):
    result = {}
    batches = [ips[i : i + 100] for i in range(0, len(ips), 100)]
    for batch in batches:
        for attempt in range(5):
            status, data = 0, None
            try:
                async with session.post(
                    "http://ip-api.com/batch", json=batch, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    status = resp.status
                    if status == 200:
                        data = await resp.json()
            except Exception:
                pass
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict) and entry.get("status") == "success":
                        result[entry["query"]] = entry.get("countryCode", "").upper()
                break
            await asyncio.sleep(min(15, 3 * (attempt + 1)))
        await asyncio.sleep(1.5)
    return result


async def main():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        all_proxies = []
        seen = set()
        for src in SOURCES:
            items = await scrape_source(session, src)
            for item in items:
                key = (item["address"], item["protocol"])
                if key not in seen:
                    seen.add(key)
                    all_proxies.append(item)

        # --- Validate: dead-first filter + working test (only alive goes to geolocate) ---
        dead_set = load_dead_set()
        print(f"[Validate] Loaded dead list: {len(dead_set)}")
        initial = len(all_proxies)
        filtered = [p for p in all_proxies if p["address"] not in dead_set]
        print(f"[Validate] After dead filter: {initial} -> {len(filtered)} (removed {initial - len(filtered)})")
        if filtered:
            semaphore = asyncio.Semaphore(CONCURRENCY)
            tasks_v = [test_proxy(p["address"], p["protocol"], semaphore) for p in filtered]
            results_v = await asyncio.gather(*tasks_v)
            working = [p for p, ok in zip(filtered, results_v) if ok]
            dead_new = [p["address"] for p, ok in zip(filtered, results_v) if not ok]
            print(f"[Validate] Working: {len(working)}, Dead new: {len(dead_new)}")
            dead_set.update(dead_new)
            save_dead_set(dead_set)
            print(f"[Validate] Dead list total: {len(dead_set)}")
            all_proxies = working
        else:
            print("[Validate] All proxies filtered by dead list")
            save_dead_set(dead_set)
            all_proxies = []

        if not all_proxies:
            print("[Validate] No working proxies, skipping geolocate and saving empty")
            if COUNTRY_DIR.exists():
                shutil.rmtree(COUNTRY_DIR)
            COUNTRY_DIR.mkdir(parents=True, exist_ok=True)
            summary = {
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "sources": [{"id": s["id"], "name": s["name"], "format": s["format"]} for s in SOURCES],
                "total_scraped": initial,
                "validated": len(filtered) if 'filtered' in locals() else 0,
                "working": 0,
                "dead_new": len(dead_new) if 'dead_new' in locals() else 0,
                "dead_total": len(dead_set),
                "geolocated": 0,
                "stored_count": 0,
                "no_country_count": 0,
                "country_count": 0,
                "protocol_counts": {},
                "country_counts": {},
            }
            (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
            print(json.dumps(summary, indent=2))
            return

        ips = list({p["address"].rsplit(":", 1)[0] for p in all_proxies})
        country_map = await geolocate_batch(session, ips)
        for p in all_proxies:
            cc = country_map.get(p["address"].rsplit(":", 1)[0])
            if cc:
                p["country"] = cc

        grouped = defaultdict(set)
        protocol_counts = defaultdict(int)
        no_country_count = 0
        for p in all_proxies:
            if not p["country"]:
                no_country_count += 1
                continue
            grouped[(p["country"], p["protocol"])].add(p["address"])
            protocol_counts[p["protocol"]] += 1

        if COUNTRY_DIR.exists():
            shutil.rmtree(COUNTRY_DIR)
        COUNTRY_DIR.mkdir(parents=True, exist_ok=True)

        country_counts = defaultdict(int)
        for (cc, proto), addrs in grouped.items():
            cc_dir = COUNTRY_DIR / cc
            cc_dir.mkdir(parents=True, exist_ok=True)
            (cc_dir / "{}.txt".format(proto.lower())).write_text(
                "\n".join(sorted(addrs)) + "\n", encoding="utf-8"
            )
            country_counts[cc] += len(addrs)

        summary = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "sources": [{"id": s["id"], "name": s["name"], "format": s["format"]} for s in SOURCES],
            "total_scraped": initial,
            "validated": len(filtered),
            "working": len(all_proxies),
            "dead_new": len(dead_new),
            "dead_total": len(dead_set),
            "geolocated": len(country_map),
            "stored_count": sum(len(addrs) for addrs in grouped.values()),
            "no_country_count": no_country_count,
            "country_count": len(country_counts),
            "protocol_counts": dict(sorted(protocol_counts.items())),
            "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
        }
        (ROOT / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())