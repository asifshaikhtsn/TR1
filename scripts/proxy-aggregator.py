import asyncio
import json
import re
import time
from collections import defaultdict
from pathlib import Path

import aiohttp

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ROOT / "output"
COUNTRIES_FILE = ROOT / "countries.txt"

TT3_COUNTRIES = ["AE", "AT", "AU", "BE", "CA", "CH", "DE", "DK", "ES",
                 "FI", "FR", "GB", "IE", "IT", "LU", "NL",
                 "NO", "NZ", "SE", "US", "PL", "PT", "CR", "PR"]

# Every source the TrafficFlare software uses. Each entry can be:
#   format: "text"      -> ip:port per line (country via ip-api geolocation)
#   format: "json"      -> structured rows with optional country/protocol fields
#   format: "country"   -> per-country files, one fetch per selected country
ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
_TEXT_PROTOCOLS = ("HTTP", "HTTPS", "SOCKS4", "SOCKS5")


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
    # ---- text sources (country via ip-api geolocation) ----
    {"id": "worldpool", "name": "Worldpool", "format": "text",
     "url": "https://raw.githubusercontent.com/CelestialBrain/worldpool/main/proxies/all.txt"},
    {"id": "stormsia-proxy-list", "name": "Stormsia Proxy List", "format": "text",
     "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/working_proxies.txt"},
    {"id": "proxy-pulse", "name": "Proxy Pulse", "format": "text",
     "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/all.txt"},
    {"id": "thordata-awesome-free-proxy-list", "name": "Thordata Awesome Free Proxy List", "format": "text",
     "url": "https://raw.githubusercontent.com/Thordata/awesome-free-proxy-list/main/proxies/http.txt"},
    {"id": "vpslab-free-proxy-list", "name": "VPSLab Free Proxy List", "format": "text",
     "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_proxies.txt"},
    {"id": "trio666-proxy-checker", "name": "trio666 Proxy Checker", "format": "text",
     "url": "https://raw.githubusercontent.com/trio666/proxy-checker/main/all.txt"},
    {"id": "argh94-proxy-list", "name": "Argh94 Proxy List", "format": "text",
     "url": "https://raw.githubusercontent.com/Argh94/Proxy-List/main/HTTP.txt"},
    {"id": "proxyscraper-proxyscraper", "name": "ProxyScraper ProxyScraper", "format": "text",
     "url": "https://raw.githubusercontent.com/ProxyScraper/ProxyScraper/main/http.txt"},
    {"id": "kangproxy-tested", "name": "KangProxy TESTED", "format": "text",
     "url": "https://cdn.jsdelivr.net/gh/officialputuid/KangProxy@main/xResults/RAW.txt"},
    {"id": "kangproxy-http", "name": "KangProxy HTTP", "format": "text",
     "url": "https://cdn.jsdelivr.net/gh/officialputuid/KangProxy@main/http/http.txt"},
    {"id": "tuanminpay-live-proxy", "name": "TuanMinPay Live Proxy", "format": "text",
     "url": "https://raw.githubusercontent.com/tuanminpay/live-proxy/master/http.txt"},
    {"id": "vmheaven-free-proxy-list", "name": "VMHeaven Free Proxy List", "format": "text",
     "url": "https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/refs/heads/main/http.txt"},
    {"id": "riommo-proxyfree", "name": "RioMMO ProxyFree", "format": "text",
     "url": "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/HTTP.txt"},
    # ---- country-template source (one fetch per selected country, no geolocation needed) ----
    {"id": "iplocate-country", "name": "IPLocate Country Proxies", "format": "country",
     "url_template": "https://raw.githubusercontent.com/iplocate/free-proxy-list/main/countries/{country}/proxies.txt"},
    # ---- json sources (country/protocol extracted from fields) ----
    {"id": "naravid-checked-proxies", "name": "Naravid Checked Proxies", "format": "json",
     "url": "https://raw.githubusercontent.com/naravid19/checked-proxies/main/proxies_pretty.json",
     "country_field": "geolocation.country.iso_code", "protocol_field": "protocol", "latency_field": "timeout"},
    {"id": "bes-public-proxy-list", "name": "Bes-js Public Proxy List", "format": "json",
     "url": "https://raw.githubusercontent.com/Bes-js/public-proxy-list/main/proxies_geolocation.json",
     "country_field": "geolocation.countryCode", "protocol_field": "protocol"},
    {"id": "gifted-free-proxies", "name": "Gifted Free Proxies", "format": "json",
     "url": "https://proxies.gifted.co.ke/files/proxies.json"},
    {"id": "proxyscrape-free-proxy-list", "name": "ProxyScrape Free Proxy List", "format": "json",
     "url": "https://raw.githubusercontent.com/proxyscrape/free-proxy-list/main/proxies/all/data.json",
     "country_field": "country_code", "protocol_field": "protocol", "latency_field": "latency"},
    {"id": "hproxy-free-proxy-list", "name": "HProxy Free Proxy List", "format": "json",
     "url": "https://cdn.jsdelivr.net/gh/hproxy-com/free-proxy-list@main/all.json",
     "country_field": "country", "protocol_field": "protocols", "latency_field": "latency_ms"},
    {"id": "proxyscrape-direct-api", "name": "ProxyScrape Direct API", "format": "json",
     "url": "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=all&format=json",
     "country_field": "country", "protocol_field": "protocol", "latency_field": "latency"},
    {"id": "proxifly-free-proxy-list", "name": "Proxifly Free Proxy List", "format": "json",
     "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json",
     "country_field": "country", "protocol_field": "protocol", "latency_field": "latency"},
]


def load_selected_countries():
    if COUNTRIES_FILE.exists():
        vals = []
        for line in COUNTRIES_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip().upper()
            if line and not line.startswith("#"):
                vals.append(line)
        if vals:
            return vals
    return TT3_COUNTRIES


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


def parse_text_proxies(text):
    proxies = set()
    for line in text.splitlines():
        m = ADDRESS_RE.search(line)
        if m:
            proxies.add(m.group(1))
    return proxies


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


async def scrape_source(session, src, selected_countries):
    if src["format"] == "country":
        rows = []
        for country in selected_countries:
            url = src["url_template"].replace("{country}", country).replace("{COUNTRY}", country)
            text = await fetch_text(session, url)
            for addr in parse_text_proxies(text):
                rows.append({"address": addr, "protocol": "HTTP", "country": country})
        return rows
    if src["format"] == "json":
        text = await fetch_text(session, src["url"])
        if not text:
            return []
        return parse_json_proxies(text, src)
    text = await fetch_text(session, src["url"])
    if not text:
        return []
    return [{"address": a, "protocol": "HTTP", "country": ""} for a in parse_text_proxies(text)]


async def geolocate_batch(session, ips):
    result = {}
    for i in range(0, len(ips), 100):
        batch = ips[i : i + 100]
        try:
            async with session.post(
                "http://ip-api.com/batch", json=batch, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
            for entry in data:
                if isinstance(entry, dict) and entry.get("status") == "success":
                    result[entry["query"]] = entry.get("countryCode", "").upper()
        except Exception:
            continue
        if i + 100 < len(ips):
            await asyncio.sleep(1.1)
    return result


async def main():
    selected = load_selected_countries()
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        all_proxies = []
        seen = set()
        for src in SOURCES:
            items = await scrape_source(session, src, selected)
            for item in items:
                key = (item["address"], item["protocol"])
                if key not in seen:
                    seen.add(key)
                    all_proxies.append(item)

        need_geo = [p for p in all_proxies if not p["country"]]
        ips = list({p["address"].rsplit(":", 1)[0] for p in need_geo})
        country_map = await geolocate_batch(session, ips)
        for p in all_proxies:
            if not p["country"]:
                ip = p["address"].rsplit(":", 1)[0]
                p["country"] = country_map.get(ip, "")

        tier3 = [p for p in all_proxies if p["country"] in selected]

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        tier3_lines = sorted(set("{}|{}|{}".format(p["address"], p["protocol"], p["country"]) for p in tier3))

        (OUTPUT_DIR / "tier3.txt").write_text("\n".join(tier3_lines) + "\n", encoding="utf-8")

        country_counts = defaultdict(int)
        for p in all_proxies:
            if p["country"]:
                country_counts[p["country"]] += 1

        summary = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "sources": [{"id": s["id"], "name": s["name"], "format": s["format"]} for s in SOURCES],
            "total_scraped": len(all_proxies),
            "geolocated": len(country_map),
            "tier3_count": len(tier3_lines),
            "selected_countries": selected,
            "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
        }
        (OUTPUT_DIR / "last_run.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())