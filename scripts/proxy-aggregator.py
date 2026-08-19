import asyncio
import io
import json
import os
import sys
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

# Sources that do NOT support server-side country filtering.
# These dump ALL proxies regardless of country, so we geolocate and filter locally.
SOURCES = [
    {
        "name": "KangProxy TESTED",
        "url": "https://cdn.jsdelivr.net/gh/officialputuid/KangProxy@main/xResults/RAW.txt",
        "protocol": "HTTP",
    },
    {
        "name": "KangProxy HTTP",
        "url": "https://cdn.jsdelivr.net/gh/officialputuid/KangProxy@main/http/http.txt",
        "protocol": "HTTP",
    },
    {
        "name": "RioMMO ProxyFree",
        "url": "https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/HTTP.txt",
        "protocol": "HTTP",
    },
    {
        "name": "Proxy Pulse",
        "url": "https://raw.githubusercontent.com/BlacKSnowDot0/Proxy-Pulse/main/all.txt",
        "protocol": "HTTP",
    },
    {
        "name": "VPSLab Free Proxy List",
        "url": "https://raw.githubusercontent.com/VPSLabCloud/VPSLab-Free-Proxy-List/main/all_proxies.txt",
        "protocol": "HTTP",
    },
    {
        "name": "Stormsia Proxy List",
        "url": "https://raw.githubusercontent.com/stormsia/proxy-list/main/working_proxies.txt",
        "protocol": "HTTP",
    },
    {
        "name": "Thordata Awesome Free Proxy List",
        "url": "https://raw.githubusercontent.com/thordata/awesome-free-proxy-list/refs/heads/main/README.md",
        "protocol": "HTTP",
    },
    {
        "name": "TuanMinPay Live Proxy",
        "url": "https://raw.githubusercontent.com/tuanminhpay/proxy/main/http.txt",
        "protocol": "HTTP",
    },
    {
        "name": "Argh94 Proxy List",
        "url": "https://raw.githubusercontent.com/argh94/awesome-free-proxy-list/main/list.txt",
        "protocol": "HTTP",
    },
    {
        "name": "ProxyScraper ProxyScraper",
        "url": "https://raw.githubusercontent.com/proxyscrapper/scrape-proxies/main/proxies.txt",
        "protocol": "HTTP",
    },
    {
        "name": "ProxyScraper all.txt",
        "url": "https://raw.githubusercontent.com/proxyscrape/awesome-proxy-list/main/all.txt",
        "protocol": "HTTP",
    },
]

ADDRESS_RE = None
import re
ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")


def load_selected_countries() -> list:
    if COUNTRIES_FILE.exists():
        vals = []
        for line in COUNTRIES_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip().upper()
            if line and not line.startswith("#"):
                vals.append(line)
        if vals:
            return vals
    return TT3_COUNTRIES


async def fetch_text(session: aiohttp.ClientSession, url: str) -> str:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
            if resp.status != 200:
                return ""
            return await resp.text()
    except Exception:
        return ""


async def scrape_source(session: aiohttp.ClientSession, src: dict) -> list:
    text = await fetch_text(session, src["url"])
    if not text:
        return []
    proxies = set()
    for line in text.splitlines():
        m = ADDRESS_RE.search(line)
        if m:
            proxies.add(m.group(1))
    return [{"address": p, "protocol": src["protocol"]} for p in proxies]


async def geolocate_batch(session: aiohttp.ClientSession, ips: list) -> dict:
    """Batch geolocation via ip-api.com (100 IPs per request, free tier)."""
    result = {}
    for i in range(0, len(ips), 100):
        batch = ips[i : i + 100]
        try:
            async with session.post(
                "http://ip-api.com/batch",
                json=batch,
                timeout=aiohttp.ClientTimeout(total=30),
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
            await asyncio.sleep(1.2)
    return result


async def main():
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        all_proxies = []
        seen = set()
        for src in SOURCES:
            items = await scrape_source(session, src)
            for item in items:
                if item["address"] not in seen:
                    seen.add(item["address"])
                    all_proxies.append(item)

        # Protocol inference from source is basic; we tag most as HTTP.
        ips = list({p["address"].rsplit(":", 1)[0] for p in all_proxies})
        country_map = await geolocate_batch(session, ips)

        for proxy in all_proxies:
            ip = proxy["address"].rsplit(":", 1)[0]
            proxy["country"] = country_map.get(ip, "")

        selected = load_selected_countries()
        tier3 = [p for p in all_proxies if p["country"] in selected]

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        all_lines = []
        for p in sorted(all_proxies, key=lambda x: x["address"]):
            if p["country"]:
                all_lines.append("{}|{}|{}".format(p["address"], p["protocol"], p["country"]))
            else:
                all_lines.append("{}|{}".format(p["address"], p["protocol"]))

        tier3_lines = ["{}|{}".format(p["address"], p["protocol"]) for p in tier3]
        tier3_lines = sorted(set(tier3_lines))

        (OUTPUT_DIR / "all.txt").write_text("\n".join(all_lines) + "\n", encoding="utf-8")
        (OUTPUT_DIR / "tier3.txt").write_text("\n".join(tier3_lines) + "\n", encoding="utf-8")

        country_counts = defaultdict(int)
        for p in all_proxies:
            if p["country"]:
                country_counts[p["country"]] += 1

        summary = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_scraped": len(all_proxies),
            "geolocated": len(country_map),
            "tier3_count": len(tier3_lines),
            "selected_countries": selected,
            "country_counts": dict(sorted(country_counts.items(), key=lambda x: -x[1])),
        }
        (OUTPUT_DIR / "last_run.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )

        print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    asyncio.run(main())