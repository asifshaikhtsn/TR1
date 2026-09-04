import asyncio
import importlib.util
import json
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

from geo_country import geolocate_ips

ADDRESS_RE = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}:\d{1,5})")
ROOT = Path.cwd()
COUNTRY_DIR = ROOT / "country"
DATA_DIR = ROOT / "data"

async def _patched_geolocate_batch(*args, **kwargs):
    ips = kwargs.get("ips")
    if ips is None:
        if not args:
            return {}
        ips = args[-1]
    return await geolocate_ips(ips)

def _load_target(path):
    target = Path(path).resolve()
    spec = importlib.util.spec_from_file_location("proxy_target_module", target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {target}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def _load_country_records():
    records = {}
    if not COUNTRY_DIR.exists():
        return records
    for cc_dir in COUNTRY_DIR.iterdir():
        if not cc_dir.is_dir():
            continue
        old_country = cc_dir.name.upper()
        for proto_file in cc_dir.iterdir():
            if not proto_file.is_file():
                continue
            protocol = proto_file.stem
            try:
                lines = proto_file.read_text(encoding="utf-8").splitlines()
            except Exception:
                continue
            for line in lines:
                match = ADDRESS_RE.search(line)
                if match:
                    records[(match.group(1), protocol)] = old_country
    return records

def _update_json_countries(country_map):
    for name in ("live_proxies.json", "all_proxies.json"):
        path = DATA_DIR / name
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = data.get("proxies")
        if not isinstance(rows, list):
            continue
        changed = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            address = str(row.get("proxy") or row.get("address") or "")
            match = ADDRESS_RE.search(address)
            if not match:
                continue
            ip = match.group(1).rsplit(":", 1)[0]
            country = country_map.get(ip)
            if country and row.get("country") != country:
                row["country"] = country
                changed = True
        if changed:
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")

def _update_summary(grouped):
    path = ROOT / "last_run.json"
    if not path.exists():
        return
    try:
        summary = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return
    country_counts = defaultdict(int)
    for (country, _protocol), addresses in grouped.items():
        country_counts[country] += len(addresses)
    if "country_count" in summary:
        summary["country_count"] = len(country_counts)
    if "country_counts" in summary:
        summary["country_counts"] = dict(sorted(country_counts.items(), key=lambda item: -item[1]))
    path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

async def _reclassify_country_tree():
    records = _load_country_records()
    if not records:
        print("[Geo] No country-tree records to reclassify.")
        return
    ips = list({address.rsplit(":", 1)[0] for address, _protocol in records})
    country_map = await geolocate_ips(ips)
    grouped = defaultdict(set)
    for (address, protocol), old_country in records.items():
        ip = address.rsplit(":", 1)[0]
        country = country_map.get(ip) or old_country or "XX"
        grouped[(country, protocol)].add(address)
    for child in list(COUNTRY_DIR.iterdir()):
        if child.is_dir():
            shutil.rmtree(child)
    for (country, protocol), addresses in grouped.items():
        out_dir = COUNTRY_DIR / country
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"{protocol}.txt").write_text("\n".join(sorted(addresses)) + "\n", encoding="utf-8")
    _update_json_countries(country_map)
    _update_summary(grouped)
    print(f"[Geo] Reclassified {len(records)} proxy/protocol records across {len({country for country, _ in grouped})} countries.")

async def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python geo_runner.py <aggregator-script>")
    module = _load_target(sys.argv[1])
    if hasattr(module, "geolocate_batch"):
        module.geolocate_batch = _patched_geolocate_batch
    await module.main()
    await _reclassify_country_tree()

if __name__ == "__main__":
    asyncio.run(main())
