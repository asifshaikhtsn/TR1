# TF Proxy Aggregator

TrafficFlare ke liye proxy aggregator — free proxy sources scrape karta hai, country geolocation karta hai, aur filtered lists publish karta hai.

## Output files

| File | Content |
|------|---------|
| `output/all.txt` | Saari proxies — `ip:port|protocol|country` format |
| `output/tier3.txt` | Sirf Tier-3 countries (TT3 preset, 24 countries) — `ip:port|protocol` |
| `output/last_run.json` | Last run ka status summary |

## Schedule

GitHub Actions har 30 minute mein `scrape.yml` chalaata hai. Manual run: Actions tab → "Scrape & Filter Proxies" → Run workflow.

## How it works

1. Bina-country-filter wale sources scrape hote hain (KangProxy, Proxy Pulse, VPSLab, Stormsia, etc.)
2. Har proxy ka country `ip-api.com` batch API se detect hota hai (100 IPs/request, free)
3. `tier3.txt` mein sirf TT3 countries filter hokar save hota hai

## Usage in TrafficFlare

Auto-proxy service mein naya source add karo:

```
URL: https://raw.githubusercontent.com/asifshaikhtsn/tf-proxy-aggregator/main/output/tier3.txt
Format: plain text
supports_country_filter: False
```
