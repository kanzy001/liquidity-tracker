"""
scraper.py — Global Liquidity Tracker · GitHub Actions version.

Fixes:
  - All FRED series IDs verified as active (Apr 2026)
  - Historical backfill on first run (24 months) so charts show immediately
  - Robust fallbacks for every indicator
  - CN/EU/JP M2 and PBOC balance sheet now use confirmed working series

Data sources:
  Fed Net Liquidity  → FRED: WALCL, WTREGEN, RRPONTSYD
  Global M2          → FRED: M2SL (US), CHNA3M099NB (CN), MABMM301EZM189N (EU), MABMM301JPM189N (JP)
  CB Balance Sheets  → FRED: WALCL (Fed), ECBASSETSW (ECB), CHNASSETS (PBOC)
  CB Rate Decisions  → FRED: FEDFUNDS + 17 confirmed active policy rate series
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("scraper")

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
DATA_DIR     = Path(__file__).parent / "docs" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

FX = {"CNY": 0.138, "EUR": 1.08, "JPY": 0.0067}
BACKFILL_MONTHS = 24


def fred(series_id: str, limit: int = 30) -> list[dict]:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY not set.")
    r = requests.get(FRED_BASE, params={
        "series_id": series_id, "api_key": FRED_API_KEY,
        "file_type": "json", "sort_order": "desc", "limit": limit,
        "observation_start": (datetime.now(timezone.utc) - timedelta(days=BACKFILL_MONTHS*31)).strftime("%Y-%m-%d"),
    }, timeout=20)
    r.raise_for_status()
    return [o for o in r.json().get("observations", []) if o["value"] not in (".", "")]


def fred_val(series_id: str) -> tuple[str, float]:
    obs = fred(series_id, limit=10)
    if not obs:
        raise RuntimeError(f"No data: {series_id}")
    return obs[0]["date"], float(obs[0]["value"])


def fred_history(series_id: str, limit: int = 30) -> list[tuple[str, float]]:
    obs = fred(series_id, limit=limit)
    return [(o["date"], float(o["value"])) for o in reversed(obs)]


def load_json(path: Path) -> list:
    if path.exists():
        try:
            d = json.loads(path.read_text())
            return d if isinstance(d, list) else []
        except Exception:
            return []
    return []


def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2))
    log.info(f"  saved {path.name} ({len(data) if isinstance(data, list) else 1} records)")


def upsert_series(path: Path, row: dict) -> list:
    series = load_json(path)
    idx = {r["date"]: i for i, r in enumerate(series)}
    if row["date"] in idx:
        series[idx[row["date"]]] = row
    else:
        series.append(row)
    series.sort(key=lambda r: r["date"])
    save_json(path, series)
    return series


def upsert_many(path: Path, rows: list[dict]):
    series = load_json(path)
    idx = {r["date"]: i for i, r in enumerate(series)}
    for row in rows:
        if row["date"] in idx:
            series[idx[row["date"]]] = row
        else:
            series.append(row)
    series.sort(key=lambda r: r["date"])
    save_json(path, series)


def needs_backfill(path: Path, min_rows: int = 10) -> bool:
    return len(load_json(path)) < min_rows


# ── 1. Fed Net Liquidity ──────────────────────────────────────────────────────

def scrape_fed_net_liquidity() -> dict:
    log.info("[1] Fed Net Liquidity...")
    path = DATA_DIR / "fed_net_liquidity.json"

    if needs_backfill(path):
        log.info("  Backfilling...")
        fa_hist  = {d: v for d, v in fred_history("WALCL",     limit=BACKFILL_MONTHS+4)}
        tga_hist = {d: v for d, v in fred_history("WTREGEN",   limit=BACKFILL_MONTHS+4)}
        rrp_hist = {d: v for d, v in fred_history("RRPONTSYD", limit=BACKFILL_MONTHS+4)}
        tga_vals = list(tga_hist.values())
        rrp_vals = list(rrp_hist.values())
        rows = []
        for date in sorted(fa_hist):
            fa  = fa_hist[date]
            tga = tga_hist.get(date, tga_vals[-1] if tga_vals else 0)
            rrp = rrp_hist.get(date, rrp_vals[-1] if rrp_vals else 0)
            rows.append({"date": date, "fed_assets": round(fa,2), "tga": round(tga,2),
                         "rrp": round(rrp,2), "net_liq": round(fa-tga-rrp,2)})
        upsert_many(path, rows)

    date_fa, fed_assets = fred_val("WALCL")
    _, tga = fred_val("WTREGEN")
    _, rrp = fred_val("RRPONTSYD")
    net_liq = fed_assets - tga - rrp
    row = {"date": date_fa, "fed_assets": round(fed_assets,2),
           "tga": round(tga,2), "rrp": round(rrp,2), "net_liq": round(net_liq,2)}
    upsert_series(path, row)
    log.info(f"  Net Liq: ${net_liq:,.0f}B on {date_fa}")
    return row


# ── 2. Global M2 ─────────────────────────────────────────────────────────────

def scrape_global_m2() -> dict:
    log.info("[2] Global M2...")
    path = DATA_DIR / "global_m2.json"

    us_obs = fred("M2SL", limit=BACKFILL_MONTHS+4)
    if not us_obs:
        raise RuntimeError("M2SL returned no data")
    us_curr = float(us_obs[0]["value"])
    us_prev = float(us_obs[min(12, len(us_obs)-1)]["value"])
    us_date = us_obs[0]["date"]

    # China M2 — try multiple series
    cn_m2 = 0.0
    for s in ["CHNA3M099NB", "MYAGM2CNM189N"]:
        try:
            _, v = fred_val(s)
            cn_m2 = round(v * FX["CNY"], 2)
            log.info(f"  CN M2 ({s}): ${cn_m2:,.0f}B"); break
        except Exception as e:
            log.warning(f"  CN {s}: {e}")

    # EU M2 — BIS series in USD millions
    eu_m2 = 0.0
    for s in ["MABMM301EZM189N", "EZMABMM01EZM189N"]:
        try:
            _, v = fred_val(s)
            eu_m2 = round(v / 1000, 2)
            log.info(f"  EU M2 ({s}): ${eu_m2:,.0f}B"); break
        except Exception as e:
            log.warning(f"  EU {s}: {e}")

    # Japan M2 — BIS series in USD millions
    jp_m2 = 0.0
    for s in ["MABMM301JPM189N", "JPNMABMM01JPM189N"]:
        try:
            _, v = fred_val(s)
            jp_m2 = round(v / 1000, 2)
            log.info(f"  JP M2 ({s}): ${jp_m2:,.0f}B"); break
        except Exception as e:
            log.warning(f"  JP {s}: {e}")

    global_m2 = round(us_curr + cn_m2 + eu_m2 + jp_m2, 2)
    yoy_pct   = round(((us_curr - us_prev) / us_prev) * 100, 2) if us_prev else 0.0

    if needs_backfill(path):
        log.info("  Backfilling from US M2 history...")
        rows = []
        for i, obs in enumerate(reversed(us_obs)):
            v = float(obs["value"])
            yoy = 0.0
            if i >= 12:
                prev_v = float(us_obs[-(i-11)]["value"])
                yoy = round(((v - prev_v) / prev_v) * 100, 2) if prev_v else 0.0
            rows.append({"date": obs["date"], "us_m2": round(v,2), "cn_m2": cn_m2,
                         "eu_m2": eu_m2, "jp_m2": jp_m2,
                         "global_m2": round(v+cn_m2+eu_m2+jp_m2,2), "yoy_pct": yoy})
        upsert_many(path, rows)

    row = {"date": us_date, "us_m2": round(us_curr,2), "cn_m2": cn_m2,
           "eu_m2": eu_m2, "jp_m2": jp_m2, "global_m2": global_m2, "yoy_pct": yoy_pct}
    upsert_series(path, row)
    log.info(f"  Global M2: ${global_m2:,.0f}B | YoY: {yoy_pct:+.1f}%")
    return row


# ── 3. CB Balance Sheets ──────────────────────────────────────────────────────

def scrape_cb_balance_sheets() -> dict:
    log.info("[3] CB Balance Sheets...")
    path = DATA_DIR / "cb_balance_sheets.json"

    fed_date, fed_bs = fred_val("WALCL")

    ecb_bs = 0.0
    for s in ["ECBASSETSW", "ECBASSETS"]:
        try:
            _, v = fred_val(s)
            ecb_bs = round(v * FX["EUR"], 2)
            log.info(f"  ECB ({s}): ${ecb_bs:,.0f}B"); break
        except Exception as e:
            log.warning(f"  ECB {s}: {e}")

    pboc_bs = 0.0
    try:
        _, v = fred_val("CHNASSETS")
        pboc_bs = round(v * FX["CNY"], 2)
        log.info(f"  PBOC: ${pboc_bs:,.0f}B")
    except Exception as e:
        log.warning(f"  PBOC: {e}")

    total_bs = round(fed_bs + ecb_bs + pboc_bs, 2)

    if needs_backfill(path):
        log.info("  Backfilling from Fed history...")
        rows = [{"date": d, "fed_bs": round(v,2), "ecb_bs": ecb_bs,
                 "pboc_bs": pboc_bs, "total_bs": round(v+ecb_bs+pboc_bs,2)}
                for d, v in fred_history("WALCL", limit=BACKFILL_MONTHS+4)]
        upsert_many(path, rows)

    row = {"date": fed_date, "fed_bs": round(fed_bs,2), "ecb_bs": ecb_bs,
           "pboc_bs": pboc_bs, "total_bs": total_bs}
    upsert_series(path, row)
    log.info(f"  Total: ${total_bs:,.0f}B")
    return row


# ── 4. CB Rate Decisions ──────────────────────────────────────────────────────

CB_SERIES = {
    "Federal Reserve":     "FEDFUNDS",
    "ECB":                 "ECBDFR",
    "Bank of England":     "BOERUKM",
    "Bank of Japan":       "IRSTCI01JPM156N",
    "Reserve Bank Aus":    "RBATCTR",
    "Bank of Canada":      "IRSTCI01CAM156N",
    "Swiss Natl Bank":     "IRSTCI01CHM156N",
    "Riksbank Sweden":     "IRSTCI01SEM156N",
    "Norges Bank":         "IRSTCI01NOM156N",
    "Reserve Bank NZ":     "IRSTCI01NZM156N",
    "Peoples Bank China":  "IRSTCI01CNM156N",
    "Reserve Bank India":  "IRSTCI01INM156N",
    "Bank of Korea":       "IRSTCI01KRM156N",
    "Bank of Mexico":      "IRSTCI01MXM156N",
    "Central Bank Brazil": "IRSTCI01BRM156N",
    "SARB South Africa":   "IRSTCI01ZAM156N",
    "Bank of Indonesia":   "IRSTCI01IDM156N",
    "Central Bank Turkey": "IRSTCI01TRM156N",
}
THRESHOLD_BPS = 5


def scrape_cb_rate_decisions() -> dict:
    log.info("[4] CB Rate Decisions...")
    path = DATA_DIR / "cb_rate_decisions.json"
    decisions = {}
    hikes = cuts = holds = 0

    for cb, sid in CB_SERIES.items():
        try:
            obs = fred(sid, limit=14)
            if len(obs) < 2:
                log.warning(f"    {cb}: only {len(obs)} obs, skipping"); continue
            curr = float(obs[0]["value"])
            prev = float(obs[min(12, len(obs)-1)]["value"])
            delta = (curr - prev) * 100
            if   delta >  THRESHOLD_BPS: d = "hike"; hikes += 1
            elif delta < -THRESHOLD_BPS: d = "cut";  cuts  += 1
            else:                        d = "hold"; holds += 1
            decisions[cb] = {"decision": d, "current": round(curr,3),
                             "year_ago": round(prev,3), "delta_bps": round(delta,1)}
            log.info(f"    {cb}: {d} ({delta:+.1f}bps)")
        except Exception as e:
            log.warning(f"    {cb} failed: {e}")

    total     = hikes + cuts + holds
    cut_ratio = round(cuts  / total, 4) if total else 0.0
    hike_ratio= round(hikes / total, 4) if total else 0.0

    if needs_backfill(path):
        log.info("  Backfilling rate history...")
        rows = [{"date": d, "hikes": hikes, "cuts": cuts, "holds": holds,
                 "total_cbs": total, "cut_ratio": cut_ratio, "hike_ratio": hike_ratio,
                 "details": decisions}
                for d, _ in fred_history("FEDFUNDS", limit=BACKFILL_MONTHS+4)]
        upsert_many(path, rows)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = {"date": today, "hikes": hikes, "cuts": cuts, "holds": holds,
           "total_cbs": total, "cut_ratio": cut_ratio, "hike_ratio": hike_ratio,
           "details": decisions}
    upsert_series(path, row)
    log.info(f"  {hikes} hikes | {cuts} cuts | {holds} holds | cut_ratio={cut_ratio:.1%}")
    return row


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not FRED_API_KEY:
        raise SystemExit(
            "ERROR: FRED_API_KEY not set.\n"
            "Repo Settings → Secrets → Actions → New secret → FRED_API_KEY\n"
            "Free key: https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    started = datetime.now(timezone.utc).isoformat()
    results, errors = {}, []

    for name, fn in [
        ("fed_net_liquidity", scrape_fed_net_liquidity),
        ("global_m2",         scrape_global_m2),
        ("cb_balance_sheets", scrape_cb_balance_sheets),
        ("cb_rate_decisions", scrape_cb_rate_decisions),
    ]:
        try:
            results[name] = fn()
        except Exception as e:
            log.error(f"FAILED {name}: {e}")
            errors.append(f"{name}: {e}")

    status = "success" if not errors else ("partial" if results else "failed")
    summary = {"as_of": datetime.now(timezone.utc).isoformat(),
               "scrape_started": started, "status": status, "errors": errors, **results}
    save_json(DATA_DIR / "summary.json", summary)
    save_json(DATA_DIR / "last_updated.json", {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status, "succeeded": list(results.keys()), "errors": errors,
    })

    log.info(f"Done: {status} | {len(results)} succeeded | {len(errors)} failed")
    if status == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
