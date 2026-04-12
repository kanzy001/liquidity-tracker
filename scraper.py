"""
scraper.py — Standalone scraper for GitHub Actions.
Reads FRED_API_KEY from environment (set as a GitHub Secret).
Writes / updates JSON files in docs/data/ which GitHub Pages serves statically.

Output files:
  docs/data/fed_net_liquidity.json
  docs/data/global_m2.json
  docs/data/cb_balance_sheets.json
  docs/data/cb_rate_decisions.json
  docs/data/summary.json          ← latest snapshot of all 4 (used by dashboard)
  docs/data/last_updated.json     ← timestamp + scrape status
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("scraper")

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
FRED_BASE    = "https://api.stlouisfed.org/fred/series/observations"
DATA_DIR     = Path(__file__).parent / "docs" / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── FX (approximate monthly averages) ────────────────────────────────────────
FX = {"CNY": 0.138, "EUR": 1.08, "JPY": 0.0067}

# ── FRED helpers ─────────────────────────────────────────────────────────────

def fred(series_id: str, limit: int = 14) -> list[dict]:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY secret is not set. Add it in repo Settings → Secrets.")
    r = requests.get(FRED_BASE, params={
        "series_id":        series_id,
        "api_key":          FRED_API_KEY,
        "file_type":        "json",
        "sort_order":       "desc",
        "limit":            limit,
        "observation_start": (datetime.now(timezone.utc) - timedelta(days=500)).strftime("%Y-%m-%d"),
    }, timeout=20)
    r.raise_for_status()
    return [o for o in r.json().get("observations", []) if o["value"] not in (".", "")]


def fred_val(series_id: str) -> tuple[str, float]:
    obs = fred(series_id, limit=10)
    if not obs:
        raise RuntimeError(f"No data for {series_id}")
    return obs[0]["date"], float(obs[0]["value"])


# ── JSON persistence ─────────────────────────────────────────────────────────

def load_json(path: Path) -> list:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            return []
    return []


def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2))
    log.info(f"  wrote {path.name} ({len(data) if isinstance(data, list) else 1} records)")


def upsert_series(path: Path, new_row: dict):
    """Load existing series, replace or append row keyed on 'date', save."""
    series = load_json(path)
    existing_dates = {r["date"]: i for i, r in enumerate(series)}
    if new_row["date"] in existing_dates:
        series[existing_dates[new_row["date"]]] = new_row
    else:
        series.append(new_row)
    series.sort(key=lambda r: r["date"])
    save_json(path, series)
    return series


# ── ECB SDW helper ───────────────────────────────────────────────────────────

def ecb_sdw(series_key: str, last_n: int = 2) -> list[tuple[str, float]]:
    url = f"https://sdw-wsrest.ecb.europa.eu/service/data/{series_key}?format=jsondata&lastNObservations={last_n}"
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    data    = r.json()
    obs_key = list(data["dataSets"][0]["series"].keys())[0]
    obs     = data["dataSets"][0]["series"][obs_key]["observations"]
    periods = data["structure"]["dimensions"]["observation"][0]["values"]
    results = []
    for i, p in enumerate(periods[-last_n:], start=len(periods) - last_n):
        val = obs.get(str(i), [None])[0]
        if val is not None:
            results.append((p["id"], float(val)))
    return results


# ── 1. Fed Net Liquidity ──────────────────────────────────────────────────────

def scrape_fed_net_liquidity() -> dict:
    log.info("[1] Fed Net Liquidity...")
    date_fa,  fed_assets = fred_val("WALCL")
    _,        tga        = fred_val("WTREGEN")
    _,        rrp        = fred_val("RRPONTSYD")
    net_liq = fed_assets - tga - rrp
    row = {
        "date":       date_fa,
        "fed_assets": round(fed_assets, 2),
        "tga":        round(tga, 2),
        "rrp":        round(rrp, 2),
        "net_liq":    round(net_liq, 2),
    }
    upsert_series(DATA_DIR / "fed_net_liquidity.json", row)
    log.info(f"  Fed Net Liq: ${net_liq:,.0f}B on {date_fa}")
    return row


# ── 2. Global M2 ─────────────────────────────────────────────────────────────

def scrape_global_m2() -> dict:
    log.info("[2] Global M2...")

    # US — FRED M2SL (USD billions)
    us_obs   = fred("M2SL", limit=14)
    us_curr  = float(us_obs[0]["value"])
    us_prev  = float(us_obs[min(12, len(us_obs)-1)]["value"])
    us_date  = us_obs[0]["date"]

    # China — FRED MYAGM2CNM189N (CNY billions)
    try:
        _, cn_cny = fred_val("MYAGM2CNM189N")
        cn_m2 = round(cn_cny * FX["CNY"], 2)
    except Exception as e:
        log.warning(f"  CN M2 failed: {e}"); cn_m2 = 0.0

    # Eurozone — ECB SDW (EUR millions → USD B)
    try:
        ecb_pts = ecb_sdw("BSI/M.U2.Y.V.M20.X.1.U2.2300.Z01.E", last_n=2)
        eu_m2   = round((ecb_pts[-1][1] / 1000) * FX["EUR"], 2)
    except Exception as e:
        log.warning(f"  EU M2 ECB failed, trying FRED proxy: {e}")
        try:
            _, eu_raw = fred_val("MABMM301EZM189N")
            eu_m2 = round((eu_raw / 1000) * FX["EUR"], 2)
        except Exception as e2:
            log.warning(f"  EU M2 FRED proxy also failed: {e2}"); eu_m2 = 0.0

    # Japan — FRED MYAGM2JPM189N (JPY billions)
    try:
        _, jp_jpy = fred_val("MYAGM2JPM189N")
        jp_m2 = round(jp_jpy * FX["JPY"], 2)
    except Exception as e:
        log.warning(f"  JP M2 failed: {e}"); jp_m2 = 0.0

    global_m2 = round(us_curr + cn_m2 + eu_m2 + jp_m2, 2)
    yoy_pct   = round(((us_curr - us_prev) / us_prev) * 100, 2) if us_prev else 0.0

    row = {
        "date":      us_date,
        "us_m2":     round(us_curr, 2),
        "cn_m2":     cn_m2,
        "eu_m2":     eu_m2,
        "jp_m2":     jp_m2,
        "global_m2": global_m2,
        "yoy_pct":   yoy_pct,
    }
    upsert_series(DATA_DIR / "global_m2.json", row)
    log.info(f"  Global M2: ${global_m2:,.0f}B | YoY: {yoy_pct:.1f}%")
    return row


# ── 3. Central Bank Balance Sheets ───────────────────────────────────────────

def scrape_cb_balance_sheets() -> dict:
    log.info("[3] CB Balance Sheets...")

    # Fed — WALCL (USD billions)
    fed_date, fed_bs = fred_val("WALCL")

    # ECB — SDW weekly ILM (EUR millions → USD B)
    try:
        ecb_pts = ecb_sdw("ILM/W.U2.C.T000000.Z5.Z01.E", last_n=2)
        ecb_bs  = round((ecb_pts[-1][1] / 1000) * FX["EUR"], 2)
    except Exception as e:
        log.warning(f"  ECB BS SDW failed, trying FRED proxy: {e}")
        try:
            _, ecb_raw = fred_val("ECBASSETSW")
            ecb_bs = round((ecb_raw / 1000) * FX["EUR"], 2)
        except Exception as e2:
            log.warning(f"  ECB BS FRED proxy also failed: {e2}"); ecb_bs = 0.0

    # PBOC — FRED CHNASSETS (CNY billions → USD B)
    try:
        _, pboc_cny = fred_val("CHNASSETS")
        pboc_bs = round(pboc_cny * FX["CNY"], 2)
    except Exception as e:
        log.warning(f"  PBOC BS failed: {e}"); pboc_bs = 0.0

    total_bs = round(fed_bs + ecb_bs + pboc_bs, 2)
    row = {
        "date":     fed_date,
        "fed_bs":   round(fed_bs, 2),
        "ecb_bs":   ecb_bs,
        "pboc_bs":  pboc_bs,
        "total_bs": total_bs,
    }
    upsert_series(DATA_DIR / "cb_balance_sheets.json", row)
    log.info(f"  Total BS: ${total_bs:,.0f}B")
    return row


# ── 4. CB Rate Hike / Cut Ratio ───────────────────────────────────────────────

CB_SERIES = {
    "Federal Reserve":      "FEDFUNDS",
    "ECB":                  "ECBDFR",
    "Bank of England":      "BOERUKM",
    "Bank of Japan":        "IRSTCB01JPM156N",
    "Reserve Bank Aus":     "RBATCTR",
    "Bank of Canada":       "IRSTCB01CAM156N",
    "Swiss Natl Bank":      "IRSTCB01CHM156N",
    "Riksbank":             "IRSTCB01SEM156N",
    "Norges Bank":          "IRSTCB01NOM156N",
    "Reserve Bank NZ":      "IRSTCB01NZM156N",
    "Peoples Bank China":   "IRSTCB01CNM156N",
    "Reserve Bank India":   "IRSTCB01INM156N",
    "Bank of Korea":        "IRSTCB01KRM156N",
    "Bank of Mexico":       "IRSTCB01MXM156N",
    "Central Bank Brazil":  "IRSTCB01BRM156N",
    "SARB South Africa":    "IRSTCB01ZAM156N",
    "Bank of Indonesia":    "IRSTCB01IDM156N",
    "Central Bank Turkey":  "IRSTCB01TRM156N",
}
THRESHOLD_BPS = 5


def scrape_cb_rate_decisions() -> dict:
    log.info("[4] CB Rate Decisions...")
    decisions = {}
    hikes = cuts = holds = 0

    for cb, series_id in CB_SERIES.items():
        try:
            obs      = fred(series_id, limit=14)
            if len(obs) < 2:
                continue
            current  = float(obs[0]["value"])
            year_ago = float(obs[min(12, len(obs)-1)]["value"])
            delta    = (current - year_ago) * 100   # → basis points

            if   delta >  THRESHOLD_BPS: decision = "hike"; hikes += 1
            elif delta < -THRESHOLD_BPS: decision = "cut";  cuts  += 1
            else:                        decision = "hold"; holds += 1

            decisions[cb] = {
                "decision":  decision,
                "current":   round(current, 3),
                "year_ago":  round(year_ago, 3),
                "delta_bps": round(delta, 1),
            }
            log.info(f"    {cb}: {decision} ({delta:+.1f}bps)")
        except Exception as e:
            log.warning(f"    {cb} failed: {e}")

    total      = hikes + cuts + holds
    cut_ratio  = round(cuts  / total, 4) if total else 0.0
    hike_ratio = round(hikes / total, 4) if total else 0.0

    row = {
        "date":       datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "hikes":      hikes,
        "cuts":       cuts,
        "holds":      holds,
        "total_cbs":  total,
        "cut_ratio":  cut_ratio,
        "hike_ratio": hike_ratio,
        "details":    decisions,
    }
    upsert_series(DATA_DIR / "cb_rate_decisions.json", row)
    log.info(f"  {hikes} hikes | {cuts} cuts | {holds} holds | cut_ratio={cut_ratio:.1%}")
    return row


# ── Master runner ─────────────────────────────────────────────────────────────

def main():
    if not FRED_API_KEY:
        raise SystemExit(
            "ERROR: FRED_API_KEY is not set.\n"
            "Add it as a GitHub Secret: repo Settings → Secrets and variables → Actions → New secret\n"
            "Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    started = datetime.now(timezone.utc).isoformat()
    results = {}
    errors  = []

    tasks = [
        ("fed_net_liquidity", scrape_fed_net_liquidity),
        ("global_m2",         scrape_global_m2),
        ("cb_balance_sheets", scrape_cb_balance_sheets),
        ("cb_rate_decisions", scrape_cb_rate_decisions),
    ]

    for name, fn in tasks:
        try:
            results[name] = fn()
        except Exception as e:
            log.error(f"FAILED {name}: {e}")
            errors.append(f"{name}: {e}")

    # Write summary.json — the dashboard reads this for the top metrics
    summary = {
        "as_of":             datetime.now(timezone.utc).isoformat(),
        "scrape_started":    started,
        "status":            "success" if not errors else ("partial" if results else "failed"),
        "errors":            errors,
        **results,
    }
    save_json(DATA_DIR / "summary.json", summary)

    # Write last_updated.json — lightweight ping for uptime checks
    save_json(DATA_DIR / "last_updated.json", {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status":    summary["status"],
        "succeeded": list(results.keys()),
        "errors":    errors,
    })

    if errors:
        log.warning(f"Completed with {len(errors)} error(s): {errors}")
    else:
        log.info("All 4 indicators scraped successfully.")

    if summary["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
