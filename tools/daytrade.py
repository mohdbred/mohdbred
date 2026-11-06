#!/usr/bin/env python3
"""Deterministic daily pipeline for Mohd's paper day-trading routine.

One program, three modes, the SAME filters every time:

  shortlist  pre-market (about 8:31 AM New York): market mood + top 5 charts
  signals    after the opening range (about 9:52 AM New York): breakouts on the
             candidate list, each run through entry, exit, Time Machine,
             budget and risk, with a rule verdict
  review     after the close: what the rules would have done today, with the
             real outcome of every breakout (the learning loop)

Lessons built in (Nov 6 2026): during the session the scanner's volume is
TODAY'S PARTIAL volume, so liquidity and "yesterday's change" are always taken
from COMPLETED daily bars only; and every run prints a self-check so a run
that filters out nearly everything is reported as a data problem, not as
"no trade".

Reads only public data (Yahoo, Nasdaq Trader, SEC via the skills). Never
places an order. Educational, not financial advice.
"""
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
DXB = ZoneInfo("Asia/Dubai")
SK = os.path.expanduser("~/.claude/skills")
UA = {"User-Agent": "Mozilla/5.0"}

CONFIG = {
    "budget": 1000, "cash_buffer_pct": 10, "max_position_pct": 50,
    "max_order_usd": 500, "min_order_usd": 5, "max_loss_per_trade_usd": 10,
    "max_daily_loss_usd": 20, "max_trades_per_day": 2, "max_orders_per_day": 6,
    "min_reward_risk": 2.0, "require_stop": True, "no_entries_before": "09:45",
    "no_entries_after": "11:30", "flat_by": "15:55", "fractional": False,
    "broker": "alpaca", "paper": True, "blocked": [], "allow_options": False,
    "allow_margin": False, "allow_short": False, "pdt_guard": True,
    "sec_user_agent": os.environ.get("SEC_USER_AGENT", "Mohd Belal link.belal@gmail.com"),
}

# The strategy's filters, in one place.
UPTREND = {"breakout_20d", "bull_flag", "reclaim_ma20"}
MIN_PRICE, MAX_PRICE, MAX_RSI = 10.0, 150.0, 80.0
MIN_PREV_VOLUME = 3_000_000
CHG_MIN, CHG_MAX = -2.0, 10.0
VIX_MAX, GAP_MAX = 30.0, 1.5
N_CANDIDATES, N_SHORTLIST, N_NEWS, MAX_SIGNALS = 40, 5, 40, 8
EVENT_WORDS = re.compile(
    r"\b(ahead of|earnings near|on the horizon|to report|set to report|preview|"
    r"what to expect|scheduled|on deck|before (the|its) report)\b", re.I)


# ----------------------------------------------------------------- helpers
def log(msg):
    print(msg, file=sys.stderr, flush=True)


def run(args, timeout=900):
    """Run a skill command, return (exit code, stdout)."""
    p = subprocess.run([sys.executable] + args, capture_output=True, text=True, timeout=timeout)
    return p.returncode, p.stdout


def run_json(args, timeout=900):
    code, out = run(args + ["--json"], timeout)
    try:
        return json.loads(out)
    except ValueError:
        return {"_error": "no JSON (exit %d): %s" % (code, out.strip()[:300])}


def get_json(url, tries=3):
    for i in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=30))
        except Exception as e:  # network hiccup or 429
            if i == tries - 1:
                raise
            log("retry %s (%s)" % (url[:80], e))
            time.sleep(3 * (i + 1))


def chart(sym, rng, iv):
    u = "https://query2.finance.yahoo.com/v8/finance/chart/%s?range=%s&interval=%s" % (
        urllib.parse.quote(sym), rng, iv)
    return get_json(u)["chart"]["result"][0]


def ny_date(ts):
    return dt.datetime.fromtimestamp(ts, NY).date()


def completed_daily(sym_list, asof):
    """Daily bars from COMPLETED sessions only (date < asof): close and volume."""
    # Yahoo's batch "spark" endpoint has closes but NO volume, so use the v8
    # chart per symbol (8 at a time).
    from concurrent.futures import ThreadPoolExecutor

    def one(sym):
        try:
            r = chart(sym.replace(".", "-"), "2mo", "1d")
            q = r["indicators"]["quote"][0]
            bars = [(ny_date(t), c, v) for t, c, v in zip(r["timestamp"], q["close"], q["volume"])
                    if c is not None and ny_date(t) < asof]
            if len(bars) < 21:
                return sym, None
            vols = [b[2] or 0 for b in bars[-20:]]
            return sym, {"prev_date": str(bars[-1][0]), "prev_close": bars[-1][1],
                         "prev_volume": bars[-1][2] or 0,
                         "avg_volume_20": sum(vols) / len(vols),
                         "prev_chg": (bars[-1][1] / bars[-2][1] - 1) * 100}
        except Exception as e:
            log("daily %s: %s" % (sym, e))
            return sym, None

    with ThreadPoolExecutor(8) as ex:
        return {s: d for s, d in ex.map(one, sym_list) if d}


def atr_and_high(sym, asof):
    r = chart(sym.replace(".", "-"), "3mo", "1d")
    q = r["indicators"]["quote"][0]
    rows = [(h, l, c) for t, h, l, c in zip(r["timestamp"], q["high"], q["low"], q["close"])
            if None not in (h, l, c) and ny_date(t) < asof]
    tr = [max(rows[i][0] - rows[i][1], abs(rows[i][0] - rows[i - 1][2]), abs(rows[i][1] - rows[i - 1][2]))
          for i in range(1, len(rows))]
    return sum(tr[-14:]) / 14, max(h for h, _, _ in rows[-20:])


def setup_workdir(wd):
    os.makedirs(os.path.join(wd, "trader"), exist_ok=True)
    os.chdir(wd)
    path = "trader/config.json"
    if not os.path.exists(path):
        json.dump(CONFIG, open(path, "w"), indent=2)


def dubai(ny_hhmm, date):
    t = dt.datetime.combine(date, dt.time.fromisoformat(ny_hhmm), NY).astimezone(DXB)
    return t.strftime("%-I:%M %p")


def ny_to_dubai_label(label, date):
    """'9:50 AM' (New York) -> '6:50 PM Dubai (9:50 AM NY)'."""
    t = dt.datetime.strptime(label, "%I:%M %p").time()
    d = dt.datetime.combine(date, t, NY).astimezone(DXB)
    return "%s Dubai (%s NY)" % (d.strftime("%-I:%M %p"), label)


# ----------------------------------------------------------------- stages
def market_gate(asof):
    g = {"no_trade": False, "reasons": []}
    try:
        s = chart("SPY", "5d", "1d")
        q = s["indicators"]["quote"][0]
        days = [(ny_date(t), o, c) for t, o, c in zip(s["timestamp"], q["open"], q["close"]) if c]
        prev = [d for d in days if d[0] < asof][-1]
        today = [d for d in days if d[0] == asof]
        g["spy_prev_close"] = prev[2]
        last = s["meta"]["regularMarketPrice"]
        g["spy_last"] = last
        ref = today[0][1] if today and today[0][1] else last
        g["spy_gap_pct"] = (ref / prev[2] - 1) * 100
        v = chart("^VIX", "5d", "1d")["meta"]
        g["vix"] = v["regularMarketPrice"]
        if g["vix"] > VIX_MAX:
            g["reasons"].append("VIX %.1f > %.0f" % (g["vix"], VIX_MAX))
        if abs(g["spy_gap_pct"]) > GAP_MAX:
            g["reasons"].append("SPY gap %+.2f%% beyond +-%.1f%%" % (g["spy_gap_pct"], GAP_MAX))
        g["no_trade"] = bool(g["reasons"])
    except Exception as e:
        g["error"] = str(e)
    return g


def sectors(asof):
    out = []
    for s in "SPY QQQ IWM SMH XLK XLF XLE XLV XLI XLY XLP XLU XLB XLRE XLC".split():
        try:
            m = chart(s, "5d", "1d")
            q = m["indicators"]["quote"][0]
            cl = [(ny_date(t), c) for t, c in zip(m["timestamp"], q["close"]) if c]
            done = [c for d, c in cl if d < asof]
            last = m["meta"]["regularMarketPrice"]
            out.append({"symbol": s, "prev_session_pct": (done[-1] / done[-2] - 1) * 100,
                        "now_vs_prev_close_pct": (last / done[-1] - 1) * 100})
        except Exception as e:
            log("sector %s failed: %s" % (s, e))
    return out


def candidates(asof, now_arg):
    """Same filters in every mode. Uses completed sessions only."""
    if not os.path.exists("trader/universe.txt") or time.time() - os.path.getmtime("trader/universe.txt") > 20 * 3600:
        run([SK + "/trader-scanner/scan.py", "universe"])
    args = [SK + "/trader-scanner/scan.py", "daily", "--min-price", str(MIN_PRICE),
            "--min-volume", "0", "--no-write"]
    if now_arg:
        args += ["--now", now_arg]
    daily = run_json(args, timeout=1200)
    if "_error" in daily:
        return {"error": daily["_error"], "counts": {}}
    hits = daily.get("hits", [])
    counts = {"universe": daily.get("universe"), "scanned": daily.get("scanned"),
              "setups_matched": len(hits)}
    up = [h for h in hits if set(h["setups"]) & UPTREND and h["price"] <= MAX_PRICE and h["rsi"] <= MAX_RSI]
    counts["uptrend_price_rsi"] = len(up)
    prev = completed_daily([h["symbol"] for h in up], asof)
    keep = []
    for h in up:
        p = prev.get(h["symbol"])
        if not p:
            continue
        h.update(p)
        if p["prev_volume"] >= MIN_PREV_VOLUME and CHG_MIN <= p["prev_chg"] <= CHG_MAX:
            keep.append(h)
    counts["with_completed_data"] = len(prev)
    counts["after_volume_and_change"] = len(keep)
    keep.sort(key=lambda h: -h["prev_chg"])
    return {"daily": daily, "list": keep, "counts": counts}


def news_flags(symbols, hours=168):
    """Flag tickers with an upcoming-event headline. Chunked; SEC then headlines-only fallback."""
    flags, ok = {}, set()
    for i in range(0, len(symbols), 5):
        chunk = symbols[i:i + 5]
        base = [SK + "/trader-news/news.py", "--tickers"] + chunk + [
            "--hours", str(hours), "--all", "--dry-run", "--no-moves", "--state", "trader/ns.json"]
        d = None
        for extra in ([], ["--no-sec"]):
            try:
                d = run_json(base + extra, timeout=240)
                if "_error" not in d:
                    break
            except subprocess.TimeoutExpired:
                d = None
        if not d or "_error" in d:
            continue
        ok.update(chunk)
        for it in d.get("items", []):
            if EVENT_WORDS.search(it.get("title", "")):
                flags.setdefault(it.get("ticker"), it["title"][:100])
    return flags, ok


def self_check(c, extra=None):
    """Turn suspicious counts into an explicit warning instead of a silent 'no trade'."""
    w = []
    n = c.get("counts", {})
    if c.get("error"):
        w.append("DATA CHECK FAILED: daily scan error: %s" % c["error"])
    if (n.get("scanned") or 0) < 500:
        w.append("DATA CHECK FAILED: only %s charts scanned (expected ~1,000+)" % n.get("scanned"))
    if n.get("uptrend_price_rsi") and n.get("with_completed_data", 0) < 0.8 * n["uptrend_price_rsi"]:
        w.append("DATA CHECK: completed-session data missing for %d of %d candidates" % (
            n["uptrend_price_rsi"] - n.get("with_completed_data", 0), n["uptrend_price_rsi"]))
    if n.get("uptrend_price_rsi", 0) >= 20 and n.get("after_volume_and_change", 0) < 5:
        w.append("DATA CHECK: volume/change filter kept only %d of %d; check the data before trusting 'no trade'"
                 % (n.get("after_volume_and_change", 0), n["uptrend_price_rsi"]))
    if extra:
        w += extra
    return w


def outcome(sym, date, trigger_hhmm, stop, target, flat="15:55"):
    """What actually happened after the trigger: stop, target, or the 15:55 close."""
    """Starts at the bar AFTER the trigger bar (the signal is only known when the
    trigger bar closes) and fills at that bar's open. R is measured on the planned
    risk; net_R also subtracts COST_R."""
    r = chart(sym.replace(".", "-"), "5d", "5m")
    q = r["indicators"]["quote"][0]
    risk = (target - stop) / 3.0
    fill = last = None
    res = None
    for t, o, h, l, c in zip(r["timestamp"], q["open"], q["high"], q["low"], q["close"]):
        tt = dt.datetime.fromtimestamp(t, NY)
        if tt.date() != date or h is None:
            continue
        hhmm = tt.strftime("%H:%M")
        if hhmm <= trigger_hhmm:          # the trigger bar itself happened before the entry
            continue
        if hhmm >= flat:
            break
        if fill is None:
            fill = o
        if l <= stop:
            res = {"outcome": "stop", "at": hhmm, "exit": stop}
            break
        if h >= target:
            res = {"outcome": "target", "at": hhmm, "exit": target}
            break
        last = c
    if fill is None:
        return {"outcome": "no data"}
    if res is None:
        res = {"outcome": "flat at 15:55", "exit": round(last, 2)}
    res["fill"] = round(fill, 2)
    res["R"] = round((res["exit"] - fill) / risk, 2)
    res["net_R"] = round(res["R"] - COST_R, 2)
    return res


def intraday_stats(sym, date, or_bars=("09:30", "09:35", "09:40")):
    """Opening-range volume vs the same window over the last 14 sessions (RVOL),
    today's opening-range width, and the time of the newest bar (staleness)."""
    r = chart(sym.replace(".", "-"), "1mo", "5m")
    q = r["indicators"]["quote"][0]
    per_day, newest = {}, None
    for t, v in zip(r["timestamp"], q["volume"]):
        tt = dt.datetime.fromtimestamp(t, NY)
        if tt.date() == date:
            newest = tt
        if tt.strftime("%H:%M") in or_bars and v is not None:
            per_day.setdefault(tt.date(), []).append(v)
    today = sum(per_day.get(date, []))
    past = [sum(v) for d, v in sorted(per_day.items()) if d < date and len(v) == len(or_bars)][-14:]
    avg = sum(past) / len(past) if past else 0
    return {"or_volume": today, "or_volume_avg14": round(avg),
            "rvol": round(today / avg, 2) if avg else None,
            "newest_bar": newest.strftime("%H:%M") if newest else None}


def spy_above_or_mid(date, now):
    """Is SPY trading above the midpoint of its own 15-minute opening range?"""
    r = chart("SPY", "1d", "5m")
    q = r["indicators"]["quote"][0]
    hi = lo = last = None
    for t, h, l, c in zip(r["timestamp"], q["high"], q["low"], q["close"]):
        tt = dt.datetime.fromtimestamp(t, NY)
        if tt.date() != date or h is None or tt > now:
            continue
        if tt.strftime("%H:%M") < "09:45":
            hi = h if hi is None else max(hi, h)
            lo = l if lo is None else min(lo, l)
        last = c
    if hi is None or last is None:
        return None
    return last > (hi + lo) / 2


def earnings_today(date):
    """Symbols reporting on `date` from Nasdaq's public earnings calendar.
    Returns None if the calendar cannot be reached (then the headline check is used)."""
    try:
        d = get_json("https://api.nasdaq.com/api/calendar/earnings?date=%s" % date.isoformat(), tries=2)
        rows = (d.get("data") or {}).get("rows") or []
        return {r["symbol"].upper(): r.get("time", "") for r in rows if r.get("symbol")}
    except Exception as e:
        log("earnings calendar unavailable: %s" % e)
        return None


# FOMC decision days (14:00 ET). Source: federalreserve.gov. Add 2027 dates when published.
FOMC_DAYS = {"2026-12-09"}
COST_R = 0.15          # assumed round-trip slippage + spread, in R (research: ~0.1-0.2R)
MIN_RVOL = 1.0         # opening-range volume at least its 14-day average
OR_WIDTH_ATR = (0.15, 0.60)   # opening range between 15% and 60% of ATR(14): under test


def analyse_trigger(h, date, now_arg):
    """entry -> exit -> time machine -> budget -> risk for one breakout."""
    s = h["symbol"]
    nowa = ["--now", now_arg] if now_arg else []
    e = run_json([SK + "/trader-entry/entry.py", "check", s, "--write"] + nowa)
    if e.get("status") != "triggered":
        return {"symbol": s, "skipped": "entry check: %s" % e.get("status", e.get("_error"))}
    x = run_json([SK + "/trader-exit/exit.py", "plan", "--write"] + nowa)
    tm = run_json([SK + "/trader-timemachine/timemachine.py", "run", "--until", "15:55"])
    sz = run_json([SK + "/trader-budget/size.py", "--write"])
    rk = run_json([SK + "/trader-risk/check.py", "--no-write"] + nowa)
    res = (rk.get("results") or [{}])[0]
    mc, hist = tm.get("mc", {}), tm.get("history", {})
    a = {
        "symbol": s, "trigger_time": e["trigger"]["time"], "trigger_bar_start": e["trigger"]["bar_start"],
        "trigger_price": e["trigger"]["price"], "or_high": e["opening_range"]["high"],
        "or_low": e["opening_range"]["low"], "entry": x.get("entry"), "stop": x.get("stop"),
        "target": x.get("target"), "risk_per_share": x.get("risk_per_share"),
        "shares": res.get("amount"), "position_usd": res.get("value_usd"), "max_loss": res.get("max_loss"),
        "risk_verdict": res.get("verdict"), "risk_reasons": res.get("reasons"),
        "tm_target_first": mc.get("p_target"), "tm_stop_first": mc.get("p_stop"),
        "tm_neither": mc.get("p_neither"), "tm_avg_R": mc.get("avg_R"),
        "hist_fired": hist.get("triggered"), "hist_target": hist.get("target"),
        "hist_stop": hist.get("stop"), "hist_avg_R": hist.get("avg_R"), "tm_verdict": tm.get("verdict"),
        "prev_chg": h.get("prev_chg"), "setups": h.get("setups"),
    }
    why, cautions = [], []
    if a["risk_verdict"] != "PASS":
        why.append("risk check %s %s" % (a["risk_verdict"], a["risk_reasons"] or ""))
    # Stock in play? (Zarattini, Barbon & Aziz 2024: the ORB edge came from high relative volume.)
    try:
        st = intraday_stats(s, date)
        a.update(st)
        if st["rvol"] is None:
            cautions.append("relative volume unknown")
        elif st["rvol"] < MIN_RVOL:
            why.append("not in play: opening-range volume %.1fx its 14-day average (need %.1fx)"
                       % (st["rvol"], MIN_RVOL))
    except Exception as e:
        cautions.append("relative volume check failed: %s" % e)
    # Opening-range width vs ATR (too narrow = noise, too wide = no room for 2R).
    try:
        atr, _ = atr_and_high(s, date)
        a["atr"] = round(atr, 2)
        w = (a["or_high"] - a["or_low"]) / atr
        a["or_width_atr"] = round(w, 2)
        if not OR_WIDTH_ATR[0] <= w <= OR_WIDTH_ATR[1]:
            why.append("opening range is %.0f%% of ATR (allowed %.0f-%.0f%%)"
                       % (100 * w, 100 * OR_WIDTH_ATR[0], 100 * OR_WIDTH_ATR[1]))
    except Exception as e:
        cautions.append("range-width check failed: %s" % e)
    # The $500 cap often binds before the $10 risk: say so, it shrinks the real risk.
    if a["max_loss"] is not None and a["max_loss"] < 6:
        cautions.append("position cap binds: real risk only $%.2f of the $10 allowed" % a["max_loss"])
    # Per-stock Time Machine history is ~20-30 past setups: informative, too noisy to veto alone.
    if a["tm_target_first"] is not None and a["tm_target_first"] < 0.5 * (a["tm_stop_first"] or 0):
        cautions.append("Time Machine leans to the stop (%.0f%% target vs %.0f%% stop; small sample)"
                        % (100 * a["tm_target_first"], 100 * a["tm_stop_first"]))
    if a["hist_avg_R"] is not None and a["hist_avg_R"] - COST_R < 0:
        cautions.append("this rule on %s, last 59 sessions: avg %.2fR before costs, %.2fR after %.2fR costs"
                        % (s, a["hist_avg_R"], a["hist_avg_R"] - COST_R, COST_R))
    if date.isoformat() in FOMC_DAYS:
        cautions.append("FOMC decision at 14:00 ET today: be flat by 13:55 ET (10:55 PM Dubai)")
    a["verdict"] = "MEETS RULES" if not why else "SKIP per your rules"
    a["why"], a["cautions"] = why, cautions
    return a


# ----------------------------------------------------------------- modes
def mode_shortlist(args):
    now = dt.datetime.now(NY)
    asof = now.date()
    c = candidates(asof, None)
    top = c.get("list", [])[:N_NEWS]
    flags, checked = news_flags([h["symbol"] for h in top]) if top else ({}, set())
    short = []
    for h in top:
        if h["symbol"] in flags:
            h["dropped"] = "event headline: " + flags[h["symbol"]]
            continue
        try:
            h["atr"], h["high20"] = atr_and_high(h["symbol"], asof)
        except Exception as e:
            h["atr"], h["high20"] = None, None
            log("atr %s: %s" % (h["symbol"], e))
        if h["atr"]:
            risk = 0.5 * h["atr"]
            h["rough_shares"] = int(min(10 / risk, 500 / h["prev_close"]))
        short.append(h)
        if len(short) == N_SHORTLIST:
            break
    out = {"mode": "shortlist", "now_ny": now.strftime("%Y-%m-%d %H:%M"),
           "gate": market_gate(asof), "sectors": sectors(asof), "counts": c.get("counts"),
           "news_checked": len(checked), "dropped_for_events": {k: v for k, v in flags.items()},
           "shortlist": [{k: h.get(k) for k in ("symbol", "prev_close", "prev_chg", "prev_volume", "setups",
                                                 "rsi", "atr", "high20", "rough_shares")} for h in short],
           "warnings": self_check(c, [] if short else ["no stock passed every filter"])}
    return out


def mode_signals(args, now_override=None, review=False):
    now = dt.datetime.strptime(now_override, "%Y-%m-%d %H:%M").replace(tzinfo=NY) if now_override else dt.datetime.now(NY)
    asof = now.date()
    now_arg = now.strftime("%Y-%m-%d %H:%M")
    c = candidates(asof, now_arg)
    cand = c.get("list", [])[:N_CANDIDATES]
    wl = dict(c.get("daily", {}))
    wl["hits"] = cand
    json.dump(wl, open("trader/candidates.json", "w"), indent=1)
    orb = run_json([SK + "/trader-scanner/scan.py", "orb", "--watchlist", "trader/candidates.json",
                    "--min-price", "0", "--no-write", "--now", now_arg], timeout=600) if cand else {"hits": []}
    extra = []
    oc = orb.get("counts", {})
    if cand and oc and oc.get("scanned", 0) < 0.5 * len(cand):
        extra.append("DATA CHECK: only %s of %d candidate charts usable for the opening range"
                     % (oc.get("scanned"), len(cand)))
    trig = sorted(orb.get("hits", []), key=lambda h: h.get("bar_start", ""))[:MAX_SIGNALS]
    byname = {h["symbol"]: h for h in cand}
    flags, _ = news_flags([t["symbol"] for t in trig], hours=72) if trig else ({}, set())
    cal = earnings_today(asof)
    if cal is None:
        extra.append("earnings calendar unreachable: upcoming earnings checked by headline search only")
    try:
        spy_ok = spy_above_or_mid(asof, now)
    except Exception:
        spy_ok = None
    sig = []
    for t in trig:
        a = analyse_trigger(byname.get(t["symbol"], {"symbol": t["symbol"]}), asof, now_arg)
        if "verdict" not in a:
            sig.append(a)
            continue
        if t["symbol"] in flags:
            a["why"].append("event headline: " + flags[t["symbol"]])
        if cal and t["symbol"] in cal:
            a["why"].append("reports earnings today (%s)" % (cal[t["symbol"]] or "time unknown"))
        if spy_ok is False:
            a["cautions"].append("SPY is below its own opening-range midpoint: weak market at signal time")
        if not review and a.get("newest_bar"):
            lag = (now - dt.datetime.combine(asof, dt.time.fromisoformat(a["newest_bar"]), NY)).total_seconds() / 60
            if lag > 15:
                a["cautions"].append("data looks stale: newest 5-minute bar is %d minutes old" % lag)
        a["verdict"] = "MEETS RULES" if not a["why"] else "SKIP per your rules"
        if review and a.get("stop"):
            a["actual"] = outcome(a["symbol"], asof, a["trigger_bar_start"], a["stop"], a["target"])
        if not review and a.get("entry"):
            try:
                last = chart(a["symbol"].replace(".", "-"), "1d", "5m")["meta"]["regularMarketPrice"]
                a["price_now"] = last
                a["late_ok"] = last <= a["entry"] + 0.25 * a["risk_per_share"] and last > a["stop"]
            except Exception:
                pass
        sig.append(a)
    # Strongest "in play" first among the ones that meet the rules.
    sig.sort(key=lambda a: (a.get("verdict") != "MEETS RULES", -(a.get("rvol") or 0)))
    summary = None
    if review:
        done = [a for a in sig if a.get("actual", {}).get("R") is not None]
        meets = [a for a in done if a["verdict"] == "MEETS RULES"]
        summary = {"breakouts": len(done),
                   "all_net_R": round(sum(a["actual"]["net_R"] for a in done), 2),
                   "meets_rules": len(meets),
                   "meets_rules_net_R": round(sum(a["actual"]["net_R"] for a in meets), 2),
                   "cost_R_assumed": COST_R}
    return {"mode": "review" if review else "signals", "now_ny": now_arg,
            "gate": market_gate(asof), "spy_above_or_mid": spy_ok, "counts": c.get("counts"),
            "review_summary": summary,
            "candidates": [{k: h.get(k) for k in ("symbol", "prev_close", "prev_chg", "setups", "rsi")} for h in cand],
            "orb_counts": oc, "triggered": [t["symbol"] for t in trig], "signals": sig,
            "warnings": self_check(c, extra)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["shortlist", "signals", "review"])
    ap.add_argument("--dir", default=os.path.expanduser("~/trading"), help="work folder (default ~/trading)")
    ap.add_argument("--now", help='New York time "YYYY-MM-DD HH:MM" (signals/review); review defaults to today 11:30')
    a = ap.parse_args()
    setup_workdir(a.dir)
    code, clock = run([SK + "/trader-alerts/alerts.py", "clock"] + (["--now", a.now] if a.now else []))
    if "OPEN" not in clock and "opens" in clock.lower() and a.mode != "shortlist" and not a.now:
        pass
    if a.mode == "shortlist":
        out = mode_shortlist(a)
    elif a.mode == "signals":
        out = mode_signals(a, a.now)
    else:
        out = mode_signals(a, a.now or dt.datetime.now(NY).strftime("%Y-%m-%d") + " 11:30", review=True)
    out["clock"] = clock.strip()
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
