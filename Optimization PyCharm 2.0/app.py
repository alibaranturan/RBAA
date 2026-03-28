import os
import gc
import sys
import inspect
import threading

import numpy as np
import pandas as pd

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ── Otomatik dizin tespiti ────────────────────────────────────────────────────
# Hangi dizinden başlatılırsa başlatılsın (relative/absolute path, PyCharm,
# terminal vs.) app.py'nin GERÇEK konumunu bulur ve CWD olarak kilitler.
def _resolve_base_dir() -> str:
    """app.py'nin gerçek klasörünü bulur — CWD'den bağımsız."""
    def _has_data(d):
        return os.path.isdir(os.path.join(d, 'Data'))

    # 1) inspect → kaynak dosyanın gerçek yolu (symlink çözümler)
    try:
        _src = inspect.getfile(inspect.currentframe())
        candidate = os.path.dirname(os.path.realpath(_src))
        if _has_data(candidate):
            return candidate
    except Exception:
        pass
    # 2) __file__ → absolute path ile çöz
    try:
        candidate = os.path.dirname(os.path.realpath(os.path.abspath(__file__)))
        if _has_data(candidate):
            return candidate
    except Exception:
        pass
    # 3) sys.argv[0] → python app.py şeklinde çalıştırıldıysa
    try:
        if sys.argv[0].endswith('.py'):
            candidate = os.path.dirname(os.path.realpath(os.path.abspath(sys.argv[0])))
            if _has_data(candidate):
                return candidate
    except Exception:
        pass
    # 4) Data/ klasörünü Desktop altında ara (son çare)
    desktop = os.path.expanduser('~/Desktop')
    for root, dirs, files in os.walk(desktop):
        if 'Data' in dirs and 'app.py' in files:
            return root
    # 5) CWD fallback
    return os.getcwd()

BASE_DIR = _resolve_base_dir()
DATA_DIR = os.path.join(BASE_DIR, 'Data')
os.makedirs(DATA_DIR, exist_ok=True)
os.chdir(BASE_DIR)   # CWD'yi sabitle
print(f"[app.py] BASE_DIR → {BASE_DIR}", flush=True)
print(f"[app.py] DATA_DIR → {DATA_DIR}", flush=True)

ASSET_COLORS = ['#0071e3', '#30d158', '#ff453a', '#ff9f0a', '#ffd60a', '#bf5af2', '#64d2ff']

ASSETS = [
    "SPTR5BNK Index",
    "DJITR Index",
    "SPXT Index",
    "XAUUSD Curncy",
    "XAGUSD Curncy",
    "LT09TRUU Index",
    "LD12TRUU Index",
]


_prices_cache = None
_prices_lock  = threading.Lock()

def _load_prices():
    """CSV'yi bir kez okur, sonraki çağrılarda cache'den döner."""
    global _prices_cache
    with _prices_lock:
        if _prices_cache is not None:
            return _prices_cache
        csv_path  = os.path.join(DATA_DIR, '01_PriceData_provided.csv')
        xlsx_path = os.path.join(DATA_DIR, '01_PriceData_provided.xlsx')
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path, parse_dates=["Dates"])
        else:
            df = pd.read_excel(xlsx_path, parse_dates=["Dates"])
        _prices_cache = df.sort_values("Dates").set_index("Dates")
        return _prices_cache


# ── 1. Data Validation ────────────────────────────────────────────
@app.route('/api/data-validation')
def data_validation():
    try:
        csv_path = os.path.join(DATA_DIR, '01_PriceData_provided.csv')
        df = pd.read_csv(csv_path)

        dates = pd.to_datetime(df["Dates"], errors="coerce")
        value_cols = [c for c in df.columns if c != "Dates"]
        num = df[value_cols].apply(pd.to_numeric, errors="coerce")
        stats_df = num.describe().T[["min", "max", "mean"]].round(4)

        stats = {
            col: {
                "min": float(stats_df.loc[col, "min"]),
                "max": float(stats_df.loc[col, "max"]),
                "mean": float(stats_df.loc[col, "mean"]),
            }
            for col in stats_df.index
        }

        result = {
            "status": "success",
            "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
            "columns": df.columns.tolist(),
            "date_validation": {
                "nat_count": int(dates.isna().sum()),
                "min_date": str(dates.min().date()),
                "max_date": str(dates.max().date()),
                "is_sorted": bool(dates.is_monotonic_increasing),
                "duplicate_dates": int(dates.duplicated().sum()),
            },
            "non_numeric_total": int(num.isna().sum().sum()),
            "stats": stats,
        }
        # ── Save validation summary → 1.1_DataValidation_summary.json ──
        import json as _json
        out_path = os.path.join(DATA_DIR, '1.1_DataValidation_summary.json')
        with open(out_path, 'w') as _f:
            _json.dump(result, _f, indent=2)
        result["saved_path"] = "Data/1.1_DataValidation_summary.json"
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 2. Rejim Tarihleri CSV Generator ─────────────────────────────
@app.route('/api/regime-dates')
def regime_dates_api():
    try:
        data = [
            ["Downturn",    "2001-06-01", "2001-10-31",  5],
            ["Goldilocks",  "2001-11-01", "2004-05-31", 31],
            ["Downturn",    "2004-06-01", "2006-08-31", 27],
            ["Goldilocks",  "2006-09-01", "2007-09-30", 13],
            ["Downturn",    "2007-10-01", "2008-10-31", 13],
            ["Stagflation", "2008-11-01", "2009-09-30", 11],
            ["Goldilocks",  "2009-10-01", "2011-03-31", 18],
            ["Downturn",    "2011-04-01", "2012-02-29", 11],
            ["Goldilocks",  "2012-03-01", "2019-10-31", 92],
            ["Downturn",    "2019-11-01", "2020-03-31",  5],
            ["Stagflation", "2020-04-01", "2020-05-31",  2],
            ["Goldilocks",  "2020-06-01", "2021-03-31", 10],
            ["Overheating", "2021-04-01", "2022-10-31", 19],
            ["Downturn",    "2022-11-01", "2024-04-30", 18],
            ["Goldilocks",  "2024-05-01", "2025-08-31", 16],
        ]
        df = pd.DataFrame(data, columns=["Regime", "Start_Date", "End_Date", "Duration_Months"])
        out = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        df.to_csv(out, index=False)

        summary = df.groupby("Regime")["Duration_Months"].sum().to_dict()

        return jsonify({
            "status": "success",
            "saved_path": "Data/02_RegimeDates_output.csv",
            "rows": len(df),
            "data": df.to_dict(orient='records'),
            "summary": summary,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 3. Log Return Calculator ──────────────────────────────────────
@app.route('/api/log-returns')
def log_returns_api():
    try:
        df = _load_prices()
        assets = [c for c in df.columns if c in ASSETS or len(df.columns) <= 8]
        log_ret = np.log(df[assets] / df[assets].shift(1)).dropna()
        log_ret.to_csv(os.path.join(DATA_DIR, '03_LogReturns_output.csv'))

        sample = []
        for date, row in log_ret.head(5).iterrows():
            d = {"Date": str(date.date())}
            d.update({k: round(float(v), 6) for k, v in row.items()})
            sample.append(d)

        stats_df = log_ret.describe().round(6)
        stats = {
            col: {stat: round(float(stats_df.loc[stat, col]), 6) for stat in stats_df.index}
            for col in stats_df.columns
        }

        # Full T-bill (LD12TRUU) series for risk-free rate card
        tbill_col = "LD12TRUU Index"
        rf_series = []
        if tbill_col in log_ret.columns:
            for date, val in log_ret[tbill_col].items():
                rf_series.append({"Date": str(date.date()), "rf": round(float(val) * 100, 6)})

        return jsonify({
            "status": "success",
            "saved_path": "Data/03_LogReturns_output.csv",
            "rows": len(log_ret),
            "assets": assets,
            "sample": sample,
            "stats": stats,
            "rf_series": rf_series,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 4 & 5: Raw/Log grafikler artık tamamen tarayıcıda SVG olarak üretiliyor.
#           Bu endpoint'ler kaldırıldı — Flask'ta matplotlib kullanılmıyor.


# ── 6. Regime Attribute Assignment ───────────────────────────────
REGIME_COLORS = {
    'Goldilocks':  '#30d158',
    'Downturn':    '#ff453a',
    'Stagflation': '#ffd60a',
    'Overheating': '#ff9f0a',
    'Unknown':     '#8e8e93',
}

REGIME_DATA = [
    ["Downturn",    "2001-06-01", "2001-10-31"],
    ["Goldilocks",  "2001-11-01", "2004-05-31"],
    ["Downturn",    "2004-06-01", "2006-08-31"],
    ["Goldilocks",  "2006-09-01", "2007-09-30"],
    ["Downturn",    "2007-10-01", "2008-10-31"],
    ["Stagflation", "2008-11-01", "2009-09-30"],
    ["Goldilocks",  "2009-10-01", "2011-03-31"],
    ["Downturn",    "2011-04-01", "2012-02-29"],
    ["Goldilocks",  "2012-03-01", "2019-10-31"],
    ["Downturn",    "2019-11-01", "2020-03-31"],
    ["Stagflation", "2020-04-01", "2020-05-31"],
    ["Goldilocks",  "2020-06-01", "2021-03-31"],
    ["Overheating", "2021-04-01", "2022-10-31"],
    ["Downturn",    "2022-11-01", "2024-04-30"],
    ["Goldilocks",  "2024-05-01", "2025-08-31"],
]

@app.route('/api/regime-attribute')
def regime_attribute():
    try:
        # Load prices
        csv_path = os.path.join(DATA_DIR, '01_PriceData_provided.csv')
        df = pd.read_csv(csv_path, parse_dates=["Dates"])
        df = df.sort_values("Dates").reset_index(drop=True)

        # Build regime lookup from hardcoded data (or regime_dates.csv if exists)
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            rdf = pd.read_csv(rcsv, parse_dates=["Start_Date", "End_Date"])
        else:
            rdf = pd.DataFrame(REGIME_DATA, columns=["Regime", "Start_Date", "End_Date"])
            rdf["Start_Date"] = pd.to_datetime(rdf["Start_Date"])
            rdf["End_Date"]   = pd.to_datetime(rdf["End_Date"])

        # Vectorized assignment
        regime_col = pd.Series("Unknown", index=df.index, dtype="object")
        for _, row in rdf.iterrows():
            mask = (df["Dates"] >= row["Start_Date"]) & (df["Dates"] <= row["End_Date"])
            regime_col[mask] = row["Regime"]
        df["Regime"] = regime_col

        # Save
        out_path = os.path.join(DATA_DIR, '06_PriceDataWithRegime_output.csv')
        df.to_csv(out_path, index=False)

        # Value counts
        counts = df["Regime"].value_counts().to_dict()
        total  = len(df)
        distribution = {
            k: {"days": int(v), "pct": round(v / total * 100, 1)}
            for k, v in counts.items()
        }

        # All rows
        all_rows = [
            {"date": str(r["Dates"].date()), "regime": r["Regime"]}
            for _, r in df[["Dates", "Regime"]].iterrows()
        ]

        return jsonify({
            "status":       "success",
            "saved_path":   "Data/06_PriceDataWithRegime_output.csv",
            "total_rows":   total,
            "distribution": distribution,
            "rows":         all_rows,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 7. Annualized Return by Regime ───────────────────────────────
@app.route('/api/annualized-return')
def annualized_return():
    try:
        # Load prices (Dates as column)
        csv_path = os.path.join(DATA_DIR, '01_PriceData_provided.csv')
        df = pd.read_csv(csv_path, parse_dates=["Dates"])
        df = df.sort_values("Dates").reset_index(drop=True)

        # Load regime periods
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            rdf = pd.read_csv(rcsv, parse_dates=["Start_Date", "End_Date"])
        else:
            rdf = pd.DataFrame(REGIME_DATA, columns=["Regime", "Start_Date", "End_Date"])
            rdf["Start_Date"] = pd.to_datetime(rdf["Start_Date"])
            rdf["End_Date"]   = pd.to_datetime(rdf["End_Date"])

        assets  = [c for c in df.columns if c != "Dates"]
        regimes = ["Goldilocks", "Downturn", "Stagflation", "Overheating"]

        # ── Calculate annualized return per (regime, asset) ──
        results = {}
        for regime in regimes:
            periods = rdf[rdf["Regime"] == regime]
            asset_results = {}
            for asset in assets:
                compound_growth = 1.0
                total_days      = 0
                for _, period in periods.iterrows():
                    mask   = (df["Dates"] >= period["Start_Date"]) & \
                             (df["Dates"] <= period["End_Date"])
                    subset = df.loc[mask, asset].dropna()
                    if len(subset) < 2:
                        continue
                    compound_growth *= float(subset.iloc[-1]) / float(subset.iloc[0])
                    total_days      += len(subset)
                if total_days < 2:
                    asset_results[asset] = None
                else:
                    r_total    = compound_growth - 1
                    annualized = (1 + r_total) ** (252 / total_days) - 1
                    asset_results[asset] = round(annualized * 100, 2)
            results[regime] = asset_results

        # ── Save CSV ──
        rows_out = [
            {"Regime": r, "Asset": a, "Annualized_Return_Pct": results[r].get(a)}
            for r in regimes for a in assets
        ]
        pd.DataFrame(rows_out).to_csv(
            os.path.join(DATA_DIR, '07_AnnualizedReturnsByRegime_output.csv'), index=False
        )

        return jsonify({
            "status":   "success",
            "regimes":  regimes,
            "assets":   assets,
            "results":  results,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 8. Return Statistics (Avg Return + Volatility) ───────────────
@app.route('/api/return-stats')
def return_stats():
    gc.collect()
    try:
        df = _load_prices()   # DatetimeIndex

        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            rdf = pd.read_csv(rcsv, parse_dates=["Start_Date", "End_Date"])
        else:
            rdf = pd.DataFrame(REGIME_DATA, columns=["Regime", "Start_Date", "End_Date"])
            rdf["Start_Date"] = pd.to_datetime(rdf["Start_Date"])
            rdf["End_Date"]   = pd.to_datetime(rdf["End_Date"])

        assets  = list(df.columns)
        regimes = ["Goldilocks", "Downturn", "Stagflation", "Overheating"]

        # Calculate log returns WITHIN each regime period (avoid cross-boundary returns)
        period_buckets = []
        for _, period in rdf.iterrows():
            mask   = (df.index >= period["Start_Date"]) & (df.index <= period["End_Date"])
            subset = df[mask]
            if len(subset) < 2:
                continue
            lr = np.log(subset / subset.shift(1)).dropna()
            lr["_Regime"] = period["Regime"]
            period_buckets.append(lr)

        if not period_buckets:
            return jsonify({"status": "error", "message": "No regime data found"}), 500

        all_ret = pd.concat(period_buckets)

        avg_return       = {}
        avg_return_daily = {}
        volatility       = {}
        volatility_daily = {}
        for regime in regimes:
            r_data = all_ret[all_ret["_Regime"] == regime][assets]
            avg_return[regime]       = {}
            avg_return_daily[regime] = {}
            volatility[regime]       = {}
            volatility_daily[regime] = {}
            for asset in assets:
                s = r_data[asset].dropna()
                if len(s) < 2:
                    avg_return[regime][asset]       = None
                    avg_return_daily[regime][asset] = None
                    volatility[regime][asset]       = None
                    volatility_daily[regime][asset] = None
                else:
                    avg_return[regime][asset]       = round(float(s.mean()) * 252 * 100,       4)
                    avg_return_daily[regime][asset] = round(float(s.mean()) * 100,             6)
                    volatility[regime][asset]       = round(float(s.std())  * np.sqrt(252) * 100, 4)
                    volatility_daily[regime][asset] = round(float(s.std())  * 100,             6)

        return jsonify({
            "status":            "success",
            "regimes":           regimes,
            "assets":            assets,
            "avg_return":        avg_return,
            "avg_return_daily":  avg_return_daily,
            "volatility":        volatility,
            "volatility_daily":  volatility_daily,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 9. Volatility by Regime ──────────────────────────────────────
@app.route('/api/volatility')
def volatility_by_regime_api():
    try:
        df = _load_prices()   # DatetimeIndex

        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            rdf = pd.read_csv(rcsv, parse_dates=["Start_Date", "End_Date"])
        else:
            rdf = pd.DataFrame(REGIME_DATA, columns=["Regime", "Start_Date", "End_Date"])
            rdf["Start_Date"] = pd.to_datetime(rdf["Start_Date"])
            rdf["End_Date"]   = pd.to_datetime(rdf["End_Date"])

        assets  = list(df.columns)
        regimes = ["Goldilocks", "Downturn", "Stagflation", "Overheating"]

        # Log returns per period — no cross-boundary contamination
        period_buckets = []
        for _, period in rdf.iterrows():
            mask   = (df.index >= period["Start_Date"]) & (df.index <= period["End_Date"])
            subset = df[mask]
            if len(subset) < 2:
                continue
            lr = np.log(subset / subset.shift(1)).dropna()
            lr["_Regime"] = period["Regime"]
            period_buckets.append(lr)

        if not period_buckets:
            return jsonify({"status": "error", "message": "No regime data found"}), 500

        all_ret = pd.concat(period_buckets)

        vol_ann   = {}
        vol_daily = {}
        for regime in regimes:
            r_data = all_ret[all_ret["_Regime"] == regime][assets]
            vol_ann[regime]   = {}
            vol_daily[regime] = {}
            for asset in assets:
                s = r_data[asset].dropna()
                if len(s) < 2:
                    vol_ann[regime][asset]   = None
                    vol_daily[regime][asset] = None
                else:
                    vol_ann[regime][asset]   = round(float(s.std()) * np.sqrt(252) * 100, 4)
                    vol_daily[regime][asset] = round(float(s.std()) * 100,             6)

        # Save CSV
        rows_out = [
            {
                "Regime":           regime,
                "Asset":            asset,
                "Volatility_Ann":   vol_ann[regime].get(asset),
                "Volatility_Daily": vol_daily[regime].get(asset),
            }
            for regime in regimes for asset in assets
        ]
        pd.DataFrame(rows_out).to_csv(
            os.path.join(DATA_DIR, '09_VolatilityByRegime_output.csv'), index=False
        )

        return jsonify({
            "status":    "success",
            "regimes":   regimes,
            "assets":    assets,
            "vol_ann":   vol_ann,
            "vol_daily": vol_daily,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 10. Annualized Return per Asset per Regime (XLSM) ────────────
XLSM_PATH = os.path.join(os.path.dirname(BASE_DIR), 'Optimization 1.0.xlsm')

XLSM_ASSETS  = [
    'SPTR5BNK Index', 'DJITR Index', 'SPXT Index',
    'XAUUSD Curncy', 'XAGUSD Curncy', 'LT09TRUU Index', 'LD12TRUU Index',
]
XLSM_REGIMES = ['Goldilocks', 'Downturn', 'Stagflation', 'Overheating']

@app.route('/api/annualized-return-xlsm')
def annualized_return_xlsm():
    gc.collect()
    try:
        # skiprows=3: rows 0-2 are sheet metadata; row 3 is the real header
        xl = pd.read_excel(XLSM_PATH, sheet_name='value', skiprows=3, engine='openpyxl')
        xl = xl.sort_values('Dates').reset_index(drop=True)

        # Publication lag: shift regime flags 1 row down
        # (today's regime decision is based on yesterday's published data)
        xl[XLSM_REGIMES] = xl[XLSM_REGIMES].shift(1)

        # Daily simple returns
        for asset in XLSM_ASSETS:
            xl[f'ret_{asset}'] = xl[asset].pct_change()

        # CAGR per (regime, asset)
        results = {}
        for regime in XLSM_REGIMES:
            results[regime] = {}
            mask = xl[regime] == 1
            for asset in XLSM_ASSETS:
                sub = xl.loc[mask & xl[f'ret_{asset}'].notna(), f'ret_{asset}']
                if len(sub) < 2:
                    results[regime][asset] = None
                else:
                    mu   = sub.mean()
                    cagr = (1 + mu) ** 252 - 1
                    results[regime][asset] = round(float(cagr) * 100, 4)

        # Save CSV
        rows_out = [
            {'Regime': r, 'Asset': a, 'Annualized_Return_Pct': results[r].get(a)}
            for r in XLSM_REGIMES for a in XLSM_ASSETS
        ]
        pd.DataFrame(rows_out).to_csv(
            os.path.join(DATA_DIR, '10_AnnualizedReturnXlsm_output.csv'), index=False
        )

        return jsonify({
            'status':  'success',
            'regimes': XLSM_REGIMES,
            'assets':  XLSM_ASSETS,
            'results': results,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── 11. Average Daily Log Return by Regime ───────────────────────
@app.route('/api/avg-return-daily')
def avg_return_daily_api():
    gc.collect()
    try:
        df = _load_prices()

        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            rdf = pd.read_csv(rcsv, parse_dates=["Start_Date", "End_Date"])
        else:
            rdf = pd.DataFrame(REGIME_DATA, columns=["Regime", "Start_Date", "End_Date"])
            rdf["Start_Date"] = pd.to_datetime(rdf["Start_Date"])
            rdf["End_Date"]   = pd.to_datetime(rdf["End_Date"])

        assets  = list(df.columns)
        regimes = ["Goldilocks", "Downturn", "Stagflation", "Overheating"]

        # Log returns per period — no cross-boundary contamination
        period_buckets = []
        for _, period in rdf.iterrows():
            mask   = (df.index >= period["Start_Date"]) & (df.index <= period["End_Date"])
            subset = df[mask]
            if len(subset) < 2:
                continue
            lr = np.log(subset / subset.shift(1)).dropna()
            lr["_Regime"] = period["Regime"]
            period_buckets.append(lr)

        if not period_buckets:
            return jsonify({"status": "error", "message": "No regime data found"}), 500

        all_ret = pd.concat(period_buckets)

        results = {}
        for regime in regimes:
            r_data = all_ret[all_ret["_Regime"] == regime][assets]
            results[regime] = {}
            for asset in assets:
                s = r_data[asset].dropna()
                results[regime][asset] = round(float(s.mean()) * 100, 6) if len(s) >= 2 else None

        # Save CSV
        rows_out = [
            {"Regime": r, "Asset": a, "Avg_Daily_LogRet_Pct": results[r].get(a)}
            for r in regimes for a in assets
        ]
        pd.DataFrame(rows_out).to_csv(
            os.path.join(DATA_DIR, '11_AvgReturnByRegime_output.csv'), index=False
        )

        return jsonify({
            "status":   "success",
            "regimes":  regimes,
            "assets":   assets,
            "results":  results,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 12. Risk-Free Rate (T-Bill Daily Log Return) ─────────────────
TBILL_COL = "LD12TRUU Index"   # Bloomberg 1-3 Month U.S. T-Bill TR Index

@app.route('/api/risk-free-rate')
def risk_free_rate_api():
    gc.collect()
    try:
        df = _load_prices()

        if TBILL_COL not in df.columns:
            return jsonify({
                "status": "error",
                "message": f"T-bill column '{TBILL_COL}' not found in dataset."
            }), 500

        # Daily log return of T-bill: rf_t = ln(P_t / P_{t-1})
        tbill = df[[TBILL_COL]].dropna()
        rf    = np.log(tbill / tbill.shift(1)).dropna()
        rf.columns = ["risk_free_rate"]
        rf.index.name = "date"

        # Save CSV
        rf.to_csv(os.path.join(DATA_DIR, '12_RiskFreeRateDaily_output.csv'))

        # Stats
        s = rf["risk_free_rate"]
        stats = {
            "count": int(len(s)),
            "mean":  round(float(s.mean()) * 100, 6),
            "std":   round(float(s.std())  * 100, 6),
            "min":   round(float(s.min())  * 100, 6),
            "max":   round(float(s.max())  * 100, 6),
        }

        # Sample — first 5 rows
        sample = [
            {"date": str(idx.date()), "risk_free_rate": round(float(val) * 100, 6)}
            for idx, val in s.head(5).items()
        ]

        # Date range
        date_range = {
            "start": str(s.index[0].date()),
            "end":   str(s.index[-1].date()),
        }

        return jsonify({
            "status":      "success",
            "tbill_col":   TBILL_COL,
            "observations": int(len(s)),
            "date_range":  date_range,
            "stats":       stats,
            "sample":      sample,
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ── 13. Portfolio Return ──────────────────────────────────────────
@app.route('/api/portfolio-return')
def portfolio_return_api():
    gc.collect()
    try:
        # Load log returns
        log_ret = pd.read_csv(os.path.join(DATA_DIR, '03_LogReturns_output.csv'), index_col=0, parse_dates=True)
        log_ret.index.name = 'date'

        # Load or create equal weights
        weights_path = os.path.join(DATA_DIR, '13_PortfolioWeights_provided.csv')
        if os.path.exists(weights_path):
            wdf = pd.read_csv(weights_path)
            wdf.columns = ['asset', 'weight'] if len(wdf.columns) >= 2 else wdf.columns
            weights_s = wdf.set_index('asset')['weight']
        else:
            assets = log_ret.columns.tolist()
            weights_s = pd.Series(1.0 / len(assets), index=assets)

        # Align
        common = [a for a in weights_s.index if a in log_ret.columns]
        w = weights_s[common].astype(float)
        w = w / w.sum()

        # Compute r_p,t = Σ(w_i * r_i,t)
        ret_matrix = log_ret[common].dropna()
        port_ret = ret_matrix.dot(w)

        # Save
        port_df = pd.DataFrame({'portfolio_return': port_ret})
        port_df.index.name = 'date'
        port_df.to_csv(os.path.join(DATA_DIR, '13_PortfolioReturns_output.csv'))

        series = [{'date': str(d.date()), 'r': round(float(v), 8)} for d, v in port_ret.items()]
        stats = {
            'mean': round(float(port_ret.mean()), 8),
            'std':  round(float(port_ret.std()),  8),
            'min':  round(float(port_ret.min()),  8),
            'max':  round(float(port_ret.max()),  8),
            'count': len(port_ret),
        }
        weights_list = [{'asset': a, 'weight': round(float(v), 6)} for a, v in w.items()]

        return jsonify({
            'status': 'success',
            'rows': len(port_df),
            'saved_path': 'portfolio_returns.csv',
            'series': series,
            'stats': stats,
            'weights': weights_list,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── 14. Excess Returns ───────────────────────────────────────────
@app.route('/api/excess-returns')
def excess_returns_api():
    gc.collect()
    try:
        TBILL_COL = "LD12TRUU Index"

        # ── 1. Load asset log returns ─────────────────────────────────────────
        log_ret = pd.read_csv(os.path.join(DATA_DIR, '03_LogReturns_output.csv'),
                              index_col=0, parse_dates=True)
        log_ret.sort_index(inplace=True)

        if TBILL_COL not in log_ret.columns:
            return jsonify({'status': 'error',
                            'message': f'T-bill column "{TBILL_COL}" not found in log_returns.csv'}), 500

        # ── 2. Extract risk-free rate ─────────────────────────────────────────
        rf = log_ret[TBILL_COL]

        # ── 3. Compute excess return for each of the 7 asset columns ─────────
        #       excess_i,t = r_i,t − rf_t   (applied column-by-column)
        all_cols    = log_ret.columns.tolist()          # all 7 columns
        asset_excess = log_ret[all_cols].subtract(rf, axis=0)
        asset_excess.columns = [f"{c}_excess" for c in all_cols]
        asset_excess.index.name = 'date'
        asset_excess.dropna(inplace=True)

        # ── 4. Save asset_excess_returns.csv ─────────────────────────────────
        asset_excess.to_csv(os.path.join(DATA_DIR, '14_AssetExcessReturns_output.csv'))

        # ── 5. Portfolio excess (optional — needs portfolio_returns.csv) ──────
        port_stats = None
        port_path  = os.path.join(DATA_DIR, '13_PortfolioReturns_output.csv')
        if os.path.exists(port_path):
            port_ret = pd.read_csv(port_path, index_col=0, parse_dates=True)
            common   = log_ret.index.intersection(port_ret.index)
            rf_aln   = rf.loc[common]
            pr_aln   = port_ret.loc[common, 'portfolio_return']
            port_ex  = (pr_aln - rf_aln).rename('excess_portfolio_return')
            pd.DataFrame(port_ex).to_csv(
                os.path.join(DATA_DIR, '14_PortfolioExcessReturns_output.csv'))
            port_stats = {
                'mean':  round(float(port_ex.mean()), 8),
                'std':   round(float(port_ex.std()),  8),
                'min':   round(float(port_ex.min()),  8),
                'max':   round(float(port_ex.max()),  8),
                'count': int(len(port_ex)),
            }

        # ── 6. Per-asset summary stats ────────────────────────────────────────
        asset_stats = {
            col: {
                'mean': round(float(asset_excess[col].mean()), 8),
                'std':  round(float(asset_excess[col].std()),  8),
                'min':  round(float(asset_excess[col].min()),  8),
                'max':  round(float(asset_excess[col].max()),  8),
            }
            for col in asset_excess.columns
        }

        gc.collect()
        return jsonify({
            'status':      'success',
            'rows':        int(len(asset_excess)),
            'assets':      all_cols,
            'asset_stats': asset_stats,
            'port_stats':  port_stats,
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── 15. Sharpe Ratios — Asset & Portfolio ────────────────────────
@app.route('/api/sharpe-ratios')
def sharpe_ratios_api():
    """
    Computes daily + annualized Sharpe ratios for every asset and for the
    equal-weight portfolio.

    Formula:
      Daily Sharpe      = mean(excess_return) / std(excess_return, ddof=1)
      Annualized Sharpe = Daily Sharpe × √252

    Inputs:  asset_excess_returns.csv, portfolio_excess_returns.csv
    Outputs: asset_sharpe_ratios.csv, portfolio_sharpe_ratio.csv
    """
    gc.collect()
    TRADING_DAYS = 252
    try:
        # ── 1. Load asset excess returns ──────────────────────────────────
        asset_excess = pd.read_csv(
            os.path.join(DATA_DIR, '14_AssetExcessReturns_output.csv'),
            index_col=0, parse_dates=True,
        )
        asset_excess.sort_index(inplace=True)
        asset_excess.dropna(inplace=True)

        # ── 2. Load portfolio excess returns ──────────────────────────────
        port_excess = pd.read_csv(
            os.path.join(DATA_DIR, '14_PortfolioExcessReturns_output.csv'),
            index_col=0, parse_dates=True,
        )
        port_excess.sort_index(inplace=True)
        port_excess.dropna(inplace=True)

        PORT_COL = 'excess_portfolio_return'
        if PORT_COL not in port_excess.columns:
            return jsonify({'status': 'error',
                            'message': f"Column '{PORT_COL}' not found"}), 500

        # ── 3. Per-asset Sharpe ────────────────────────────────────────────
        asset_rows = []
        for col in asset_excess.columns:
            s = asset_excess[col].dropna()
            if len(s) < 2:
                continue
            mean_e = float(s.mean())
            std_e  = float(s.std(ddof=1))
            daily  = mean_e / std_e if std_e != 0 else None
            asset_rows.append({
                'asset':              col.replace('_excess', ''),
                'mean_excess_return': round(mean_e,                              10),
                'std_return':         round(std_e,                               10),
                'sharpe_ratio':       round(daily,                                8) if daily is not None else None,
                'annualized_sharpe':  round(daily * (TRADING_DAYS ** 0.5),       8) if daily is not None else None,
            })

        asset_sharpe_df = pd.DataFrame(asset_rows)
        asset_sharpe_df.to_csv(
            os.path.join(DATA_DIR, '15_AssetSharpeRatios_output.csv'), index=False)

        # ── 4. Portfolio Sharpe ────────────────────────────────────────────
        ps        = port_excess[PORT_COL].dropna()
        port_mean = float(ps.mean())
        port_std  = float(ps.std(ddof=1))
        port_d    = port_mean / port_std if port_std != 0 else None
        port_ann  = port_d * (TRADING_DAYS ** 0.5) if port_d is not None else None

        portfolio_row = {
            'portfolio':          'Equal-Weight Portfolio',
            'mean_excess_return': round(port_mean, 10),
            'std_return':         round(port_std,  10),
            'sharpe_ratio':       round(port_d,     8) if port_d  is not None else None,
            'annualized_sharpe':  round(port_ann,   8) if port_ann is not None else None,
        }
        pd.DataFrame([portfolio_row]).to_csv(
            os.path.join(DATA_DIR, '15_PortfolioSharpeRatio_output.csv'), index=False)

        gc.collect()
        return jsonify({
            'status':        'success',
            'asset_sharpe':  asset_rows,
            'portfolio':     portfolio_row,
            'observations':  int(len(ps)),
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── 16. Correlation Matrices by Regime ───────────────────────────
REGIMES_CORR = ['Goldilocks', 'Downturn', 'Stagflation', 'Overheating']

@app.route('/api/correlation-matrices')
def correlation_matrices_api():
    gc.collect()
    try:
        # ── 1. Load log returns ────────────────────────────────────────────
        log_ret = pd.read_csv(
            os.path.join(DATA_DIR, '03_LogReturns_output.csv'),
            parse_dates=['Dates'], index_col='Dates',
        )
        log_ret.sort_index(inplace=True)

        # ── 2. Load regime labels ──────────────────────────────────────────
        regime_csv = os.path.join(DATA_DIR, '06_PriceDataWithRegime_output.csv')
        if not os.path.exists(regime_csv):
            return jsonify({
                'status': 'error',
                'message': 'Run card 06 (Regime Attribute) first to generate 06_PriceDataWithRegime_output.csv',
            }), 400

        regime_df = pd.read_csv(
            regime_csv,
            parse_dates=['Dates'],
            usecols=['Dates', 'Regime'],
        ).set_index('Dates')
        regime_df.sort_index(inplace=True)

        # ── 3. Merge on date index ─────────────────────────────────────────
        df = log_ret.join(regime_df, how='inner').sort_index()
        asset_cols = [c for c in df.columns if c != 'Regime']

        # ── 4. Compute Pearson correlation per regime ──────────────────────
        result_matrices = {}
        for regime in REGIMES_CORR:
            subset = df[df['Regime'] == regime][asset_cols].dropna(how='all')
            if len(subset) < 2:
                result_matrices[regime] = None
                continue
            corr = subset.corr(method='pearson').round(4)

            # Save CSV
            fname = f'16_Correlation{regime}_output.csv'
            corr.to_csv(os.path.join(DATA_DIR, fname))

            # Convert to nested dict (JSON serialisable)
            result_matrices[regime] = corr.to_dict()

        gc.collect()
        return jsonify({
            'status':   'success',
            'assets':   asset_cols,
            'regimes':  REGIMES_CORR,
            'matrices': result_matrices,
            'saved':    [f'Data/16_Correlation{r}_output.csv' for r in REGIMES_CORR],
        })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ── /data/<filename> — Data klasöründeki dosyaları servis eder ───
@app.route('/data/<path:filename>')
def serve_data_file(filename):
    """Data/ klasöründeki CSV/XLSX dosyalarını frontend'e sunar."""
    return send_from_directory(DATA_DIR, filename)


# ── Ping / Health-check ───────────────────────────────────────────
@app.route('/api/ticker')
def ticker():
    """Returns latest price + 1-day change for the 7 Bloomberg assets."""
    try:
        ASSETS = [
            "SPTR5BNK Index", "DJITR Index", "SPXT Index",
            "XAUUSD Curncy", "XAGUSD Curncy", "LT09TRUU Index", "LD12TRUU Index",
        ]
        SHORT = {
            "SPTR5BNK Index": "S&P Banks",
            "DJITR Index":    "Dow Jones",
            "SPXT Index":     "S&P 500",
            "XAUUSD Curncy":  "Gold",
            "XAGUSD Curncy":  "Silver",
            "LT09TRUU Index": "US Treasury",
            "LD12TRUU Index": "US Corp Bond",
        }
        csv_path = os.path.join(DATA_DIR, "01_PriceData_provided.csv")
        df = pd.read_csv(csv_path, parse_dates=["Dates"]).sort_values("Dates").set_index("Dates")[ASSETS].dropna(how="all")
        last2 = df.tail(2)
        if len(last2) < 2:
            return jsonify({"status": "error", "message": "Not enough data"})
        prev, last = last2.iloc[0], last2.iloc[1]
        last_date = df.index[-1].strftime("%Y-%m-%d")
        items = []
        for a in ASSETS:
            p  = float(last[a]) if pd.notna(last[a]) else None
            p0 = float(prev[a]) if pd.notna(prev[a]) else None
            chg = round((p - p0) / p0 * 100, 2) if (p and p0 and p0 != 0) else None
            items.append({"key": a, "short": SHORT[a], "price": round(p, 4) if p else None, "chg": chg})
        return jsonify({"status": "success", "date": last_date, "items": items})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/ping')
def ping():
    import socket
    return jsonify({
        'status':   'online',
        'base_dir': BASE_DIR,
        'data_dir': DATA_DIR,
        'host':     socket.gethostname(),
        'port':     5050,
    })


@app.route('/api/regime-info')
def regime_info():
    """Returns last known regime date for use as placeholder in date inputs."""
    try:
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if os.path.exists(rcsv):
            _rdf = pd.read_csv(rcsv, parse_dates=['End_Date'])
            last_date = _rdf['End_Date'].max().strftime('%Y-%m-%d')
        else:
            last_date = None
        return jsonify({'status': 'success', 'last_regime_date': last_date})
    except Exception as _e:
        return jsonify({'status': 'error', 'message': str(_e), 'last_regime_date': None}), 200


# ── Operations Log  ──────────────────────────────────────────────────────────
import json as _json
from datetime import datetime as _dt
from flask import request as _request

LOG_FILE = os.path.join(DATA_DIR, 'operations_log.json')

def _read_log():
    if not os.path.exists(LOG_FILE):
        return []
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            return _json.load(f)
    except Exception:
        return []

def _write_log(entries):
    with open(LOG_FILE, 'w', encoding='utf-8') as f:
        _json.dump(entries, f, ensure_ascii=False, indent=2)

@app.route('/api/log', methods=['GET'])
def get_log():
    return jsonify({'status': 'success', 'entries': _read_log()})

@app.route('/api/log', methods=['POST'])
def post_log():
    body = _request.get_json(force=True, silent=True) or {}
    entry = {
        'id':        int(_dt.now().timestamp() * 1000),
        'timestamp': _dt.now().isoformat(timespec='seconds'),
        'type':      body.get('type', 'info'),
        'card':      body.get('card', ''),
        'message':   body.get('message', '').strip(),
        'file':      body.get('file', ''),
    }
    if not entry['message']:
        return jsonify({'status': 'error', 'message': 'message required'}), 400
    entries = _read_log()
    entries.insert(0, entry)
    entries = entries[:500]
    _write_log(entries)
    return jsonify({'status': 'success', 'entry': entry})

@app.route('/api/log/<int:entry_id>', methods=['DELETE'])
def delete_log_entry(entry_id):
    entries = _read_log()
    entries = [e for e in entries if e.get('id') != entry_id]
    _write_log(entries)
    return jsonify({'status': 'success'})

@app.route('/api/log/clear', methods=['POST'])
def clear_log():
    _write_log([])
    return jsonify({'status': 'success'})



# ── Covariance & Correlation Matrices by Regime  ─────────────────────────────
@app.route('/api/cov-corr-matrices')
def cov_corr_matrices_api():
    import gc as _gc
    try:
        import pandas as _pd
        import numpy  as _np
        from datetime import datetime as _dt

        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        CORR_DIR = os.path.join(DATA_DIR, "Correlation")
        os.makedirs(CORR_DIR, exist_ok=True)

        # ── Optional date range or months-back from query params ───────────────
        start_str  = request.args.get('start_date', '').strip()
        end_str    = request.args.get('end_date',   '').strip()
        months_str = request.args.get('months',     '').strip()
        date_start = _pd.to_datetime(start_str) if start_str else None
        date_end   = _pd.to_datetime(end_str)   if end_str   else None

        # Months-back: resolve using last regime date as end_date anchor
        if months_str.isdigit() and int(months_str) > 0 and date_start is None:
            _months_val = int(months_str)
            # Load regime dates to find last end date
            _rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
            if os.path.exists(_rcsv):
                _rdf_tmp = _pd.read_csv(_rcsv, parse_dates=['End_Date'])
                _last_regime = _rdf_tmp['End_Date'].max()
                if date_end is None:
                    date_end = _last_regime
                date_start = date_end - _pd.DateOffset(months=_months_val)
            del _rcsv

        has_filter = date_start is not None or date_end is not None

        # ── Load data ─────────────────────────────────────────────────────────
        df_idx = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")

        regime_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date","End_Date"]
        ).sort_values("Start_Date").reset_index(drop=True)

        # ── Helper: build log-return buckets (optionally filtered by date range) ──
        def _build_buckets(df_prices, rdf, d_start, d_end):
            bkts = []
            for _, p in rdf.iterrows():
                mask = (df_prices.index >= p["Start_Date"]) & (df_prices.index <= p["End_Date"])
                if d_start is not None:
                    mask &= df_prices.index >= d_start
                if d_end is not None:
                    mask &= df_prices.index <= d_end
                subset = df_prices.loc[mask, ASSETS].copy()
                if len(subset) < 2:
                    continue
                lr = _np.log(subset / subset.shift(1)).dropna()
                if lr.empty:
                    continue
                lr["_Regime"] = p["Regime"]
                bkts.append(lr)
            return bkts

        # ── Log returns within each regime period ─────────────────────────────
        buckets_filtered = _build_buckets(df_idx, regime_df, date_start, date_end)
        buckets_all      = _build_buckets(df_idx, regime_df, None, None) if has_filter else buckets_filtered

        del df_idx, regime_df
        _gc.collect()

        def _compute_matrices(buckets):
            if not buckets:
                return {}, {}, {}
            all_ret = _pd.concat(buckets).sort_index()
            cm, cv, ob = {}, {}, {}
            for regime in REGIMES:
                sub = all_ret.loc[all_ret["_Regime"] == regime, ASSETS].copy()
                if sub.shape[0] < 2:
                    continue
                cm[regime] = sub.corr(method="pearson")
                cv[regime] = sub.cov()
                ob[regime] = int(sub.shape[0])
            return cm, cv, ob

        corr_mats, cov_mats, obs           = _compute_matrices(buckets_filtered)
        corr_all,  cov_all,  obs_all       = _compute_matrices(buckets_all) if has_filter else (corr_mats, cov_mats, obs)

        _gc.collect()

        # ── Save CSVs (always save the filtered / primary matrices) ───────────
        saved = []
        for regime, m in corr_mats.items():
            p = os.path.join(CORR_DIR, f"Correlation_{regime}_output.csv")
            m.to_csv(p)
            saved.append(f"Correlation/Correlation_{regime}_output.csv")
        for regime, m in cov_mats.items():
            p = os.path.join(CORR_DIR, f"Covariance_{regime}_output.csv")
            m.to_csv(p)
            saved.append(f"Correlation/Covariance_{regime}_output.csv")

        # ── Build response ────────────────────────────────────────────────────
        def mat2dict(df):
            return {
                row: {col: round(float(df.loc[row, col]), 6)
                      for col in df.columns}
                for row in df.index
            }

        result = {
            'status':           'success',
            'assets':           ASSETS,
            'regimes':          REGIMES,
            'obs_counts':       obs,
            'correlation':      {r: mat2dict(m) for r, m in corr_mats.items()},
            'covariance':       {r: mat2dict(m) for r, m in cov_mats.items()},
            'has_filter':       has_filter,
            'filter_start':     start_str or None,
            'filter_end':       end_str   or None,
            'obs_counts_all':   obs_all,
            'correlation_all':  {r: mat2dict(m) for r, m in corr_all.items()},
            'covariance_all':   {r: mat2dict(m) for r, m in cov_all.items()},
            'saved':            saved,
        }
        del corr_mats, cov_mats, corr_all, cov_all
        _gc.collect()
        return jsonify(result)

    except Exception as e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(e)})


# ── 14. Min Variance / Max Sharpe Optimization ────────────────────────────────
@app.route('/api/min-max-optimization')
def min_max_optimization():
    import gc as _gc
    import numpy as _np
    import pandas as _pd
    from scipy.optimize import minimize as _minimize
    from flask import request as _req

    try:
        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        CORR_DIR = os.path.join(DATA_DIR, "Correlation")
        OPT_DIR  = os.path.join(DATA_DIR, "Optimization")
        MV_DIR   = os.path.join(OPT_DIR,  "MinimumRisk")
        MS_DIR   = os.path.join(OPT_DIR,  "MaxSharpe")
        MR_DIR   = os.path.join(OPT_DIR,  "MaxReturn")
        os.makedirs(MV_DIR, exist_ok=True)
        os.makedirs(MS_DIR, exist_ok=True)
        os.makedirs(MR_DIR, exist_ok=True)

        # ── Load average returns ──────────────────────────────────────────────
        avg_ret_df = _pd.read_csv(os.path.join(DATA_DIR, "11_AvgReturnByRegime_output.csv"))
        mu_map = {}
        for _, row in avg_ret_df.iterrows():
            r = row["Regime"]; a = row["Asset"]
            mu_map.setdefault(r, {})[a] = float(row["Avg_Daily_LogRet_Pct"]) / 100.0
        del avg_ret_df
        _gc.collect()

        # ── Load regime dates ─────────────────────────────────────────────────
        regime_dates_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date", "End_Date"]
        )

        # ── Load risk-free rates ──────────────────────────────────────────────
        rf_df = _pd.read_csv(
            os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv"),
            parse_dates=["date"]
        ).sort_values("date").set_index("date")

        rf_by_regime = {}
        for regime in REGIMES:
            rows = regime_dates_df[regime_dates_df["Regime"] == regime]
            vals = []
            for _, row in rows.iterrows():
                mask = (rf_df.index >= row["Start_Date"]) & (rf_df.index <= row["End_Date"])
                sub  = rf_df.loc[mask, "risk_free_rate"]
                if len(sub) > 0:
                    vals.extend(sub.tolist())
            rf_by_regime[regime] = float(_np.mean(vals)) if vals else 0.0

        del regime_dates_df, rf_df
        _gc.collect()

        # ── Load covariance matrices ──────────────────────────────────────────
        cov_map = {}
        for regime in REGIMES:
            cov_path = os.path.join(CORR_DIR, f"Covariance_{regime}_output.csv")
            if not os.path.exists(cov_path):
                raise FileNotFoundError(f"Missing {cov_path}")
            cov_df = _pd.read_csv(cov_path, index_col=0)
            cov_map[regime] = cov_df.loc[ASSETS, ASSETS].values.astype(float)
        _gc.collect()

        n = len(ASSETS)
        mv_weights_rows   = []
        mv_summary_rows   = []
        ms_weights_rows   = []
        ms_summary_rows   = []
        mr_weights_rows   = []
        mr_summary_rows   = []
        mv_weights_result = {}
        mv_summary_result = {}
        ms_weights_result = {}
        ms_summary_result = {}
        mr_weights_result = {}
        mr_summary_result = {}

        for regime in REGIMES:
            Sigma = cov_map[regime]
            mu    = _np.array([mu_map.get(regime, {}).get(a, 0.0) for a in ASSETS])
            rf    = rf_by_regime.get(regime, 0.0)

            w0 = _np.ones(n) / n
            bounds  = [(0.0, 1.0)] * n
            cons_eq = {'type': 'eq', 'fun': lambda w: _np.sum(w) - 1.0}

            # ── Min Variance ──────────────────────────────────────────────────
            def neg_var(w):  return w @ Sigma @ w
            res_mv = _minimize(neg_var, w0, method='SLSQP',
                               bounds=bounds, constraints=[cons_eq],
                               options={'ftol': 1e-12, 'maxiter': 1000})
            w_mv = _np.clip(res_mv.x, 0.0, 1.0)
            w_mv /= w_mv.sum()
            var_mv  = float(w_mv @ Sigma @ w_mv)
            vol_d   = float(_np.sqrt(max(var_mv, 0.0)))
            ret_d   = float(w_mv @ mu)
            vol_ann = vol_d * _np.sqrt(252)
            ret_ann = ret_d * 252
            sharpe  = (ret_d - rf) / vol_d if vol_d > 1e-12 else 0.0

            mv_weights_result[regime]  = {ASSETS[i]: round(float(w_mv[i]), 8) for i in range(n)}
            mv_summary_result[regime]  = {
                'return_ann': round(ret_ann, 8),
                'vol_ann':    round(float(vol_ann), 8),
                'sharpe':     round(float(sharpe), 6),
            }
            for i, a in enumerate(ASSETS):
                mv_weights_rows.append({'Regime': regime, 'Asset': a, 'Weight': round(float(w_mv[i]), 8)})
            mv_summary_rows.append({
                'Regime': regime,
                'Objective_MinVariance': round(var_mv, 10),
                'Portfolio_Return_Daily': round(ret_d, 8),
                'Portfolio_Return_Annualized': round(ret_ann, 8),
                'Portfolio_Volatility_Daily': round(vol_d, 8),
                'Portfolio_Volatility_Annualized': round(float(vol_ann), 8),
                'Portfolio_Sharpe': round(float(sharpe), 6),
                'Risk_Free_Rate_Used': round(rf, 8),
                'Weight_Sum': round(float(w_mv.sum()), 8),
                'Min_Weight': round(float(w_mv.min()), 8),
                'Max_Weight': round(float(w_mv.max()), 8),
                'Solver_Success': bool(res_mv.success),
                'Solver_Message': res_mv.message,
            })

            # ── Max Sharpe ────────────────────────────────────────────────────
            def neg_sharpe(w):
                port_ret = float(w @ mu)
                port_vol = float(_np.sqrt(max(w @ Sigma @ w, 1e-20)))
                return -(port_ret - rf) / port_vol

            res_ms = _minimize(neg_sharpe, w0, method='SLSQP',
                               bounds=bounds, constraints=[cons_eq],
                               options={'ftol': 1e-12, 'maxiter': 1000})
            w_ms = _np.clip(res_ms.x, 0.0, 1.0)
            w_ms /= w_ms.sum()
            var_ms   = float(w_ms @ Sigma @ w_ms)
            vol_d_ms = float(_np.sqrt(max(var_ms, 0.0)))
            ret_d_ms = float(w_ms @ mu)
            vol_ann_ms = vol_d_ms * _np.sqrt(252)
            ret_ann_ms = ret_d_ms * 252
            sharpe_ms  = (ret_d_ms - rf) / vol_d_ms if vol_d_ms > 1e-12 else 0.0

            ms_weights_result[regime]  = {ASSETS[i]: round(float(w_ms[i]), 8) for i in range(n)}
            ms_summary_result[regime]  = {
                'return_ann': round(ret_ann_ms, 8),
                'vol_ann':    round(float(vol_ann_ms), 8),
                'sharpe':     round(float(sharpe_ms), 6),
            }
            for i, a in enumerate(ASSETS):
                ms_weights_rows.append({'Regime': regime, 'Asset': a, 'Weight': round(float(w_ms[i]), 8)})
            ms_summary_rows.append({
                'Regime': regime,
                'Objective_MaxSharpe': round(float(-neg_sharpe(w_ms)), 6),
                'Portfolio_Return_Daily': round(ret_d_ms, 8),
                'Portfolio_Return_Annualized': round(ret_ann_ms, 8),
                'Portfolio_Volatility_Daily': round(vol_d_ms, 8),
                'Portfolio_Volatility_Annualized': round(float(vol_ann_ms), 8),
                'Portfolio_Variance': round(var_ms, 10),
                'Portfolio_Sharpe': round(float(sharpe_ms), 6),
                'Risk_Free_Rate_Used': round(rf, 8),
                'Weight_Sum': round(float(w_ms.sum()), 8),
                'Min_Weight': round(float(w_ms.min()), 8),
                'Max_Weight': round(float(w_ms.max()), 8),
                'Solver_Success': bool(res_ms.success),
                'Solver_Message': res_ms.message,
            })

            # ── Max Return ────────────────────────────────────────────────────
            def neg_ret(w): return -(float(w @ mu))
            res_mr = _minimize(neg_ret, w0, method='SLSQP',
                               bounds=bounds, constraints=[cons_eq],
                               options={'ftol': 1e-12, 'maxiter': 1000})
            w_mr = _np.clip(res_mr.x, 0.0, 1.0)
            w_mr /= w_mr.sum()
            var_mr    = float(w_mr @ Sigma @ w_mr)
            vol_d_mr  = float(_np.sqrt(max(var_mr, 0.0)))
            ret_d_mr  = float(w_mr @ mu)
            vol_ann_mr = vol_d_mr * _np.sqrt(252)
            ret_ann_mr = ret_d_mr * 252
            sharpe_mr  = (ret_d_mr - rf) / vol_d_mr if vol_d_mr > 1e-12 else 0.0

            mr_weights_result[regime] = {ASSETS[i]: round(float(w_mr[i]), 8) for i in range(n)}
            mr_summary_result[regime] = {
                'return_ann': round(ret_ann_mr, 8),
                'vol_ann':    round(float(vol_ann_mr), 8),
                'sharpe':     round(float(sharpe_mr), 6),
            }
            for i, a in enumerate(ASSETS):
                mr_weights_rows.append({'Regime': regime, 'Asset': a, 'Weight': round(float(w_mr[i]), 8)})
            mr_summary_rows.append({
                'Regime': regime,
                'Portfolio_Return_Daily': round(ret_d_mr, 8),
                'Portfolio_Return_Annualized': round(ret_ann_mr, 8),
                'Portfolio_Volatility_Daily': round(vol_d_mr, 8),
                'Portfolio_Volatility_Annualized': round(float(vol_ann_mr), 8),
                'Portfolio_Variance': round(var_mr, 10),
                'Portfolio_Sharpe': round(float(sharpe_mr), 6),
                'Risk_Free_Rate_Used': round(rf, 8),
                'Weight_Sum': round(float(w_mr.sum()), 8),
                'Solver_Success': bool(res_mr.success),
                'Solver_Message': res_mr.message,
            })
            # Per-regime weight files for portfolio-allocations
            _pd.DataFrame([
                {'Asset': ASSETS[i], 'Weight': round(float(w_mv[i]), 8)} for i in range(n)
            ]).to_csv(os.path.join(MV_DIR, f"MinimumRisk_Weights_{regime}_output.csv"), index=False)
            _pd.DataFrame([
                {'Asset': ASSETS[i], 'Weight': round(float(w_ms[i]), 8)} for i in range(n)
            ]).to_csv(os.path.join(MS_DIR, f"MaxSharpe_Weights_{regime}_output.csv"), index=False)
            _pd.DataFrame([
                {'Asset': ASSETS[i], 'Weight': round(float(w_mr[i]), 8)} for i in range(n)
            ]).to_csv(os.path.join(MR_DIR, f"MaxReturn_Weights_{regime}_output.csv"), index=False)

        del cov_map, mu_map
        _gc.collect()

        # ── Save CSVs ─────────────────────────────────────────────────────────
        saved = []
        _pd.DataFrame(mv_weights_rows).to_csv(
            os.path.join(MV_DIR, "MinimumRisk_Weights_output.csv"), index=False)
        saved.append("Optimization/MinimumRisk/MinimumRisk_Weights_output.csv")

        _pd.DataFrame(mv_summary_rows).to_csv(
            os.path.join(MV_DIR, "MinimumRisk_Summary_output.csv"), index=False)
        saved.append("Optimization/MinimumRisk/MinimumRisk_Summary_output.csv")

        _pd.DataFrame(ms_weights_rows).to_csv(
            os.path.join(MS_DIR, "MaxSharpe_Weights_output.csv"), index=False)
        saved.append("Optimization/MaxSharpe/MaxSharpe_Weights_output.csv")

        _pd.DataFrame(ms_summary_rows).to_csv(
            os.path.join(MS_DIR, "MaxSharpe_Summary_output.csv"), index=False)
        saved.append("Optimization/MaxSharpe/MaxSharpe_Summary_output.csv")

        _pd.DataFrame(mr_weights_rows).to_csv(
            os.path.join(MR_DIR, "MaxReturn_Weights_output.csv"), index=False)
        saved.append("Optimization/MaxReturn/MaxReturn_Weights_output.csv")

        _pd.DataFrame(mr_summary_rows).to_csv(
            os.path.join(MR_DIR, "MaxReturn_Summary_output.csv"), index=False)
        saved.append("Optimization/MaxReturn/MaxReturn_Summary_output.csv")

        _gc.collect()
        return jsonify({
            'status':        'success',
            'regimes':       REGIMES,
            'assets':        ASSETS,
            'min_variance':  {'weights': mv_weights_result, 'summary': mv_summary_result},
            'max_sharpe':    {'weights': ms_weights_result, 'summary': ms_summary_result},
            'max_return':    {'weights': mr_weights_result, 'summary': mr_summary_result},
            'saved':         saved,
        })

    except Exception as e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(e)})


# ── 15. Optimization Visualization ────────────────────────────────────────────
@app.route('/api/optimization-visualization')
def optimization_visualization():
    import gc as _gc
    import pandas as _pd

    try:
        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        OPT_DIR = os.path.join(DATA_DIR, "Optimization")
        MV_DIR  = os.path.join(OPT_DIR,  "MinimumRisk")
        MS_DIR  = os.path.join(OPT_DIR,  "MaxSharpe")
        MR_DIR  = os.path.join(OPT_DIR,  "MaxReturn")

        def _require(path):
            if not os.path.exists(path):
                raise FileNotFoundError(f"Required file not found: {path}. Run Card 14 first.")
            return path

        mv_sum_path = _require(os.path.join(MV_DIR, "MinimumRisk_Summary_output.csv"))
        mv_wgt_path = _require(os.path.join(MV_DIR, "MinimumRisk_Weights_output.csv"))
        ms_sum_path = _require(os.path.join(MS_DIR, "MaxSharpe_Summary_output.csv"))
        ms_wgt_path = _require(os.path.join(MS_DIR, "MaxSharpe_Weights_output.csv"))
        mr_sum_path = _require(os.path.join(MR_DIR, "MaxReturn_Summary_output.csv"))
        mr_wgt_path = _require(os.path.join(MR_DIR, "MaxReturn_Weights_output.csv"))

        mv_sum = _pd.read_csv(mv_sum_path)
        mv_wgt = _pd.read_csv(mv_wgt_path)
        ms_sum = _pd.read_csv(ms_sum_path)
        ms_wgt = _pd.read_csv(ms_wgt_path)
        mr_sum = _pd.read_csv(mr_sum_path)
        mr_wgt = _pd.read_csv(mr_wgt_path)

        def _sum_to_dict(df):
            result = {}
            for _, row in df.iterrows():
                result[row["Regime"]] = {
                    'return_ann': float(row.get("Portfolio_Return_Annualized", 0)),
                    'vol_ann':    float(row.get("Portfolio_Volatility_Annualized", 0)),
                    'sharpe':     float(row.get("Portfolio_Sharpe", 0)),
                }
            return result

        def _wgt_to_dict(df):
            result = {}
            for regime, grp in df.groupby("Regime"):
                result[regime] = {
                    row["Asset"]: float(row["Weight"])
                    for _, row in grp.iterrows()
                }
            return result

        mv_summary_result = _sum_to_dict(mv_sum)
        ms_summary_result = _sum_to_dict(ms_sum)
        mr_summary_result = _sum_to_dict(mr_sum)
        mv_weights_result = _wgt_to_dict(mv_wgt)
        ms_weights_result = _wgt_to_dict(ms_wgt)
        mr_weights_result = _wgt_to_dict(mr_wgt)

        # Build list-form summaries for frontend
        mv_sum_list = [
            {'Regime': r, **mv_summary_result.get(r, {})}
            for r in REGIMES if r in mv_summary_result
        ]
        ms_sum_list = [
            {'Regime': r, **ms_summary_result.get(r, {})}
            for r in REGIMES if r in ms_summary_result
        ]
        mr_sum_list = [
            {'Regime': r, **mr_summary_result.get(r, {})}
            for r in REGIMES if r in mr_summary_result
        ]

        del mv_sum, mv_wgt, ms_sum, ms_wgt, mr_sum, mr_wgt
        _gc.collect()

        return jsonify({
            'status':       'success',
            'regimes':      REGIMES,
            'assets':       ASSETS,
            'min_variance': {
                'summary': mv_sum_list,
                'weights': mv_weights_result,
                'summary_dict': mv_summary_result,
            },
            'max_sharpe': {
                'summary': ms_sum_list,
                'weights': ms_weights_result,
                'summary_dict': ms_summary_result,
            },
            'max_return': {
                'summary': mr_sum_list,
                'weights': mr_weights_result,
                'summary_dict': mr_summary_result,
            },
        })

    except Exception as e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(e)})


# ── 16. Efficient Frontier ────────────────────────────────────────────────────
@app.route('/api/efficient-frontier')
def efficient_frontier():
    import gc as _gc
    import numpy as _np
    import pandas as _pd
    from scipy.optimize import minimize as _minimize
    from flask import request as _req

    try:
        n_points = int(_req.args.get('n_points', 30))
        n_points = max(5, min(200, n_points))

        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        CORR_DIR = os.path.join(DATA_DIR, "Correlation")
        EF_DIR   = os.path.join(DATA_DIR, "Optimization", "EfficientFrontier")
        os.makedirs(EF_DIR, exist_ok=True)

        # ── Load avg returns ──────────────────────────────────────────────────
        avg_ret_df = _pd.read_csv(os.path.join(DATA_DIR, "11_AvgReturnByRegime_output.csv"))
        mu_map = {}
        for _, row in avg_ret_df.iterrows():
            mu_map.setdefault(row["Regime"], {})[row["Asset"]] = float(row["Avg_Daily_LogRet_Pct"]) / 100.0
        del avg_ret_df
        _gc.collect()

        # ── Load regime dates + RF rates ──────────────────────────────────────
        regime_dates_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date", "End_Date"]
        )
        rf_df = _pd.read_csv(
            os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv"),
            parse_dates=["date"]
        ).sort_values("date").set_index("date")

        rf_by_regime = {}
        for regime in REGIMES:
            rows = regime_dates_df[regime_dates_df["Regime"] == regime]
            vals = []
            for _, row in rows.iterrows():
                mask = (rf_df.index >= row["Start_Date"]) & (rf_df.index <= row["End_Date"])
                sub  = rf_df.loc[mask, "risk_free_rate"]
                if len(sub) > 0:
                    vals.extend(sub.tolist())
            rf_by_regime[regime] = float(_np.mean(vals)) if vals else 0.0

        del regime_dates_df, rf_df
        _gc.collect()

        # ── Load covariance matrices ──────────────────────────────────────────
        cov_map = {}
        for regime in REGIMES:
            cov_path = os.path.join(CORR_DIR, f"Covariance_{regime}_output.csv")
            if not os.path.exists(cov_path):
                raise FileNotFoundError(f"Missing {cov_path}")
            cov_df = _pd.read_csv(cov_path, index_col=0)
            cov_map[regime] = cov_df.loc[ASSETS, ASSETS].values.astype(float)
        _gc.collect()

        n = len(ASSETS)
        frontier_result  = {}
        ms_result        = {}
        mv_result        = {}
        mr_result        = {}
        all_ef_rows      = []
        ef_weight_rows   = []   # per-point weights — accumulated across ALL regimes
        ms_summary_rows  = []
        ms_weights_rows  = []
        mr_summary_rows  = []
        mr_weights_rows  = []
        saved            = []

        for regime in REGIMES:
            Sigma = cov_map[regime]
            mu    = _np.array([mu_map.get(regime, {}).get(a, 0.0) for a in ASSETS])
            rf    = rf_by_regime.get(regime, 0.0)
            w0    = _np.ones(n) / n
            bounds   = [(0.0, 1.0)] * n
            cons_sum = {'type': 'eq', 'fun': lambda w: _np.sum(w) - 1.0}

            # Min variance (unconstrained return)
            def min_var_obj(w):  return w @ Sigma @ w
            res_mv = _minimize(min_var_obj, w0, method='SLSQP',
                               bounds=bounds, constraints=[cons_sum],
                               options={'ftol': 1e-12, 'maxiter': 1000})
            w_mv_opt = _np.clip(res_mv.x, 0.0, 1.0); w_mv_opt /= w_mv_opt.sum()
            ret_min  = float(w_mv_opt @ mu)
            vol_min  = float(_np.sqrt(max(w_mv_opt @ Sigma @ w_mv_opt, 0.0)))

            # Max return (unconstrained vol)
            res_maxret = _minimize(lambda w: -(w @ mu), w0, method='SLSQP',
                                   bounds=bounds, constraints=[cons_sum],
                                   options={'ftol': 1e-12, 'maxiter': 1000})
            w_maxret = _np.clip(res_maxret.x, 0.0, 1.0); w_maxret /= w_maxret.sum()
            ret_max  = float(w_maxret @ mu)

            # Build grid of target returns
            if ret_max <= ret_min:
                ret_max = ret_min * 1.1 + 1e-8
            targets = _np.linspace(ret_min, ret_max, n_points)

            ef_points = []
            for k, tgt in enumerate(targets):
                cons_ret = [
                    cons_sum,
                    {'type': 'eq', 'fun': lambda w, t=tgt: w @ mu - t},
                ]
                res_ef = _minimize(min_var_obj, w0, method='SLSQP',
                                   bounds=bounds, constraints=cons_ret,
                                   options={'ftol': 1e-12, 'maxiter': 1000})
                w_ef = _np.clip(res_ef.x, 0.0, 1.0)
                if w_ef.sum() > 1e-10:
                    w_ef /= w_ef.sum()
                p_var  = float(w_ef @ Sigma @ w_ef)
                p_vol  = float(_np.sqrt(max(p_var, 0.0)))
                p_ret  = float(w_ef @ mu)
                p_sr   = (p_ret - rf) / p_vol if p_vol > 1e-12 else 0.0
                # Annualize
                vol_ann = p_vol * _np.sqrt(252)
                ret_ann = p_ret * 252
                ef_points.append({
                    'vol':            round(float(vol_ann), 8),
                    'ret':            round(float(ret_ann), 8),
                    'sharpe':         round(float(p_sr), 6),
                    'solver_success': bool(res_ef.success),
                    'weights':        {ASSETS[i]: round(float(w_ef[i]), 8) for i in range(n)},
                })
                all_ef_rows.append({
                    'Regime':           regime,
                    'Point_ID':         k,
                    'Target_Return':    round(float(tgt), 10),
                    'Portfolio_Return': round(p_ret, 10),
                    'Portfolio_Variance': round(p_var, 10),
                    'Portfolio_Volatility': round(p_vol, 10),
                    'Portfolio_Sharpe': round(float(p_sr), 6),
                    'Solver_Success':   bool(res_ef.success),
                    'Solver_Message':   res_ef.message,
                })
                for i, a in enumerate(ASSETS):
                    ef_weight_rows.append({
                        'Regime': regime, 'Point_ID': k,
                        'Asset': a, 'Weight': round(float(w_ef[i]), 8),
                    })

            frontier_result[regime] = ef_points
            mv_result[regime] = {
                'vol': round(float(vol_min * _np.sqrt(252)), 8),
                'ret': round(float(ret_min * 252), 8),
            }

            # Max Sharpe
            def neg_sharpe(w):
                pv = float(_np.sqrt(max(w @ Sigma @ w, 1e-20)))
                return -(w @ mu - rf) / pv
            res_ms_ef = _minimize(neg_sharpe, w0, method='SLSQP',
                                  bounds=bounds, constraints=[cons_sum],
                                  options={'ftol': 1e-12, 'maxiter': 1000})
            w_ms = _np.clip(res_ms_ef.x, 0.0, 1.0); w_ms /= w_ms.sum()
            ms_var   = float(w_ms @ Sigma @ w_ms)
            ms_vol_d = float(_np.sqrt(max(ms_var, 0.0)))
            ms_ret_d = float(w_ms @ mu)
            ms_vol_ann = ms_vol_d * _np.sqrt(252)
            ms_ret_ann = ms_ret_d * 252
            ms_sharpe  = (ms_ret_d - rf) / ms_vol_d if ms_vol_d > 1e-12 else 0.0
            ms_result[regime] = {
                'vol':    round(float(ms_vol_ann), 8),
                'ret':    round(float(ms_ret_ann), 8),
                'sharpe': round(float(ms_sharpe),  6),
                'weights': {ASSETS[i]: round(float(w_ms[i]), 8) for i in range(n)},
            }
            for i, a in enumerate(ASSETS):
                ms_weights_rows.append({'Regime': regime, 'Asset': a, 'Weight': round(float(w_ms[i]), 8)})
            ms_summary_rows.append({
                'Regime': regime,
                'Portfolio_Return_Annualized': round(ms_ret_ann, 8),
                'Portfolio_Volatility_Annualized': round(float(ms_vol_ann), 8),
                'Portfolio_Sharpe': round(float(ms_sharpe), 6),
            })

            # Max Return (w_maxret already computed above for grid building)
            mr_var_ef    = float(w_maxret @ Sigma @ w_maxret)
            mr_vol_d_ef  = float(_np.sqrt(max(mr_var_ef, 0.0)))
            mr_ret_d_ef  = float(w_maxret @ mu)
            mr_vol_ann_ef = mr_vol_d_ef * _np.sqrt(252)
            mr_ret_ann_ef = mr_ret_d_ef * 252
            mr_sharpe_ef  = (mr_ret_d_ef - rf) / mr_vol_d_ef if mr_vol_d_ef > 1e-12 else 0.0
            mr_result[regime] = {
                'vol':    round(float(mr_vol_ann_ef), 8),
                'ret':    round(float(mr_ret_ann_ef), 8),
                'sharpe': round(float(mr_sharpe_ef),  6),
                'weights': {ASSETS[i]: round(float(w_maxret[i]), 8) for i in range(n)},
            }
            for i, a in enumerate(ASSETS):
                mr_weights_rows.append({'Regime': regime, 'Asset': a, 'Weight': round(float(w_maxret[i]), 8)})
            mr_summary_rows.append({
                'Regime': regime,
                'Portfolio_Return_Annualized': round(mr_ret_ann_ef, 8),
                'Portfolio_Volatility_Annualized': round(float(mr_vol_ann_ef), 8),
                'Portfolio_Sharpe': round(float(mr_sharpe_ef), 6),
            })

        del cov_map, mu_map
        _gc.collect()

        # ── Save CSVs ─────────────────────────────────────────────────────────
        for regime in REGIMES:
            regime_rows = [r for r in all_ef_rows if r['Regime'] == regime]
            if regime_rows:
                _pd.DataFrame(regime_rows).to_csv(
                    os.path.join(EF_DIR, f"EfficientFrontier_{regime}_output.csv"), index=False)
                saved.append(f"Optimization/EfficientFrontier/EfficientFrontier_{regime}_output.csv")
            # Save per-point weights CSV
            ew_rows = [r for r in ef_weight_rows if r['Regime'] == regime]
            if ew_rows:
                _pd.DataFrame(ew_rows).to_csv(
                    os.path.join(EF_DIR, f"EfficientFrontier_Weights_{regime}_output.csv"), index=False)
                saved.append(f"Optimization/EfficientFrontier/EfficientFrontier_Weights_{regime}_output.csv")

        _pd.DataFrame(ms_summary_rows).to_csv(
            os.path.join(EF_DIR, "EfficientFrontier_MaxSharpe_Summary_output.csv"), index=False)
        saved.append("Optimization/EfficientFrontier/EfficientFrontier_MaxSharpe_Summary_output.csv")

        _pd.DataFrame(ms_weights_rows).to_csv(
            os.path.join(EF_DIR, "EfficientFrontier_MaxSharpe_Weights_output.csv"), index=False)
        saved.append("Optimization/EfficientFrontier/EfficientFrontier_MaxSharpe_Weights_output.csv")

        _pd.DataFrame(mr_summary_rows).to_csv(
            os.path.join(EF_DIR, "EfficientFrontier_MaxReturn_Summary_output.csv"), index=False)
        saved.append("Optimization/EfficientFrontier/EfficientFrontier_MaxReturn_Summary_output.csv")

        _pd.DataFrame(mr_weights_rows).to_csv(
            os.path.join(EF_DIR, "EfficientFrontier_MaxReturn_Weights_output.csv"), index=False)
        saved.append("Optimization/EfficientFrontier/EfficientFrontier_MaxReturn_Weights_output.csv")

        _gc.collect()
        return jsonify({
            'status':     'success',
            'regimes':    REGIMES,
            'assets':     ASSETS,
            'n_points':   n_points,
            'frontier':   frontier_result,
            'max_sharpe': ms_result,
            'max_return': mr_result,
            'min_vol':    mv_result,
            'saved':      saved,
        })

    except Exception as e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(e)})



@app.route('/api/portfolio-allocations')
def portfolio_allocations():
    """
    Portfolio Allocation Explorer — NOT a notebook cell.
    Reads existing CSV outputs from Data/Optimization/ folders.
    Returns all portfolio data (Min Variance, Max Sharpe, Efficient Frontier) per regime.
    No matplotlib, no subprocess — pure CSV read + JSON response.
    """
    import gc as _gc
    import os as _os
    try:
        OPT = _os.path.join(BASE_DIR, 'Data', 'Optimization')
        mv_sum = pd.read_csv(_os.path.join(OPT, 'MinimumRisk', 'MinimumRisk_Summary_output.csv'))
        ms_sum = pd.read_csv(_os.path.join(OPT, 'MaxSharpe',   'MaxSharpe_Summary_output.csv'))
        try:
            mr_sum = pd.read_csv(_os.path.join(OPT, 'MaxReturn', 'MaxReturn_Summary_output.csv'))
        except Exception:
            mr_sum = None
        _gc.collect()

        regimes = mv_sum['Regime'].tolist()
        assets  = None
        result  = {'regimes': regimes, 'portfolios': {}}

        for regime in regimes:
            pd_data = {}

            # ── Min Variance ────────────────────────────────────────────
            try:
                wdf = pd.read_csv(_os.path.join(OPT, 'MinimumRisk', f'MinimumRisk_Weights_{regime}_output.csv'))
                row = mv_sum[mv_sum['Regime'] == regime].iloc[0]
                if assets is None:
                    assets = wdf['Asset'].tolist()
                pd_data['min_variance'] = {
                    'weights':    {r['Asset']: round(float(r['Weight']), 6) for _, r in wdf.iterrows()},
                    'return_ann': round(float(row['Portfolio_Return_Annualized']), 6),
                    'vol_ann':    round(float(row['Portfolio_Volatility_Annualized']), 6),
                    'sharpe':     round(float(row['Portfolio_Sharpe']), 6),
                }
            except Exception as _e:
                pd_data['min_variance'] = {'error': str(_e)}
            _gc.collect()

            # ── Max Sharpe ───────────────────────────────────────────────
            try:
                wdf2 = pd.read_csv(_os.path.join(OPT, 'MaxSharpe', f'MaxSharpe_Weights_{regime}_output.csv'))
                row2 = ms_sum[ms_sum['Regime'] == regime].iloc[0]
                pd_data['max_sharpe'] = {
                    'weights':    {r['Asset']: round(float(r['Weight']), 6) for _, r in wdf2.iterrows()},
                    'return_ann': round(float(row2['Portfolio_Return_Annualized']), 6),
                    'vol_ann':    round(float(row2['Portfolio_Volatility_Annualized']), 6),
                    'sharpe':     round(float(row2['Portfolio_Sharpe']), 6),
                }
            except Exception as _e:
                pd_data['max_sharpe'] = {'error': str(_e)}
            _gc.collect()

            # ── Max Return ───────────────────────────────────────────────
            try:
                wdf3 = pd.read_csv(_os.path.join(OPT, 'MaxReturn', f'MaxReturn_Weights_{regime}_output.csv'))
                if mr_sum is not None:
                    row3 = mr_sum[mr_sum['Regime'] == regime].iloc[0]
                    pd_data['max_return'] = {
                        'weights':    {r['Asset']: round(float(r['Weight']), 6) for _, r in wdf3.iterrows()},
                        'return_ann': round(float(row3['Portfolio_Return_Annualized']), 6),
                        'vol_ann':    round(float(row3['Portfolio_Volatility_Annualized']), 6),
                        'sharpe':     round(float(row3['Portfolio_Sharpe']), 6),
                    }
                else:
                    raise FileNotFoundError("MaxReturn summary not found — run Card 14 first.")
            except Exception as _e:
                pd_data['max_return'] = {'error': str(_e)}
            _gc.collect()

            # ── Efficient Frontier (annualised, with per-point weights) ─
            try:
                efdf   = pd.read_csv(_os.path.join(OPT, 'EfficientFrontier', f'EfficientFrontier_{regime}_output.csv'))
                wf_path = _os.path.join(OPT, 'EfficientFrontier', f'EfficientFrontier_Weights_{regime}_output.csv')
                wf_df  = pd.read_csv(wf_path) if _os.path.exists(wf_path) else None
                pts    = []
                for _, r in efdf.iterrows():
                    pid = int(r['Point_ID'])
                    pt  = {
                        'id':     pid,
                        'vol':    round(float(r['Portfolio_Volatility']) * (252 ** 0.5), 6),
                        'ret':    round(float(r['Portfolio_Return'])     * 252,           6),
                        'sharpe': round(float(r['Portfolio_Sharpe']),                     6),
                    }
                    if wf_df is not None:
                        pw = wf_df[wf_df['Point_ID'] == pid]
                        if not pw.empty:
                            pt['weights'] = {row['Asset']: round(float(row['Weight']), 6)
                                             for _, row in pw.iterrows()}
                    pts.append(pt)
                min_vol_id = min(pts, key=lambda p: p['vol'])['id']    if pts else 0
                max_sh_id  = max(pts, key=lambda p: p['sharpe'])['id'] if pts else 0
                pd_data['frontier'] = {'points': pts, 'min_vol_id': min_vol_id, 'max_sh_id': max_sh_id}
            except Exception as _e:
                pd_data['frontier'] = {'error': str(_e), 'points': []}
            _gc.collect()

            result['portfolios'][regime] = pd_data

        result['assets'] = assets or []
        result['status'] = 'success'

        _gc.collect()
        return jsonify(result)

    except Exception as _e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(_e)}), 500


@app.route('/api/ef-vol-target')
def ef_vol_target():
    """
    Finds the max-return portfolio on the efficient frontier subject to each of
    up to 3 annualised volatility targets, using a covariance matrix estimated
    from an optional date range (or all-time if dates are omitted).

    Query params:
        target_vols – comma-separated volatility targets in % (e.g. "10,15,20")
        target_vol  – single target in % (legacy; ignored when target_vols present)
        start_date  – YYYY-MM-DD  (optional)
        end_date    – YYYY-MM-DD  (optional)
    """
    import gc as _gc
    try:
        import pandas as _pd
        import numpy as _np
        from scipy.optimize import minimize as _minimize

        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        n = len(ASSETS)

        # ── Parse volatility targets ───────────────────────────────────────────
        raw_vols = request.args.get('target_vols', '') or request.args.get('target_vol', '15')
        try:
            target_vols = [float(v.strip()) for v in raw_vols.split(',') if v.strip()]
        except ValueError:
            target_vols = [15.0]
        if not target_vols:
            target_vols = [15.0]

        start_str  = request.args.get('start_date', '').strip()
        end_str    = request.args.get('end_date',   '').strip()
        months_str = request.args.get('months',     '').strip()
        date_start = _pd.to_datetime(start_str) if start_str else None
        date_end   = _pd.to_datetime(end_str)   if end_str   else None

        # Months-back: use last regime date as end anchor
        if months_str.isdigit() and int(months_str) > 0 and date_start is None:
            _months_ev = int(months_str)
            _rcsv_ev = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
            if os.path.exists(_rcsv_ev):
                _rdf_ev_tmp = _pd.read_csv(_rcsv_ev, parse_dates=['End_Date'])
                _last_ev = _rdf_ev_tmp['End_Date'].max()
                if date_end is None:
                    date_end = _last_ev
                date_start = date_end - _pd.DateOffset(months=_months_ev)
            del _rcsv_ev

        use_alltime = date_start is None and date_end is None

        # ── Load prices ───────────────────────────────────────────────────────
        df_idx = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")

        regime_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date","End_Date"]
        ).sort_values("Start_Date").reset_index(drop=True)

        # ── Build per-regime log-return buckets (filtered by date range) ──────
        buckets_cov = []
        for _, p in regime_df.iterrows():
            mask = (df_idx.index >= p["Start_Date"]) & (df_idx.index <= p["End_Date"])
            if date_start is not None:
                mask &= df_idx.index >= date_start
            if date_end is not None:
                mask &= df_idx.index <= date_end
            subset = df_idx.loc[mask, ASSETS].copy()
            if len(subset) < 2:
                continue
            lr = _np.log(subset / subset.shift(1)).dropna()
            if not lr.empty:
                lr["_Regime"] = p["Regime"]
                buckets_cov.append(lr)

        del df_idx
        _gc.collect()

        if not buckets_cov:
            return jsonify({'status': 'error',
                            'message': 'No data in the specified date range for covariance estimation.'}), 400

        cov_ret = _pd.concat(buckets_cov).sort_index()

        # ── Load risk-free rates and compute per-regime daily mean rf ─────────
        rf_df_path = os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv")
        rf_by_regime_ev = {}
        try:
            _rf_ev = _pd.read_csv(rf_df_path, parse_dates=["date"]).sort_values("date").set_index("date")
            _rdf_ev = _pd.read_csv(
                os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
                parse_dates=["Start_Date", "End_Date"]
            )
            for regime in REGIMES:
                _rows = _rdf_ev[_rdf_ev["Regime"] == regime]
                _vals = []
                for _, _row in _rows.iterrows():
                    _m = (_rf_ev.index >= _row["Start_Date"]) & (_rf_ev.index <= _row["End_Date"])
                    _sub = _rf_ev.loc[_m, "risk_free_rate"]
                    if len(_sub):
                        _vals.extend(_sub.tolist())
                rf_by_regime_ev[regime] = float(_np.mean(_vals)) if _vals else 0.0
            del _rf_ev, _rdf_ev
        except Exception:
            rf_by_regime_ev = {r: 0.0 for r in REGIMES}
        _gc.collect()

        # ── Pre-compute per-regime mu and Sigma (shared across all targets) ───
        regime_params = {}
        for regime in REGIMES:
            sub = cov_ret.loc[cov_ret["_Regime"] == regime, ASSETS]
            if sub.shape[0] < n + 1:
                regime_params[regime] = None
            else:
                regime_params[regime] = (
                    _np.array(sub.mean()),
                    _np.array(sub.cov()),
                    rf_by_regime_ev.get(regime, 0.0),
                )

        del cov_ret
        _gc.collect()

        # ── Helper: optimize for one (regime, target_vol) pair ────────────────
        def _optimize_regime(mu, Sigma, rf, tv_pct):
            tv_ann = tv_pct / 100.0
            tv_d   = tv_ann / _np.sqrt(252)
            w0     = _np.ones(n) / n
            bounds = [(0.0, 1.0)] * n
            cons   = [
                {'type': 'eq',   'fun': lambda w: w.sum() - 1.0},
                {'type': 'ineq', 'fun': lambda w, S=Sigma: tv_d**2 - float(w @ S @ w)},
            ]
            res = _minimize(lambda w: -float(w @ mu), w0,
                            method='SLSQP', bounds=bounds, constraints=cons,
                            options={'ftol': 1e-12, 'maxiter': 2000})
            w_opt  = _np.clip(res.x, 0.0, 1.0); w_opt /= w_opt.sum()
            var_d  = float(w_opt @ Sigma @ w_opt)
            vol_d  = float(_np.sqrt(max(var_d, 0.0)))
            ret_d  = float(w_opt @ mu)
            # Correct Sharpe: excess return over risk-free / vol
            sharpe = (ret_d - rf) / vol_d if vol_d > 1e-12 else 0.0
            return {
                'weights': {ASSETS[i]: round(float(w_opt[i]), 6) for i in range(n)},
                'ret_ann': round(ret_d * 252, 6),
                'vol_ann': round(vol_d * _np.sqrt(252), 6),
                'sharpe':  round(sharpe, 6),
                'rf_used': round(rf, 8),
                'status':  'optimal' if res.success else 'suboptimal',
            }

        # ── Run all scenarios ─────────────────────────────────────────────────
        scenarios = []
        for tv_pct in target_vols:
            results_out = {}
            for regime in REGIMES:
                params = regime_params.get(regime)
                if params is None:
                    sub = cov_ret.loc[cov_ret["_Regime"] == regime] if 'cov_ret' in dir() else None
                    cnt = 0 if sub is None else sub.shape[0]
                    results_out[regime] = {'error': f'Insufficient observations ({cnt}) for regime.'}
                else:
                    mu, Sigma, rf = params
                    try:
                        results_out[regime] = _optimize_regime(mu, Sigma, rf, tv_pct)
                    except Exception as _oe:
                        results_out[regime] = {'error': str(_oe)}
            scenarios.append({'target_vol': tv_pct, 'results': results_out})
            _gc.collect()

        # ── Save weights cache for Card 19 rolling backtest ───────────────────
        import json as _json
        ev_cache_dir = os.path.join(DATA_DIR, "EfVolOpt")
        os.makedirs(ev_cache_dir, exist_ok=True)
        cache_data = {
            'assets':          ASSETS,
            'target_vols':     target_vols,
            'cov_start_date':  date_start.strftime('%Y-%m-%d') if date_start else None,
            'cov_end_date':    date_end.strftime('%Y-%m-%d')   if date_end   else None,
            'regime_weights': {}   # {tv_str: {regime: [w0,...,wn-1]}}
        }
        for sc in scenarios:
            tv_str = str(sc['target_vol'])
            cache_data['regime_weights'][tv_str] = {}
            for regime, res in sc['results'].items():
                if 'weights' in res:
                    cache_data['regime_weights'][tv_str][regime] = [
                        res['weights'].get(a, 0.0) for a in ASSETS
                    ]
        try:
            with open(os.path.join(ev_cache_dir, 'ev_weights_cache.json'), 'w') as _f:
                _json.dump(cache_data, _f)
        except Exception:
            pass  # non-fatal

        return jsonify({
            'status':      'success',
            'regimes':     REGIMES,
            'assets':      ASSETS,
            'scenarios':   scenarios,
            'cov_start':   start_str or None,
            'cov_end':     end_str   or None,
            'cov_alltime': use_alltime,
        })

    except Exception as _e:
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(_e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/risk-metrics')
def risk_metrics():
    """
    Per-asset × per-regime risk metrics matrix.
    Returns Sharpe, Sortino, Max Drawdown (%), and Calmar ratio
    for each of the 7 assets across 4 regimes.
    """
    import gc as _gc
    try:
        import pandas as _pd
        import numpy  as _np

        ASSETS = [
            "SPTR5BNK Index", "DJITR Index", "SPXT Index",
            "XAUUSD Curncy",  "XAGUSD Curncy",
            "LT09TRUU Index", "LD12TRUU Index",
        ]
        ASSET_LABELS = [
            "SPTR5BNK", "DJITR", "SPXT",
            "XAUUSD", "XAGUSD",
            "LT09TRUU", "LD12TRUU",
        ]
        REGIMES = ["Goldilocks", "Downturn", "Stagflation", "Overheating"]

        # ── Load prices ───────────────────────────────────────────────────────
        prices = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")[ASSETS].dropna(how='all')

        regime_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date", "End_Date"]
        ).sort_values("Start_Date").reset_index(drop=True)

        rf_path = os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv")
        if os.path.exists(rf_path):
            rf_series = _pd.read_csv(
                rf_path, parse_dates=["date"]
            ).sort_values("date").set_index("date")["risk_free_rate"]
        else:
            rf_series = _pd.Series(dtype=float)

        # ── Daily log returns ─────────────────────────────────────────────────
        log_ret = _np.log(prices / prices.shift(1)).dropna(how='all')

        # ── RF lookup ─────────────────────────────────────────────────────────
        def get_rf_for_dates(dates):
            if rf_series.empty:
                return _pd.Series(0.0, index=dates)
            idxs = rf_series.index.searchsorted(dates, side='right') - 1
            vals = [float(rf_series.iloc[i]) if i >= 0 else 0.0 for i in idxs]
            return _pd.Series(vals, index=dates)

        # ── Compute metrics per regime × asset ────────────────────────────────
        metrics = {m: {} for m in ('sharpe', 'sortino', 'maxdd', 'calmar')}
        n_days  = {}

        for regime in REGIMES:
            mask = _pd.Series(False, index=log_ret.index)
            for _, row in regime_df[regime_df['Regime'] == regime].iterrows():
                mask |= (log_ret.index >= row['Start_Date']) & \
                        (log_ret.index <= row['End_Date'])

            r_ret = log_ret[mask]
            for m in metrics:
                metrics[m][regime] = {}
            n_days[regime] = int(mask.sum())

            if r_ret.empty:
                for label in ASSET_LABELS:
                    for m in metrics:
                        metrics[m][regime][label] = None
                continue

            rf_d = get_rf_for_dates(r_ret.index)

            for asset, label in zip(ASSETS, ASSET_LABELS):
                if asset not in r_ret.columns:
                    for m in metrics:
                        metrics[m][regime][label] = None
                    continue

                arr = r_ret[asset].dropna()
                if len(arr) < 5:
                    for m in metrics:
                        metrics[m][regime][label] = None
                    continue

                rf_aligned = rf_d.reindex(arr.index).fillna(0.0)
                excess     = arr - rf_aligned

                # ── Sharpe ────────────────────────────────────────────────────
                ex_std = float(excess.std(ddof=1))
                sharpe = (round(float(excess.mean()) / ex_std * _np.sqrt(252), 4)
                          if ex_std > 1e-12 else None)

                # ── Sortino ───────────────────────────────────────────────────
                # Annualized excess return / (annualized downside std)
                # = mean(exc)*sqrt(252) / std_d(exc)
                downside = excess[excess < 0]
                ds_std   = float(downside.std(ddof=1)) if len(downside) > 1 else 0.0
                sortino  = (round(float(excess.mean()) * _np.sqrt(252) / ds_std, 4)
                            if ds_std > 1e-12 else None)

                # ── Max Drawdown (%) ──────────────────────────────────────────
                cum      = arr.cumsum()
                roll_max = cum.cummax()
                dd       = (cum - roll_max)
                maxdd    = round(float(dd.min()) * 100.0, 4)

                # ── Calmar ────────────────────────────────────────────────────
                ann_ret = float(arr.mean()) * 252 * 100   # %
                calmar  = (round(ann_ret / abs(maxdd), 4)
                           if abs(maxdd) > 1e-6 else None)

                metrics['sharpe'][regime][label]  = sharpe
                metrics['sortino'][regime][label] = sortino
                metrics['maxdd'][regime][label]   = maxdd
                metrics['calmar'][regime][label]  = calmar

        # ── Save to CSV ───────────────────────────────────────────────────────
        rows = []
        for metric_key in ('sharpe', 'sortino', 'maxdd', 'calmar'):
            for regime in REGIMES:
                row = {'metric': metric_key, 'regime': regime}
                for label in ASSET_LABELS:
                    row[label] = metrics[metric_key][regime].get(label)
                rows.append(row)
        import pandas as _pd_save
        _pd_save.DataFrame(rows).to_csv(
            os.path.join(DATA_DIR, '27_RiskMetrics_output.csv'), index=False
        )

        _gc.collect()
        return jsonify({
            'status':  'success',
            'assets':  ASSET_LABELS,
            'regimes': REGIMES,
            'n_days':  n_days,
            'metrics': metrics,
        })

    except Exception as _e:
        import traceback as _tb
        return jsonify({
            'status':  'error',
            'message': str(_e),
            'trace':   _tb.format_exc(),
        }), 500


@app.route('/api/rolling-backtest')
def rolling_backtest():
    """
    Self-contained rolling-window portfolio backtest (regime-agnostic covariance).

    At each rebalancing date T the function:
      1. Takes all data from [T - window_months, T] for all assets.
      2. Computes mu and Sigma from the full window (no regime filtering).
      3. Maximizes return s.t. annualised vol ≤ target using that Sigma.
      4. Applies the resulting weights until the next rebalancing date.

    No dependency on any other card's pre-computed cache.

    Query params:
        window_months  – rolling covariance window in months (default 24)
        rebal_months   – rebalancing frequency in months (default 1)
        target_vols    – comma-separated annualised vol targets in % (default "15")
    """
    import gc as _gc
    from scipy.optimize import minimize as _minimize_rb
    try:
        import pandas as _pd
        import numpy as _np
        from dateutil.relativedelta import relativedelta

        ASSETS  = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                   "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index","LD12TRUU Index"]
        REGIMES = ["Goldilocks","Downturn","Stagflation","Overheating"]
        n = len(ASSETS)

        window_months = max(1, int(request.args.get('window_months', 24)))
        rebal_months  = max(1, int(request.args.get('rebal_months',  1)))

        raw_vols = request.args.get('target_vols', '15')
        try:
            target_vols = [float(v.strip()) for v in raw_vols.split(',') if v.strip()]
        except ValueError:
            target_vols = [15.0]
        if not target_vols:
            target_vols = [15.0]

        # ── Load data ─────────────────────────────────────────────────────────
        prices = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")[ASSETS].dropna(how='all')

        regime_df = _pd.read_csv(
            os.path.join(DATA_DIR, "02_RegimeDates_output.csv"),
            parse_dates=["Start_Date","End_Date"]
        ).sort_values("Start_Date").reset_index(drop=True)

        rf_path = os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv")
        if os.path.exists(rf_path):
            rf_series = _pd.read_csv(
                rf_path, parse_dates=["date"]
            ).sort_values("date").set_index("date")["risk_free_rate"]
        else:
            rf_series = _pd.Series(dtype=float)

        # ── Daily log returns ─────────────────────────────────────────────────
        log_ret = _np.log(prices / prices.shift(1)).dropna(how='all')

        # ── Build date → regime map ───────────────────────────────────────────
        regime_map = {}
        for _, row in regime_df.iterrows():
            mask = (log_ret.index >= row['Start_Date']) & (log_ret.index <= row['End_Date'])
            for dt in log_ret.index[mask]:
                regime_map[dt] = row['Regime']

        # ── Risk-free rate lookup ──────────────────────────────────────────────
        def get_rf_daily(dt):
            if rf_series.empty:
                return 0.0
            idx = rf_series.index.searchsorted(dt, side='right') - 1
            if idx < 0:
                return 0.0
            return float(rf_series.iloc[idx])

        # ── Strategy keys and labels ───────────────────────────────────────────
        def _tv_label(tv):
            return f'σ≤{int(tv) if tv == int(tv) else tv}%'

        strat_vol_keys = [f'vol_{v}' for v in target_vols]
        strat_labels   = {f'vol_{v}': _tv_label(v) for v in target_vols}
        strat_labels['eqWeight'] = 'Equal Weight'
        strat_labels['riskFree'] = 'Risk-Free'

        # ── Generate rebalancing dates ─────────────────────────────────────────
        all_lr_dates      = log_ret.index
        first_date        = all_lr_dates[0]
        last_date         = all_lr_dates[-1]
        warnings_out      = []

        # Warmup: need window_months of price data before first portfolio
        warmup_end = first_date + relativedelta(months=window_months)

        rebal_dates = []
        cur = first_date
        while cur <= last_date:
            future = all_lr_dates[all_lr_dates >= cur]
            if len(future):
                rebal_dates.append(future[0])
            cur += relativedelta(months=rebal_months)

        if not rebal_dates:
            return jsonify({'status': 'error', 'message': 'No rebalancing dates generated.'}), 400

        # ── Helper: max-return within vol target ──────────────────────────────
        def _optimize_max_ret(mu, Sigma, tv_pct):
            tv_d   = (tv_pct / 100.0) / _np.sqrt(252)
            w0     = _np.ones(n) / n
            bounds = [(0.0, 1.0)] * n
            cons   = [
                {'type': 'eq',   'fun': lambda w: w.sum() - 1.0},
                {'type': 'ineq', 'fun': lambda w, S=Sigma: tv_d**2 - float(w @ S @ w)},
            ]
            res = _minimize_rb(lambda w: -float(w @ mu), w0, method='SLSQP',
                               bounds=bounds, constraints=cons,
                               options={'ftol': 1e-12, 'maxiter': 2000})
            w_out = _np.clip(res.x, 0.0, 1.0)
            s = w_out.sum()
            return w_out / s if s > 1e-6 else w0

        # ── Walk-forward rolling backtest ─────────────────────────────────────
        w_eq       = _np.ones(n) / n
        port_ret   = {k: [] for k in strat_vol_keys + ['eqWeight', 'riskFree']}
        port_dates = []
        warmup_skipped = 0
        data_skip      = 0

        cur_w = {vk: w_eq.copy() for vk in strat_vol_keys}

        for ri, rebal_dt in enumerate(rebal_dates):
            next_dt = rebal_dates[ri + 1] if ri + 1 < len(rebal_dates) else last_date

            # ── Warmup: need window_months of data before first portfolio ──────
            if rebal_dt < warmup_end:
                warmup_skipped += 1
                continue

            # ── Rolling window: all data in (rebal_dt - window_months, rebal_dt] ──
            window_start = rebal_dt - relativedelta(months=window_months)
            w_mask       = (log_ret.index > window_start) & (log_ret.index <= rebal_dt)
            r_sub        = log_ret.loc[w_mask, ASSETS].dropna()

            if r_sub.shape[0] >= n + 1:
                mu_r    = _np.array(r_sub.mean())
                Sigma_r = _np.array(r_sub.cov())
                for vk, tv_pct in zip(strat_vol_keys, target_vols):
                    try:
                        cur_w[vk] = _optimize_max_ret(mu_r, Sigma_r, tv_pct)
                    except Exception:
                        pass  # keep previous weights
            else:
                data_skip += 1

            # ── Apply weights for holding period ──────────────────────────────
            hold_mask = (log_ret.index > rebal_dt) & (log_ret.index <= next_dt)
            hold      = log_ret.loc[hold_mask, ASSETS].fillna(0.0)
            for day_dt, row in hold.iterrows():
                r_arr  = _np.array(row.values, dtype=float)
                rf_day = get_rf_daily(day_dt)
                port_ret['eqWeight'].append(float(w_eq @ r_arr))
                for vk in strat_vol_keys:
                    port_ret[vk].append(float(cur_w[vk] @ r_arr))
                port_ret['riskFree'].append(rf_day)
                port_dates.append(str(day_dt.date()))

            _gc.collect()

        # ── Warnings ──────────────────────────────────────────────────────────
        if warmup_skipped > 0:
            warnings_out.append(
                f'{warmup_skipped} rebalancing period(s) skipped: '
                f'need {window_months} months of price data for first covariance estimate. '
                f'Simulation starts from {warmup_end.date()}.'
            )
        if data_skip > 0:
            warnings_out.append(
                f'{data_skip} rebalancing date(s) had insufficient data in the '
                f'{window_months}-month rolling window; previous weights were held.'
            )

        if not port_dates:
            return jsonify({
                'status': 'error',
                'message': f'No data produced. Try reducing window_months (currently {window_months}) '
                           f'or ensure regime data covers the price history.'
            }), 400

        # ── Cumulative returns (%) ─────────────────────────────────────────────
        def cumret(daily):
            cum, result = 0.0, []
            for r in daily:
                cum += r
                result.append(round(cum * 100, 4))
            return result

        cum_series = {k: cumret(port_ret[k]) for k in port_ret}

        # ── Excess return (above risk-free) ───────────────────────────────────
        rf_arr = _np.array(port_ret['riskFree'])
        excess = {}
        for k in strat_vol_keys + ['eqWeight']:
            arr       = _np.array(port_ret[k])
            excess[k] = cumret((arr - rf_arr).tolist())

        # ── 12-month rolling Sharpe (≈252 trading days) ───────────────────────
        roll_win = 252
        def rolling_sharpe_fn(daily, rf_d):
            result = [None] * len(daily)
            arr = _np.array(daily) - _np.array(rf_d)
            for i in range(roll_win, len(arr) + 1):
                w = arr[i - roll_win:i]
                sd = float(w.std(ddof=1))
                result[i - 1] = round(float(w.mean()) / sd * _np.sqrt(252), 4) if sd > 1e-12 else 0.0
            return result

        rf_d   = port_ret['riskFree']
        roll_sh = {k: rolling_sharpe_fn(port_ret[k], rf_d) for k in strat_vol_keys + ['eqWeight']}

        # ── Summary stats ─────────────────────────────────────────────────────
        summary = {}
        for k in strat_vol_keys + ['eqWeight']:
            arr  = _np.array(port_ret[k])
            rf_a = _np.array(rf_d)
            ex_d = arr - rf_a
            mu_d = float(arr.mean())
            sd_d = float(arr.std(ddof=1)) if arr.std(ddof=1) > 1e-12 else 1e-12
            ann_ret = mu_d * 252
            ann_vol = sd_d * _np.sqrt(252)
            sharpe  = (float(ex_d.mean()) / float(ex_d.std(ddof=1)) * _np.sqrt(252)
                       if ex_d.std(ddof=1) > 1e-12 else 0.0)
            summary[k] = {
                'Ann. Return (%)':  round(ann_ret * 100, 4),
                'Ann. Vol (%)':     round(ann_vol * 100, 4),
                'Sharpe Ratio':     round(sharpe,  4),
                'Total Return (%)': round(float(arr.sum()) * 100, 4),
            }

        # ── Alpha vs Equal-Weight ─────────────────────────────────────────────
        eq_np   = _np.array(port_ret['eqWeight'])
        eq_mean = float(eq_np.mean())
        eq_var  = float(eq_np.var(ddof=1)) if eq_np.var(ddof=1) > 1e-12 else 1e-12

        alpha_ew     = {}   # annualised simple alpha vs EW (%)
        jensens_alpha = {}  # Jensen's alpha (%)
        cum_alpha    = {}   # cumulative daily alpha series

        for k in strat_vol_keys:
            arr       = _np.array(port_ret[k])
            daily_exc = arr - eq_np
            # Annualised simple alpha
            alpha_ew[k] = round(float(daily_exc.mean()) * 252 * 100, 4)
            # Jensen's alpha: α = (Rp_mean - Rf_mean - β*(Rm_mean - Rf_mean)) * 252 * 100
            rf_mean_d   = float(rf_arr.mean())
            beta_k      = float(_np.cov(arr, eq_np)[0, 1] / eq_var)
            jensens_alpha[k] = round(
                (float(arr.mean()) - rf_mean_d - beta_k * (eq_mean - rf_mean_d)) * 252 * 100, 4
            )
            # Cumulative alpha (running sum of daily alpha × 100)
            cum_alpha[k] = cumret(daily_exc.tolist())

            # Append to summary
            summary[k]['Alpha vs EW (%)']  = alpha_ew[k]
            summary[k]["Jensen's α (%)"]   = jensens_alpha[k]

        # ── Persist run to rolling backtest log (JSONL, append-only) ─────────
        import json as _json_rb
        from datetime import datetime as _dt_rb

        _ts       = _dt_rb.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]   # ms precision
        _log_entry = {
            'ts':            _ts,
            'window_months': window_months,
            'rebal_months':  rebal_months,
            'target_vols':   target_vols,
            'date_start':    port_dates[0] if port_dates else None,
            'date_end':      port_dates[-1] if port_dates else None,
            'n_days':        len(port_dates),
            'eq_weight': {
                'ann_ret':   summary['eqWeight']['Ann. Return (%)'],
                'ann_vol':   summary['eqWeight']['Ann. Vol (%)'],
                'sharpe':    summary['eqWeight']['Sharpe Ratio'],
                'total_ret': summary['eqWeight']['Total Return (%)'],
            },
            'strategies': {
                k: {
                    'label':         strat_labels.get(k, k),
                    'ann_ret':       summary[k]['Ann. Return (%)'],
                    'ann_vol':       summary[k]['Ann. Vol (%)'],
                    'sharpe':        summary[k]['Sharpe Ratio'],
                    'total_ret':     summary[k]['Total Return (%)'],
                    'alpha_ew':      alpha_ew[k],
                    'jensens_alpha': jensens_alpha[k],
                }
                for k in strat_vol_keys
            },
        }
        _log_path = os.path.join(DATA_DIR, 'rolling_backtest_log.jsonl')
        try:
            with open(_log_path, 'a', encoding='utf-8') as _lf:
                _lf.write(_json_rb.dumps(_log_entry) + '\n')
        except Exception:
            pass

        # ── Find best-Sharpe run from log ────────────────────────────────────
        best_alpha_run = None
        try:
            if os.path.exists(_log_path):
                _best_val = -float('inf')
                with open(_log_path, 'r', encoding='utf-8') as _lf:
                    for _line in _lf:
                        _line = _line.strip()
                        if not _line:
                            continue
                        try:
                            _entry = _json_rb.loads(_line)
                            for _k, _st in _entry.get('strategies', {}).items():
                                _sh = _st.get('sharpe', -999.0)
                                if _sh > _best_val:
                                    _best_val = _sh
                                    best_alpha_run = {
                                        'ts':            _entry.get('ts', '?'),
                                        'window_months': _entry.get('window_months'),
                                        'rebal_months':  _entry.get('rebal_months'),
                                        'target_vols':   _entry.get('target_vols'),
                                        'date_start':    _entry.get('date_start'),
                                        'date_end':      _entry.get('date_end'),
                                        'strategy_key':  _k,
                                        'strategy_label': _st.get('label', _k),
                                        'alpha_ew':      _st.get('alpha_ew'),
                                        'jensens_alpha': _st.get('jensens_alpha'),
                                        'ann_ret':       _st.get('ann_ret'),
                                        'ann_vol':       _st.get('ann_vol'),
                                        'sharpe':        _sh,
                                        'total_ret':     _st.get('total_ret'),
                                    }
                        except Exception:
                            pass
        except Exception:
            pass

        # ── Regime spans for chart background ────────────────────────────────
        REGIME_DATA_STATIC = [
            ["Downturn",    "2001-06-01", "2001-10-31"],
            ["Goldilocks",  "2001-11-01", "2004-05-31"],
            ["Downturn",    "2004-06-01", "2006-08-31"],
            ["Goldilocks",  "2006-09-01", "2007-09-30"],
            ["Downturn",    "2007-10-01", "2008-10-31"],
            ["Stagflation", "2008-11-01", "2009-09-30"],
            ["Goldilocks",  "2009-10-01", "2011-03-31"],
            ["Downturn",    "2011-04-01", "2012-02-29"],
            ["Goldilocks",  "2012-03-01", "2019-10-31"],
            ["Downturn",    "2019-11-01", "2020-03-31"],
            ["Stagflation", "2020-04-01", "2020-05-31"],
            ["Goldilocks",  "2020-06-01", "2021-03-31"],
            ["Overheating", "2021-04-01", "2022-10-31"],
            ["Downturn",    "2022-11-01", "2024-04-30"],
            ["Goldilocks",  "2024-05-01", "2025-08-31"],
        ]
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        try:
            if os.path.exists(rcsv):
                _rdf = _pd.read_csv(rcsv)
                regime_spans = _rdf[["Regime","Start_Date","End_Date"]].to_dict(orient='records')
            else:
                regime_spans = [{"Regime": r[0], "Start_Date": r[1], "End_Date": r[2]}
                                for r in REGIME_DATA_STATIC]
        except Exception:
            regime_spans = [{"Regime": r[0], "Start_Date": r[1], "End_Date": r[2]}
                            for r in REGIME_DATA_STATIC]

        _gc.collect()
        return jsonify({
            'status':          'success',
            'window_months':   window_months,
            'rebal_months':    rebal_months,
            'target_vols':     target_vols,
            'strat_keys':      strat_vol_keys + ['eqWeight'] + ['riskFree'],
            'strat_labels':    strat_labels,
            'dates':           port_dates,
            'series':          cum_series,
            'excess':          excess,
            'rolling_sharpe':  roll_sh,
            'cum_alpha':       cum_alpha,
            'alpha_ew':        alpha_ew,
            'jensens_alpha':   jensens_alpha,
            'summary':         summary,
            'warnings':        warnings_out,
            'n_rebal':         len(rebal_dates),
            'warmup_skipped':  warmup_skipped,
            'regime_spans':    regime_spans,
            'run_ts':          _ts,
            'best_alpha_run':  best_alpha_run,
        })

    except Exception as _e:
        import traceback as _tb
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(_e) + '\n' + _tb.format_exc()}), 500


@app.route('/api/regime-summary')
def regime_summary():
    """Current regime + suggested allocation + expected Sharpe for the dashboard panel."""
    try:
        model = request.args.get('model', 'MaxSharpe')  # MaxSharpe | MinimumRisk | MaxReturn

        # ── 1. Current regime (last row of regime dates) ───────────────────────
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        if not os.path.exists(rcsv):
            return jsonify({'status': 'error', 'message': 'Regime dates file not found'}), 200
        rdf = pd.read_csv(rcsv, parse_dates=['Start_Date', 'End_Date'])
        rdf = rdf.sort_values('End_Date')
        last_row    = rdf.iloc[-1]
        curr_regime = str(last_row['Regime'])
        last_date   = last_row['End_Date'].strftime('%Y-%m-%d')
        duration    = int(last_row.get('Duration_Months', 0))

        # ── 2. Weights for current regime ──────────────────────────────────────
        MODEL_DIR_MAP = {
            'MaxSharpe':    'MaxSharpe',
            'MinimumRisk':  'MinimumRisk',
            'MaxReturn':    'MaxReturn',
        }
        opt_dir = MODEL_DIR_MAP.get(model, 'MaxSharpe')
        # Try regime-specific file first, fall back to combined weights file
        w_specific = os.path.join(DATA_DIR, 'Optimization', opt_dir,
                                  f'{opt_dir}_Weights_{curr_regime}_output.csv')
        w_combined = os.path.join(DATA_DIR, 'Optimization', opt_dir,
                                  f'{opt_dir}_Weights_output.csv')

        weights_raw = {}
        if os.path.exists(w_specific):
            wdf = pd.read_csv(w_specific)
            # columns: Asset, Weight
            for _, r in wdf.iterrows():
                weights_raw[str(r['Asset'])] = float(r['Weight'])
        elif os.path.exists(w_combined):
            wdf = pd.read_csv(w_combined)
            # columns: Regime, Asset, Weight
            sub = wdf[wdf['Regime'] == curr_regime]
            for _, r in sub.iterrows():
                weights_raw[str(r['Asset'])] = float(r['Weight'])

        # ── 3. Asset-class grouping ────────────────────────────────────────────
        EQUITY   = ['SPTR5BNK Index', 'DJITR Index', 'SPXT Index']
        BONDS    = ['LT09TRUU Index', 'LD12TRUU Index']
        METALS   = ['XAUUSD Curncy',  'XAGUSD Curncy']
        TBILL    = ['LD12TRUU Index']   # dual-role: risk-free proxy (already in BONDS)

        eq_w  = sum(weights_raw.get(a, 0) for a in EQUITY)
        bd_w  = sum(weights_raw.get(a, 0) for a in BONDS)
        mt_w  = sum(weights_raw.get(a, 0) for a in METALS)
        other = max(0.0, 1.0 - eq_w - bd_w - mt_w)

        alloc = {
            'equity':  round(eq_w  * 100, 1),
            'bonds':   round(bd_w  * 100, 1),
            'metals':  round(mt_w  * 100, 1),
            'other':   round(other * 100, 1),
        }

        # Individual asset weights (non-zero only)
        asset_weights = [
            {'asset': a, 'weight': round(w * 100, 2)}
            for a, w in sorted(weights_raw.items(), key=lambda x: -x[1])
            if w > 0.001
        ]

        # ── 4. Expected Sharpe from summary CSV (annualised) ──────────────────
        s_csv = os.path.join(DATA_DIR, 'Optimization', opt_dir,
                             f'{opt_dir}_Summary_output.csv')
        sharpe = None
        ret_ann = None
        vol_ann = None
        rf_used = None          # risk-free rate used in optimisation (annualised %)
        sharpe_method = None    # label shown in UI
        if os.path.exists(s_csv):
            sdf = pd.read_csv(s_csv)
            row = sdf[sdf['Regime'] == curr_regime]
            if not row.empty:
                r = row.iloc[0]
                # Annualised return & vol are available directly
                if 'Portfolio_Return_Annualized' in r.index and pd.notna(r['Portfolio_Return_Annualized']):
                    ret_ann = round(float(r['Portfolio_Return_Annualized']) * 100, 2)
                if 'Portfolio_Volatility_Annualized' in r.index and pd.notna(r['Portfolio_Volatility_Annualized']):
                    vol_ann = round(float(r['Portfolio_Volatility_Annualized']) * 100, 2)
                # RF used in optimisation (daily → annualised %)
                if 'Risk_Free_Rate_Used' in r.index and pd.notna(r['Risk_Free_Rate_Used']):
                    rf_used = round(float(r['Risk_Free_Rate_Used']) * 252 * 100, 2)
                # Compute proper annualised Sharpe: (r_p - r_f) / σ_p
                if ret_ann is not None and vol_ann is not None and vol_ann > 0:
                    rf_for_sharpe = rf_used if rf_used is not None else 0.0
                    sharpe = round((ret_ann - rf_for_sharpe) / vol_ann, 4)
                    sharpe_method = 'opt_all_periods'   # optimisation used ALL regime periods
                else:
                    # fallback: raw daily objective * sqrt(252)
                    for col in ['Portfolio_Sharpe', 'Objective_MaxSharpe']:
                        if col in r.index and pd.notna(r[col]):
                            sharpe = round(float(r[col]) * (252 ** 0.5), 4)
                            sharpe_method = 'daily_annualised'
                            break

        # ── 5. Vol-targeted allocations from cache ────────────────────────
        EQUITY_A  = ['SPTR5BNK Index', 'DJITR Index', 'SPXT Index']
        BONDS_A   = ['LT09TRUU Index', 'LD12TRUU Index']
        METALS_A  = ['XAUUSD Curncy',  'XAGUSD Curncy']
        RF_COL    = 'LD12TRUU Index'

        # Pre-load returns across ALL periods of the current regime type
        # (matches the optimisation approach — not just the last period)
        _prices   = _load_prices()
        _all_regime_periods = rdf[rdf['Regime'] == curr_regime]
        _buckets  = []
        for _, _pr in _all_regime_periods.iterrows():
            _m = (_prices.index >= pd.to_datetime(_pr['Start_Date'])) & \
                 (_prices.index <= pd.to_datetime(_pr['End_Date']))
            _sub = _prices[_m]
            if len(_sub) > 2:
                _lr = np.log(_sub / _sub.shift(1)).dropna()
                _buckets.append(_lr)
        _log_ret  = pd.concat(_buckets) if _buckets else pd.DataFrame()
        _rf_daily = float(_log_ret[RF_COL].mean()) if (not _log_ret.empty and RF_COL in _log_ret.columns) else 0.0
        _rf_ann   = round(_rf_daily * 252 * 100, 2)   # annualised %

        vol_targets = []
        cache_path = os.path.join(DATA_DIR, 'EfVolOpt', 'ev_weights_cache.json')
        if os.path.exists(cache_path):
            import json as _json
            with open(cache_path) as _f:
                cache = _json.load(_f)
            assets_list = cache.get('assets', [])
            rw = cache.get('regime_weights', {})
            for tv in sorted(cache.get('target_vols', [5.0, 10.0, 15.0])):
                tv_key = str(float(tv))
                regime_block = rw.get(tv_key, {})
                raw_w = regime_block.get(curr_regime, [])
                if not raw_w:
                    # fallback: try any available regime
                    raw_w = next(iter(regime_block.values()), [])
                if raw_w and len(raw_w) == len(assets_list):
                    w_map = dict(zip(assets_list, raw_w))
                    eq_w_ = sum(w_map.get(a, 0) for a in EQUITY_A)
                    bd_w_ = sum(w_map.get(a, 0) for a in BONDS_A)
                    mt_w_ = sum(w_map.get(a, 0) for a in METALS_A)
                    top_assets = sorted(
                        [{'asset': a, 'weight': round(v*100, 2)} for a, v in w_map.items() if v > 0.001],
                        key=lambda x: -x['weight']
                    )

                    # ── Risk metrics for this vol-target portfolio ────────
                    vt_sharpe = None; vt_sortino = None
                    vt_ret_ann = None; vt_vol_ann = None
                    vt_maxdd = None;  vt_calmar  = None
                    if not _log_ret.empty:
                        common = [a for a in assets_list if a in _log_ret.columns]
                        if common:
                            w_vec     = np.array([w_map.get(a, 0) for a in common])
                            port_d    = _log_ret[common].values @ w_vec
                            mu_d      = float(port_d.mean())
                            sig_d     = float(port_d.std())
                            ret_a     = mu_d  * 252 * 100
                            vol_a     = sig_d * np.sqrt(252) * 100
                            vt_ret_ann = round(ret_a, 2)
                            vt_vol_ann = round(vol_a, 2)
                            # Sharpe
                            excess_d  = port_d - _rf_daily
                            ex_std    = float(np.std(excess_d, ddof=1))
                            if vol_a > 0:
                                vt_sharpe = round((ret_a - _rf_ann) / vol_a, 4)
                            # Sortino
                            down      = excess_d[excess_d < 0]
                            ds_std    = float(down.std(ddof=1)) if len(down) > 1 else 0.0
                            if ds_std > 1e-12:
                                vt_sortino = round(float(excess_d.mean()) * np.sqrt(252) / ds_std, 4)
                            # Max Drawdown
                            cum       = np.cumsum(port_d)
                            roll_max  = np.maximum.accumulate(cum)
                            dd        = cum - roll_max
                            vt_maxdd  = round(float(dd.min()) * 100.0, 4)
                            # Calmar
                            if vt_maxdd is not None and abs(vt_maxdd) > 1e-6:
                                vt_calmar = round(ret_a / abs(vt_maxdd), 4)

                    vol_targets.append({
                        'target_vol':       tv,
                        'alloc': {
                            'equity': round(eq_w_ * 100, 1),
                            'bonds':  round(bd_w_ * 100, 1),
                            'metals': round(mt_w_ * 100, 1),
                        },
                        'asset_weights':    top_assets,
                        'regime_available': curr_regime in regime_block,
                        'sharpe':           vt_sharpe,
                        'sortino':          vt_sortino,
                        'maxdd':            vt_maxdd,
                        'calmar':           vt_calmar,
                        'return_ann':       vt_ret_ann,
                        'vol_ann':          vt_vol_ann,
                        'rf_ann':           _rf_ann,
                    })

        return jsonify({
            'status':         'success',
            'regime':         curr_regime,
            'as_of':          last_date,
            'duration_months': duration,
            'model':          model,
            'alloc':          alloc,
            'asset_weights':  asset_weights,
            'sharpe':         sharpe,
            'sharpe_method':  sharpe_method,
            'return_ann':     ret_ann,
            'vol_ann':        vol_ann,
            'rf_ann':         rf_used,
            'vol_targets':    vol_targets,
        })

    except Exception as _e:
        import traceback as _tb
        return jsonify({'status': 'error', 'message': str(_e) + '\n' + _tb.format_exc()}), 200


# ─────────────────────────────────────────────────────────────────────────────
# CARD 3.6 — Risk Parity (Equal Risk Contribution) Optimization
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/risk-parity')
def risk_parity_opt():
    """
    Risk Parity — Equal Risk Contribution (ERC) optimization using full-period data.

    Step 1 — Inverse-Volatility (IV) warm-start:
        w_i  ∝  1 / σ_i   (σ_i = sqrt(Σ_ii))

    Step 2 — ERC iterative SLSQP:
        minimize   Σ_i ( w_i·(Σw)_i  −  (w'Σw)/n )²
        subject to  Σ w_i = 1,   w_i ≥ 0

    Convergence criterion: scipy ftol=1e-14 / maxiter=3000.

    Saves:
      Data/Optimization/RiskParity/RiskParity_Weights_output.csv
      Data/Optimization/RiskParity/RiskParity_Summary_output.csv
    """
    import gc as _gc
    from scipy.optimize import minimize as _minimize_rp
    try:
        import pandas as _pd
        import numpy  as _np

        # Corp 2Y is excluded from ERC — used as the blending (dilution) asset
        ASSETS_ERC = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                      "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index"]
        CORP2Y_KEY = "LD12TRUU Index"
        ASSETS_ALL = ASSETS_ERC + [CORP2Y_KEY]
        n = len(ASSETS_ERC)

        RP_DIR = os.path.join(DATA_DIR, "Optimization", "RiskParity")
        os.makedirs(RP_DIR, exist_ok=True)

        # ── Full-period price data → log returns ──────────────────────────────
        prices = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")[ASSETS_ALL].dropna(how='all')
        log_ret_all = _np.log(prices / prices.shift(1)).dropna(how='all')

        # ERC uses only the 6 risky assets
        log_ret = log_ret_all[ASSETS_ERC]
        mu    = _np.array(log_ret.mean())
        Sigma = _np.array(log_ret.cov())

        # Corp 2Y stats (used as blending / dilution asset)
        c2y_ret_daily = float(log_ret_all[CORP2Y_KEY].mean())
        c2y_vol_daily = float(log_ret_all[CORP2Y_KEY].std())
        c2y_ret_ann   = round(c2y_ret_daily * 252, 8)
        c2y_vol_ann   = round(c2y_vol_daily * _np.sqrt(252), 8)

        del prices; _gc.collect()

        # ── Full-period risk-free rate (for Sharpe denominator) ───────────────
        rf_path = os.path.join(DATA_DIR, "12_RiskFreeRateDaily_output.csv")
        if os.path.exists(rf_path):
            rf_series = _pd.read_csv(rf_path, parse_dates=["date"]) \
                          .sort_values("date")["risk_free_rate"]
            rf_daily = float(rf_series.mean()) if len(rf_series) else 0.0
        else:
            rf_daily = 0.0
        rf_ann = round(rf_daily * 252, 8)

        # ── ERC optimizer — log-barrier formulation (globally convex) ────────
        # minimize  w'Σw/2 − Σ log(wᵢ)
        # KKT: (Σw)ᵢ = 1/wᵢ  →  wᵢ·(Σw)ᵢ = 1 ∀i  →  exact equal RC
        def _erc(S):
            def _obj(w):
                return 0.5 * float(w @ S @ w) - float(_np.sum(_np.log(w)))

            def _grad(w):
                return (S @ w) - 1.0 / w

            w0  = _np.ones(n) / n
            res = _minimize_rp(
                _obj, w0, method='L-BFGS-B', jac=_grad,
                bounds=[(1e-8, None)] * n,
                options={'ftol': 1e-20, 'gtol': 1e-10, 'maxiter': 5000}
            )
            w_out = _np.abs(res.x)
            if w_out.sum() > 1e-10:
                w_out /= w_out.sum()
            return w_out, bool(res.success), int(res.nit)

        # ── IV warm-start weights ─────────────────────────────────────────────
        vols = _np.sqrt(_np.diag(Sigma))
        vols = _np.where(vols < 1e-12, 1e-12, vols)
        w_iv = (1.0 / vols) / (1.0 / vols).sum()

        # ── ERC weights ───────────────────────────────────────────────────────
        w_erc, converged, n_iter = _erc(Sigma)

        # ── Risk contributions ────────────────────────────────────────────────
        pvar   = float(w_erc @ Sigma @ w_erc)
        rc_abs = w_erc * (Sigma @ w_erc)
        rc_pct = (rc_abs / pvar * 100.0) if pvar > 1e-12 else _np.full(n, 100.0/n)

        # ── ERC performance (Sharpe vs actual rf) ─────────────────────────────
        vol_d_e   = float(_np.sqrt(max(pvar, 0.0)))
        ret_d_e   = float(w_erc @ mu)
        vol_ann_e = vol_d_e * _np.sqrt(252)
        ret_ann_e = ret_d_e * 252
        sharpe_e  = (ret_d_e - rf_daily) / vol_d_e if vol_d_e > 1e-12 else 0.0

        # ── IV performance ────────────────────────────────────────────────────
        pvar_iv    = float(w_iv @ Sigma @ w_iv)
        vol_d_iv   = float(_np.sqrt(max(pvar_iv, 0.0)))
        ret_d_iv   = float(w_iv @ mu)
        vol_ann_iv = vol_d_iv * _np.sqrt(252)
        ret_ann_iv = ret_d_iv * 252
        sharpe_iv  = (ret_d_iv - rf_daily) / vol_d_iv if vol_d_iv > 1e-12 else 0.0

        # ── Save CSVs ─────────────────────────────────────────────────────────
        saved = []
        _pd.DataFrame([{
            'Asset': ASSETS_ERC[i],
            'IV_Weight':             round(float(w_iv[i]),  8),
            'ERC_Weight':            round(float(w_erc[i]), 8),
            'Risk_Contribution_Pct': round(float(rc_pct[i]),4),
        } for i in range(n)]).to_csv(
            os.path.join(RP_DIR, "RiskParity_Weights_output.csv"), index=False)
        saved.append("Optimization/RiskParity/RiskParity_Weights_output.csv")

        _pd.DataFrame([{
            'ERC_Return_Ann':     round(float(ret_ann_e),  8),
            'ERC_Volatility_Ann': round(float(vol_ann_e),  8),
            'ERC_Sharpe':         round(float(sharpe_e),   6),
            'IV_Return_Ann':      round(float(ret_ann_iv), 8),
            'IV_Volatility_Ann':  round(float(vol_ann_iv), 8),
            'IV_Sharpe':          round(float(sharpe_iv),  6),
            'Corp2Y_Return_Ann':  c2y_ret_ann,
            'Corp2Y_Vol_Ann':     c2y_vol_ann,
            'Converged':          converged,
            'Iterations':         n_iter,
        }]).to_csv(os.path.join(RP_DIR, "RiskParity_Summary_output.csv"), index=False)
        saved.append("Optimization/RiskParity/RiskParity_Summary_output.csv")

        _gc.collect()
        return jsonify({
            'status': 'success',
            'assets': ASSETS_ERC,
            'corp2y_ret_ann': c2y_ret_ann,
            'corp2y_vol_ann': c2y_vol_ann,
            'iv_weights':       {ASSETS_ERC[i]: round(float(w_iv[i]),  6) for i in range(n)},
            'erc_weights':      {ASSETS_ERC[i]: round(float(w_erc[i]), 6) for i in range(n)},
            'risk_contrib_pct': {ASSETS_ERC[i]: round(float(rc_pct[i]),4) for i in range(n)},
            'erc_summary': {
                'return_ann': round(float(ret_ann_e), 6),
                'vol_ann':    round(float(vol_ann_e), 6),
                'sharpe':     round(float(sharpe_e),  6),
                'converged':  converged,
                'n_iter':     n_iter,
            },
            'iv_summary': {
                'return_ann': round(float(ret_ann_iv), 6),
                'vol_ann':    round(float(vol_ann_iv), 6),
                'sharpe':     round(float(sharpe_iv),  6),
            },
            'rf_ann': rf_ann,
            'saved':  saved,
        })

    except Exception as _e:
        import traceback as _tb
        _gc.collect()
        return jsonify({'status': 'error', 'message': str(_e) + '\n' + _tb.format_exc()}), 500


# ─────────────────────────────────────────────────────────────────────────────
# CARD 4.2 — Risk Parity Walk-Forward Backtest
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/api/rp-backtest')
def rp_backtest():
    """
    Walk-forward Risk Parity backtest (regime-agnostic covariance).

    At each rebalancing date T:
      1. Estimates Σ from all data in [T − window_months, T].
      2. Computes ERC weights (same SLSQP as Card 3.6).
      3. Applies weights until next rebalancing date.
      4. Compares against Equal-Weight benchmark.

    Query params:
        window_months  – rolling covariance window in months (default 24)
        rebal_months   – rebalancing frequency in months    (default 1)
    """
    import gc as _gc
    from scipy.optimize import minimize as _minimize_rpbt
    try:
        import pandas as _pd
        import numpy  as _np
        from dateutil.relativedelta import relativedelta

        # ERC runs on 6 assets (same as Card 3.6); Corp 2Y is the CAL blending asset
        ASSETS_ERC = ["SPTR5BNK Index","DJITR Index","SPXT Index",
                      "XAUUSD Curncy","XAGUSD Curncy","LT09TRUU Index"]
        CORP2Y_KEY = "LD12TRUU Index"
        ASSETS_ALL = ASSETS_ERC + [CORP2Y_KEY]
        n = len(ASSETS_ERC)

        window_months     = max(1, int(request.args.get('window_months', 24)))
        rebal_months      = max(1, int(request.args.get('rebal_months',  1)))
        _vt_raw           = request.args.get('vol_target', None)
        vol_target_custom = float(_vt_raw) if _vt_raw else None

        # ── Load data ─────────────────────────────────────────────────────────
        prices_all = _pd.read_csv(
            os.path.join(DATA_DIR, "01_PriceData_provided.csv"),
            parse_dates=["Dates"]
        ).sort_values("Dates").set_index("Dates")[ASSETS_ALL].dropna(how='all')

        prices_erc  = prices_all[ASSETS_ERC]
        prices_c2y  = prices_all[CORP2Y_KEY]

        # Corp 2Y daily log-return → used as rf substitute in CAL blending
        c2y_logret  = _np.log(prices_c2y / prices_c2y.shift(1)).dropna()

        def get_rf_daily(dt):
            idx = c2y_logret.index.searchsorted(dt, side='right') - 1
            return float(c2y_logret.iloc[idx]) if idx >= 0 else 0.0

        log_ret  = _np.log(prices_erc / prices_erc.shift(1)).dropna(how='all')

        # ── ERC via log-barrier L-BFGS-B (fast, warm-started) ──────────────────
        # min  0.5·w'Σw − Σ log(wᵢ)  → KKT: (Σw)ᵢ = 1/wᵢ → equal RC
        def _erc(Sigma, w_warm=None):
            if w_warm is not None and w_warm.min() > 1e-8:
                w0 = w_warm.copy()
            else:
                vols = _np.sqrt(_np.diag(Sigma))
                vols = _np.where(vols < 1e-12, 1e-12, vols)
                w0   = (1.0 / vols) / (1.0 / vols).sum()

            def _obj(w):
                return 0.5 * float(w @ Sigma @ w) - float(_np.sum(_np.log(w)))
            def _grad(w):
                return Sigma @ w - 1.0 / w

            res = _minimize_rpbt(_obj, w0, method='L-BFGS-B', jac=_grad,
                                 bounds=[(1e-8, None)] * n,
                                 options={'ftol': 1e-14, 'maxiter': 300, 'gtol': 1e-7})
            w_out = _np.abs(res.x)
            w_out /= w_out.sum()
            return w_out

        # ── Generate rebalancing dates ────────────────────────────────────────
        all_dates   = log_ret.index
        first_date  = all_dates[0]
        last_date   = all_dates[-1]
        warmup_end  = first_date + relativedelta(months=window_months)
        warnings_out = []

        rebal_dates = []
        cur = first_date
        while cur <= last_date:
            future = all_dates[all_dates >= cur]
            if len(future): rebal_dates.append(future[0])
            cur += relativedelta(months=rebal_months)

        if not rebal_dates:
            return jsonify({'status':'error','message':'No rebalancing dates generated.'}), 400

        # ── CAL scaling helper for ERC ─────────────────────────────────────────
        # ERC gives a risky portfolio with some vol σ_erc.
        # If σ_erc > target → scale down + put rest in risk-free (CAL below tangent).
        # If σ_erc ≤ target → hold full ERC (already within vol budget).
        def _erc_cal_scale(w_erc, Sigma, tv_pct):
            tv_d     = (tv_pct / 100.0) / _np.sqrt(252)
            vol_erc  = float(_np.sqrt(max(float(w_erc @ Sigma @ w_erc), 0.0)))
            if vol_erc > tv_d and vol_erc > 1e-12:
                scale   = tv_d / vol_erc
                return w_erc * scale, 1.0 - scale   # (w_risky, rf_frac)
            return w_erc.copy(), 0.0

        # ── Vol target — single user-specified value, default 5 % ───────────
        TARGET_VOL     = vol_target_custom if vol_target_custom else 5.0
        strat_vol_keys = ['vol_erc', 'mv_vol']

        # ── Max-Return within vol budget (Mean Variance) ──────────────────────
        def _optimize_max_ret_rp(mu, Sigma, tv_pct, w_warm=None):
            tv_d   = (tv_pct / 100.0) / _np.sqrt(252)
            w0     = w_warm if w_warm is not None else _np.ones(n) / n
            bounds = [(0.0, 1.0)] * n
            cons   = [
                {'type': 'eq',   'fun': lambda w: w.sum() - 1.0},
                {'type': 'ineq', 'fun': lambda w, S=Sigma: tv_d**2 - float(w @ S @ w)},
            ]
            res = _minimize_rpbt(lambda w: -float(w @ mu), w0, method='SLSQP',
                                 bounds=bounds, constraints=cons,
                                 options={'ftol': 1e-10, 'maxiter': 300})
            w_out = _np.clip(res.x, 0.0, 1.0)
            s = w_out.sum()
            return w_out / s if s > 1e-6 else w0

        # ── Walk-forward loop ─────────────────────────────────────────────────
        w_eq        = _np.ones(n) / n
        all_keys    = strat_vol_keys + ['riskParity', 'eqWeight', 'riskFree']
        port_ret    = {k: [] for k in all_keys}
        port_dates  = []
        warmup_skipped = 0
        data_skip      = 0

        cur_w_erc   = w_eq.copy()
        cur_w_mv    = w_eq.copy()
        cur_w_vol   = {'vol_erc': w_eq.copy(), 'mv_vol': w_eq.copy()}
        cur_rf_frac = {'vol_erc': 0.0,         'mv_vol': 0.0}

        for ri, rebal_dt in enumerate(rebal_dates):
            next_dt = rebal_dates[ri+1] if ri+1 < len(rebal_dates) else last_date

            if rebal_dt < warmup_end:
                warmup_skipped += 1
                continue

            window_start = rebal_dt - relativedelta(months=window_months)
            w_mask  = (log_ret.index > window_start) & (log_ret.index <= rebal_dt)
            r_sub   = log_ret.loc[w_mask].dropna()

            if r_sub.shape[0] >= n + 1:
                try:
                    mu_r      = _np.array(r_sub.mean())
                    Sigma_r   = _np.array(r_sub.cov())
                    cur_w_erc = _erc(Sigma_r, cur_w_erc)
                    cur_w_vol['vol_erc'], cur_rf_frac['vol_erc'] = _erc_cal_scale(cur_w_erc, Sigma_r, TARGET_VOL)
                    cur_w_mv  = _optimize_max_ret_rp(mu_r, Sigma_r, TARGET_VOL, cur_w_mv)
                except Exception:
                    pass  # keep previous weights
            else:
                data_skip += 1

            hold_mask = (log_ret.index > rebal_dt) & (log_ret.index <= next_dt)
            hold      = log_ret.loc[hold_mask].fillna(0.0)
            for day_dt, row in hold.iterrows():
                r_arr  = _np.array(row.values, dtype=float)
                rf_day = get_rf_daily(day_dt)
                port_ret['vol_erc'].append(float(cur_w_vol['vol_erc'] @ r_arr) + cur_rf_frac['vol_erc'] * rf_day)
                port_ret['mv_vol'].append(float(cur_w_mv @ r_arr))
                port_ret['riskParity'].append(float(cur_w_erc @ r_arr))
                port_ret['eqWeight'].append(float(w_eq @ r_arr))
                port_ret['riskFree'].append(rf_day)
                port_dates.append(str(day_dt.date()))

            _gc.collect()

        if warmup_skipped:
            warnings_out.append(
                f'{warmup_skipped} rebalancing period(s) skipped: '
                f'need {window_months} months for first covariance estimate. '
                f'Simulation starts {warmup_end.date()}.'
            )
        if data_skip:
            warnings_out.append(
                f'{data_skip} rebalancing date(s) lacked sufficient window data; previous weights held.'
            )
        if not port_dates:
            return jsonify({'status':'error','message':
                f'No data produced. Try reducing window_months (currently {window_months}).'}), 400

        # ── Cumulative returns ────────────────────────────────────────────────
        def cumret(daily):
            cum = 0.0; out = []
            for r in daily: cum += r; out.append(round(cum*100, 4))
            return out

        rf_list = port_ret['riskFree']
        rf_arr  = _np.array(rf_list)

        series_out = {k: cumret(port_ret[k]) for k in all_keys}
        excess_out = {k: cumret((_np.array(port_ret[k]) - rf_arr).tolist())
                      for k in all_keys if k != 'riskFree'}

        # ── Summary stats ─────────────────────────────────────────────────────
        def _stats(daily, rf_d):
            arr   = _np.array(daily)
            rfa   = _np.array(rf_d)
            ex    = arr - rfa
            mu_d  = float(arr.mean())
            sd_d  = float(arr.std(ddof=1)) if arr.std(ddof=1) > 1e-12 else 1e-12
            ex_sd = float(ex.std(ddof=1))  if ex.std(ddof=1)  > 1e-12 else 1e-12
            ann_ret = mu_d * 252
            ann_vol = sd_d * _np.sqrt(252)
            sharpe  = float(ex.mean()) / ex_sd * _np.sqrt(252)
            down    = ex[ex < 0]
            ds_std  = float(down.std(ddof=1)) if len(down) > 1 else 1e-12
            sortino = float(ex.mean()) * _np.sqrt(252) / ds_std
            cum_c   = _np.cumsum(arr)
            maxdd   = float((cum_c - _np.maximum.accumulate(cum_c)).min()) * 100.0
            calmar  = (ann_ret * 100) / abs(maxdd) if abs(maxdd) > 1e-6 else 0.0
            return {
                'Ann. Return (%)':  round(ann_ret*100, 4),
                'Ann. Vol (%)':     round(ann_vol*100, 4),
                'Sharpe':           round(sharpe,  4),
                'Sortino':          round(sortino, 4),
                'Max DD (%)':       round(maxdd,   4),
                'Calmar':           round(calmar,  4),
                'Total Return (%)': round(float(arr.sum())*100, 4),
            }

        summary = {k: _stats(port_ret[k], rf_list)
                   for k in all_keys if k != 'riskFree'}

        # ── Alpha vs Equal-Weight ─────────────────────────────────────────────
        eq_np  = _np.array(port_ret['eqWeight'])
        eq_var = float(eq_np.var(ddof=1)) if eq_np.var(ddof=1) > 1e-12 else 1e-12

        alpha_ew      = {}
        jensens_alpha = {}
        cum_alpha     = {}
        for vk in strat_vol_keys + ['riskParity']:
            s_np = _np.array(port_ret[vk])
            alpha_ew[vk] = round(float((s_np - eq_np).mean()) * 252 * 100, 4)
            beta = float(_np.cov(s_np, eq_np)[0,1] / eq_var)
            jensens_alpha[vk] = round(
                (float(s_np.mean()) - float(rf_arr.mean())
                 - beta * (float(eq_np.mean()) - float(rf_arr.mean()))) * 252 * 100, 4
            )
            cum_alpha[vk] = cumret((s_np - eq_np).tolist())

        # ── 12-month rolling Sharpe ───────────────────────────────────────────
        roll_win = 252
        def _rolling_sh(daily, rf_d):
            result = [None] * len(daily)
            arr = _np.array(daily) - _np.array(rf_d)
            for i in range(roll_win, len(arr) + 1):
                w  = arr[i-roll_win:i]
                sd = float(w.std(ddof=1))
                result[i-1] = round(float(w.mean()) / sd * _np.sqrt(252), 4) if sd > 1e-12 else 0.0
            return result

        rolling_sharpe = {k: _rolling_sh(port_ret[k], rf_list)
                          for k in all_keys if k != 'riskFree'}

        strat_labels = {
            'vol_erc':    f'ERC + CAL Vol{TARGET_VOL:.1f}%',
            'mv_vol':     f'Mean Variance Vol{TARGET_VOL:.1f}%',
            'riskParity': 'Risk Parity (ERC)',
            'eqWeight':   'Equal Weight',
            'riskFree':   'Risk-Free',
        }

        # Regime spans for chart background
        REGIME_DATA_STATIC = [
            ["Downturn","2001-06-01","2001-10-31"],["Goldilocks","2001-11-01","2004-05-31"],
            ["Downturn","2004-06-01","2006-08-31"],["Goldilocks","2006-09-01","2007-09-30"],
            ["Downturn","2007-10-01","2008-10-31"],["Stagflation","2008-11-01","2009-09-30"],
            ["Goldilocks","2009-10-01","2011-03-31"],["Downturn","2011-04-01","2012-02-29"],
            ["Goldilocks","2012-03-01","2019-10-31"],["Downturn","2019-11-01","2020-03-31"],
            ["Stagflation","2020-04-01","2020-05-31"],["Goldilocks","2020-06-01","2021-03-31"],
            ["Overheating","2021-04-01","2022-10-31"],["Downturn","2022-11-01","2024-04-30"],
            ["Goldilocks","2024-05-01","2025-08-31"],
        ]
        rcsv = os.path.join(DATA_DIR, '02_RegimeDates_output.csv')
        try:
            regime_spans = _pd.read_csv(rcsv)[["Regime","Start_Date","End_Date"]].to_dict('records') \
                if os.path.exists(rcsv) else \
                [{"Regime":r[0],"Start_Date":r[1],"End_Date":r[2]} for r in REGIME_DATA_STATIC]
        except Exception:
            regime_spans = [{"Regime":r[0],"Start_Date":r[1],"End_Date":r[2]} for r in REGIME_DATA_STATIC]

        _gc.collect()
        return jsonify({
            'status':         'success',
            'window_months':  window_months,
            'rebal_months':   rebal_months,
            'dates':          port_dates,
            'strat_keys':     all_keys,
            'strat_labels':   strat_labels,
            'series':         series_out,
            'excess':         excess_out,
            'rolling_sharpe': rolling_sharpe,
            'cum_alpha':      cum_alpha,
            'alpha_ew':       alpha_ew,
            'jensens_alpha':  jensens_alpha,
            'summary':        summary,
            'warnings':       warnings_out,
            'n_rebal':        len(rebal_dates),
            'warmup_skipped': warmup_skipped,
            'regime_spans':   regime_spans,
        })

    except Exception as _e:
        import traceback as _tb
        _gc.collect()
        return jsonify({'status':'error','message': str(_e)+'\n'+_tb.format_exc()}), 500


if __name__ == '__main__':
    app.run(debug=True, use_reloader=True, port=5050, host='0.0.0.0', threaded=True)

