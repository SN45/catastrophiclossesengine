# backend/transform_forecast.py
import os, io, json
from collections import defaultdict
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---- CONFIG ----
BUCKET = os.environ.get("BUCKET", "catasloss-sujan-us")
REGION = os.environ.get("AWS_REGION", "us-east-2")

# --- Simple calibration / guardrails (tune as needed) ---
CAL = {
    "k_wind": 0.0010,         # wind sensitivity
    "k_flood": 0.0005,        # flood sensitivity
    "step_cap_share": 0.0002, # max loss per step = 0.02% of TIV_home
    "wind_norm": 30.0,        # normalize wind m/s (≈ gale/TS)
    "rain_norm": 75.0,        # normalize rain mm per step
}

s3 = boto3.client("s3", region_name=REGION)

# ---------- S3 helpers ----------
def _list_common_prefixes(prefix: str, delimiter: str = "/"):
    prefixes = []
    token = None
    while True:
        kwargs = {"Bucket": BUCKET, "Prefix": prefix, "Delimiter": delimiter}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        prefixes.extend([p["Prefix"] for p in resp.get("CommonPrefixes", [])])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return prefixes

def latest_run(prefix="raw/owm_forecast/"):
    runs = _list_common_prefixes(prefix, "/")
    runs = [r for r in runs if "run_dt=" in r]
    if not runs:
        raise SystemExit(f"[transform] No runs under s3://{BUCKET}/{prefix}")
    runs.sort()
    latest = runs[-1]
    print("[transform] using run:", latest)
    return latest

def read_parquet_s3(key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

def write_parquet_s3(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.read())

def write_json_s3(obj, key: str):
    body = json.dumps(obj, default=str).encode("utf-8")
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

# ---------- small utils ----------
def nn(lat, lon, grid):
    best = None; bd = 1e18
    for (a, b) in grid:
        d = (a - lat) * (a - lat) + (b - lon) * (b - lon)
        if d < bd:
            bd = d
            best = (a, b)
    return best

# ---------- main ----------
def main():
    # 1) Locate latest raw forecast run and collect all json files
    run_prefix = latest_run("raw/owm_forecast/")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=run_prefix)
    keys = [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]
    token = resp.get("NextContinuationToken")
    while token:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=run_prefix, ContinuationToken=token)
        keys.extend([o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")])
        token = resp.get("NextContinuationToken")
    if not keys:
        raise SystemExit("[transform] No forecast json objects found.")

    # Build grid from filenames we actually have
    grid_set = set()
    for k in keys:
        fn = k.split("/")[-1]
        lat = float(fn.split("lat=")[1].split("_")[0])
        lon = float(fn.split("lon=")[1].split(".json")[0])
        grid_set.add((lat, lon))
    GRID = sorted(grid_set)
    print(f"[transform] raw cells: {len(GRID)}")

    # 2) Read reference data
    nri = read_parquet_s3("ref/nri/nri_tracts.parquet")
    book = read_parquet_s3("ref/book/book_exposure.parquet")
    nri["geoid"] = nri["geoid"].astype(str)
    book["geoid"] = book["geoid"].astype(str)
    print(f"[transform] NRI tracts: {len(nri)}  book rows: {len(book)}")
    if len(nri) == 0 or len(book) == 0:
        raise SystemExit("[transform] Empty NRI or book; aborting.")

    # 3) Load raw forecast into cell time-series
    cells = defaultdict(list)   # (lat,lon) -> [{dt, wind_ms, rain_mm}, ...]
    for key in keys:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        js = json.loads(body)
        fn = key.split("/")[-1]
        lat = float(fn.split("lat=")[1].split("_")[0])
        lon = float(fn.split("lon=")[1].split(".json")[0])
        for itm in js.get("list", []):
            ts = datetime.utcfromtimestamp(int(itm.get("dt", 0)))
            wind = float(itm.get("wind", {}).get("speed", 0.0))
            rain = float(itm.get("rain", {}).get("3h", 0.0))  # 3-hour accum
            cells[(lat, lon)].append({"dt": ts, "wind_ms": wind, "rain_mm": rain})
    cells = {k: v for k, v in cells.items() if v}
    if not cells:
        raise SystemExit("[transform] No cell time-series parsed.")
    print(f"[transform] populated cells: {len(cells)}")

    # 4) Map tracts to nearest grid cell, explode hazards to tract×time rows
    rows = []
    nri_slim = nri[["geoid", "state", "county", "centroid_lat", "centroid_lon", "eal_total"]].copy()
    for r in nri_slim.itertuples(index=False):
        glat, glon = nn(float(r.centroid_lat), float(r.centroid_lon), GRID)
        series = cells.get((glat, glon), [])
        for s in series:
            rows.append({
                "geoid": r.geoid,
                "dt": s["dt"],
                "wind_ms": s["wind_ms"],
                "rain_mm": s["rain_mm"],
            })
    haz = pd.DataFrame(rows)
    if haz.empty:
        raise SystemExit("[transform] No hazards computed; check raw ingest / tract mapping.")
    haz["dt"] = pd.to_datetime(haz["dt"], utc=True)
    print(f"[transform] haz rows: {len(haz)}")

    # 5) Merge refs + book, compute intensities and expected loss (calibrated + capped)
    df = (haz
          .merge(nri_slim[["geoid", "state", "county", "eal_total"]], on="geoid", how="left")
          .merge(book[["geoid", "tiv_home"]], on="geoid", how="left"))
    df["tiv_home"] = pd.to_numeric(df["tiv_home"], errors="coerce").fillna(0.0).clip(lower=0)
    df["eal_total"] = pd.to_numeric(df["eal_total"], errors="coerce").fillna(0.0).clip(lower=0)

    max_eal = float(df["eal_total"].max()) if len(df) else 0.0
    vuln = (df["eal_total"] / (max_eal or 1.0)).fillna(0.2)        # 0..1-ish
    base_vuln = (0.02 + 0.3 * vuln).clip(0.02, 0.5)                # guardrails

    wind_ms = pd.to_numeric(df["wind_ms"], errors="coerce").fillna(0)
    rain_mm = pd.to_numeric(df["rain_mm"], errors="coerce").fillna(0)
    df["wind_intensity"]  = (wind_ms / CAL["wind_norm"]).clip(0, 1.5)
    df["flood_intensity"] = (rain_mm / CAL["rain_norm"]).clip(0, 1.5)

    raw_step = df["tiv_home"] * (
        CAL["k_wind"]  * df["wind_intensity"]  * base_vuln +
        CAL["k_flood"] * df["flood_intensity"] * base_vuln
    )
    step_cap = CAL["step_cap_share"] * df["tiv_home"]   # e.g., 0.02% of TIV per step
    df["el_total"] = raw_step.clip(lower=0, upper=step_cap).fillna(0.0)

    # 6) Aggregate to tract×time; then compute bands/top
    agg = df.groupby(["geoid", "dt"], as_index=False)[["el_total"]].sum()
    print(f"[transform] agg (tract×time) rows: {len(agg)}")

    bands = (agg.groupby("geoid")["el_total"]
                .agg(p50=lambda s: float(s.quantile(0.5)),
                     p90=lambda s: float(s.quantile(0.9)))
                .reset_index())
    bands = bands.merge(nri_slim[["geoid", "state"]], on="geoid", how="left")

    tot = agg.groupby("geoid", as_index=False)["el_total"].sum().rename(columns={"el_total": "el_total_sum"})
    top = (tot.merge(nri_slim[["geoid", "state", "county"]], on="geoid", how="left")
              .sort_values("el_total_sum", ascending=False)
              .head(1000))

    # 7) County rollup
    nri2 = nri_slim.copy()
    nri2["geoid"] = nri2["geoid"].astype(str)
    nri2["COUNTYFIPS"] = nri2["geoid"].str[:5]
    t2c = nri2.set_index("geoid")[["COUNTYFIPS", "county", "state"]]

    agg2 = agg.merge(t2c, left_on="geoid", right_index=True, how="left")
    county_sum = agg2.groupby("COUNTYFIPS", as_index=False).agg(el_total_sum=("el_total", "sum"))
    county_time = agg2.groupby(["COUNTYFIPS", "dt"], as_index=False)["el_total"].sum()
    percs = county_time.groupby("COUNTYFIPS")["el_total"].agg(
        p50=lambda s: float(s.quantile(0.5)),
        p90=lambda s: float(s.quantile(0.9)),
    ).reset_index()

    county_meta = t2c.reset_index().drop_duplicates("COUNTYFIPS").set_index("COUNTYFIPS")
    counties = (county_sum.set_index("COUNTYFIPS")
                .join(percs.set_index("COUNTYFIPS"))
                .join(county_meta[["county", "state"]], how="left")
                .reset_index())

    print(f"[transform][debug] counties rows: {len(counties)}")

    run_id = run_prefix.split("run_dt=")[1].strip("/")

    # parquet
    write_parquet_s3(agg, f"proc/losses/run_dt={run_id}/by_tract.parquet")

    # JSON: bands
    bands_json = {
        "run": run_id,
        "bands": [
            {"geoid": str(r.geoid), "state": str(r.state or ""),
             "p50": float(r.p50), "p90": float(r.p90)}
            for r in bands.itertuples(index=False)
        ]
    }
    write_json_s3(bands_json, f"proc/losses/run_dt={run_id}/bands.json")

    # JSON: top tracts
    top_json = {
        "run": run_id,
        "top": [
            {"geoid": str(r.geoid), "state": str(r.state or ""), "county": str(r.county or ""),
             "el_total_sum": float(r.el_total_sum)}
            for r in top.itertuples(index=False)
        ]
    }
    write_json_s3(top_json, f"proc/losses/run_dt={run_id}/top.json")

    # JSON: counties list
    counties_out = []
    for r in counties.itertuples(index=False):
        fips = str(r.COUNTYFIPS).zfill(5)
        counties_out.append({
            "fips": fips,
            "name": str(r.county or ""),
            "state": str(r.state or ""),
            "p50": float((r.p50 if r.p50 is not None else 0.0)),
            "p90": float((r.p90 if r.p90 is not None else 0.0)),
            "el_total_sum": float((r.el_total_sum if r.el_total_sum is not None else 0.0)),
        })
    write_json_s3({"run": run_id, "counties": counties_out}, f"proc/losses/run_dt={run_id}/counties.json")
    print(f"[transform] wrote counties.json with {len(counties_out)} counties")

    # JSON: per-county timeseries
    out_base = f"proc/losses/run_dt={run_id}/timeseries/"
    for fips, grp in county_time.groupby("COUNTYFIPS"):
        series = [{"dt": str(pd.to_datetime(r.dt).tz_convert("UTC")),
                   "el_total": float(r.el_total)} for r in grp.sort_values("dt").itertuples(index=False)]
        write_json_s3({"fips": str(fips).zfill(5), "series": series}, out_base + f"county_{str(fips).zfill(5)}.json")

    print(f"[transform] wrote proc/losses for run {run_id} (tract bands, top, counties, county timeseries).")

if __name__ == "__main__":
    main()
