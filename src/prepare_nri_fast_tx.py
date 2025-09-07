# data/prepare_nri_fast_tx.py
# TX-only extractor for FEMA NRI (FileGDB). Filters by STATEABBRV / STATEFIPS / STCOFIPS,
# builds tract geoid from TRACTFIPS (or synthesizes), computes eal_* from *_EALT,
# writes data/nri_tracts.parquet.

import os, sys, pathlib, re
import numpy as np
import pandas as pd
import geopandas as gpd
from pyogrio import read_dataframe, list_layers

GDB_PATH = "/Volumes/Ext_ssd/project/catas/data/NRI_GDB_CensusTracts_unzipped/NRI_GDB_CensusTracts.gdb"  # <- update if needed
LAYER = "NRI_CensusTracts"
OUT_PARQUET = pathlib.Path("data/nri_tracts.parquet")

os.environ["OGR_ORGANIZE_POLYGONS"] = "SKIP"

def pick(cols, *cands):
    d = {c.lower(): c for c in cols}
    for k in cands:
        if k.lower() in d: return d[k.lower()]
    return None

def normalize_layer_names(layers_obj):
    if isinstance(layers_obj, np.ndarray):
        return [str(x[0]) if isinstance(x, (list, tuple, np.ndarray)) else str(x) for x in layers_obj.tolist()]
    out = []
    for x in layers_obj:
        out.append(str(x[0]) if isinstance(x, (list, tuple)) and x else str(x))
    return out

def main():
    # Ensure layer exists
    names = normalize_layer_names(list_layers(GDB_PATH))
    print("[prepare_nri] available layers:", names)
    if LAYER not in names:
        print(f"[prepare_nri] Layer '{LAYER}' not found.")
        sys.exit(1)

    # Small sample for field names
    sample = read_dataframe(GDB_PATH, layer=LAYER, max_features=5)
    scols = list(sample.columns)

    # Read full layer (avoid FileGDB where-syntax issues)
    print("[prepare_nri] reading full layer; will filter to Texas using state fields ...")
    gdf = read_dataframe(GDB_PATH, layer=LAYER, open_options={"OGR_ORGANIZE_POLYGONS": "SKIP"})
    cols = list(gdf.columns)

    # Identify relevant fields
    STATEABBRV = pick(cols, "STATEABBRV", "STUSPS", "STATE", "ST_ABBR")
    STATEFIPS  = pick(cols, "STATEFIPS")
    STCOFIPS   = pick(cols, "STCOFIPS")      # 5-digit state+county
    COUNTYFIPS = pick(cols, "COUNTYFIPS")    # 3-digit county only
    COUNTY     = pick(cols, "COUNTY", "COUNTY_NAME", "NAMELSAD", "COUNTYNAME")
    TRACTFIPS  = pick(cols, "TRACTFIPS", "TRACT_GEOID", "GEOID10", "GEOID", "GISJOIN")

    # Robust TX filter (prefer STATEABBRV, else STATEFIPS, else STCOFIPS)
    if STATEABBRV and STATEABBRV in gdf.columns:
        gdf = gdf[gdf[STATEABBRV].astype(str).str.upper() == "TX"]
    elif STATEFIPS and STATEFIPS in gdf.columns:
        gdf = gdf[gdf[STATEFIPS].astype(str).str.zfill(2) == "48"]
    elif STCOFIPS and STCOFIPS in gdf.columns:
        gdf[STCOFIPS] = gdf[STCOFIPS].astype(str).str.zfill(5)
        gdf = gdf[gdf[STCOFIPS].str.startswith("48")]
    else:
        # Last resort: combine STATEFIPS + COUNTYFIPS if both exist
        if STATEFIPS and COUNTYFIPS:
            sf = gdf[STATEFIPS].astype(str).str.zfill(2)
            cf = gdf[COUNTYFIPS].astype(str).str.zfill(3)
            gdf = gdf[(sf + cf).str.startswith("48")]
        else:
            print("[prepare_nri] ERROR: No usable state field (STATEABBRV/STATEFIPS/STCOFIPS).")
            print("[prepare_nri] Columns:", cols[:80])
            sys.exit(1)

    print(f"[prepare_nri] TX features after filter: {len(gdf)}")
    if len(gdf) == 0:
        print("[prepare_nri] 0 rows after filter. Columns:", cols[:80])
        sys.exit(1)

    # Build geoid
    if TRACTFIPS and TRACTFIPS in gdf.columns:
        geoid = gdf[TRACTFIPS].astype(str).str.zfill(11)
    else:
        # synthesize from STCOFIPS + tract code if present
        TRACTCE = pick(cols, "TRACT", "TRACTCE10", "TRACTCE")
        if STCOFIPS and TRACTCE and STCOFIPS in gdf.columns and TRACTCE in gdf.columns:
            geoid = gdf[STCOFIPS].astype(str).str.zfill(5) + gdf[TRACTCE].astype(str).str.zfill(6)
        else:
            geoid = (gdf[STCOFIPS].astype(str).str.zfill(5) if STCOFIPS else "48") + gdf.index.astype(str).str.zfill(6)
    geoid = geoid.astype(str).str[:11]

    # Compute EALs
    ealt_cols = [c for c in cols if re.match(r".*_EALT$", str(c))]
    if ealt_cols:
        eal_total = gdf[ealt_cols].sum(axis=1, numeric_only=True)
        wind_codes  = ("HRCN", "TORN", "HAIL", "SWND", "LTNG", "ICEST", "HWAV")
        flood_codes = ("RFLD", "CFLD", "STORM", "TSUN")
        eal_wind = gdf[[c for c in ealt_cols if any(code in c for code in wind_codes)]].sum(axis=1, numeric_only=True)
        eal_flood = gdf[[c for c in ealt_cols if any(code in c for code in flood_codes)]].sum(axis=1, numeric_only=True)
        if eal_wind.isna().all() or (eal_wind == 0).all(): eal_wind = eal_total * 0.4
        if eal_flood.isna().all() or (eal_flood == 0).all(): eal_flood = eal_total * 0.3
    else:
        gdf_proj = gdf.to_crs(3857)
        area_norm = (gdf_proj.area / gdf_proj.area.max()).fillna(0)
        eal_total = area_norm * 1e6
        eal_wind  = eal_total * 0.4
        eal_flood = eal_total * 0.3

    # Centroids (projected → WGS84)
    cent_ll = gdf.to_crs(3857).centroid.to_crs(4326)

    out = pd.DataFrame({
        "geoid": geoid,
        "state": "TX",
        "county": gdf[COUNTY] if COUNTY else "",
        "eal_total": pd.to_numeric(eal_total, errors="coerce").fillna(0),
        "eal_wind":  pd.to_numeric(eal_wind,  errors="coerce").fillna(0),
        "eal_flood": pd.to_numeric(eal_flood, errors="coerce").fillna(0),
        "centroid_lat": cent_ll.y.values,
        "centroid_lon": cent_ll.x.values,
    })

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT_PARQUET, index=False)
    print(f"[prepare_nri] wrote {OUT_PARQUET} rows={len(out)}")

if __name__ == "__main__":
    main()
