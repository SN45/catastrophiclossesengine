# data/make_book.py
import os
import numpy as np, pandas as pd
from pathlib import Path

# where your prepared NRI parquet is
NRI_PARQUET = Path("data/nri_tracts.parquet")
OUT = Path("data/book_exposure.parquet")

# Target statewide residential TIV for TX (tuneable)
# You can override via env: TARGET_HOME_TIV=900000000000  (=$900B)
TARGET_HOME_TIV = float(os.environ.get("TARGET_HOME_TIV", 8.0e11))  # $800B

def main():
    if not NRI_PARQUET.exists():
        print(f"[make_book] NRI file not found at {NRI_PARQUET}")
        return

    nri = pd.read_parquet(NRI_PARQUET)

    # Base shape: use NRI EAL_total as weight (non-zero, stable)
    w = nri["eal_total"].fillna(0).astype(float)
    if w.max() == 0:
        # fallback uniform weights
        w = pd.Series(1.0, index=nri.index)

    w = w.replace(0, w[w > 0].min())  # avoid zeros
    w = w / w.sum()

    # Start with a proportional residential exposure by tract
    tiv_home_raw = TARGET_HOME_TIV * w.values

    # Add very small noise so every tract isn't perfectly proportional
    rng = np.random.default_rng(42)
    noise = rng.normal(loc=1.0, scale=0.05, size=len(nri))  # ±5%
    tiv_home = np.maximum(0, tiv_home_raw * noise)

    # Build the synthetic book (home line only; others optional)
    book = pd.DataFrame({
        "geoid": nri["geoid"].astype(str),
        "tiv_home": tiv_home.round(0)
    })

    # Sanity print
    print("[make_book] rows:", len(book))
    print("[make_book] sum(tiv_home) ≈ $", f"{book['tiv_home'].sum():,.0f}")
    print("[make_book] median tract tiv_home ≈ $", f"{book['tiv_home'].median():,.0f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    book.to_parquet(OUT, index=False)
    print(f"[make_book] wrote {OUT}")

if __name__ == "__main__":
    main()
