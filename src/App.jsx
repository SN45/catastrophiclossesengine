// src/App.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import axios from "axios";
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup,
} from "react-simple-maps";
import { geoCentroid } from "d3-geo";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  Label,
} from "recharts";

const API = import.meta.env.VITE_API_URL;
const US_COUNTIES_TOPO = "https://unpkg.com/us-atlas@3/counties-10m.json";

/* ---------- helpers ---------- */
const fmtCurrencyShort = (n) => {
  const v = Number(n || 0);
  if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(2)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(1)}K`;
  return `$${Math.round(v).toLocaleString()}`;
};
const fmtMonthYY = (d) => {
  const dt = d instanceof Date ? d : new Date(d);
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const yy = String(dt.getUTCFullYear()).slice(-2);
  return `${mm}/${yy}`;
};
function shade(value, max) {
  const t = Math.max(0, Math.min(1, (value || 0) / (max || 1)));
  const start = [219, 234, 254]; // blue-100
  const end = [30, 64, 175];     // blue-800
  const mix = (a, b) => Math.round(a + (b - a) * t);
  return `rgb(${mix(start[0], end[0])}, ${mix(start[1], end[1])}, ${mix(
    start[2],
    end[2]
  )})`;
}

export default function App() {
  const [countiesMeta, setCountiesMeta] = useState(null); // { run, counties: [...] }
  const [runId, setRunId] = useState(null);               // "run_dt=YYYYmmddTHHMMSSZ"
  const [selected, setSelected] = useState(null);         // { fips, name }
  const [series, setSeries] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  // fixed map (no zoom-on-select)
  const [center] = useState([-99.5, 31.0]);
  const [zoom] = useState(3.8);

  const centroidsRef = useRef(new Map()); // fips -> [lon,lat]

  // search state
  const [q, setQ] = useState("");
  const [openSug, setOpenSug] = useState(false);
  const [activeSug, setActiveSug] = useState(0);

  /* load county metrics + CAPTURE RUN */
  useEffect(() => {
    let mounted = true;
    (async () => {
      try {
        setLoading(true);
        const r = await axios.get(`${API}/loss/counties`, { timeout: 20000 });
        if (!mounted) return;
        setCountiesMeta(r.data);
        setRunId(r.data?.run || null);   // <— capture the run used by the list
      } catch (e) {
        console.error(e);
        setError("Failed to load /loss/counties");
      } finally {
        setLoading(false);
      }
    })();
    return () => { mounted = false; };
  }, []);

  const countyIndex = useMemo(() => {
    const m = new Map();
    if (countiesMeta?.counties) for (const c of countiesMeta.counties) m.set(c.fips, c);
    return m;
  }, [countiesMeta]);

  const maxSum = useMemo(() => {
    if (!countiesMeta?.counties?.length) return 1;
    return Math.max(...countiesMeta.counties.map((c) => c.el_total_sum || 0), 1);
  }, [countiesMeta]);

  // Top list sorted by 5-day EL descending
  const topSorted = useMemo(() => {
    let arr = countiesMeta?.counties ? [...countiesMeta.counties] : [];
    arr.sort((a, b) => (b.el_total_sum || 0) - (a.el_total_sum || 0));
    return arr;
  }, [countiesMeta]);

  // Search suggestions: top 3 by EL among name matches
  const suggestions = useMemo(() => {
    if (!q.trim() || !countiesMeta?.counties) return [];
    const t = q.trim().toLowerCase();
    return [...countiesMeta.counties]
      .filter((c) => (c.name || "").toLowerCase().includes(t))
      .sort((a, b) => (b.el_total_sum || 0) - (a.el_total_sum || 0))
      .slice(0, 3);
  }, [q, countiesMeta]);

  /* load a county timeseries using the SAME run as the list */
  async function loadCounty(fips, name) {
    setSelected({ fips, name });
    setSeries(null);
    setError("");
    try {
      const q = runId ? `&run=${encodeURIComponent(runId)}` : "";
      const r = await axios.get(`${API}/loss/county?fips=${fips}${q}`, { timeout: 20000 });
      let cum = 0;
      const data = (r.data.series || []).map((s) => {
        const step = Number(s.el_total) || 0;
        cum += step;
        return { dt: new Date(s.dt), el_total: step, cum };
      });
      setSeries(data);
    } catch (e) {
      console.error(e);
      setError(`No timeseries for ${name} (${fips})`);
    }
  }

  function selectWithoutZoom(fips, fallbackName) {
    const meta = countyIndex.get(fips);
    loadCounty(fips, meta?.name || fallbackName || fips);
  }

  // search events
  function onSearchKeyDown(e) {
    if (!openSug && (e.key === "ArrowDown" || e.key === "Enter")) {
      setOpenSug(true);
      return;
    }
    if (openSug && suggestions.length) {
      if (e.key === "ArrowDown") setActiveSug((i) => Math.min(i + 1, suggestions.length - 1));
      else if (e.key === "ArrowUp") setActiveSug((i) => Math.max(i - 1, 0));
      else if (e.key === "Enter") {
        const pick = suggestions[activeSug] || suggestions[0];
        if (pick) {
          selectWithoutZoom(pick.fips, pick.name);
          setOpenSug(false);
          e.preventDefault();
        }
      } else if (e.key === "Escape") setOpenSug(false);
    }
  }

  return (
    <div className="min-h-screen bg-white text-slate-900">
      <header className="px-6 py-5 border-b border-slate-200 bg-white sticky top-0 z-10">
        <div className="flex items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div>
              <h1 className="text-3xl md:text-4xl font-semibold tracking-tight">
                Catastrophe Loss Radar — <span className="text-blue-700">Texas</span>
              </h1>
              <p className="text-slate-500 mt-1">
                Click a county or search to see forecasted <em className="not-italic font-medium">expected losses</em> (home wind + flood).
              </p>
            </div>
            
          </div>

          {/* Search with suggestions */}
          <div className="w-96 relative">
            <input
              value={q}
              onChange={(e) => { setQ(e.target.value); setOpenSug(true); setActiveSug(0); }}
              onKeyDown={onSearchKeyDown}
              onBlur={() => setTimeout(() => setOpenSug(false), 120)}
              placeholder="Search county (e.g., Collin)"
              className="w-full rounded-xl border border-slate-300 px-4 py-2 outline-none focus:ring-2 focus:ring-blue-500"
            />
            {openSug && suggestions.length > 0 && (
              <div className="absolute z-20 mt-1 w-full bg-white border border-slate-200 rounded-xl shadow-lg overflow-hidden">
                {suggestions.map((s, idx) => (
                  <button
                    key={s.fips}
                    className={`w-full text-left px-4 py-2 hover:bg-blue-50 ${idx === activeSug ? "bg-blue-50" : ""}`}
                    onMouseDown={(e) => { e.preventDefault(); selectWithoutZoom(s.fips, s.name); setOpenSug(false); }}
                    title={`County: ${s.name}`}
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <div className="font-medium">{s.name}</div>
                        <div className="text-xs text-slate-500">FIPS {s.fips}</div>
                      </div>
                      <div className="text-blue-700 font-semibold">{fmtCurrencyShort(s.el_total_sum)}</div>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
        </div>
      </header>

      <main className="grid md:grid-cols-5 gap-6 p-6">
        {/* LEFT: map + notes */}
        <section className="md:col-span-3 space-y-4">
          <div className="rounded-2xl bg-white border border-slate-200 p-3 shadow-sm">
            <div className="w-full h-[560px]">
              <ComposableMap
                projection="geoAlbersUsa"
                projectionConfig={{ scale: 1000 }}
                style={{ background: "white", borderRadius: "1rem", width: "100%", height: "100%" }}
              >
                <ZoomableGroup center={center} zoom={zoom} minZoom={3.2} maxZoom={10} transitionDuration={0}>
                  <Geographies geography={US_COUNTIES_TOPO}>
                    {({ geographies }) => {
                      geographies
                        .filter((g) => String(g.id).padStart(5, "0").startsWith("48"))
                        .forEach((g) => {
                          const fips = String(g.id).padStart(5, "0");
                          if (!centroidsRef.current.has(fips)) {
                            centroidsRef.current.set(fips, geoCentroid(g));
                          }
                        });

                      return geographies
                        .filter((geo) => String(geo.id).padStart(5, "0").startsWith("48"))
                        .map((geo) => {
                          const fips = String(geo.id).padStart(5, "0");
                          const meta = countyIndex.get(fips);
                          const name = geo.properties?.NAME || meta?.name || fips;
                          const baseFill = "rgb(243, 244, 246)";
                          const fill = meta ? shade(meta.el_total_sum || 0, maxSum) : baseFill;
                          const isActive = selected?.fips === fips;
                          return (
                            <Geography
                              key={geo.rsmKey}
                              geography={geo}
                              onClick={() => selectWithoutZoom(fips, name)}
                              style={{
                                default: { fill, outline: "none", stroke: isActive ? "#2563eb" : "#cbd5e1", strokeWidth: isActive ? 1.4 : 0.7, transition: "stroke 120ms ease" },
                                hover:   { fill, outline: "none", stroke: "#1d4ed8", strokeWidth: 1.1 },
                                pressed: { fill, outline: "none", stroke: "#1e40af", strokeWidth: 1.3 },
                              }}
                            >
                              <title>{`County: ${name}`}</title>
                            </Geography>
                          );
                        });
                    }}
                  </Geographies>
                </ZoomableGroup>
              </ComposableMap>
            </div>
            <div className="text-center mt-2 text-xl font-semibold tracking-wide text-slate-700">TEXAS</div>
          </div>

          {/* Method notes */}
          <div className="rounded-2xl bg-white border border-slate-200 p-5 shadow-sm">
            <h3 className="text-base font-semibold mb-2">Method notes</h3>
            <ul className="text-sm text-slate-700 list-disc pl-5 space-y-1">
              <li><strong>EL (Expected Loss)</strong> = loss forecast per time step (USD).</li>
              <li><strong>Wind intensity</strong> = clamp(wind_ms / 25, 0..1.5) • <strong>Flood intensity</strong> = clamp(rain_mm / 50, 0..1.5)</li>
              <li><strong>Vulnerability</strong> = normalized FEMA NRI <code>EAL_total</code>.</li>
              <li>EL_home_wind = TIV_home × wind_intensity × (0.02 + 0.3 × vulnerability)</li>
              <li>EL_home_flood = TIV_home × flood_intensity × (0.01 + 0.3 × vulnerability)</li>
              <li><strong>County EL</strong> = sum over tracts and time windows. <strong>FIPS</strong> = county code.</li>
            </ul>
          </div>
        </section>

        {/* RIGHT: county panel + list */}
        <aside className="md:col-span-2 space-y-4">
          <div className="rounded-2xl bg-white border border-slate-200 p-5 shadow-sm">
            <h2 className="text-lg font-medium mb-2">County</h2>
            {!selected && <p className="text-slate-500">Click a county on the map.</p>}
            {selected && (
              <div className="text-slate-800">
                <div className="text-2xl font-semibold">{selected.name}</div>
                <div className="text-slate-500">FIPS: {selected.fips}</div>
              </div>
            )}
          </div>

          <div className="rounded-2xl bg-white border border-slate-200 p-5 shadow-sm">
            <h2 className="text-lg font-medium mb-3">Expected Loss (next 5 days)</h2>
            {!series && !error && (
              <div className="h-80 flex items-center justify-center text-slate-400">
                {selected ? "Loading…" : "Pick a county"}
              </div>
            )}
            {error && <div className="text-rose-600">{error}</div>}
            {series && series.length > 0 && (
              <div className="h-80">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={series} margin={{ top: 12, right: 24, left: 14, bottom: 34 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="dt" tickFormatter={fmtMonthYY} interval="preserveStartEnd" minTickGap={28}>
                      <Label value="Forecast time (UTC)" offset={-18} position="insideBottom" />
                    </XAxis>
                    <YAxis tickFormatter={fmtCurrencyShort} width={72}>
                      
                    </YAxis>
                    <Tooltip
                      formatter={(v, k) => [fmtCurrencyShort(v), k === "cum" ? "Cumulative" : "Step EL"]}
                      labelFormatter={(l) => `UTC ${fmtMonthYY(l)}`}
                      contentStyle={{ background: "white", border: "1px solid #e2e8f0", color: "#0f172a" }}
                    />
                    {/* Cumulative curve */}
                    <Line type="monotone" dataKey="cum" stroke="#2563eb" strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>

          <div className="rounded-2xl bg-white border border-slate-200 p-5 shadow-sm">
            <div className="flex items-center justify-between mb-2">
              <h2 className="text-lg font-medium">Top Risk Counties</h2>
              <div className="text-sm text-slate-500 pr-2">5-day EL</div>
            </div>
            {loading && <div className="text-slate-500">Loading…</div>}
            {topSorted && (
              <ul className="divide-y divide-slate-200">
                {topSorted.slice(0, 12).map((c) => (
                  <li key={c.fips} className="py-2 flex items-center justify-between">
                    <button
                      className="text-left text-slate-800 hover:text-blue-700"
                      onClick={() => selectWithoutZoom(c.fips, c.name || c.fips)}
                      title={`County: ${c.name || c.fips}`}
                    >
                      <div className="font-medium">{c.name || c.fips}</div>
                      <div className="text-slate-500 text-sm">FIPS {c.fips}</div>
                    </button>
                    <div className="text-blue-700 font-semibold tabular-nums">
                      {fmtCurrencyShort(c.el_total_sum)}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </aside>
      </main>
    </div>
  );
}
