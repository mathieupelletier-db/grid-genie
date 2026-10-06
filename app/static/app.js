/* Grid Genie — MapLibre GL front-end */
"use strict";

// ------------------------------------------------------------------------------------------
// i18n
// ------------------------------------------------------------------------------------------
const I18N = {
  fr: {
    subtitle: "Exploitation du réseau de distribution", filters: "Filtres", ced: "Centre d'exploitation (CED)",
    line: "Ligne", all: "Tous", overload_min: "Surcharge min. (hiver 2026)", select_area: "▭ Sélection spatiale",
    layers: "Couches", ops: "Opérations", tab_outages: "Pannes en cours", tab_zones: "Zones à risque",
    tab_tx: "Transfos à risque", assistant: "Assistant Genie", new_conv: "Nouvelle conversation",
    l_chi_hex: "Carte de chaleur CHI (6 mois)", l_zones: "Zones de protection", l_right_of_way: "Emprises",
    l_lines: "Lignes", l_spans: "Portées (longueur)", l_poles: "Poteaux (âge)", l_transformers: "Transformateurs (surcharge)",
    l_devices: "Coupe-circuits", l_substations: "Postes", l_outages_active: "Pannes en cours", l_genie: "Résultat Genie",
    draw_hint: "Cliquez-glissez sur la carte pour tracer un rectangle", sel_title: "Sélection",
    poles: "Poteaux", poles_pre_1985: "Poteaux avant 1985", transformers: "Transformateurs",
    transformers_over_150: "Transfos > 150 % (hiver 2026)", customers: "Clients", conductor_km: "Conducteur (km)",
    area_km2: "Surface (km²)", thinking: "Genie analyse la question", shown_on_map: "objets affichés sur la carte",
    rows: "lignes", sql: "SQL généré", no_genie: "Espace Genie non configuré.", customers_out: "clients hors tension",
    active_outages: "pannes en cours", high_risk: "transfos à risque élevé",
    samples: ["Où sont les transformateurs installés avant 1985 en Beauce ?",
      "Combien de clients vont être affectés si le coupe-circuit « LAV_Y3Z4G » lâche ?",
      "Quelle est la distance maximale au poste pour la ligne « LAV_SVR_242 » ?",
      "Quelles sont les zones de protection ayant eu le plus d'interruptions au cours des 3 derniers mois ?",
      "Quelles sont les zones de protection ayant eu le plus de clients-heures interrompus (CHI) au cours des 6 derniers mois ?",
      "Afficher l'emprise (surface) de chaque ligne autour du poste « LAV SVR »",
      "Affiche le 1% des clients ayant la plus longue distance au poste de distribution pour le CED MAT (Matapédia)",
      "Sur la ligne « LAV_SVR_242 », quels sont les transformateurs ayant eu une surcharge de plus de 150% durant l'hiver 2025 ?",
      "Quels sont les poteaux les plus âgés de la ligne « LAV_SVR_242 » ?",
      "Où sont les portées les plus longues ?"],
  },
  en: {
    subtitle: "Distribution network operations", filters: "Filters", ced: "Operating centre (CED)", line: "Line",
    all: "All", overload_min: "Min. overload (winter 2026)", select_area: "▭ Spatial selection", layers: "Layers",
    ops: "Operations", tab_outages: "Active outages", tab_zones: "Risk zones", tab_tx: "At-risk transformers",
    assistant: "Genie assistant", new_conv: "New conversation",
    l_chi_hex: "CHI heat map (6 months)", l_zones: "Protection zones", l_right_of_way: "Rights-of-way",
    l_lines: "Lines", l_spans: "Spans (length)", l_poles: "Poles (age)", l_transformers: "Transformers (overload)",
    l_devices: "Fuse cutouts", l_substations: "Substations", l_outages_active: "Active outages", l_genie: "Genie result",
    draw_hint: "Click and drag on the map to draw a rectangle", sel_title: "Selection",
    poles: "Poles", poles_pre_1985: "Poles before 1985", transformers: "Transformers",
    transformers_over_150: "Transformers > 150 % (winter 2026)", customers: "Customers", conductor_km: "Conductor (km)",
    area_km2: "Area (km²)", thinking: "Genie is analysing the question", shown_on_map: "objects shown on the map",
    rows: "rows", sql: "Generated SQL", no_genie: "Genie space not configured.", customers_out: "customers out",
    active_outages: "active outages", high_risk: "high-risk transformers",
    samples: ["Where are the transformers installed before 1985 in Beauce?",
      "How many customers will be affected if fuse cutout LAV_Y3Z4G fails?",
      "What is the maximum distance to the substation for line LAV_SVR_242?",
      "Which protection zones had the highest CHI over the last 6 months?",
      "Show the right-of-way area of each line around substation LAV SVR",
      "On line LAV_SVR_242, which transformers were overloaded above 150% during winter 2025?",
      "Which transformers are most at risk of overload next winter?"],
  },
};
let LANG = "fr";
const t = (k) => (I18N[LANG][k] ?? k);

function applyI18n() {
  document.documentElement.lang = LANG;
  document.querySelectorAll("[data-i18n]").forEach((el) => { el.textContent = t(el.dataset.i18n); });
  document.getElementById("lang-toggle").textContent = LANG === "fr" ? "EN" : "FR";
  renderLayerToggles();
  renderSuggestions(I18N[LANG].samples);
  loadOps();
}

// ------------------------------------------------------------------------------------------
// State & helpers
// ------------------------------------------------------------------------------------------
const state = { ced: "", line: "", ovMin: 0, meta: null, conversationId: null, tab: "outages", drawing: false };
const api = async (path, opts) => {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
};
const qs = () => {
  const p = new URLSearchParams();
  if (state.ced) p.set("ced", state.ced);
  if (state.line) p.set("line_id", state.line);
  return p.toString();
};
const fmt = (v) => (typeof v === "number" ? v.toLocaleString(LANG === "fr" ? "fr-CA" : "en-CA") : v ?? "–");
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// ------------------------------------------------------------------------------------------
// Map
// ------------------------------------------------------------------------------------------
const OSM_FALLBACK = {
  version: 8,
  glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
  sources: { osm: { type: "raster", tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"], tileSize: 256,
                    attribution: "© OpenStreetMap contributors" } },
  layers: [{ id: "osm", type: "raster", source: "osm" }],
};
const BASEMAPS = {
  dark: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
  light: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
};
const theme = () => document.documentElement.dataset.theme || "dark";
const map = new maplibregl.Map({
  container: "map",
  style: BASEMAPS[theme()],
  center: [-71.0, 47.0], zoom: 6, attributionControl: { compact: true },
});
map.addControl(new maplibregl.NavigationControl(), "top-right");
map.addControl(new maplibregl.ScaleControl({ unit: "metric" }), "bottom-right");
let styleFailed = false;
map.on("error", (e) => {
  if (!styleFailed && !map.isStyleLoaded() && String(e?.error?.message || "").match(/style|Failed to fetch/i)) {
    styleFailed = true; map.setStyle(OSM_FALLBACK);
  }
});

// ------------------------------------------------------------------------------------------
// Theme (light / dark): UI via CSS variables, basemap swapped while keeping our gg-* layers
// ------------------------------------------------------------------------------------------
const MAP_THEME = {
  dark:  { lines: "#60a5fa", subFill: "#0f1720", subStroke: "#ffffff", label: "#ffffff", halo: "#000000" },
  light: { lines: "#1d4ed8", subFill: "#ffffff", subStroke: "#111827", label: "#111827", halo: "#ffffff" },
};
function applyMapTheme() {
  const c = MAP_THEME[theme()];
  const set = (id, prop, v) => { if (map.getLayer(id)) map.setPaintProperty(id, prop, v); };
  set("gg-lines", "line-color", c.lines);
  set("gg-substations", "circle-color", c.subFill);
  set("gg-substations", "circle-stroke-color", c.subStroke);
  set("gg-substations-label", "text-color", c.label);
  set("gg-substations-label", "text-halo-color", c.halo);
}
function setTheme(next) {
  document.documentElement.dataset.theme = next;
  localStorage.setItem("gg-theme", next);
  document.getElementById("theme-toggle").textContent = next === "dark" ? "☀️" : "🌙";
  if (styleFailed) { applyMapTheme(); return; }  // OSM raster fallback has no light/dark variant
  map.setStyle(BASEMAPS[next], {
    transformStyle: (prev, nextStyle) => {
      if (!prev) return nextStyle;
      const keep = (id) => id.startsWith("gg-");
      return {
        ...nextStyle,
        sources: { ...nextStyle.sources, ...Object.fromEntries(Object.entries(prev.sources).filter(([id]) => keep(id))) },
        layers: [...nextStyle.layers, ...prev.layers.filter((l) => keep(l.id))],
      };
    },
  });
  map.once("style.load", applyMapTheme);
}
document.getElementById("theme-toggle").textContent = theme() === "dark" ? "☀️" : "🌙";
document.getElementById("theme-toggle").addEventListener("click", () => setTheme(theme() === "dark" ? "light" : "dark"));

// Layer catalogue: id → {endpoint, visible, render(sourceId)}
const LAYER_DEFS = [
  { id: "chi_hex", visible: true, color: "#f97316", add: (s) => [{
      id: s, type: "fill", source: s,
      paint: { "fill-color": ["interpolate", ["linear"], ["get", "chi_6m"], 0, "#fde68a", 200, "#f97316", 1000, "#b91c1c", 3000, "#7f1d1d"],
               "fill-opacity": 0.5 } }] },
  { id: "zones", visible: false, color: "#a78bfa", add: (s) => [
      { id: s, type: "fill", source: s, paint: { "fill-color": ["match", ["get", "risk_tier"], "ÉLEVÉ", "#ef4444", "MOYEN", "#f59e0b", "#a78bfa"], "fill-opacity": 0.18 } },
      { id: `${s}-outline`, type: "line", source: s, paint: { "line-color": ["match", ["get", "risk_tier"], "ÉLEVÉ", "#ef4444", "MOYEN", "#f59e0b", "#a78bfa"], "line-width": 1.2, "line-dasharray": [2, 1] } }] },
  { id: "right_of_way", visible: false, color: "#38bdf8", add: (s) => [{
      id: s, type: "fill", source: s, paint: { "fill-color": "#38bdf8", "fill-opacity": 0.28, "fill-outline-color": "#0ea5e9" } }] },
  { id: "lines", visible: true, color: "#60a5fa", add: (s) => [{
      id: s, type: "line", source: s, paint: { "line-color": MAP_THEME[theme()].lines, "line-width": ["interpolate", ["linear"], ["zoom"], 8, 1, 14, 3] } }] },
  { id: "spans", visible: false, color: "#e879f9", add: (s) => [{
      id: s, type: "line", source: s,
      paint: { "line-color": ["interpolate", ["linear"], ["get", "length_m"], 50, "#334155", 150, "#a855f7", 300, "#e879f9", 450, "#fdf4ff"],
               "line-width": ["interpolate", ["linear"], ["get", "length_m"], 50, 1, 450, 5] } }] },
  { id: "poles", visible: false, color: "#facc15", minzoom: 11, add: (s) => [{
      id: s, type: "circle", source: s, minzoom: 11,
      paint: { "circle-radius": 3, "circle-color": ["interpolate", ["linear"], ["get", "install_year"], 1960, "#ef4444", 1985, "#f59e0b", 2005, "#facc15", 2024, "#22c55e"],
               "circle-stroke-width": 0.5, "circle-stroke-color": "#000" } }] },
  { id: "transformers", visible: true, color: "#22c55e", add: (s) => [{
      id: s, type: "circle", source: s,
      paint: { "circle-radius": ["interpolate", ["linear"], ["zoom"], 8, 2.5, 14, 6],
               "circle-color": ["interpolate", ["linear"], ["coalesce", ["get", "max_overload_pct_hiver_2026"], 0], 80, "#22c55e", 120, "#facc15", 150, "#f97316", 200, "#dc2626"],
               "circle-stroke-width": ["case", ["==", ["get", "risk_tier"], "ÉLEVÉ"], 2, 0.5],
               "circle-stroke-color": ["case", ["==", ["get", "risk_tier"], "ÉLEVÉ"], "#ffffff", "#000000"] } }] },
  { id: "devices", visible: false, color: "#f472b6", add: (s) => [{
      id: s, type: "circle", source: s, paint: { "circle-radius": 5, "circle-color": "#f472b6", "circle-stroke-width": 1, "circle-stroke-color": "#fff" } }] },
  { id: "substations", visible: true, color: "#ffffff", add: (s) => [
      { id: s, type: "circle", source: s, paint: { "circle-radius": 8, "circle-color": MAP_THEME[theme()].subFill, "circle-stroke-width": 3, "circle-stroke-color": MAP_THEME[theme()].subStroke } },
      { id: `${s}-label`, type: "symbol", source: s,
        layout: { "text-field": ["get", "substation_name"], "text-size": 12, "text-offset": [0, 1.4], "text-font": ["Open Sans Bold", "Noto Sans Regular"] },
        paint: { "text-color": MAP_THEME[theme()].label, "text-halo-color": MAP_THEME[theme()].halo, "text-halo-width": 1.5 } }] },
  { id: "outages_active", visible: true, color: "#ef4444", add: (s) => [{
      id: s, type: "circle", source: s, paint: { "circle-radius": 9, "circle-color": "rgba(239,68,68,0.35)", "circle-stroke-width": 2.5, "circle-stroke-color": "#ef4444" } }] },
];

function renderLayerToggles() {
  const host = document.getElementById("layer-toggles");
  host.innerHTML = "";
  for (const d of LAYER_DEFS) {
    const row = document.createElement("label");
    row.className = "toggle";
    row.innerHTML = `<input type="checkbox" ${d.visible ? "checked" : ""}/> <span class="sw" style="background:${d.color}"></span> ${t("l_" + d.id)}`;
    row.querySelector("input").addEventListener("change", (e) => { d.visible = e.target.checked; setVisibility(d); });
    host.appendChild(row);
  }
  document.getElementById("legend").innerHTML = `
    <div>CHI 6 m</div><div class="ramp" style="background:linear-gradient(90deg,#fde68a,#f97316,#b91c1c,#7f1d1d)"></div>
    <div>${t("l_transformers")}: <span style="color:#22c55e">●</span> &lt;100% <span style="color:#facc15">●</span> 120% <span style="color:#f97316">●</span> 150% <span style="color:#dc2626">●</span> 200%+ · ◯ = ${LANG === "fr" ? "risque élevé hiver 2027" : "high risk winter 2027"}</div>`;
}

function setVisibility(d) {
  const v = d.visible ? "visible" : "none";
  for (const l of map.getStyle()?.layers || []) {
    if (l.id === `gg-${d.id}` || l.id.startsWith(`gg-${d.id}-`)) map.setLayoutProperty(l.id, "visibility", v);
  }
  if (d.visible && !d.loadedKey) loadLayer(d);
}

async function loadLayer(d) {
  const key = qs();
  if (d.loadedKey === key && map.getSource(`gg-${d.id}`)) return;
  d.loadedKey = key;
  const data = await api(`/api/layers/${d.id}?${key}`);
  const sid = `gg-${d.id}`;
  if (map.getSource(sid)) { map.getSource(sid).setData(data); }
  else {
    map.addSource(sid, { type: "geojson", data });
    for (const spec of d.add(sid)) {
      spec.layout = { ...(spec.layout || {}), visibility: d.visible ? "visible" : "none" };
      map.addLayer(spec, map.getLayer("gg-genie-fill") ? "gg-genie-fill" : undefined);
      if (spec.type !== "symbol") bindPopup(spec.id);
    }
  }
  if (d.id === "transformers") applyOverloadFilter();
}

function reloadLayers() {
  for (const d of LAYER_DEFS) { d.loadedKey = null; if (d.visible) loadLayer(d).catch(console.error); }
}

function applyOverloadFilter() {
  if (!map.getLayer("gg-transformers")) return;
  map.setFilter("gg-transformers", state.ovMin > 0 ? [">=", ["coalesce", ["get", "max_overload_pct_hiver_2026"], 0], state.ovMin] : null);
}

function popupHtml(props) {
  const rows = Object.entries(props).filter(([k]) => !k.startsWith("_"))
    .map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(fmt(v))}</td></tr>`).join("");
  return `<div class="popup"><table>${rows}</table></div>`;
}
function bindPopup(layerId) {
  map.on("click", layerId, (e) => {
    if (state.drawing) return;
    new maplibregl.Popup({ maxWidth: "340px" }).setLngLat(e.lngLat).setHTML(popupHtml(e.features[0].properties)).addTo(map);
  });
  map.on("mouseenter", layerId, () => { map.getCanvas().style.cursor = "pointer"; });
  map.on("mouseleave", layerId, () => { map.getCanvas().style.cursor = state.drawing ? "crosshair" : ""; });
}

function fitTo(bounds) { if (bounds) map.fitBounds([[bounds[0], bounds[1]], [bounds[2], bounds[3]]], { padding: 40, duration: 800 }); }

// ------------------------------------------------------------------------------------------
// Filters
// ------------------------------------------------------------------------------------------
async function initFilters() {
  state.meta = await api("/api/meta");
  const cedSel = document.getElementById("ced-filter");
  for (const c of state.meta.ceds) cedSel.insertAdjacentHTML("beforeend", `<option value="${c.ced_code}">${esc(c.ced_name)}</option>`);
  fillLines();
  cedSel.addEventListener("change", () => {
    state.ced = cedSel.value; state.line = ""; fillLines();
    fitTo(state.ced ? state.meta.ced_bounds[state.ced] : null);
    reloadLayers(); loadOps();
  });
  document.getElementById("line-filter").addEventListener("change", (e) => {
    state.line = e.target.value;
    fitTo(state.line ? state.meta.line_bounds[state.line] : (state.ced ? state.meta.ced_bounds[state.ced] : null));
    reloadLayers(); loadOps();
  });
  const ov = document.getElementById("ov-threshold");
  ov.addEventListener("input", () => { state.ovMin = +ov.value; document.getElementById("ov-val").textContent = ov.value; applyOverloadFilter(); });
}
function fillLines() {
  const sel = document.getElementById("line-filter");
  sel.innerHTML = `<option value="">${t("all")}</option>`;
  for (const l of state.meta.lines.filter((l) => !state.ced || l.ced_code === state.ced))
    sel.insertAdjacentHTML("beforeend", `<option value="${l.line_id}">${l.line_id} · ${esc(l.substation_name)}</option>`);
}

// ------------------------------------------------------------------------------------------
// Spatial selection (rectangle)
// ------------------------------------------------------------------------------------------
let dragStart = null;
document.getElementById("select-btn").addEventListener("click", () => {
  state.drawing = !state.drawing;
  document.body.classList.toggle("drawing", state.drawing);
  document.getElementById("select-btn").classList.toggle("active", state.drawing);
  state.drawing ? map.dragPan.disable() : map.dragPan.enable();
  if (state.drawing) showSelection(`<i>${t("draw_hint")}</i>`);
});
map.on("mousedown", (e) => { if (state.drawing) dragStart = e.lngLat; });
map.on("mousemove", (e) => { if (state.drawing && dragStart) drawRect(dragStart, e.lngLat); });
map.on("mouseup", async (e) => {
  if (!state.drawing || !dragStart) return;
  const poly = drawRect(dragStart, e.lngLat);
  dragStart = null; state.drawing = false; map.dragPan.enable();
  document.body.classList.remove("drawing");
  document.getElementById("select-btn").classList.remove("active");
  showSelection(`<span class="dots">${t("sel_title")}</span>`);
  try {
    const res = await api("/api/select", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ geometry: poly }) });
    const s = res.stats;
    const keys = ["area_km2", "poles", "poles_pre_1985", "transformers", "transformers_over_150", "customers", "conductor_km"];
    let html = `<b>${t("sel_title")}</b> <span class="badge">ST_Intersects</span><table>${keys.map((k) => `<tr><td>${t(k)}</td><td>${fmt(+s[k] || 0)}</td></tr>`).join("")}</table>`;
    if (res.top_transformers.length) html += `<div style="margin-top:6px;color:var(--muted)">Top risque / risk</div>` +
      res.top_transformers.slice(0, 5).map((r) => `<div>${r.transformer_id} · ${Math.round(100 * r.risk_score)} % · ${r.risk_tier}</div>`).join("");
    showSelection(html);
  } catch (err) { showSelection(`⚠️ ${esc(err.message)}`); }
});
function drawRect(a, b) {
  const poly = { type: "Polygon", coordinates: [[[a.lng, a.lat], [b.lng, a.lat], [b.lng, b.lat], [a.lng, b.lat], [a.lng, a.lat]]] };
  const data = { type: "Feature", geometry: poly, properties: {} };
  if (map.getSource("gg-selection")) map.getSource("gg-selection").setData(data);
  else {
    map.addSource("gg-selection", { type: "geojson", data });
    map.addLayer({ id: "gg-selection", type: "fill", source: "gg-selection", paint: { "fill-color": "#ffb020", "fill-opacity": 0.12 } });
    map.addLayer({ id: "gg-selection-line", type: "line", source: "gg-selection", paint: { "line-color": "#ffb020", "line-width": 2 } });
  }
  return poly;
}
function showSelection(html) { const el = document.getElementById("selection-result"); el.classList.remove("hidden"); el.innerHTML = html; }

// ------------------------------------------------------------------------------------------
// Operations panel (Lakebase)
// ------------------------------------------------------------------------------------------
document.querySelectorAll(".tab").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === b));
  state.tab = b.dataset.tab; loadOps();
}));

async function loadOps() {
  const list = document.getElementById("ops-list");
  const p = new URLSearchParams(); if (state.ced) p.set("ced", state.ced); if (state.line && state.tab === "tx") p.set("line_id", state.line);
  const path = { outages: "/api/ops/active_outages", zones: "/api/ops/zone_risk", tx: "/api/ops/transformer_risk" }[state.tab];
  try {
    const [res, sum] = await Promise.all([api(`${path}?${p}`), api(`/api/ops/summary?${state.ced ? "ced=" + state.ced : ""}`)]);
    document.getElementById("lb-latency").textContent = `Lakebase · ${res.latency_ms} ms`;
    renderKpis(sum.rows);
    list.innerHTML = res.rows.map((r) => {
      if (state.tab === "outages") return item("outage", `${r.zone_id} · ${r.line_id}`, `${fmt(r.customers_interrupted)} ${t("customers_out")} · ${r.elapsed_h} h · ${r.cause}`, r);
      if (state.tab === "zones") return item(r.risk_tier, `${r.zone_id} · ${r.line_id} · ${r.risk_tier}`,
        `CHI 6 m ${fmt(+r.chi_6m)} · ${r.interruptions_6m} int. · ${LANG === "fr" ? "poteaux" : "poles"} ${r.avg_pole_age_years} ans` +
        ((LANG === "fr" ? r.recommendation_fr : r.recommendation_en) ? `<br>💡 ${esc(LANG === "fr" ? r.recommendation_fr : r.recommendation_en)}` : ""), r);
      return item(r.risk_tier, `${r.transformer_id} · ${r.line_id} · ${Math.round(100 * r.risk_score_next_winter)} %`,
        `${r.kva} kVA · ${r.n_customers} ${t("customers").toLowerCase()} · max ${fmt(r.max_overload_pct_last_winter)} % (hiver 2026)`, r);
    }).join("") || "<i>—</i>";
    list.querySelectorAll(".item").forEach((el) => el.addEventListener("click", () => map.flyTo({ center: [+el.dataset.lon, +el.dataset.lat], zoom: 15 })));
  } catch (err) { list.innerHTML = `<i>⚠️ ${esc(err.message)}</i>`; }
}
const item = (cls, title, sub, r) => `<div class="item ${cls}" data-lon="${r.lon}" data-lat="${r.lat}"><div class="t">${esc(title)}</div><div class="s">${sub}</div></div>`;
function renderKpis(rows) {
  const tot = rows.reduce((a, r) => ({ out: a.out + (+r.active_outages || 0), cust: a.cust + (+r.customers_out || 0), hr: a.hr + (+r.high_risk_transformers || 0) }), { out: 0, cust: 0, hr: 0 });
  document.getElementById("ops-kpis").innerHTML =
    `<span class="${tot.out ? "alert" : ""}"><b>${tot.out}</b> ${t("active_outages")}</span><span class="${tot.cust ? "alert" : ""}"><b>${fmt(tot.cust)}</b> ${t("customers_out")}</span><span><b>${tot.hr}</b> ${t("high_risk")}</span>`;
}

// ------------------------------------------------------------------------------------------
// Genie chat
// ------------------------------------------------------------------------------------------
const chatLog = document.getElementById("chat-log");
document.getElementById("chat-form").addEventListener("submit", (e) => { e.preventDefault(); ask(document.getElementById("chat-input").value); });
document.getElementById("chat-input").addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); ask(e.target.value); } });
document.getElementById("chat-reset").addEventListener("click", () => { state.conversationId = null; chatLog.innerHTML = ""; clearGenieLayer(); renderSuggestions(I18N[LANG].samples); });

function renderSuggestions(list) {
  const host = document.getElementById("chat-suggestions");
  host.innerHTML = "";
  for (const q of list || []) {
    const b = document.createElement("button"); b.textContent = q; b.onclick = () => ask(q); host.appendChild(b);
  }
}

async function ask(question) {
  question = (question || "").trim();
  if (!question) return;
  document.getElementById("chat-input").value = "";
  document.getElementById("chat-suggestions").innerHTML = "";
  chatLog.insertAdjacentHTML("beforeend", `<div class="msg user">${esc(question)}</div>`);
  const bot = document.createElement("div"); bot.className = "msg bot"; bot.innerHTML = `<span class="dots">${t("thinking")}</span>`;
  chatLog.appendChild(bot); chatLog.scrollTop = chatLog.scrollHeight;
  try {
    const { conversation_id, message_id } = await api("/api/genie/ask", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question, conversation_id: state.conversationId }) });
    state.conversationId = conversation_id;
    let res;
    for (let i = 0; i < 120; i++) {
      await new Promise((r) => setTimeout(r, 1500));
      res = await api(`/api/genie/messages/${conversation_id}/${message_id}`);
      if (["COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"].includes(res.status)) break;
      bot.innerHTML = `<span class="dots">${t("thinking")} (${(res.status || "").toLowerCase().replaceAll("_", " ")})</span>`;
    }
    renderAnswer(bot, res);
  } catch (err) { bot.innerHTML = `⚠️ ${esc(err.message)}`; }
  chatLog.scrollTop = chatLog.scrollHeight;
}

function renderAnswer(el, res) {
  let html = res.text ? `<div>${esc(res.text).replace(/\n/g, "<br>")}</div>` : "";
  if (res.description && !res.text) html += `<div>${esc(res.description)}</div>`;
  const geo = resultToGeoJSON(res.columns, res.rows);
  const visibleCols = res.columns.map((c, i) => [c, i]).filter(([c]) => !isGeomCol(c));
  if (res.rows.length) {
    html += `<table><tr>${visibleCols.map(([c]) => `<th>${esc(c)}</th>`).join("")}</tr>` +
      res.rows.slice(0, 50).map((r) => `<tr>${visibleCols.map(([, i]) => `<td>${esc(fmtCell(r[i]))}</td>`).join("")}</tr>`).join("") + "</table>";
    html += `<div class="meta">${fmt(res.row_count)} ${t("rows")}${geo.features.length ? ` · <span class="map-note">🗺️ ${geo.features.length} ${t("shown_on_map")}</span>` : ""}</div>`;
  }
  if (res.sql) html += `<details><summary class="meta">${t("sql")}</summary><pre>${esc(res.sql)}</pre></details>`;
  el.innerHTML = html || "—";
  if (geo.features.length) showGenieLayer(geo);
  if (res.suggested_questions?.length) renderSuggestions(res.suggested_questions);
}
const fmtCell = (v) => { const n = Number(v); return v !== null && v !== "" && !isNaN(n) && String(v).length < 20 ? fmt(Math.round(n * 100) / 100) : v; };
const isGeomCol = (c) => /^(geojson|geom|geometry|.*_geom|wkt)$/i.test(c);

// Turn a Genie result set into GeoJSON: GeoJSON column, WKT column, or lon/lat columns
function resultToGeoJSON(cols, rows) {
  const gi = cols.findIndex((c) => isGeomCol(c));
  const lon = cols.findIndex((c) => /^(lon|longitude|x)$/i.test(c));
  const lat = cols.findIndex((c) => /^(lat|latitude|y)$/i.test(c));
  const features = [];
  for (const r of rows) {
    let g = null;
    if (gi >= 0 && r[gi]) g = parseGeom(r[gi]);
    else if (lon >= 0 && lat >= 0 && r[lon] != null) g = { type: "Point", coordinates: [+r[lon], +r[lat]] };
    if (!g) continue;
    const props = {}; cols.forEach((c, i) => { if (i !== gi) props[c] = r[i]; });
    features.push({ type: "Feature", geometry: g, properties: props });
  }
  return { type: "FeatureCollection", features };
}
function parseGeom(v) {
  const s = String(v).trim();
  if (s.startsWith("{")) { try { return JSON.parse(s); } catch { return null; } }
  return parseWKT(s.replace(/^SRID=\d+;/i, ""));
}
function parseWKT(wkt) {
  const m = wkt.match(/^(\w+)\s*(?:Z|M|ZM)?\s*\((.*)\)$/is);
  if (!m) return null;
  const type = m[1].toUpperCase(), body = m[2];
  const pts = (s) => s.split(",").map((p) => p.trim().split(/\s+/).slice(0, 2).map(Number));
  const rings = (s) => s.match(/\(([^()]+)\)/g).map((r) => pts(r.slice(1, -1)));
  switch (type) {
    case "POINT": return { type: "Point", coordinates: pts(body)[0] };
    case "LINESTRING": return { type: "LineString", coordinates: pts(body) };
    case "POLYGON": return { type: "Polygon", coordinates: rings(body) };
    case "MULTIPOINT": return { type: "MultiPoint", coordinates: pts(body.replace(/[()]/g, "")) };
    case "MULTILINESTRING": return { type: "MultiLineString", coordinates: rings(body) };
    case "MULTIPOLYGON": return { type: "MultiPolygon", coordinates: body.split(/\)\s*\)\s*,\s*\(\s*\(/).map((p) => rings(`(${p.replace(/^\(+|\)+$/g, "")})`)) };
    default: return null;
  }
}

function clearGenieLayer() { if (map.getSource("gg-genie")) map.getSource("gg-genie").setData({ type: "FeatureCollection", features: [] }); }
function showGenieLayer(fc) {
  if (!map.getSource("gg-genie")) {
    map.addSource("gg-genie", { type: "geojson", data: fc });
    map.addLayer({ id: "gg-genie-fill", type: "fill", source: "gg-genie", filter: ["==", ["geometry-type"], "Polygon"], paint: { "fill-color": "#ffb020", "fill-opacity": 0.35 } });
    map.addLayer({ id: "gg-genie-line", type: "line", source: "gg-genie", filter: ["!=", ["geometry-type"], "Point"], paint: { "line-color": "#ffb020", "line-width": 3.5 } });
    map.addLayer({ id: "gg-genie-pt", type: "circle", source: "gg-genie", filter: ["==", ["geometry-type"], "Point"],
      paint: { "circle-radius": 7, "circle-color": "#ffb020", "circle-stroke-width": 2, "circle-stroke-color": "#111" } });
    ["gg-genie-fill", "gg-genie-line", "gg-genie-pt"].forEach(bindPopup);
  } else map.getSource("gg-genie").setData(fc);
  const b = new maplibregl.LngLatBounds();
  const walk = (c) => (typeof c[0] === "number" ? b.extend(c) : c.forEach(walk));
  fc.features.forEach((f) => walk(f.geometry.coordinates));
  if (!b.isEmpty()) map.fitBounds(b, { padding: 60, maxZoom: 15, duration: 900 });
}

// ------------------------------------------------------------------------------------------
// Boot
// ------------------------------------------------------------------------------------------
document.getElementById("lang-toggle").addEventListener("click", () => { LANG = LANG === "fr" ? "en" : "fr"; applyI18n(); });
map.on("load", async () => {
  applyI18n();
  try { await initFilters(); } catch (e) { console.error(e); }
  for (const d of LAYER_DEFS) if (d.visible) loadLayer(d).catch(console.error);
});
// Re-add our layers if the basemap style is swapped (fallback)
map.on("styledata", () => { if (styleFailed && !map.getSource("gg-lines")) { for (const d of LAYER_DEFS) { d.loadedKey = null; if (d.visible) loadLayer(d).catch(console.error); } } });
