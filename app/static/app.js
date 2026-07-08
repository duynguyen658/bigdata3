const map = L.map("map", {
  zoomControl: false,
  preferCanvas: true,
}).setView([10.7769, 106.7009], 11);

L.control.zoom({ position: "bottomleft" }).addTo(map);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 18,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

let heatLayer = null;
let markers = L.layerGroup().addTo(map);

const state = {
  mode: "current",
  horizon: 1,
  model: "random_forest",
};

const modeButtons = [...document.querySelectorAll(".mode-button")];
const forecastControls = document.querySelector("#forecast-controls");
const horizon = document.querySelector("#horizon");
const modelSelect = document.querySelector("#model");
const horizonLabel = document.querySelector("#horizon-label");
const panelTitle = document.querySelector("#panel-title");
const statusEl = document.querySelector("#status");
const maxAqiEl = document.querySelector("#max-aqi");
const pointCountEl = document.querySelector("#point-count");
const dataAsOfEl = document.querySelector("#data-as-of");
const freshnessEl = document.querySelector("#freshness");
const hotspotContextEl = document.querySelector("#hotspot-context");
const hotspotsEl = document.querySelector("#hotspots");
const metricMaeEl = document.querySelector("#metric-mae");
const metricRmseEl = document.querySelector("#metric-rmse");
const metricR2El = document.querySelector("#metric-r2");
const metricsStrip = document.querySelector(".metrics-strip");

const heatGradient = {
  0.1: "#2fbe63",
  0.35: "#d7cb34",
  0.55: "#dc8b31",
  0.75: "#cf413c",
  1.0: "#8f2f7f",
};

function colorForAqi(aqi) {
  if (aqi <= 50) return "oklch(78% 0.15 148)";
  if (aqi <= 100) return "oklch(86% 0.15 92)";
  if (aqi <= 150) return "oklch(78% 0.15 62)";
  if (aqi <= 200) return "oklch(68% 0.2 31)";
  return "oklch(58% 0.18 345)";
}

function pollutantText(values = {}) {
  const pm25 = values.pm25 === undefined ? "--" : Number(values.pm25).toFixed(1);
  const pm10 = values.pm10 === undefined ? "--" : Number(values.pm10).toFixed(1);
  return `PM2.5 ${pm25} | PM10 ${pm10}`;
}

function shortDate(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return new Intl.DateTimeFormat("vi-VN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatMetric(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toFixed(2);
}

function metricForHorizon(metrics = []) {
  const candidates = metrics.filter((item) => Number(item.horizon_hour) === state.horizon);
  const preferred = candidates.find((item) => item.split === "test") || candidates.find((item) => item.split === "validation");
  return preferred || null;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function setMode(mode) {
  state.mode = mode;
  modeButtons.forEach((button) => button.classList.toggle("is-active", button.dataset.mode === mode));
  forecastControls.classList.toggle("is-hidden", mode === "current");
  metricsStrip.classList.toggle("is-hidden", mode === "current");
  panelTitle.textContent = mode === "current" ? "Current AQI" : `Forecast H+${state.horizon}`;
}

function drawPoints(points) {
  if (heatLayer) {
    map.removeLayer(heatLayer);
  }
  markers.clearLayers();

  const heatPoints = points.map((point) => [
    point.latitude,
    point.longitude,
    Math.min((point.aqi || 0) / 220, 1),
  ]);
  heatLayer = L.heatLayer(heatPoints, {
    radius: 28,
    blur: 22,
    maxZoom: 13,
    gradient: heatGradient,
  }).addTo(map);

  points.slice(0, 24).forEach((point) => {
    const timestamp = state.mode === "current" ? point.observation_ts : point.target_ts || point.forecast_ts;
    const marker = L.circleMarker([point.latitude, point.longitude], {
      radius: 7,
      weight: 2,
      color: "oklch(16% 0.03 185)",
      fillColor: colorForAqi(point.aqi || 0),
      fillOpacity: 0.92,
    }).bindPopup(`<b>AQI ${point.aqi ?? "--"}</b><br>${pollutantText(point.values)}<br>${shortDate(timestamp)}`);
    markers.addLayer(marker);
  });
}

function renderHotspots(points) {
  hotspotsEl.innerHTML = "";
  const top = [...points].sort((a, b) => (b.aqi || 0) - (a.aqi || 0)).slice(0, 10);
  hotspotContextEl.textContent = state.mode === "current" ? "latest" : `${state.model}, H+${state.horizon}`;

  if (!top.length) {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = "Chua co artifact du lieu de hien thi.";
    hotspotsEl.appendChild(empty);
    return;
  }

  top.forEach((point, index) => {
    const li = document.createElement("li");
    const button = document.createElement("button");
    button.type = "button";
    button.innerHTML = `
      <span class="aqi-chip" style="background:${colorForAqi(point.aqi || 0)}">${point.aqi ?? "--"}</span>
      <span class="place">
        <strong>Hotspot ${index + 1}</strong>
        <span>${Number(point.latitude).toFixed(4)}, ${Number(point.longitude).toFixed(4)}</span>
      </span>
      <span class="pollutants">${pollutantText(point.values)}</span>
    `;
    button.addEventListener("click", () => {
      map.flyTo([point.latitude, point.longitude], 13, { duration: 0.7 });
    });
    li.appendChild(button);
    hotspotsEl.appendChild(li);
  });
}

function renderKpis(payload, points) {
  const topAqi = points.reduce((max, point) => Math.max(max, point.aqi || 0), 0);
  const freshnessCounts = points.reduce((acc, point) => {
    const key = point.freshness_status || "forecast";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  maxAqiEl.textContent = points.length ? topAqi : "--";
  pointCountEl.textContent = String(payload.count ?? points.length);
  dataAsOfEl.textContent = shortDate(payload.data_as_of || payload.target_as_of);
  freshnessEl.textContent =
    state.mode === "current"
      ? Object.entries(freshnessCounts)
          .map(([key, count]) => `${key} ${count}`)
          .join(", ") || "--"
      : `H+${state.horizon}`;
}

async function renderMetrics() {
  if (state.mode === "current") return;
  const payload = await fetchJson(
    `/api/metrics?model=${encodeURIComponent(state.model)}&split=test`
  ).catch(() => ({ metrics: [] }));
  const metric = metricForHorizon(payload.metrics || []);
  metricMaeEl.textContent = formatMetric(metric?.mae);
  metricRmseEl.textContent = formatMetric(metric?.rmse);
  metricR2El.textContent = formatMetric(metric?.r2);
}

async function loadData() {
  setMode(state.mode);
  horizonLabel.textContent = `H+${state.horizon}`;
  statusEl.textContent = "Dang tai...";

  const url =
    state.mode === "current"
      ? "/api/current"
      : `/api/forecast?horizon=${state.horizon}&model=${encodeURIComponent(state.model)}`;
  const payload = await fetchJson(url);
  const points = payload.points || [];

  drawPoints(points);
  renderHotspots(points);
  renderKpis(payload, points);
  await renderMetrics();

  if (points.length) {
    statusEl.textContent =
      state.mode === "current"
        ? `${points.length.toLocaleString("vi-VN")} diem, ${shortDate(payload.data_as_of)}`
        : `${points.length.toLocaleString("vi-VN")} diem, ${state.model}, H+${state.horizon}`;
  } else {
    statusEl.textContent = "Chua co du lieu";
  }
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setMode(button.dataset.mode);
    loadData().catch((error) => {
      statusEl.textContent = error.message;
    });
  });
});

horizon.addEventListener("input", (event) => {
  state.horizon = Number(event.target.value);
  loadData().catch((error) => {
    statusEl.textContent = error.message;
  });
});

modelSelect.addEventListener("change", () => {
  state.model = modelSelect.value;
  loadData().catch((error) => {
    statusEl.textContent = error.message;
  });
});

loadData().catch((error) => {
  statusEl.textContent = error.message;
});
