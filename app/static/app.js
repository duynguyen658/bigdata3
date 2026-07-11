const map = L.map("map", {
  zoomControl: false,
  preferCanvas: true,
}).setView([10.7769, 106.7009], 11);

L.control.zoom({ position: "bottomleft" }).addTo(map);

const cityBaseLayer = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  maxZoom: 19,
});

const localBaseLayer = L.gridLayer({
  attribution: "Local AQI grid",
});

localBaseLayer.createTile = (coords) => {
  const tile = document.createElement("canvas");
  tile.width = 256;
  tile.height = 256;
  const ctx = tile.getContext("2d");

  ctx.fillStyle = "#e9f1ef";
  ctx.fillRect(0, 0, 256, 256);

  ctx.strokeStyle = "rgba(73, 102, 102, 0.14)";
  ctx.lineWidth = 1;
  for (let i = 0; i <= 256; i += 64) {
    ctx.beginPath();
    ctx.moveTo(i, 0);
    ctx.lineTo(i, 256);
    ctx.moveTo(0, i);
    ctx.lineTo(256, i);
    ctx.stroke();
  }

  const offset = ((coords.x + coords.y) % 4) * 18;
  ctx.strokeStyle = "rgba(51, 132, 151, 0.22)";
  ctx.lineWidth = 9;
  ctx.beginPath();
  ctx.moveTo(-20, 46 + offset);
  ctx.bezierCurveTo(66, 82 + offset, 112, 14 + offset, 276, 72 + offset);
  ctx.stroke();

  ctx.strokeStyle = "rgba(96, 113, 105, 0.16)";
  ctx.lineWidth = 3;
  for (let y = 36; y < 256; y += 74) {
    ctx.beginPath();
    ctx.moveTo(0, y + ((coords.x % 2) * 10));
    ctx.lineTo(256, y + 26 + ((coords.y % 2) * 8));
    ctx.stroke();
  }

  return tile;
};

localBaseLayer.addTo(map);

const labelLayer = L.layerGroup().addTo(map);
[
  ["Quận 1", 10.7769, 106.7009],
  ["Bình Thạnh", 10.803, 106.707],
  ["Tân Bình", 10.8015, 106.652],
  ["Thủ Đức", 10.849, 106.771],
  ["Nhà Bè", 10.6956, 106.7403],
].forEach(([name, lat, lon]) => {
  L.marker([lat, lon], {
    interactive: false,
    icon: L.divIcon({
      className: "district-label",
      html: name,
      iconSize: null,
    }),
  }).addTo(labelLayer);
});

let heatLayer = null;
let markers = L.layerGroup().addTo(map);

const state = {
  mode: "current",
  horizon: 1,
  model: "random_forest",
  basemap: "local",
};

const modelLabels = {
  random_forest: "Random Forest",
  gbt: "GBTRegressor",
};

const modelShortLabels = {
  random_forest: "RF",
  gbt: "GBT",
};

const freshnessLabels = {
  fresh: "mới",
  delayed: "trễ",
  stale: "cũ",
  missing: "thiếu",
  forecast: "dự báo",
};

const modeButtons = [...document.querySelectorAll(".mode-button")];
const basemapButtons = [...document.querySelectorAll(".map-style-button")];
const forecastControls = document.querySelector("#forecast-controls");
const horizon = document.querySelector("#horizon");
const modelSelect = document.querySelector("#model");
const horizonLabel = document.querySelector("#horizon-label");
const modeEyebrow = document.querySelector("#mode-eyebrow");
const panelTitle = document.querySelector("#panel-title");
const statusEl = document.querySelector("#status");
const artifactNoteEl = document.querySelector("#artifact-note");
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
  0.08: "#2fbe63",
  0.32: "#d7cb34",
  0.54: "#dc8b31",
  0.76: "#cf413c",
  1.0: "#8f2f7f",
};

function colorForAqi(aqi) {
  if (aqi <= 50) return "oklch(74% 0.17 148)";
  if (aqi <= 100) return "oklch(84% 0.17 92)";
  if (aqi <= 150) return "oklch(78% 0.16 64)";
  if (aqi <= 200) return "oklch(66% 0.2 30)";
  return "oklch(55% 0.18 343)";
}

function categoryText(aqi) {
  if (aqi === null || aqi === undefined) return "không rõ";
  if (aqi <= 50) return "tốt";
  if (aqi <= 100) return "trung bình";
  if (aqi <= 150) return "nhạy cảm";
  if (aqi <= 200) return "xấu";
  return "rất xấu";
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

function average(values) {
  const finite = values.map(Number).filter(Number.isFinite);
  if (!finite.length) return null;
  return finite.reduce((sum, value) => sum + value, 0) / finite.length;
}

function metricForHorizon(metrics = []) {
  const candidates = metrics.filter((item) => Number(item.horizon_hour) === state.horizon);
  const testRows = candidates.filter((item) => item.split === "test");
  const preferred = testRows.length ? testRows : candidates.filter((item) => item.split === "validation");
  if (!preferred.length) return null;
  return {
    mae: average(preferred.map((item) => item.mae)),
    rmse: average(preferred.map((item) => item.rmse)),
    r2: average(preferred.map((item) => item.r2)),
  };
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.json();
}

function setBasemap(basemap) {
  state.basemap = basemap;
  basemapButtons.forEach((button) => button.classList.toggle("is-active", button.dataset.basemap === basemap));

  if (basemap === "city") {
    if (!map.hasLayer(localBaseLayer)) localBaseLayer.addTo(map);
    if (!map.hasLayer(cityBaseLayer)) cityBaseLayer.addTo(map);
    return;
  }

  if (map.hasLayer(cityBaseLayer)) map.removeLayer(cityBaseLayer);
  if (!map.hasLayer(localBaseLayer)) localBaseLayer.addTo(map);
}

cityBaseLayer.on("tileerror", () => {
  if (state.basemap !== "city") return;
  setBasemap("local");
  artifactNoteEl.textContent = "Không tải được nền bản đồ TP.HCM; đang dùng nền local.";
});

function requestCityBasemap({ announce = false } = {}) {
  return fetch("https://a.tile.openstreetmap.org/11/1630/962.png", {
    mode: "no-cors",
    cache: "no-store",
  })
    .then(() => setBasemap("city"))
    .catch(() => {
      setBasemap("local");
      if (announce) {
        artifactNoteEl.textContent = "Không tải được nền bản đồ TP.HCM; đang dùng nền local.";
      }
    });
}

function setMode(mode) {
  state.mode = mode;
  modeButtons.forEach((button) => button.classList.toggle("is-active", button.dataset.mode === mode));
  forecastControls.classList.toggle("is-hidden", mode === "current");
  metricsStrip.classList.toggle("is-hidden", mode === "current");
  modeEyebrow.textContent = mode === "current" ? "Heatmap hiện tại" : "Heatmap dự báo";
  panelTitle.textContent = mode === "current" ? "AQI hiện tại" : `Dự báo H+${state.horizon}`;
}

function drawPoints(points) {
  if (heatLayer) {
    map.removeLayer(heatLayer);
  }
  markers.clearLayers();

  const heatPoints = points
    .filter((point) => Number.isFinite(Number(point.latitude)) && Number.isFinite(Number(point.longitude)))
    .map((point) => [point.latitude, point.longitude, Math.min((point.aqi || 0) / 220, 1)]);

  heatLayer = L.heatLayer(heatPoints, {
    radius: 32,
    blur: 24,
    maxZoom: 13,
    gradient: heatGradient,
  }).addTo(map);

  const bounds = [];
  points.slice(0, 28).forEach((point) => {
    if (!Number.isFinite(Number(point.latitude)) || !Number.isFinite(Number(point.longitude))) return;
    const timestamp = state.mode === "current" ? point.observation_ts : point.target_ts || point.forecast_ts;
    const marker = L.circleMarker([point.latitude, point.longitude], {
      radius: 7.5,
      weight: 2,
      color: "oklch(14% 0.03 190)",
      fillColor: colorForAqi(point.aqi || 0),
      fillOpacity: 0.94,
    }).bindPopup(`<b>AQI ${point.aqi ?? "--"} · ${categoryText(point.aqi)}</b><br>${pollutantText(point.values)}<br>${shortDate(timestamp)}`);
    markers.addLayer(marker);
    bounds.push([point.latitude, point.longitude]);
  });

  if (bounds.length) {
    map.fitBounds(bounds, { padding: [70, 70], maxZoom: 12 });
  }
}

function emptyMessage() {
  if (state.mode === "current") {
    return "Chưa có dữ liệu hiện tại. Có thể chuyển sang Dự báo để xem forecast artifact nếu có.";
  }
  return "Chưa có forecast artifact phù hợp với mô hình hoặc horizon này.";
}

function renderHotspots(points) {
  hotspotsEl.innerHTML = "";
  const top = [...points].sort((a, b) => (b.aqi || 0) - (a.aqi || 0)).slice(0, 10);
  hotspotContextEl.textContent = state.mode === "current" ? "mới nhất" : `${modelShortLabels[state.model]}, H+${state.horizon}`;

  if (!top.length) {
    const empty = document.createElement("li");
    empty.className = "empty";
    empty.textContent = emptyMessage();
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
        <strong>#${index + 1} · ${categoryText(point.aqi)}</strong>
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

function artifactText(payload) {
  const artifact = payload.artifact || {};
  if (artifact.read_error) return "Kho dữ liệu hiện tại đang lỗi đọc; API trả trạng thái rỗng an toàn.";
  if (artifact.exists === false) return "Artifact hiện chưa có sẵn.";
  if (state.mode === "forecast" && payload.target_as_of) return `Target mới nhất ${shortDate(payload.target_as_of)}.`;
  if (payload.data_as_of) return `Dữ liệu cập nhật tới ${shortDate(payload.data_as_of)}.`;
  return "Đang chờ dữ liệu từ artifact.";
}

function renderKpis(payload, points) {
  const topAqi = points.reduce((max, point) => Math.max(max, point.aqi || 0), 0);
  const freshnessCounts = points.reduce((acc, point) => {
    const key = point.freshness_status || "forecast";
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  maxAqiEl.textContent = points.length ? String(topAqi) : "--";
  pointCountEl.textContent = String(payload.count ?? points.length);
  dataAsOfEl.textContent = shortDate(payload.data_as_of || payload.target_as_of);
  freshnessEl.textContent =
    state.mode === "current"
      ? Object.entries(freshnessCounts)
          .map(([key, count]) => `${freshnessLabels[key] || key} ${count}`)
          .join(", ") || "--"
      : `${modelShortLabels[state.model]} · H+${state.horizon}`;
  artifactNoteEl.textContent = artifactText(payload);
}

async function renderMetrics() {
  if (state.mode === "current") return;
  const payload = await fetchJson(`/api/metrics?model=${encodeURIComponent(state.model)}&split=test`).catch(() => ({ metrics: [] }));
  const metric = metricForHorizon(payload.metrics || []);
  metricMaeEl.textContent = formatMetric(metric?.mae);
  metricRmseEl.textContent = formatMetric(metric?.rmse);
  metricR2El.textContent = formatMetric(metric?.r2);
}

async function loadData() {
  setMode(state.mode);
  horizonLabel.textContent = `H+${state.horizon}`;
  statusEl.textContent = "Đang tải...";
  artifactNoteEl.textContent = "Đang đọc artifact...";

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
        ? `${points.length.toLocaleString("vi-VN")} điểm · ${shortDate(payload.data_as_of)}`
        : `${points.length.toLocaleString("vi-VN")} điểm · H+${state.horizon}`;
  } else {
    statusEl.textContent = state.mode === "current" ? "Không có dữ liệu hiện tại" : "Không có forecast";
  }
}

modeButtons.forEach((button) => {
  button.addEventListener("click", () => {
    setMode(button.dataset.mode);
    loadData().catch((error) => {
      statusEl.textContent = error.message;
      artifactNoteEl.textContent = "Không tải được dữ liệu từ API.";
    });
  });
});

basemapButtons.forEach((button) => {
  button.addEventListener("click", () => {
    if (button.dataset.basemap === "city") {
      requestCityBasemap({ announce: true });
      return;
    }
    setBasemap("local");
  });
});

function autoEnableCityBasemap() {
  requestCityBasemap();
}

horizon.addEventListener("input", (event) => {
  state.horizon = Number(event.target.value);
  loadData().catch((error) => {
    statusEl.textContent = error.message;
    artifactNoteEl.textContent = "Không tải được dữ liệu forecast.";
  });
});

modelSelect.addEventListener("change", () => {
  state.model = modelSelect.value;
  loadData().catch((error) => {
    statusEl.textContent = error.message;
    artifactNoteEl.textContent = "Không tải được dữ liệu forecast.";
  });
});

autoEnableCityBasemap();
loadData().catch((error) => {
  statusEl.textContent = error.message;
  artifactNoteEl.textContent = "Không tải được dữ liệu từ API.";
});
