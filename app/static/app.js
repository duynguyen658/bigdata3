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

const horizon = document.querySelector("#horizon");
const modelSelect = document.querySelector("#model");
const horizonLabel = document.querySelector("#horizon-label");
const statusEl = document.querySelector("#status");
const maxAqiEl = document.querySelector("#max-aqi");
const hotspotsEl = document.querySelector("#hotspots");

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

function pollutantText(values) {
  const pm25 = values.pm25 === undefined ? "--" : values.pm25.toFixed(1);
  const pm10 = values.pm10 === undefined ? "--" : values.pm10.toFixed(1);
  return `PM2.5 ${pm25} | PM10 ${pm10}`;
}

async function loadForecast(hourValue) {
  const modelValue = modelSelect.value;
  horizonLabel.textContent = `${hourValue}h`;
  statusEl.textContent = "Dang cap nhat heatmap...";
  const response = await fetch(`/api/forecast?horizon=${hourValue}&model=${encodeURIComponent(modelValue)}`);
  const payload = await response.json();
  const points = payload.points || [];

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

  const top = [...points].sort((a, b) => (b.aqi || 0) - (a.aqi || 0)).slice(0, 10);
  maxAqiEl.textContent = top[0]?.aqi ?? "--";
  hotspotsEl.innerHTML = "";

  top.forEach((point, index) => {
    const marker = L.circleMarker([point.latitude, point.longitude], {
      radius: 7,
      weight: 2,
      color: "oklch(16% 0.03 185)",
      fillColor: colorForAqi(point.aqi || 0),
      fillOpacity: 0.92,
    }).bindPopup(`<b>AQI ${point.aqi}</b><br>${pollutantText(point.values)}<br>${point.forecast_ts}`);
    markers.addLayer(marker);

    const li = document.createElement("li");
    li.innerHTML = `
      <span class="aqi-chip" style="background:${colorForAqi(point.aqi || 0)}">${point.aqi ?? "--"}</span>
      <span class="place">
        <strong>Hotspot ${index + 1}</strong>
        <span>${point.latitude.toFixed(4)}, ${point.longitude.toFixed(4)}</span>
      </span>
      <span class="pollutants">${pollutantText(point.values)}</span>
    `;
    li.addEventListener("click", () => {
      map.flyTo([point.latitude, point.longitude], 13, { duration: 0.7 });
      marker.openPopup();
    });
    hotspotsEl.appendChild(li);
  });

  statusEl.textContent = points.length
    ? `${points.length.toLocaleString("vi-VN")} o luoi, ${modelSelect.options[modelSelect.selectedIndex].text}, +${hourValue}h`
    : "Chua co du lieu. Hay chay train_forecast_spark.py";
}

horizon.addEventListener("input", (event) => {
  loadForecast(Number(event.target.value)).catch((error) => {
    statusEl.textContent = error.message;
  });
});

modelSelect.addEventListener("change", () => {
  loadForecast(Number(horizon.value)).catch((error) => {
    statusEl.textContent = error.message;
  });
});

loadForecast(Number(horizon.value)).catch((error) => {
  statusEl.textContent = error.message;
});
