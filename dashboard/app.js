const API_BASE = "http://127.0.0.1:8001/api";

const kpiCards = document.getElementById("kpiCards");
const telemetryTableBody = document.querySelector("#telemetryTable tbody");
const alertsList = document.getElementById("alertsList");
const speedSeriesBox = document.getElementById("speedSeries");
const zoneTrafficBox = document.getElementById("zoneTraffic");
const refreshBtn = document.getElementById("refreshBtn");
const lastUpdated = document.getElementById("lastUpdated");
const searchInput = document.getElementById("searchInput");
const telemetryStatus = document.getElementById("telemetryStatus");
const dbStatus = document.getElementById("dbStatus");
const mapCount = document.getElementById("mapCount");

const selectedBikeEmpty = document.getElementById("selectedBikeEmpty");
const selectedBikeCard = document.getElementById("selectedBikeCard");
const clearSelectionBtn = document.getElementById("clearSelectionBtn");
const focusBikeBtn = document.getElementById("focusBikeBtn");
const filterBikeBtn = document.getElementById("filterBikeBtn");

const detailBike = document.getElementById("detailBike");
const detailDriver = document.getElementById("detailDriver");
const detailTrip = document.getElementById("detailTrip");
const detailSpeed = document.getElementById("detailSpeed");
const detailEngine = document.getElementById("detailEngine");
const detailZone = document.getElementById("detailZone");
const detailTime = document.getElementById("detailTime");
const detailLat = document.getElementById("detailLat");
const detailLon = document.getElementById("detailLon");
const detailStatus = document.getElementById("detailStatus");

let telemetryData = [];
let latestPositionsData = [];
let selectedBike = null;
let map;
let markersLayer;
let mapHasFitted = false;
let selectedMarkerCoords = null;

function initMap() {
  if (map) return;

  map = L.map("liveMap").setView([0.333, 32.57], 15);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  markersLayer = L.layerGroup().addTo(map);
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Failed to fetch ${url}`);
  }
  return res.json();
}

function statusClass(status) {
  const value = (status || "").toLowerCase();
  if (value === "overspeed") return "status-overspeed";
  if (value === "idle") return "status-idle";
  if (value === "watch") return "status-watch";
  return "status-normal";
}

function markerColor(status) {
  const value = (status || "").toLowerCase();
  if (value === "overspeed") return "#dc2626";
  if (value === "idle") return "#f59e0b";
  if (value === "watch") return "#2563eb";
  return "#16a34a";
}

function renderKpis(data) {
  kpiCards.innerHTML = data
    .map(
      (item) => `
      <div class="card">
        <h3>${item.label}</h3>
        <p>${item.value}</p>
        <small>${item.hint || ""}</small>
      </div>
    `
    )
    .join("");
}

function getFilteredTelemetry() {
  const q = searchInput.value.toLowerCase().trim();

  return telemetryData.filter((row) => {
    const matchesSearch = [
      row.bike,
      row.driver,
      row.trip,
      row.zone,
      row.status,
      row.engine,
      row.ts,
    ]
      .join(" ")
      .toLowerCase()
      .includes(q);

    const matchesSelectedBike = selectedBike ? row.bike === selectedBike : true;

    return matchesSearch && matchesSelectedBike;
  });
}

function renderTelemetry(data) {
  telemetryTableBody.innerHTML = data
    .map(
      (row) => `
      <tr class="${selectedBike && row.bike === selectedBike ? "selected-row" : ""}">
        <td>${row.bike ?? "N/A"}</td>
        <td>${row.driver ?? "N/A"}</td>
        <td>${row.trip ?? "N/A"}</td>
        <td>${row.speed ?? "N/A"} km/h</td>
        <td>${row.engine ?? "N/A"}</td>
        <td>${row.zone ?? "Unknown"}</td>
        <td><span class="status-badge ${statusClass(row.status)}">${row.status ?? "Normal"}</span></td>
        <td>${row.ts ?? "N/A"}</td>
      </tr>
    `
    )
    .join("");
}

function renderAlerts(data) {
  if (!data.length) {
    alertsList.innerHTML = `<div class="alert-item"><p>No active alerts right now.</p></div>`;
    return;
  }

  alertsList.innerHTML = data
    .map(
      (alert) => `
      <div class="alert-item">
        <h3>${alert.type} - ${alert.severity}</h3>
        <p><strong>Bike:</strong> ${alert.bike}</p>
        <p><strong>Location:</strong> ${alert.location}</p>
        <p><strong>Time:</strong> ${alert.time}</p>
        <p>${alert.note}</p>
      </div>
    `
    )
    .join("");
}

function renderBarList(container, data, labelKey, valueKey) {
  if (!data.length) {
    container.innerHTML = "<p class='no-data'>No data available.</p>";
    return;
  }

  const maxValue = Math.max(...data.map((item) => Number(item[valueKey]) || 0), 1);

  container.innerHTML = data
    .map((item) => {
      const value = Number(item[valueKey]) || 0;
      const width = (value / maxValue) * 100;

      return `
        <div class="bar-row">
          <span>${item[labelKey]}</span>
          <div class="bar-track">
            <div class="bar-fill" style="width:${width}%"></div>
          </div>
          <strong>${value}</strong>
        </div>
      `;
    })
    .join("");
}

function updateDetailsPanel(row) {
  if (!row) {
    selectedBikeEmpty.classList.remove("hidden");
    selectedBikeCard.classList.add("hidden");
    return;
  }

  selectedBikeEmpty.classList.add("hidden");
  selectedBikeCard.classList.remove("hidden");

  detailBike.textContent = row.bike ?? "N/A";
  detailDriver.textContent = row.driver ?? "N/A";
  detailTrip.textContent = row.trip ?? "N/A";
  detailSpeed.textContent = `${row.speed ?? "N/A"} km/h`;
  detailEngine.textContent = row.engine ?? "N/A";
  detailZone.textContent = row.zone ?? "Unknown";
  detailTime.textContent = row.ts ?? "N/A";
  detailLat.textContent = row.latitude ?? "N/A";
  detailLon.textContent = row.longitude ?? "N/A";

  detailStatus.textContent = row.status ?? "Normal";
  detailStatus.className = `status-badge ${statusClass(row.status)}`;
}

function selectBike(row) {
  selectedBike = row?.bike || null;
  selectedMarkerCoords = row ? [Number(row.latitude), Number(row.longitude)] : null;
  updateDetailsPanel(row);
  renderTelemetry(getFilteredTelemetry());
}

function clearSelection() {
  selectedBike = null;
  selectedMarkerCoords = null;
  updateDetailsPanel(null);
  renderTelemetry(getFilteredTelemetry());
}

function renderMap(data) {
  initMap();
  markersLayer.clearLayers();

  if (!data.length) {
    mapCount.textContent = "0 bikes on map";
    return;
  }

  const bounds = [];

  data.forEach((row) => {
    const lat = Number(row.latitude);
    const lon = Number(row.longitude);

    if (Number.isNaN(lat) || Number.isNaN(lon)) return;

    const isSelected = selectedBike && row.bike === selectedBike;

    const marker = L.circleMarker([lat, lon], {
      radius: isSelected ? 11 : 8,
      color: markerColor(row.status),
      fillColor: markerColor(row.status),
      fillOpacity: 0.95,
      weight: isSelected ? 4 : 2,
    });

    marker.bindPopup(`
      <div>
        <strong>Bike:</strong> ${row.bike ?? "N/A"}<br>
        <strong>Driver:</strong> ${row.driver ?? "N/A"}<br>
        <strong>Trip:</strong> ${row.trip ?? "N/A"}<br>
        <strong>Speed:</strong> ${row.speed ?? "N/A"} km/h<br>
        <strong>Zone:</strong> ${row.zone ?? "Unknown"}<br>
        <strong>Status:</strong> ${row.status ?? "Normal"}<br>
        <strong>Time:</strong> ${row.ts ?? "N/A"}
      </div>
    `);

    marker.on("click", () => {
      selectBike(row);
      marker.openPopup();
      renderMap(latestPositionsData);
    });

    marker.addTo(markersLayer);
    bounds.push([lat, lon]);
  });

  mapCount.textContent = `${bounds.length} bikes on map`;

  if (bounds.length && !mapHasFitted) {
    map.fitBounds(bounds, { padding: [30, 30] });
    mapHasFitted = true;
  }
}

function applySearch() {
  renderTelemetry(getFilteredTelemetry());
}

async function loadDashboard() {
  try {
    telemetryStatus.textContent = "Loading...";
    dbStatus.textContent = "Loading...";

    const [kpis, telemetry, alerts, speedSeries, zoneTraffic, latestPositions] =
      await Promise.all([
        fetchJSON(`${API_BASE}/kpis`),
        fetchJSON(`${API_BASE}/telemetry`),
        fetchJSON(`${API_BASE}/alerts`),
        fetchJSON(`${API_BASE}/speed-series`),
        fetchJSON(`${API_BASE}/zone-traffic`),
        fetchJSON(`${API_BASE}/latest-positions`),
      ]);

    telemetryData = telemetry;
    latestPositionsData = latestPositions;

    renderKpis(kpis);
    renderTelemetry(getFilteredTelemetry());
    renderAlerts(alerts);
    renderBarList(speedSeriesBox, speedSeries, "time", "speed");
    renderBarList(zoneTrafficBox, zoneTraffic, "zone", "trips");
    renderMap(latestPositionsData);

    if (selectedBike) {
      const selectedRow = latestPositionsData.find((item) => item.bike === selectedBike);
      if (selectedRow) {
        updateDetailsPanel(selectedRow);
      } else {
        clearSelection();
      }
    }

    telemetryStatus.textContent = "Online";
    dbStatus.textContent = "Healthy";
    lastUpdated.textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    console.error(error);
    telemetryStatus.textContent = "Error";
    dbStatus.textContent = "Issue";
    alertsList.innerHTML = `<div class="error-box">${error.message}</div>`;
  }
}

refreshBtn.addEventListener("click", loadDashboard);
searchInput.addEventListener("input", applySearch);
clearSelectionBtn.addEventListener("click", clearSelection);

focusBikeBtn.addEventListener("click", () => {
  if (selectedMarkerCoords && map) {
    map.setView(selectedMarkerCoords, 17);
  }
});

filterBikeBtn.addEventListener("click", () => {
  if (selectedBike) {
    searchInput.value = selectedBike;
    applySearch();
  }
});

initMap();
updateDetailsPanel(null);
loadDashboard();
setInterval(loadDashboard, 15000);