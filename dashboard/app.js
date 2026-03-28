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

const routePointCount = document.getElementById("routePointCount");
const routeStartTime = document.getElementById("routeStartTime");
const routeEndTime = document.getElementById("routeEndTime");
const routeMaxSpeed = document.getElementById("routeMaxSpeed");
const routeAvgSpeed = document.getElementById("routeAvgSpeed");
const routeHistoryEmpty = document.getElementById("routeHistoryEmpty");
const routeHistoryTableBody = document.querySelector("#routeHistoryTable tbody");

const bikeZoneEventCount = document.getElementById("bikeZoneEventCount");
const bikeZoneHistoryEmpty = document.getElementById("bikeZoneHistoryEmpty");
const bikeZoneHistoryTableBody = document.querySelector("#bikeZoneHistoryTable tbody");

const zoneEventsEmpty = document.getElementById("zoneEventsEmpty");
const zoneEventsTableBody = document.querySelector("#zoneEventsTable tbody");

let telemetryData = [];
let latestPositionsData = [];
let routeHistoryData = [];
let bikeZoneHistoryData = [];
let zoneEventsData = [];

let selectedBikeData = null;
let activeBikeFilter = null;

let map;
let markersLayer;
let routeLayer;
let mapHasFitted = false;
let markerRefs = {};

function initMap() {
  if (map) return;

  map = L.map("liveMap").setView([0.333, 32.57], 15);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  markersLayer = L.layerGroup().addTo(map);
  routeLayer = L.layerGroup().addTo(map);
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

function alertTypeClass(type) {
  const value = (type || "").toLowerCase();
  if (value === "overspeed") return "alert-type-overspeed";
  if (value === "long idle") return "alert-type-long-idle";
  if (value === "stale telemetry") return "alert-type-stale-telemetry";
  if (value === "restricted zone") return "alert-type-restricted-zone";
  return "";
}

function alertBorderClass(type) {
  const value = (type || "").toLowerCase();
  if (value === "overspeed") return "alert-border-overspeed";
  if (value === "long idle") return "alert-border-long-idle";
  if (value === "stale telemetry") return "alert-border-stale-telemetry";
  if (value === "restricted zone") return "alert-border-restricted-zone";
  return "";
}

function alertSeverityClass(severity) {
  const value = (severity || "").toLowerCase();
  if (value === "high") return "alert-severity-high";
  if (value === "medium") return "alert-severity-medium";
  return "alert-severity-low";
}

function zoneBadgeClass(restricted) {
  return restricted ? "zone-badge-restricted" : "zone-badge-normal";
}

function zoneBadgeText(restricted) {
  return restricted ? "Restricted" : "Normal";
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

function updateDetailsPanel(row) {
  if (!row) {
    selectedBikeEmpty.classList.remove("hidden");
    selectedBikeCard.classList.add("hidden");
    filterBikeBtn.textContent = "Filter Table";
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

  filterBikeBtn.textContent =
    activeBikeFilter && activeBikeFilter === row.bike
      ? "Remove Table Filter"
      : "Filter Table";
}

function getVisibleTelemetry() {
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

    const matchesBikeFilter = activeBikeFilter
      ? row.bike === activeBikeFilter
      : true;

    return matchesSearch && matchesBikeFilter;
  });
}

function renderTelemetry(data) {
  telemetryTableBody.innerHTML = data
    .map(
      (row) => `
      <tr class="${
        selectedBikeData && row.bike === selectedBikeData.bike ? "selected-row" : ""
      }">
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
      <div class="alert-item ${alertBorderClass(alert.type)}">
        <div class="alert-top">
          <div>
            <h3>${alert.type}</h3>
            <p><strong>Bike:</strong> ${alert.bike}</p>
          </div>
          <div class="alert-badges">
            <span class="alert-type-badge ${alertTypeClass(alert.type)}">${alert.type}</span>
            <span class="alert-severity-badge ${alertSeverityClass(alert.severity)}">${alert.severity}</span>
          </div>
        </div>
        <p><strong>Location:</strong> ${alert.location}</p>
        <p><strong>Time:</strong> ${alert.time}</p>
        <p class="alert-note">${alert.note}</p>
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

function renderRouteHistoryTable(data) {
  if (!data.length) {
    routeHistoryEmpty.classList.remove("hidden");
    routeHistoryTableBody.innerHTML = "";
    routePointCount.textContent = "0 points";
    routeStartTime.textContent = "--";
    routeEndTime.textContent = "--";
    routeMaxSpeed.textContent = "--";
    routeAvgSpeed.textContent = "--";
    return;
  }

  routeHistoryEmpty.classList.add("hidden");

  routeHistoryTableBody.innerHTML = data
    .map(
      (row) => `
      <tr>
        <td>${row.ts ?? "N/A"}</td>
        <td>${row.speed ?? "N/A"} km/h</td>
        <td>${row.zone ?? "Unknown"}</td>
        <td><span class="status-badge ${statusClass(row.status)}">${row.status ?? "Normal"}</span></td>
      </tr>
    `
    )
    .join("");

  routePointCount.textContent = `${data.length} points`;
  routeStartTime.textContent = data[0]?.ts ?? "--";
  routeEndTime.textContent = data[data.length - 1]?.ts ?? "--";

  const speeds = data
    .map((item) => Number(item.speed))
    .filter((value) => !Number.isNaN(value));

  if (speeds.length) {
    const maxSpeed = Math.max(...speeds);
    const avgSpeed = speeds.reduce((a, b) => a + b, 0) / speeds.length;
    routeMaxSpeed.textContent = `${maxSpeed.toFixed(1)} km/h`;
    routeAvgSpeed.textContent = `${avgSpeed.toFixed(1)} km/h`;
  } else {
    routeMaxSpeed.textContent = "--";
    routeAvgSpeed.textContent = "--";
  }
}

function renderBikeZoneHistory(data) {
  if (!data.length) {
    bikeZoneHistoryEmpty.classList.remove("hidden");
    bikeZoneHistoryTableBody.innerHTML = "";
    bikeZoneEventCount.textContent = "0 transitions";
    return;
  }

  bikeZoneHistoryEmpty.classList.add("hidden");
  bikeZoneEventCount.textContent = `${data.length} transitions`;

  bikeZoneHistoryTableBody.innerHTML = data
    .map(
      (row) => `
      <tr>
        <td>${row.ts ?? "N/A"}</td>
        <td>${row.previous_zone ?? "Unknown"}</td>
        <td>${row.current_zone ?? "Unknown"}</td>
        <td><span class="zone-badge ${zoneBadgeClass(row.restricted_entry)}">${zoneBadgeText(row.restricted_entry)}</span></td>
      </tr>
    `
    )
    .join("");
}

function renderZoneEvents(data) {
  if (!data.length) {
    zoneEventsEmpty.classList.remove("hidden");
    zoneEventsTableBody.innerHTML = "";
    return;
  }

  zoneEventsEmpty.classList.add("hidden");

  zoneEventsTableBody.innerHTML = data
    .map(
      (row) => `
      <tr>
        <td>${row.ts ?? "N/A"}</td>
        <td>${row.bike ?? "N/A"}</td>
        <td>${row.previous_zone ?? "Unknown"}</td>
        <td>${row.current_zone ?? "Unknown"}</td>
        <td><span class="zone-badge ${zoneBadgeClass(row.restricted_entry)}">${zoneBadgeText(row.restricted_entry)}</span></td>
      </tr>
    `
    )
    .join("");
}

function drawRouteOnMap(data) {
  routeLayer.clearLayers();

  if (!data.length) return;

  const points = data
    .map((row) => [Number(row.latitude), Number(row.longitude)])
    .filter(([lat, lon]) => !Number.isNaN(lat) && !Number.isNaN(lon));

  if (points.length < 2) {
    if (points.length === 1) {
      L.circleMarker(points[0], {
        radius: 6,
        color: "#111827",
        fillColor: "#111827",
        fillOpacity: 0.9,
      }).addTo(routeLayer);
    }
    return;
  }

  const routeLine = L.polyline(points, {
    color: "#111827",
    weight: 4,
    opacity: 0.85,
  }).addTo(routeLayer);

  L.circleMarker(points[0], {
    radius: 6,
    color: "#16a34a",
    fillColor: "#16a34a",
    fillOpacity: 1,
  })
    .bindPopup("Route start")
    .addTo(routeLayer);

  L.circleMarker(points[points.length - 1], {
    radius: 6,
    color: "#dc2626",
    fillColor: "#dc2626",
    fillOpacity: 1,
  })
    .bindPopup("Route end")
    .addTo(routeLayer);

  map.fitBounds(routeLine.getBounds(), { padding: [30, 30] });
}

async function loadBikeHistory(bikeId) {
  try {
    const data = await fetchJSON(`${API_BASE}/bike-history/${encodeURIComponent(bikeId)}`);
    routeHistoryData = data;
    renderRouteHistoryTable(routeHistoryData);
    drawRouteOnMap(routeHistoryData);
  } catch (error) {
    console.error(error);
    routeHistoryEmpty.classList.remove("hidden");
    routeHistoryEmpty.textContent = "Failed to load route history.";
    routeHistoryTableBody.innerHTML = "";
    routeLayer.clearLayers();
  }
}

async function loadBikeZoneHistory(bikeId) {
  try {
    const data = await fetchJSON(`${API_BASE}/bike-zone-history/${encodeURIComponent(bikeId)}`);
    bikeZoneHistoryData = data;
    renderBikeZoneHistory(bikeZoneHistoryData);
  } catch (error) {
    console.error(error);
    bikeZoneHistoryEmpty.classList.remove("hidden");
    bikeZoneHistoryEmpty.textContent = "Failed to load bike zone history.";
    bikeZoneHistoryTableBody.innerHTML = "";
  }
}

async function selectBike(row) {
  selectedBikeData = row;
  updateDetailsPanel(row);
  renderTelemetry(getVisibleTelemetry());
  renderMap(latestPositionsData);

  const marker = markerRefs[row.bike];
  if (marker) marker.openPopup();

  await Promise.all([
    loadBikeHistory(row.bike),
    loadBikeZoneHistory(row.bike),
  ]);
}

function clearSelection() {
  selectedBikeData = null;
  activeBikeFilter = null;
  searchInput.value = "";
  routeHistoryData = [];
  bikeZoneHistoryData = [];
  updateDetailsPanel(null);
  renderTelemetry(getVisibleTelemetry());
  renderMap(latestPositionsData);
  renderRouteHistoryTable([]);
  renderBikeZoneHistory([]);
  routeLayer.clearLayers();
}

function renderMap(data) {
  initMap();
  markersLayer.clearLayers();
  markerRefs = {};

  if (!data.length) {
    mapCount.textContent = "0 bikes on map";
    return;
  }

  const bounds = [];

  data.forEach((row) => {
    const lat = Number(row.latitude);
    const lon = Number(row.longitude);

    if (Number.isNaN(lat) || Number.isNaN(lon)) return;

    const isSelected =
      selectedBikeData && row.bike === selectedBikeData.bike;

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

    marker.on("click", async () => {
      await selectBike(row);
    });

    marker.addTo(markersLayer);
    markerRefs[row.bike] = marker;
    bounds.push([lat, lon]);
  });

  mapCount.textContent = `${bounds.length} bikes on map`;

  if (bounds.length && !mapHasFitted) {
    map.fitBounds(bounds, { padding: [30, 30] });
    mapHasFitted = true;
  }
}

function applySearch() {
  renderTelemetry(getVisibleTelemetry());
}

async function loadDashboard() {
  try {
    telemetryStatus.textContent = "Loading...";
    dbStatus.textContent = "Loading...";

    const [kpis, telemetry, alerts, speedSeries, zoneTraffic, latestPositions, zoneEvents] =
      await Promise.all([
        fetchJSON(`${API_BASE}/kpis`),
        fetchJSON(`${API_BASE}/telemetry`),
        fetchJSON(`${API_BASE}/alerts`),
        fetchJSON(`${API_BASE}/speed-series`),
        fetchJSON(`${API_BASE}/zone-traffic`),
        fetchJSON(`${API_BASE}/latest-positions`),
        fetchJSON(`${API_BASE}/zone-events`),
      ]);

    telemetryData = telemetry;
    latestPositionsData = latestPositions;
    zoneEventsData = zoneEvents;

    if (selectedBikeData) {
      const refreshedSelectedBike = latestPositionsData.find(
        (item) => item.bike === selectedBikeData.bike
      );
      if (refreshedSelectedBike) {
        selectedBikeData = refreshedSelectedBike;
      } else {
        selectedBikeData = null;
        activeBikeFilter = null;
        routeHistoryData = [];
        bikeZoneHistoryData = [];
      }
    }

    renderKpis(kpis);
    renderAlerts(alerts);
    renderBarList(speedSeriesBox, speedSeries, "time", "speed");
    renderBarList(zoneTrafficBox, zoneTraffic, "zone", "trips");
    renderZoneEvents(zoneEventsData);
    updateDetailsPanel(selectedBikeData);
    renderTelemetry(getVisibleTelemetry());
    renderMap(latestPositionsData);

    if (!selectedBikeData) {
      renderRouteHistoryTable([]);
      renderBikeZoneHistory([]);
      routeLayer.clearLayers();
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
  if (!selectedBikeData || !map) return;

  const lat = Number(selectedBikeData.latitude);
  const lon = Number(selectedBikeData.longitude);

  if (!Number.isNaN(lat) && !Number.isNaN(lon)) {
    map.setView([lat, lon], 17);
    const marker = markerRefs[selectedBikeData.bike];
    if (marker) marker.openPopup();
  }
});

filterBikeBtn.addEventListener("click", () => {
  if (!selectedBikeData) return;

  if (activeBikeFilter === selectedBikeData.bike) {
    activeBikeFilter = null;
  } else {
    activeBikeFilter = selectedBikeData.bike;
  }

  updateDetailsPanel(selectedBikeData);
  renderTelemetry(getVisibleTelemetry());
});

initMap();
updateDetailsPanel(null);
renderRouteHistoryTable([]);
renderBikeZoneHistory([]);
renderZoneEvents([]);
loadDashboard();
setInterval(loadDashboard, 15000);