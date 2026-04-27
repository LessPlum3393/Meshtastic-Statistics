// ─── Meshtastic Statistics Client ──────────────────────────────────────────
(function () {
  'use strict';

  // ─── State ─────────────────────────────────────────────────────────────────
  const state = {
    nodes: new Map(),
    markers: new Map(),
    ws: null,
    map: null,
    clusterGroup: null,
    selectedNodeId: null,
    reconnectTimer: null,
    nodeListDirty: false,
    nodeListTimer: null,
    activeTab: 'map',
    // Chart state
    charts: {},
    history: [],           // [{ t, n, p }] from server
    serverStartedAt: null,
    lastStats: null,
    chartUpdateTimer: null,
    // Time range selections
    ranges: { nodes: 'lifetime', packets: 'lifetime' },
    // Connection lines between nodes
    connectionLines: [],
  };

  // ─── Role Labels ───────────────────────────────────────────────────────────
  const ROLE_LABELS = {
    0: 'Client', 1: 'Client Mute', 2: 'Router', 3: 'Router Client',
    4: 'Repeater', 5: 'Tracker', 6: 'Sensor', 7: 'TAK',
    8: 'Client Hidden', 9: 'Lost & Found', 10: 'TAK Tracker',
  };

  const ROLE_CLASSES = {
    2: 'role-router', 3: 'role-router',
    5: 'role-tracker', 10: 'role-tracker',
    6: 'role-sensor',
  };

  const ROLE_COLORS = {
    'Client': '#06b6d4', 'Client Mute': '#0891b2',
    'Router': '#8b5cf6', 'Router Client': '#7c3aed',
    'Repeater': '#a78bfa', 'Tracker': '#f59e0b',
    'Sensor': '#10b981', 'TAK': '#ef4444',
    'Client Hidden': '#64748b', 'Lost & Found': '#94a3b8',
    'TAK Tracker': '#f87171',
  };

  const HW_MODELS = {
    0: 'Unset', 1: 'TLORA V2', 2: 'TLORA V1', 3: 'TLORA V2.1-1.6',
    4: 'T-Beam', 5: 'Heltec V2.0', 6: 'T-Beam v0.7', 7: 'T-Echo',
    8: 'TLORA V1-1.3', 9: 'RAK4631', 10: 'Heltec V2.1', 11: 'Heltec V1',
    12: 'T-Beam S3 Core', 13: 'RAK11200', 14: 'NANO G1',
    15: 'TLORA V2.1-1.8', 16: 'TLORA T3-S3', 25: 'Station G1',
    31: 'Station G2', 37: 'Portduino', 43: 'Heltec V3',
    44: 'Heltec WSL V3', 48: 'Heltec Wireless Tracker',
    49: 'Heltec Wireless Paper', 50: 'T-Deck', 51: 'T-Watch S3',
    59: 'unPhone', 65: 'Heltec Capsule V3', 70: 'SenseCAP Indicator',
    71: 'Tracker T1000-E',
  };

  const HW_COLORS = [
    '#06b6d4', '#3b82f6', '#8b5cf6', '#10b981', '#f59e0b',
    '#ef4444', '#ec4899', '#14b8a6', '#a78bfa', '#f97316',
    '#6366f1', '#22d3ee', '#84cc16', '#f43f5e', '#0ea5e9',
  ];

  // ─── Range Helpers ────────────────────────────────────────────────────────
  const RANGE_MS = {
    '1d': 24 * 60 * 60 * 1000,
    '7d': 7 * 24 * 60 * 60 * 1000,
    '1m': 30 * 24 * 60 * 60 * 1000,
    '1y': 365 * 24 * 60 * 60 * 1000,
    'lifetime': Infinity,
  };

  function filterByRange(data, range) {
    if (range === 'lifetime') return data;
    const cutoff = Date.now() - RANGE_MS[range];
    return data.filter(d => d.t >= cutoff);
  }

  // ─── Geo helpers ──────────────────────────────────────────────────────────
  function distanceMeters(aLat, aLon, bLat, bLon) {
    const toRad = (x) => x * Math.PI / 180;
    const R = 6371000;
    const dLat = toRad(bLat - aLat);
    const dLon = toRad(bLon - aLon);
    const lat1 = toRad(aLat);
    const lat2 = toRad(bLat);
    const s1 = Math.sin(dLat / 2);
    const s2 = Math.sin(dLon / 2);
    const h = s1 * s1 + Math.cos(lat1) * Math.cos(lat2) * s2 * s2;
    return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
  }

  function formatTimeLabel(ts, range) {
    const d = new Date(ts);
    if (range === '1d') return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (range === '7d') return d.toLocaleDateString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' });
    if (range === '1m' || range === '1y') return d.toLocaleDateString([], { month: 'short', day: 'numeric' });
    // lifetime
    const age = Date.now() - (state.serverStartedAt || Date.now());
    if (age < 24 * 60 * 60 * 1000) return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    return d.toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }

  // ─── Tab Switching ────────────────────────────────────────────────────────
  function switchTab(tabId) {
    state.activeTab = tabId;
    document.querySelectorAll('.tab-btn').forEach(btn =>
      btn.classList.toggle('active', btn.dataset.tab === tabId)
    );
    document.getElementById('tabContentMap').classList.toggle('active', tabId === 'map');
    document.getElementById('tabContentCharts').classList.toggle('active', tabId === 'charts');

    if (tabId === 'map' && state.map) {
      setTimeout(() => state.map.invalidateSize(), 100);
    }
    if (tabId === 'charts') {
      updateAllCharts();
    }
  }

  // ─── Map Initialization ────────────────────────────────────────────────────
  function initMap() {
    state.map = L.map('map', {
      center: [20, 0],
      zoom: 3,
      minZoom: 2,
      maxZoom: 19,
      zoomControl: true,
      attributionControl: true,
      preferCanvas: true,
    });

    // Basemaps
    const osmUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';

    // OpenStreetMap Standard (Dark) - CSS-filtered tiles to keep "Standard" styling but dark.
    const osmStandardDark = L.tileLayer(osmUrl, {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      subdomains: 'abc',
      maxZoom: 19,
      detectRetina: true,
      className: 'osm-tiles osm-tiles-dark',
    }).addTo(state.map);

    const osmStandardLight = L.tileLayer(osmUrl, {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      subdomains: 'abc',
      maxZoom: 19,
      detectRetina: true,
      className: 'osm-tiles',
    });

    const cartoDark = L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/">CARTO</a>',
      subdomains: 'abcd',
      maxZoom: 19,
      detectRetina: true,
    });

    L.control.layers(
      { 'OpenStreetMap (Dark)': osmStandardDark, 'OpenStreetMap (Light)': osmStandardLight, 'CARTO Dark': cartoDark },
      undefined,
      { position: 'topright' }
    ).addTo(state.map);

    state.map.attributionControl.setPrefix(false);
    state.map.attributionControl.addAttribution('Meshtastic');

    L.control.scale({ imperial: false }).addTo(state.map);

    state.clusterGroup = L.markerClusterGroup({
      maxClusterRadius: 50,
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
      zoomToBoundsOnClick: true,
      disableClusteringAtZoom: 16,
      animate: true,
      // Reduce jitter while zooming/panning.
      updateWhenZooming: false,
      updateWhenIdle: true,
      animateAddingMarkers: false,
      chunkedLoading: true,
      chunkInterval: 100,
      chunkDelay: 10,
      iconCreateFunction: createClusterIcon,
    });
    state.map.addLayer(state.clusterGroup);
  }

  function createClusterIcon(cluster) {
    const count = cluster.getChildCount();
    let sizeClass, size;
    if (count < 10) { sizeClass = 'cluster-small'; size = 36; }
    else if (count < 50) { sizeClass = 'cluster-medium'; size = 44; }
    else if (count < 200) { sizeClass = 'cluster-large'; size = 52; }
    else { sizeClass = 'cluster-xlarge'; size = 58; }

    return L.divIcon({
      html: `<div class="cluster-icon ${sizeClass}"><span>${count}</span></div>`,
      className: 'mesh-cluster',
      iconSize: L.point(size, size),
      iconAnchor: L.point(size / 2, size / 2),
    });
  }

  // ─── Marker Creation ──────────────────────────────────────────────────────
  function createMarker(node) {
    if (!node.position) return null;
    const roleClass = ROLE_CLASSES[(node.user && node.user.role) || 0] || '';
    const icon = L.divIcon({
      className: 'mesh-marker',
      html: `<div class="marker-pulse"></div><div class="marker-dot ${roleClass}"></div>`,
      iconSize: [14, 14],
      iconAnchor: [7, 7],
    });
    const marker = L.marker([node.position.lat, node.position.lon], { icon });
    marker._meshRoleClass = roleClass;
    marker.on('click', () => showNodePopup(node, marker));
    return marker;
  }

  function showNodePopup(node, marker) {
    // Use fresh node data from state
    const n = state.nodes.get(node.id) || node;
    state.selectedNodeId = n.id;
    scheduleNodeListUpdate();
    const name = (n.user && n.user.longName) || n.id;
    const shortName = (n.user && n.user.shortName) || '??';
    const role = ROLE_LABELS[(n.user && n.user.role) || 0] || 'Unknown';
    const hwModel = HW_MODELS[(n.user && n.user.hwModel) || 0] || 'Unknown';
    const lastSeen = timeAgo(n.lastHeard);

    // Count connections for display
    const connectionCount = countConnections(n.id);

    let h = `<div class="popup-content"><div class="popup-header">
      <div class="popup-avatar">${escHtml(shortName)}</div>
      <div><div class="popup-title">${escHtml(name)}</div>
      <div class="popup-subtitle">${escHtml(n.id)}</div></div></div>
      <div class="popup-grid">
      <div class="popup-field"><div class="popup-label">Latitude</div><div class="popup-value mono">${n.position.lat.toFixed(6)}</div></div>
      <div class="popup-field"><div class="popup-label">Longitude</div><div class="popup-value mono">${n.position.lon.toFixed(6)}</div></div>
      <div class="popup-field"><div class="popup-label">Role</div><div class="popup-value">${role}</div></div>
      <div class="popup-field"><div class="popup-label">Hardware</div><div class="popup-value">${hwModel}</div></div>`;
    if (n.position.altitude) h += `<div class="popup-field"><div class="popup-label">Altitude</div><div class="popup-value">${n.position.altitude}m</div></div>`;
    if (n.telemetry && n.telemetry.batteryLevel > 0) h += `<div class="popup-field"><div class="popup-label">Battery</div><div class="popup-value">${n.telemetry.batteryLevel}%</div></div>`;
    if (n.snr) h += `<div class="popup-field"><div class="popup-label">SNR</div><div class="popup-value">${n.snr.toFixed(1)} dB</div></div>`;
    if (n.mapReport && n.mapReport.firmwareVersion) h += `<div class="popup-field"><div class="popup-label">Firmware</div><div class="popup-value">${escHtml(n.mapReport.firmwareVersion)}</div></div>`;
    if (connectionCount > 0) h += `<div class="popup-field"><div class="popup-label">Connections</div><div class="popup-value">${connectionCount} node${connectionCount !== 1 ? 's' : ''}</div></div>`;
    h += `<div class="popup-field full"><div class="popup-label">Last Seen</div><div class="popup-value">${lastSeen}</div></div></div></div>`;

    // Unbind any existing popup first, then rebind — fixes second-click bug
    marker.unbindPopup();
    marker.bindPopup(h, { maxWidth: 320, className: 'mesh-popup' }).openPopup();

    // Draw connection lines to neighbors
    drawConnectionLines(n.id);

    // Clear lines when popup closes
    marker.on('popupclose', () => {
      clearConnectionLines();
      if (state.selectedNodeId === n.id) {
        state.selectedNodeId = null;
        scheduleNodeListUpdate();
      }
    });
  }

  // ─── Node Upsert ──────────────────────────────────────────────────────────
  function upsertNode(node) {
    const prev = state.nodes.get(node.id) || null;
    const prevPos = prev && prev.position;
    if (node.position) {
      if (state.markers.has(node.id)) {
        const m = state.markers.get(node.id);

        // Avoid "crazy moving" pins: ignore tiny jitter and throttle rapid moves.
        // Still updates node metadata in state.nodes every time.
        const now = Date.now();
        const lastMoveAt = m._meshLastMoveAt || 0;
        const meters = prevPos ? distanceMeters(prevPos.lat, prevPos.lon, node.position.lat, node.position.lon) : Infinity;
        const tooSoon = now - lastMoveAt < 1500;
        const tinyJitter = meters < 15;

        // Reject obvious outliers (bad packets) that create huge teleports.
        const lastAcceptedAt = m._meshLastAcceptedPosAt || 0;
        const dt = lastAcceptedAt ? (now - lastAcceptedAt) : 0;
        const speed = dt > 0 ? (meters / (dt / 1000)) : 0; // m/s
        const isTeleport = meters > 2000 && dt > 0 && speed > 200; // >2km at >720km/h

        if (!isTeleport && !(tooSoon && tinyJitter)) {
          m._meshLastMoveAt = now;
          m._meshLastAcceptedPosAt = now;
          m.setLatLng([node.position.lat, node.position.lon]);
        } else if (isTeleport && prevPos) {
          // Keep state consistent with what the user sees on the map.
          node = Object.assign({}, node, { position: prevPos });
        }

        const rc = ROLE_CLASSES[(node.user && node.user.role) || 0] || '';
        if (m._meshRoleClass !== rc) {
          m._meshRoleClass = rc;
          m.setIcon(L.divIcon({
            className: 'mesh-marker',
            html: `<div class="marker-pulse"></div><div class="marker-dot ${rc}"></div>`,
            iconSize: [14, 14], iconAnchor: [7, 7],
          }));
        }
      } else {
        const m = createMarker(node);
        if (m) { state.markers.set(node.id, m); state.clusterGroup.addLayer(m); }
      }
    }

    state.nodes.set(node.id, node);
    scheduleNodeListUpdate();
  }

  // ─── Compact Node Deserialization ─────────────────────────────────────────
  function expandCompactNode(c) {
    const node = { id: c.id, lastHeard: c.lh, position: null, user: null, telemetry: null, snr: c.snr || 0, rssi: 0, mapReport: null, neighbors: [] };
    if (c.p) node.position = { lat: c.p[0], lon: c.p[1], altitude: c.p[2] || 0 };
    if (c.u) node.user = { longName: c.u.l || '', shortName: c.u.s || '', hwModel: c.u.h || 0, role: c.u.r || 0 };
    if (c.bat) node.telemetry = { batteryLevel: c.bat };
    if (c.fw) node.mapReport = { firmwareVersion: c.fw };
    if (c.nb) node.neighbors = c.nb.map(n => ({ nodeId: n.id, snr: n.snr || 0 }));
    return node;
  }

  // ─── Batch Node Loading ───────────────────────────────────────────────────
  function batchLoadNodes(compactNodes) {
    const newMarkers = [];
    for (let i = 0; i < compactNodes.length; i++) {
      const node = expandCompactNode(compactNodes[i]);
      state.nodes.set(node.id, node);
      if (node.position && !state.markers.has(node.id)) {
        const m = createMarker(node);
        if (m) { state.markers.set(node.id, m); newMarkers.push(m); }
      }
    }
    if (newMarkers.length > 0) state.clusterGroup.addLayers(newMarkers);
    scheduleNodeListUpdate();
  }

  // ─── Reset on Server Restart ──────────────────────────────────────────────
  function resetState(startedAt) {
    if (state.serverStartedAt && state.serverStartedAt !== startedAt) {
      // Server restarted — clear everything
      state.nodes.clear();
      state.markers.forEach(m => state.clusterGroup.removeLayer(m));
      state.markers.clear();
      state.history = [];
    }
    state.serverStartedAt = startedAt;
  }

  // ─── Debounced Node List ──────────────────────────────────────────────────
  function scheduleNodeListUpdate() {
    state.nodeListDirty = true;
    if (!state.nodeListTimer) {
      state.nodeListTimer = setTimeout(() => {
        state.nodeListTimer = null;
        if (state.nodeListDirty) { state.nodeListDirty = false; updateNodeList(); }
      }, 500);
    }
  }

  function updateNodeList() {
    const container = document.getElementById('nodeList');
    const term = document.getElementById('searchInput').value.toLowerCase();
    const all = [...state.nodes.values()].filter(n => n.position).sort((a, b) => b.lastHeard - a.lastHeard);
    const filtered = all.filter(n => {
      if (!term) return true;
      return ((n.user && n.user.longName) || '').toLowerCase().includes(term) || n.id.toLowerCase().includes(term);
    });

    if (!filtered.length) {
      container.innerHTML = `<div class="node-list-empty"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="32" height="32" opacity="0.4"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg><p>${term ? 'No matching nodes' : 'Waiting for nodes...'}</p><span>${term ? 'Try a different search term' : 'Nodes will appear as data arrives'}</span></div>`;
      return;
    }

    const vis = filtered.slice(0, 100);
    const frag = document.createDocumentFragment();
    vis.forEach(n => {
      const d = document.createElement('div');
      d.className = 'node-card' + (state.selectedNodeId === n.id ? ' active' : '');
      d.innerHTML = `<div class="node-avatar">${escHtml((n.user && n.user.shortName) || '??')}</div><div class="node-card-info"><div class="node-card-name">${escHtml((n.user && n.user.longName) || n.id)}</div><div class="node-card-id">${escHtml(n.id)}</div></div><div class="node-card-time">${timeAgo(n.lastHeard)}</div>`;
      d.addEventListener('click', () => {
        state.selectedNodeId = n.id;
        switchTab('map');
        if (n.position) {
          state.map.flyTo([n.position.lat, n.position.lon], 14, { duration: 1 });
          const mk = state.markers.get(n.id);
          if (mk) state.clusterGroup.zoomToShowLayer(mk, () => showNodePopup(n, mk));
        }
        updateNodeList();
      });
      frag.appendChild(d);
    });

    container.innerHTML = '';
    if (filtered.length > 100) {
      const notice = document.createElement('div');
      notice.className = 'node-list-truncated';
      notice.textContent = `Showing 100 of ${filtered.length} nodes. Use search to filter.`;
      container.appendChild(notice);
    }
    container.appendChild(frag);
  }

  // ─── Stats Update ─────────────────────────────────────────────────────────
  function updateStats(s) {
    const n = (s && Number.isFinite(s.nodesWithPosition)) ? s.nodesWithPosition : [...state.nodes.values()].filter(n => n.position).length;
    document.querySelector('#statNodes span').textContent = `${formatNumber(n)} nodes`;
    if (s) {
      state.lastStats = s;
      document.querySelector('#statPackets span').textContent = `${formatNumber(s.totalPackets)} packets`;
    }
  }

  // ─── Append live history point ────────────────────────────────────────────
  function appendLiveHistory() {
    const n = (state.lastStats && Number.isFinite(state.lastStats.nodesWithPosition)) ? state.lastStats.nodesWithPosition : [...state.nodes.values()].filter(n => n.position).length;
    const p = (state.lastStats && state.lastStats.totalPackets) || 0;
    state.history.push({ t: Date.now(), n, p });
    scheduleChartUpdate();
  }

  function scheduleChartUpdate() {
    if (!state.chartUpdateTimer) {
      state.chartUpdateTimer = setTimeout(() => {
        state.chartUpdateTimer = null;
        if (state.activeTab === 'charts') updateAllCharts();
      }, 2000);
    }
  }

  // ─── Chart Initialization ─────────────────────────────────────────────────
  function initCharts() {
    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = 'rgba(99, 179, 237, 0.1)';
    Chart.defaults.font.family = "'Inter', -apple-system, BlinkMacSystemFont, sans-serif";

    const tooltipOpts = {
      backgroundColor: 'rgba(17, 24, 39, 0.95)',
      borderColor: 'rgba(99, 179, 237, 0.3)',
      borderWidth: 1, padding: 12, cornerRadius: 8, displayColors: false,
      titleFont: { weight: '600' },
    };

    // Active Nodes
    const ctx1 = document.getElementById('activeNodesChart').getContext('2d');
    state.charts.activeNodes = new Chart(ctx1, {
      type: 'line',
      data: {
        labels: [], datasets: [{
          label: 'Active Nodes', data: [],
          borderColor: '#06b6d4', backgroundColor: makeGradient(ctx1, '#06b6d4'),
          fill: true, tension: 0.4, borderWidth: 2,
          pointRadius: 0, pointHoverRadius: 5,
          pointHoverBackgroundColor: '#06b6d4', pointHoverBorderColor: '#fff', pointHoverBorderWidth: 2,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        plugins: { legend: { display: false }, tooltip: { ...tooltipOpts, callbacks: { label: i => `${i.formattedValue} active nodes` } } },
        scales: {
          x: { grid: { display: false }, ticks: { maxTicksLimit: 10, font: { size: 11 } } },
          y: { beginAtZero: true, grid: { color: 'rgba(99,179,237,0.06)' }, ticks: { font: { size: 11 }, precision: 0 } },
        },
      },
    });

    // Packets
    const ctx2 = document.getElementById('packetsChart').getContext('2d');
    state.charts.packets = new Chart(ctx2, {
      type: 'line',
      data: {
        labels: [], datasets: [{
          label: 'Total Packets', data: [],
          borderColor: '#8b5cf6', backgroundColor: makeGradient(ctx2, '#8b5cf6'),
          fill: true, tension: 0.4, borderWidth: 2,
          pointRadius: 0, pointHoverRadius: 5,
          pointHoverBackgroundColor: '#8b5cf6', pointHoverBorderColor: '#fff', pointHoverBorderWidth: 2,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        interaction: { intersect: false, mode: 'index' },
        plugins: { legend: { display: false }, tooltip: { ...tooltipOpts, callbacks: { label: i => `${formatNumber(Number(i.raw))} packets` } } },
        scales: {
          x: { grid: { display: false }, ticks: { maxTicksLimit: 10, font: { size: 11 } } },
          y: { beginAtZero: true, grid: { color: 'rgba(99,179,237,0.06)' }, ticks: { font: { size: 11 }, callback: v => formatNumber(v) } },
        },
      },
    });

    // Role Doughnut
    const ctx3 = document.getElementById('roleChart').getContext('2d');
    state.charts.role = new Chart(ctx3, {
      type: 'doughnut',
      data: { labels: [], datasets: [{ data: [], backgroundColor: [], borderColor: '#111827', borderWidth: 2, hoverOffset: 6 }] },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '65%',
        plugins: {
          legend: { position: 'right', labels: { padding: 16, usePointStyle: true, pointStyleWidth: 10, font: { size: 12 } } },
          tooltip: { ...tooltipOpts, displayColors: true, callbacks: { label: i => ` ${i.label}: ${i.formattedValue} nodes` } },
        },
      },
    });

    // Hardware Bar
    const ctx4 = document.getElementById('hwChart').getContext('2d');
    state.charts.hw = new Chart(ctx4, {
      type: 'bar',
      data: { labels: [], datasets: [{ label: 'Nodes', data: [], backgroundColor: HW_COLORS, borderRadius: 4, barPercentage: 0.7 }] },
      options: {
        responsive: true, maintainAspectRatio: false, indexAxis: 'y',
        plugins: { legend: { display: false }, tooltip: { ...tooltipOpts } },
        scales: {
          x: { beginAtZero: true, grid: { color: 'rgba(99,179,237,0.06)' }, ticks: { precision: 0, font: { size: 11 } } },
          y: { grid: { display: false }, ticks: { font: { size: 11 } } },
        },
      },
    });
  }

  function makeGradient(ctx, color) {
    const g = ctx.createLinearGradient(0, 0, 0, 300);
    g.addColorStop(0, color + '40');
    g.addColorStop(1, color + '05');
    return g;
  }

  // ─── Chart Updates ────────────────────────────────────────────────────────
  function updateAllCharts() {
    updateTimeSeriesChart('activeNodes', 'nodes');
    updateTimeSeriesChart('packets', 'packets');
    updateRoleChart();
    updateHwChart();
    updateChartFooterStats();
  }

  function updateTimeSeriesChart(chartKey, rangeKey) {
    const chart = state.charts[chartKey];
    if (!chart) return;
    const range = state.ranges[rangeKey];
    const filtered = filterByRange(state.history, range);

    // Downsample if too many points
    const maxPts = 200;
    const data = filtered.length > maxPts ? downsample(filtered, maxPts) : filtered;

    chart.data.labels = data.map(d => formatTimeLabel(d.t, range));
    chart.data.datasets[0].data = data.map(d => chartKey === 'packets' ? d.p : d.n);
    chart.update('none');
  }

  function downsample(arr, target) {
    const step = Math.ceil(arr.length / target);
    const result = [];
    for (let i = 0; i < arr.length; i += step) {
      result.push(arr[i]);
    }
    if (result[result.length - 1] !== arr[arr.length - 1]) {
      result.push(arr[arr.length - 1]);
    }
    return result;
  }

  function updateRoleChart() {
    const chart = state.charts.role;
    if (!chart) return;
    const counts = {};
    state.nodes.forEach(n => {
      if (!n.position) return;
      const name = ROLE_LABELS[(n.user && n.user.role) || 0] || 'Unknown';
      counts[name] = (counts[name] || 0) + 1;
    });
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    chart.data.labels = sorted.map(([k]) => k);
    chart.data.datasets[0].data = sorted.map(([, v]) => v);
    chart.data.datasets[0].backgroundColor = sorted.map(([k]) => ROLE_COLORS[k] || '#64748b');
    chart.update('none');
  }

  function updateHwChart() {
    const chart = state.charts.hw;
    if (!chart) return;
    const counts = {};
    state.nodes.forEach(n => {
      if (!n.position) return;
      const name = HW_MODELS[(n.user && n.user.hwModel) || 0] || `Model ${(n.user && n.user.hwModel) || 0}`;
      if (name === 'Unset') return;
      counts[name] = (counts[name] || 0) + 1;
    });
    const sorted = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 10);
    chart.data.labels = sorted.map(([k]) => k);
    chart.data.datasets[0].data = sorted.map(([, v]) => v);
    chart.update('none');
  }

  function updateChartFooterStats() {
    const nodesWithPos = [...state.nodes.values()].filter(n => n.position).length;
    const peak = state.history.length ? Math.max(...state.history.map(h => h.n)) : 0;

    setText('#chartStatCurrent .chart-stat-value', nodesWithPos);
    setText('#chartStatPeak .chart-stat-value', peak);

    // Total time
    if (state.serverStartedAt) {
      const ms = Date.now() - state.serverStartedAt;
      setText('#chartStatUptime .chart-stat-value', formatDuration(ms));
    }

    // Packet stats
    if (state.lastStats) {
      setText('#chartStatTotalPackets .chart-stat-value', formatNumber(state.lastStats.totalPackets));
      setText('#chartStatPosPackets .chart-stat-value', formatNumber(state.lastStats.positionPackets));
      // Rate: packets per minute
      if (state.serverStartedAt) {
        const mins = (Date.now() - state.serverStartedAt) / 60000;
        const rate = mins > 0 ? Math.round(state.lastStats.totalPackets / mins) : 0;
        setText('#chartStatRate .chart-stat-value', formatNumber(rate));
      }
    }
  }

  function setText(sel, val) {
    const el = document.querySelector(sel);
    if (el) el.textContent = val;
  }

  // ─── WebSocket Connection ──────────────────────────────────────────────────
  function connectWebSocket() {
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    state.ws = new WebSocket(`${proto}//${location.host}`);

    state.ws.onopen = () => {
      setConnectionStatus(true);
      if (state.reconnectTimer) { clearTimeout(state.reconnectTimer); state.reconnectTimer = null; }
    };

    state.ws.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);

        if (data.type === 'init') {
          // Reset on server restart
          resetState(data.startedAt);
          // Load server-side history
          if (data.history && data.history.length) {
            state.history = data.history;
          }
          // Fast batch load compact nodes
          batchLoadNodes(data.nodes || []);
          updateStats(data.stats);
          if (state.activeTab === 'charts') updateAllCharts();
        } else if (data.type === 'refresh') {
          // Periodic full refresh from server (every 10 minutes)
          // Remove nodes that are no longer in the server's active set
          const serverNodeIds = new Set((data.nodes || []).map(n => n.id));
          for (const id of state.nodes.keys()) {
            if (!serverNodeIds.has(id)) removeNode(id);
          }
          // Upsert all nodes from server
          (data.nodes || []).forEach(n => upsertNode(n));
          updateStats(data.stats || null);
          appendLiveHistory();
        } else if (data.type === 'batch') {
          (data.updates || []).forEach(u => {
            if (!u) return;
            if (u.type === 'remove' && u.id) removeNode(u.id);
            else if (u.node) upsertNode(u.node);
          });
          updateStats(data.stats || null);
          appendLiveHistory();
        } else if (data.type === 'remove' && data.id) {
          removeNode(data.id);
          updateStats(data.stats || null);
        } else if (data.node) {
          upsertNode(data.node);
          updateStats(data.stats || null);
          appendLiveHistory();
        }
      } catch (err) {
        console.error('WS parse error:', err);
      }
    };

    state.ws.onclose = () => {
      setConnectionStatus(false);
      state.reconnectTimer = setTimeout(connectWebSocket, 3000);
    };

    state.ws.onerror = () => state.ws.close();
  }

  // ─── Periodic HTTP Polling (every 10 minutes) ──────────────────────────────
  // Guarantees data freshness even if WebSocket refresh messages are missed.
  function startPolling() {
    setInterval(async () => {
      try {
        const res = await fetch('/api/nodes');
        if (!res.ok) return;
        const data = await res.json();
        if (!data.nodes) return;

        // Build set of active node IDs from server
        const serverIds = new Set();
        data.nodes.forEach(n => {
          serverIds.add(n.id);
          upsertNode(n);
        });

        // Remove nodes no longer on the server
        for (const id of state.nodes.keys()) {
          if (!serverIds.has(id)) removeNode(id);
        }

        if (data.stats) updateStats(data.stats);
        appendLiveHistory();
        console.log(`🔄 HTTP poll: ${data.nodes.length} nodes refreshed`);
      } catch (err) {
        console.warn('HTTP poll failed:', err.message);
      }
    }, 10 * 60 * 1000); // every 10 minutes
  }

  function removeNode(nodeId) {
    const id = String(nodeId);
    state.nodes.delete(id);

    const m = state.markers.get(id);
    if (m) {
      state.clusterGroup.removeLayer(m);
      state.markers.delete(id);
    }

    if (state.selectedNodeId === id) {
      state.selectedNodeId = null;
      const detail = document.getElementById('nodeDetail');
      if (detail) detail.classList.add('hidden');
    }

    scheduleNodeListUpdate();
  }

  function setConnectionStatus(connected) {
    const el = document.getElementById('connectionStatus');
    el.className = connected ? 'connection-status' : 'connection-status disconnected';
    el.querySelector('.status-text').textContent = connected ? 'Live' : 'Reconnecting...';
  }

  // ─── Utilities ─────────────────────────────────────────────────────────────
  function timeAgo(ts) {
    const s = Math.floor((Date.now() - ts) / 1000);
    if (s < 10) return 'just now';
    if (s < 60) return `${s}s ago`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ago`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ago`;
    return `${Math.floor(h / 24)}d ago`;
  }

  function formatNumber(n) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }

  // ─── Connection Lines ──────────────────────────────────────────────────────
  function getConnectedNodeIds(nodeId) {
    const connectedIds = new Set();
    const selected = state.nodes.get(nodeId);

    // Direct neighbors: nodes this node reports as neighbors
    if (selected && selected.neighbors) {
      for (const nb of selected.neighbors) {
        if (nb.nodeId && nb.nodeId !== nodeId) connectedIds.add(nb.nodeId);
      }
    }

    // Reverse neighbors: other nodes that list this node in their neighbor list
    state.nodes.forEach((n) => {
      if (n.id === nodeId || !n.neighbors) return;
      for (const nb of n.neighbors) {
        if (nb.nodeId === nodeId) {
          connectedIds.add(n.id);
          break;
        }
      }
    });

    return connectedIds;
  }

  function countConnections(nodeId) {
    return getConnectedNodeIds(nodeId).size;
  }

  function drawConnectionLines(nodeId) {
    clearConnectionLines();
    const selected = state.nodes.get(nodeId);
    if (!selected || !selected.position) return;

    const connectedIds = getConnectedNodeIds(nodeId);
    const fromLatLng = [selected.position.lat, selected.position.lon];

    connectedIds.forEach(connId => {
      const peer = state.nodes.get(connId);
      if (!peer || !peer.position) return;

      const toLatLng = [peer.position.lat, peer.position.lon];

      // Determine SNR for tooltip (check both directions)
      let snr = null;
      if (selected.neighbors) {
        const nb = selected.neighbors.find(n => n.nodeId === connId);
        if (nb && nb.snr) snr = nb.snr;
      }
      if (snr === null && peer.neighbors) {
        const nb = peer.neighbors.find(n => n.nodeId === nodeId);
        if (nb && nb.snr) snr = nb.snr;
      }

      // Draw the line
      const line = L.polyline([fromLatLng, toLatLng], {
        color: '#06b6d4',
        weight: 2.5,
        opacity: 0.7,
        dashArray: '8 6',
        className: 'connection-line',
      });

      // Tooltip with connection info
      const peerName = (peer.user && peer.user.longName) || peer.id;
      let tooltipText = peerName;
      if (snr !== null) tooltipText += ` (SNR: ${snr.toFixed(1)} dB)`;
      line.bindTooltip(tooltipText, {
        permanent: false,
        direction: 'center',
        className: 'connection-tooltip',
      });

      line.addTo(state.map);
      state.connectionLines.push(line);
    });
  }

  function clearConnectionLines() {
    for (const line of state.connectionLines) {
      state.map.removeLayer(line);
    }
    state.connectionLines = [];
  }

  function formatDuration(ms) {
    const s = Math.floor(ms / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m`;
    const h = Math.floor(m / 60);
    if (h < 24) return `${h}h ${m % 60}m`;
    return `${Math.floor(h / 24)}d ${h % 24}h`;
  }

  function escHtml(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
  }

  // ─── UI Event Handlers ────────────────────────────────────────────────────
  function initUI() {
    // Tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn =>
      btn.addEventListener('click', () => switchTab(btn.dataset.tab))
    );

    // Time range buttons
    document.querySelectorAll('.chart-range-btns').forEach(group => {
      const chartKey = group.dataset.chart; // 'nodes' or 'packets'
      group.querySelectorAll('.range-btn').forEach(btn => {
        btn.addEventListener('click', () => {
          group.querySelectorAll('.range-btn').forEach(b => b.classList.remove('active'));
          btn.classList.add('active');
          state.ranges[chartKey] = btn.dataset.range;
          if (chartKey === 'nodes') updateTimeSeriesChart('activeNodes', 'nodes');
          else updateTimeSeriesChart('packets', 'packets');
        });
      });
    });

    // Sidebar toggle (collapse) — works on both desktop and mobile
    document.getElementById('sidebarToggle').addEventListener('click', () => {
      const sidebar = document.getElementById('sidebar');
      sidebar.classList.add('collapsed');
      sidebar.classList.remove('mobile-open');  // also close mobile overlay
      document.getElementById('sidebarOpenBtn').classList.add('visible');
      setTimeout(() => state.map.invalidateSize(), 350);
    });

    // Sidebar open (expand)
    document.getElementById('sidebarOpenBtn').addEventListener('click', () => {
      document.getElementById('sidebar').classList.remove('collapsed');
      document.getElementById('sidebarOpenBtn').classList.remove('visible');
      setTimeout(() => state.map.invalidateSize(), 350);
    });

    document.getElementById('mobileSidebarBtn').addEventListener('click', () => {
      document.getElementById('sidebar').classList.toggle('mobile-open');
    });

    // Keep Leaflet sized correctly when the viewport changes.
    let resizeTimer = null;
    window.addEventListener('resize', () => {
      clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        if (state.activeTab === 'map' && state.map) state.map.invalidateSize();
      }, 150);
    });

    // Geolocation button
    document.getElementById('locateBtn').addEventListener('click', () => {
      if (!navigator.geolocation) return;
      const btn = document.getElementById('locateBtn');
      btn.classList.add('locating');
      navigator.geolocation.getCurrentPosition(
        (pos) => {
          btn.classList.remove('locating');
          const lat = pos.coords.latitude;
          const lon = pos.coords.longitude;
          const acc = pos.coords.accuracy;
          state.map.flyTo([lat, lon], 14, { duration: 1.5 });
          // Remove old location marker
          if (state.locationMarker) state.map.removeLayer(state.locationMarker);
          if (state.locationCircle) state.map.removeLayer(state.locationCircle);
          // Add accuracy circle
          state.locationCircle = L.circle([lat, lon], {
            radius: acc, color: '#06b6d4', fillColor: '#06b6d4',
            fillOpacity: 0.1, weight: 1, opacity: 0.4,
          }).addTo(state.map);
          // Add location dot
          state.locationMarker = L.marker([lat, lon], {
            icon: L.divIcon({
              className: 'my-location-marker',
              html: '<div class="my-loc-dot"></div><div class="my-loc-pulse"></div>',
              iconSize: [20, 20], iconAnchor: [10, 10],
            }),
          }).addTo(state.map);
        },
        (err) => {
          btn.classList.remove('locating');
          console.warn('Geolocation error:', err.message);
        },
        { enableHighAccuracy: true, timeout: 10000 }
      );
    });

    let searchTimer = null;
    document.getElementById('searchInput').addEventListener('input', () => {
      clearTimeout(searchTimer);
      searchTimer = setTimeout(updateNodeList, 250);
    });

    document.getElementById('detailClose').addEventListener('click', () => {
      document.getElementById('nodeDetail').classList.add('hidden');
      state.selectedNodeId = null;
      updateNodeList();
    });

    // Periodic refresh
    setInterval(() => {
      state.nodeListDirty = true;
      scheduleNodeListUpdate();
    }, 30000);
  }

  // ─── Bootstrap ─────────────────────────────────────────────────────────────
  function init() {
    initMap();
    initCharts();
    initUI();
    connectWebSocket();
    startPolling();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
