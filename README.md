# Meshtastic Statistics Website

> ⚠️ **BETA WARNING:** This project is currently in beta. Features may change, and you may experience bugs or unexpected behavior.

A real-time interactive map and statistics dashboard for the Meshtastic mesh network.

## Overview

This application connects to the official public Meshtastic MQTT broker, listens to map and long-fast topics, and decodes the protobuf packets in real-time. It streams the live position, node information, and telemetry data (like battery levels and SNR) to connected clients over WebSockets. 

## Features
- **Real-Time Map**: Instantly see Meshtastic nodes popping up in real-time as packets are received.
- **Node Statistics**: Displays node battery life, SNR, firmware versions, hardware info, and more.
- **WebSocket Streaming**: Fast and lightweight push updates from the backend to the browser.
- **Protobuf Decoding**: Parses raw mesh packets natively using `protobufjs` to strip out coordinates and metadata.

## Tech Stack
- **Backend**: Node.js, Express, `ws` (WebSockets), `mqtt`, `protobufjs`.
- **Frontend**: HTML/Vanilla CSS/JS (assumed) with WebSocket connection and presumably Leaflet/Mapbox for mapping.

## Running Locally

1. **Install Dependencies**
   Run the following command to download necessary packages:
   ```bash
   npm install
   ```

2. **Start the Server**
   ```bash
   npm start
   ```
   Or, for auto-reloading during development:
   ```bash
   npm run dev
   ```

3. **View the Dashboard**
   Open your browser and navigate to `http://localhost:3000`.
