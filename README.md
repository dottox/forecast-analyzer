# Forecast Analyzer

This project is a hybrid simulation and analytics platform designed to generate synthetic IoT data, compare it against real-world weather forecasts, and store the results in high-performance databases.

It consists of a **Golang** simulator that manages thousands of IoT devices and a **Python** scraper that fetches real-world weather data.

## 🏗 Architecture

The system relies on two database engines to handle different types of data:

1.  **PostgreSQL:** Stores relational metadata (Device IDs, names, quadrants, status).
2.  **TDengine:** Stores time-series data (Sensor readings, weather forecasts, and analytical results).

### Components

*   **Simulator (Go):** 
    *   Manages the lifecycle of IoT devices.
    *   Simulates temperature fluctuations based on random walks.
    *   Batches and streams data to TDengine.
    *   Calculates statistical metrics (RMSE, MAE, Bias) comparing simulated data vs. forecasts.
*   **Forecaster (Python):** 
    *   Fetches hourly weather data from the OpenMeteo API.
    *   Stores forecast data into TDengine for comparison.

## 🚀 Tech Stack

*   **Language:** Go (1.22+), Python 3.x
*   **Databases:** TDengine, PostgreSQL
*   **APIs:** OpenMeteo (Weather Forecasts)
*   **Libraries:** 
    *   Go: `taosRestful`, `lib/pq`
    *   Python: `taosws`, `openmeteo_requests`

## 🛠️ Setup & Installation

### 1. Database Configuration
Ensure both databases are running locally.
*   **TDengine:** Port `6041` (REST) and `6030` (Connector).
*   **PostgreSQL:** Standard port. Update the connection string in `iot_device_simulator.go` (`connectToPostgreSQL` function) with your credentials.

### 2. Python Environment
The Go simulator expects the Python script to be located at `../forecast/get_forecast.py` relative to the executable.

```bash
cd forecast
python3 -m venv .venv
source .venv/bin/activate
pip install openmeteo-requests requests-cache retry-requests taos-ws-py
```

### 3. Run the Simulator
```bash
go mod tidy
go run simulator.go
```

## 💻 Usage (CLI)

Once the Go application is running, you can interact with it using the following commands in the terminal:

| Command | Description |
| :--- | :--- |
| `quadrants` | Manage geographical areas. Options: `create`, `delete`, `show`, `devices`. **(Do this first)** |
| `new` | Create and start a single new IoT device in a random quadrant. |
| `mass new` | Create and start 100 IoT devices simultaneously. |
| `forecast` | Triggers the Python script to fetch weather data for a specific date. |
| `analytics` | Compare device data against forecasts for a specific day/quadrant and calculate RMSE/MAE. |
| `debug` | Toggle verbose logging for device data generation. |

### Workflow Example
1.  **Create a Quadrant:** Run `quadrants` -> `create` -> input data (e.g., `1/-33.0/-34.0/-56.0/-57.0`).
2.  **Generate Devices:** Run `mass new`.
3.  **Fetch Forecast:** Run `forecast` -> Enter Date (e.g., `2026-01-16`).
4.  **Wait:** Allow the simulator to run for a while to generate sensor data.
5.  **Analyze:** Run `analytics` -> Select Quadrant & Date.

## 🔮 Possible Future Features

*   **Scale Data Types:** Expand support for other meteorological data types (humidity, pressure, wind speed) and integrate additional forecast models.
*   **Enhanced Device Metadata:** Incorporate more detailed device attributes and make effective use of the operational `status` field (e.g., simulating maintenance mode or errors).
*   **Geospatial Visualization:** Implement geospatial mapping features to visualize device locations and heatmaps within their quadrants.
*   **Smart Compression Policies:** Implement tiered storage policies in TDengine: use strong compression for historical/archived data while keeping high-frequency, recent data uncompressed for faster query performance.