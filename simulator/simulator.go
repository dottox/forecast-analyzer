package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

// Defines the IoT device
type Device struct {
	Id        int
	Name      string
	Value     float32 // init: 20.0
	Latitude  float32
	Longitude float32
	Quadrant  int
	Status    int
}

// Define the surface Quadrant
type Quadrant struct {
	Id     int
	MinLat float32
	MaxLat float32
	MinLon float32
	MaxLon float32
}

// Global app to control everything
type Simulator struct {
	deviceCounter int
	Devices       []*Device

	dataChan chan DataPoint
	TDengine *sql.DB

	PostgreSQL       *sql.DB
	stmtInsertDevice *sql.Stmt

	debugDevice bool
}

type DataPoint struct {
	Tbname   string
	Ts       string
	Lat      float32
	Lon      float32
	Quadrant int
	Val      float32
}

// ForecastRow estructura para leer la tabla forecasts
type ForecastRow struct {
	Ts   time.Time
	Temp float64
}

// Returns a random float between min and max
func randFloat(min, max float32) float32 {
	return min + rand.Float32()*(max-min)
}

// Creates a new device from scratch, inserting it into DB and memory
func (sim *Simulator) createNewDevice() (*Device, error) {

	// Get a random quadrant
	var quadrant int
	var minLat, maxLat, minLon, maxLon float32
	sim.PostgreSQL.QueryRow(`
		SELECT id, min_lat, max_lat, min_lon, max_lon FROM quadrants
		ORDER BY RANDOM()
		LIMIT 1;
	`).Scan(
		&quadrant, &minLat, &maxLat, &minLon, &maxLon,
	)

	// Get a random lat and lon inside the quadrant
	lat := randFloat(minLat, maxLat)
	lon := randFloat(minLon, maxLon)

	fmt.Printf("lat: %f, lon %f, quadrant %d\n", lat, lon, quadrant)

	// Creating the new device
	id := sim.deviceCounter + 1
	d := &Device{
		Id:        id,
		Name:      "d-" + fmt.Sprint(id),
		Value:     20.0,
		Latitude:  lat,
		Longitude: lon,
		Quadrant:  quadrant,
		Status:    1,
	}

	// Inserting into Postgres
	_, err := sim.stmtInsertDevice.Exec(id, d.Name, d.Latitude, d.Longitude, d.Quadrant, d.Status)
	if err != nil {
		return nil, err
	}

	// Inserting into memory
	sim.Devices = append(sim.Devices, d)

	sim.deviceCounter++

	return d, nil
}

// This function must be runned on a goroutine
// Sends data to TDengine every 5 seconds
func (sim *Simulator) startDevice(d *Device) {
	for {
		// Variate the value
		randInt := (rand.Float32() - 0.5) / 2.0 // -0.25 to +0.25
		d.Value = d.Value + randInt

		// Get the current time in that format
		timeNow := time.Now().Format("2006-01-02 15:04:05.000")

		// Create the new data point that will be queued
		dp := DataPoint{
			Tbname:   fmt.Sprintf("d-%d", d.Id),
			Ts:       timeNow,
			Lat:      d.Latitude,
			Lon:      d.Longitude,
			Quadrant: d.Quadrant,
			Val:      d.Value,
		}

		// Queue the datapoint
		sim.dataChan <- dp
		if sim.debugDevice {
			fmt.Printf("[%d] queued a new point with values: %s %f\n", d.Id, d.Name, d.Value)
		}

		// Devices has 5s cooldown
		time.Sleep(5 * time.Second)
	}
}

// Perform the connection to TDengine
func (sim *Simulator) connectToTDengine() {
	// Open connection to TDengine
	td, err := sql.Open("taosRestful", "root:taosdata@http(localhost:6041)/netlabs")
	if err != nil {
		fmt.Print("Error connecting to TDengine: ")
		log.Fatal(err)
	}

	// Creating the database
	_, err = td.Exec(`
		CREATE DATABASE IF NOT EXISTS netlabs;
	`)
	if err != nil {
		fmt.Print("Error creating netlabs database in TDengine: ")
		log.Fatal(err)
	}

	// Create the temp_devices stable
	// This table will store the data from all the temperature devices
	// This is a short term table
	_, err = td.Exec(`
		CREATE STABLE IF NOT EXISTS netlabs.temp_devices (
			ts TIMESTAMP,
			val FLOAT
		) TAGS (
			quadrant TINYINT UNSIGNED,
			lat FLOAT,
			lon FLOAT
		) KEEP 7d;
	`)
	if err != nil {
		fmt.Print("Error creating temp_devices table in TDengine: ")
		log.Fatal(err)
	}

	// Creates the forecasts stable
	_, err = td.Exec(`
		CREATE STABLE IF NOT EXISTS netlabs.forecasts (
            ts TIMESTAMP, 
            temp FLOAT
        ) TAGS (
            quadrant TINYINT UNSIGNED
        )
	`)

	// Create the daily analytics stable
	// This table will store the daily analytics results
	// This is a long term table
	_, err = td.Exec(`
		CREATE STABLE IF NOT EXISTS netlabs.daily_analytics (
			ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4',
			avg_bias FLOAT,
			rmse FLOAT,
			mae FLOAT,
			daily_max FLOAT,
			daily_min FLOAT
		) TAGS (
			quadrant TINYINT UNSIGNED
		) KEEP 3650d;
	`)
	if err != nil {
		fmt.Println("Error creating analytics in TDengine: ")
		log.Fatal(err)
	}

	// Creates the hourly analytics table
	// This table should store the hourly analytics results
	// To later be used for daily analytics
	_, err = td.Exec(`
		CREATE TABLE IF NOT EXISTS netlabs.hourly_analytics (
			ts TIMESTAMP,
			avg_val DOUBLE,
			max_val FLOAT,
			min_val FLOAT
		);
		`)
	if err != nil {
		fmt.Println("Error creating hourly_analytics in TDengine: ")
		log.Fatal(err)
	}

	// Create the stream that will fill the hourly analytics table
	// not working...

	//  -- To save hourly analytics of all devices --
	// _, err = td.Exec(`
	// 	CREATE STREAM IF NOT EXISTS netlabs.stream_hourly_analytics
	// 	PERIOD(1h)
	// 	INTO netlabs.hourly_analytics (ts, avg_val, max_val, min_val)
	// 	AS
	// 		SELECT NOW AS ts,
	// 			avg(val) AS avg_val,
	// 			max(val) AS max_val,
	// 			min(val) AS min_val
	// 		FROM netlabs.temp_devices
	// 		WHERE ts >= NOW - 1h AND ts < NOW
	// `)

	// -- To save hourly analytics per quadrant --
	//	_, err = td.Exec(`
	//	CREATE STREAM IF NOT EXISTS netlabs.stream_hourly_analytics
	// 	INTERVAL(1h) SLIDING(1h)
	// 	FROM netlabs.temp_devices
	// 	PARTITION BY quadrant
	// 	INTO netlabs.hourly_analytics (ts, avg_val, max_val, min_val)
	// 	AS
	// 		SELECT
	// 			_twstart AS ts,
	// 			avg(val) AS avg_val,
	// 			max(val) AS max_val,
	// 			min(val) AS min_val
	// 		FROM netlabs.temp_devices
	// 		WHERE ts >= _twstart AND ts < _twend
	// 		PARTITION BY quadrant;
	//	`)

	// if err != nil {
	// 	fmt.Println("Error creating stream in hourly_analytics in TDengine: ")
	// 	log.Fatal(err)
	// }

	sim.TDengine = td
}

func (sim *Simulator) connectToPostgreSQL() {
	// Open connection to PostgreSQL
	psql, err := sql.Open("postgres", "postgres://isuarez:isuarez@isuarez-IdeaPad-3-14IIL05/netlabs?sslmode=verify-full")
	if err != nil {
		fmt.Print("Error connecting to postgres: ")
		log.Fatal(err)
	}

	// Creating the table for quadrants
	_, err = psql.Exec(`
		CREATE TABLE IF NOT EXISTS quadrants (
			id SMALLINT PRIMARY KEY,
			min_lat REAL,
			max_lat REAL,
			min_lon REAL,
			max_lon REAL
		);
	`)
	if err != nil {
		fmt.Print("[ERROR]: creating the quadrants table in postgres: ")
		log.Fatal(err)
	}

	// Creating the table for devices
	_, err = psql.Exec(`
		CREATE TABLE IF NOT EXISTS devices (
			id INTEGER PRIMARY KEY,
			name varchar(12),
			latitude REAL,
			longitude REAL,
			quadrant SMALLINT,
			status SMALLINT,
			FOREIGN KEY (quadrant) REFERENCES quadrants(id) ON DELETE CASCADE
		);
	`)
	if err != nil {
		fmt.Print("[ERROR]: creating the devices table in postgres: ")
		log.Fatal(err)
	}

	// Prepare the insert statement
	// Used when creating new devices
	sim.stmtInsertDevice, err = psql.Prepare(`
		INSERT INTO devices (id, name, latitude, longitude, quadrant, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`)
	if err != nil {
		fmt.Print("[ERROR]: preparing of the insert device statement in postgres: ")
		log.Fatal(err)
	}

	// Create an index for devices quadrant
	// Used when querying devices by quadrant
	_, err = psql.Exec(`
		CREATE INDEX IF NOT EXISTS idx_devices_quadrant ON devices(quadrant);
	`)
	if err != nil {
		fmt.Print("[ERROR]: creating index for devices quadrant in postgres: ")
		log.Fatal(err)
	}

	sim.PostgreSQL = psql
}

// Load all the devices from the PostgreSQL
func (sim *Simulator) loadDevicesFromDB() (int, error) {
	sim.Devices = make([]*Device, 0)

	// Perform te query to search for all the devices
	rows, err := sim.PostgreSQL.Query(
		"SELECT * FROM devices",
	)
	if err != nil {
		return 0, err
	}

	// For each row in the query
	countDevicesLoaded := 0
	for rows.Next() {
		// Extract the info about the device
		var id, quadrant, status int
		var name string
		var latitude, longitude float32
		err = rows.Scan(&id, &name, &latitude, &longitude, &quadrant, &status)
		if err != nil {
			fmt.Printf("[ERROR]: while loading device %d -> %v\n", countDevicesLoaded+1, err)
		}

		// Creates the device
		d := &Device{
			Id:        id,
			Name:      name,
			Value:     20.0,
			Latitude:  latitude,
			Longitude: longitude,
			Quadrant:  quadrant,
			Status:    status,
		}

		sim.Devices = append(sim.Devices, d)

		// Start the device if its status is 1
		if status == 1 {
			go sim.startDevice(d)
		}

		countDevicesLoaded++
	}

	sim.deviceCounter += countDevicesLoaded
	return countDevicesLoaded, nil

}

// Uses the python script to save the forecast for every quadrant for the given day
func (sim *Simulator) saveForecast(day string) {

	// Get all the quadrants
	var quadrants []*Quadrant
	rows, err := sim.PostgreSQL.Query(`
		SELECT id, min_lat, max_lat, min_lon, max_lon FROM quadrants;
	`)
	if err != nil {
		fmt.Println("Error querying quadrants from Postgres:", err)
		log.Fatal(err)
	}

	// Save all quadrants into a slice
	for rows.Next() {
		q := &Quadrant{}
		err := rows.Scan(&q.Id, &q.MinLat, &q.MaxLat, &q.MinLon, &q.MaxLon)
		if err != nil {
			fmt.Println("Error scanning quadrant row:", err)
			log.Fatal(err)
		}
		quadrants = append(quadrants, q)
	}
	rows.Close()

	// Call the python script to save the forecast of the day for every quadrant
	for _, q := range quadrants {
		middleLat := fmt.Sprint(q.MinLat + ((q.MaxLat - q.MinLat) / 2))
		middleLon := fmt.Sprint(q.MinLon + ((q.MaxLon - q.MinLon) / 2))

		fmt.Printf("Calculating forecast for day %s with lat %s and lon %s, quadrant %d...\n", day, middleLat, middleLon, q.Id)
		output, err := exec.Command(
			"../forecast/.venv/bin/python3",
			"../forecast/get_forecast.py",
			"--lat", middleLat,
			"--lon", middleLon,
			"--quadrant", fmt.Sprint(q.Id),
			"--day", day,
		).Output()
		if err != nil {
			fmt.Println(output)
			log.Fatal(err)
		}
	}

	fmt.Println("---- FORECAST FINISHED WITHOUT PROBLEMS ----")
}

// Get user input for analytics
func (sim *Simulator) getAnalyticsInput() (int, string, error) {
	reader := bufio.NewReader(os.Stdin)

	// Show available quadrants
	availableQuadrants, err := sim.showQuadrants()
	if err != nil {
		return 0, "", fmt.Errorf("Error getting quadrants: %v", err)
	}

	// Ask the user for a quadrant
	fmt.Print("- Enter quadrant: ")
	quadrant, err := reader.ReadString('\n')
	if err != nil {
		return 0, "", fmt.Errorf("Error reading quadrant: %v", err)
	}
	quadrantInt, err := strconv.Atoi(quadrant[:len(quadrant)-1])
	if err != nil {
		return 0, "", fmt.Errorf("Error converting quadrant to int: %v", err)
	}

	// Check if the quadrant exists
	if !slices.Contains(availableQuadrants, quadrantInt) {
		return 0, "", fmt.Errorf("quadrant %d does not exist", quadrantInt)
	}

	// Ask the user for a day to analyze
	fmt.Print("- Enter day to analyze (YYYY-MM-DD): ")
	time, err := reader.ReadString('\n')
	if err != nil {
		return 0, "", fmt.Errorf("Error reading day: %v", err)
	}
	time = time[:len(time)-1] // removing newline

	return quadrantInt, time, nil
}

// Process the analytics for a given quadrant and day
func (sim *Simulator) processAnalytics(dateStr string, quadrant int) error {

	// Parsing the date
	startTime, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return fmt.Errorf("invalid date: %v", err)
	}
	endTime := startTime.Add(24 * time.Hour)

	// Formatting the date so we have 00:00 time
	startStr := startTime.Format("2006-01-02 15:04:05")
	endStr := endTime.Format("2006-01-02 15:04:05")

	fmt.Printf("analyzing quadrant %d for date %s...\n", quadrant, dateStr)

	// ------ FORECAST -------
	// Get the forecast data for that day and quadrant
	rows, err := sim.TDengine.Query(`
		SELECT ts, temp 
		FROM netlabs.forecasts
		WHERE ts >= '?' AND ts < '?' AND quadrant = ?;
	`, startStr, endStr, quadrant)
	if err != nil {
		return fmt.Errorf("error reading forecast: %v", err)
	}

	// Map the forecast data
	forecastMap := make(map[time.Time]float64)
	for rows.Next() {
		var f ForecastRow
		if err := rows.Scan(&f.Ts, &f.Temp); err != nil {
			rows.Close() // Cerrar en caso de error
			return err
		}
		forecastMap[f.Ts] = f.Temp
	}
	rows.Close()

	// Check if we have any forecast data for that day and quadrant
	if len(forecastMap) == 0 {
		return fmt.Errorf("no forecast data for the date %s", dateStr)
	}

	// ------ IOT DATA -------
	// Get the IoT data for that day and quadrant,
	// using intervals of 1h to match the forecast data
	rowsReal, err := sim.TDengine.Query(`
		SELECT _wstart as ts, avg(val), max(val), min(val)
		FROM netlabs.temp_devices
		WHERE ts >= '?' AND ts < '?' AND quadrant = ?
		INTERVAL(1h);
	`, startStr, endStr, quadrant)
	if err != nil {
		return fmt.Errorf("error reading devices: %v", err)
	}

	// Variables to calculate the metrics
	var biases, squaredErrors, absErrors []float64
	var dailyMax = -math.MaxFloat64
	var dailyMin = math.MaxFloat64
	var count int

	// For each hourly interval
	for rowsReal.Next() {
		// NullFloat64 to handle possible nulls
		var ts time.Time
		var avgVal, maxVal, minVal sql.NullFloat64

		err := rowsReal.Scan(&ts, &avgVal, &maxVal, &minVal)
		if err != nil {
			fmt.Printf("error reading row: %v\n", err)
			continue
		}

		// Checking if we have valid data
		if !avgVal.Valid || !maxVal.Valid || !minVal.Valid {
			continue
		}

		// Match the timestamp with the forecast data: X0:00
		tKey := ts.Truncate(time.Hour)
		forecastVal, exists := forecastMap[tKey]

		// If we have both forecast and real data, calculate metrics
		if exists {
			val := avgVal.Float64

			bias := forecastVal - val
			biases = append(biases, bias)

			sqErr := math.Pow(forecastVal-val, 2)
			squaredErrors = append(squaredErrors, sqErr)

			absErr := math.Abs(forecastVal - val)
			absErrors = append(absErrors, absErr)

			count++
		}

		// DailyMax and DailyMin will keep the extreme values of the day
		if maxVal.Valid && maxVal.Float64 > dailyMax {
			dailyMax = maxVal.Float64
		}
		if minVal.Valid && minVal.Float64 < dailyMin {
			dailyMin = minVal.Float64
		}
	}
	rowsReal.Close()

	// If we don't have any coincidence between forecast and real data, return error
	if count == 0 {
		return fmt.Errorf("no real data for the date %s", dateStr)
	}

	// Calculate final metrics
	// Bias: how much the forecast overestimates/underestimates on average
	// RMSE: how much the forecast deviates from the real value
	// MAE: average absolute error between forecast and real value
	var sumBias, sumSqErr, sumAbsErr float64
	for i := 0; i < count; i++ {
		sumBias += biases[i]
		sumSqErr += squaredErrors[i]
		sumAbsErr += absErrors[i]
	}
	finalAvgBias := sumBias / float64(count)
	finalRMSE := math.Sqrt(sumSqErr / float64(count))
	finalMAE := sumAbsErr / float64(count)

	// Display metrics
	fmt.Printf("Analytics for quadrant %d on day %s:\n", quadrant, dateStr)
	fmt.Printf("  Avg Bias: %.4f\n", finalAvgBias)
	fmt.Printf("  RMSE: %.4f\n", finalRMSE)
	fmt.Printf("  MAE: %.4f\n", finalMAE)
	fmt.Printf("  Daily Max: %.4f\n", dailyMax)
	fmt.Printf("  Daily Min: %.4f\n", dailyMin)

	// Insert the analytics into TDengine
	_, err = sim.TDengine.Exec(`
		INSERT INTO daily_analytics (tbname, ts, quadrant, avg_bias, rmse, mae, daily_max, daily_min)
		VALUES ("?", '?', ?, ?, ?, ?, ?, ?);
	`, fmt.Sprintf("aq-%d", quadrant), startStr, quadrant, finalAvgBias, finalRMSE, finalMAE, dailyMax, dailyMin)
	if err != nil {
		return fmt.Errorf("error saving analytics: %v", err)
	}

	return nil
}

// Show and return all the quadrants available
func (sim *Simulator) showQuadrants() ([]int, error) {
	// Get the quadrants
	rows, err := sim.PostgreSQL.Query(`SELECT ID FROM quadrants;`)
	if err != nil {
		return nil, fmt.Errorf("Error displaying quadrants: %v", err)
	}

	// Display user the list of quadrants
	availableQuadrants := make([]int, 0)
	for rows.Next() {
		var id int
		err := rows.Scan(&id)
		if err != nil {
			fmt.Println("Error scanning one quadrant...")
			continue
		}
		availableQuadrants = append(availableQuadrants, id)
	}
	if len(availableQuadrants) == 0 {
		return nil, fmt.Errorf("no quadrants available, create one first")
	}
	fmt.Printf("Available quadrants: %v\n", availableQuadrants)
	return availableQuadrants, nil
}

// Perform actions over quadrants
// Actions: create, delete, show, devices
func (sim *Simulator) quadrantAction(action string) {
	switch action {
	case "create":
		// Get the quadrant data from user
		var id int
		var minLat, maxLat, minLon, maxLon float32
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter id, latitude and longitude (id/min_lat/max_lat/min_lon/max_lon): ")
		input, _ := reader.ReadString('\n')
		_, err := fmt.Sscanf(input, "%d/%f/%f/%f/%f", &id, &minLat, &maxLat, &minLon, &maxLon)
		if err != nil {
			fmt.Println("Invalid input format.")
			return
		}

		// Insert the quadrant into the DB
		_, err = sim.PostgreSQL.Exec(`
			INSERT INTO quadrants (id, min_lat, max_lat, min_lon, max_lon)
			VALUES ($1, $2, $3, $4, $5)
		`, id, minLat, maxLat, minLon, maxLon)
		if err != nil {
			fmt.Printf("[ERROR]: creating quadrant: %v\n", err)
			return
		}
		fmt.Println("Quadrant created successfully.")

	case "delete":
		_, err := sim.showQuadrants()
		if err != nil {
			fmt.Printf("[ERROR]: %v\n", err)
			return
		}

		// Ask the user for the quadrant id to delete
		var id int
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter quadrant ID to delete: ")
		input, _ := reader.ReadString('\n')
		_, err = fmt.Sscanf(input, "%d", &id)
		if err != nil {
			fmt.Println("Invalid input format.")
			return
		}

		// Delete the quadrant from the DB
		_, err = sim.PostgreSQL.Exec(
			"DELETE FROM quadrants WHERE id = $1",
			id,
		)
		if err != nil {
			fmt.Printf("[ERROR]: deleting quadrant: %v\n", err)
			return
		}
		fmt.Println("Quadrant deleted successfully.")

	// Show available quadrants
	case "show":
		_, err := sim.showQuadrants()
		if err != nil {
			fmt.Printf("[ERROR]: %v\n", err)
			return
		}

	// Show devices ID's in a quadrant
	case "devices":
		_, err := sim.showQuadrants()
		if err != nil {
			fmt.Printf("[ERROR]: %v\n", err)
			return
		}

		// Ask the user for a quadrant to display its devices
		var id int
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter quadrant ID to show its devices: ")
		input, _ := reader.ReadString('\n')
		_, err = fmt.Sscanf(input, "%d", &id)
		if err != nil {
			fmt.Println("Invalid input format.")
			return
		}

		// Perform the query to get the devices
		// This query uses the index created on devices.quadrant
		rows, err := sim.PostgreSQL.Query(
			"SELECT id FROM devices WHERE quadrant = $1",
			id,
		)
		if err != nil {
			fmt.Printf("[ERROR]: querying devices: %v\n", err)
			return
		}

		// Display the devices
		fmt.Printf("Devices in Quadrant %d:\n-", id)
		for rows.Next() {
			var deviceId int
			err := rows.Scan(&deviceId)
			if err != nil {
				fmt.Printf("[ERROR]: scanning device: %v\n", err)
				continue
			}
			fmt.Printf(" %d", deviceId)
		}
		rows.Close()
		fmt.Println("")

	// Unknown action handler
	default:
		fmt.Println("Unknown action. Use (create/delete/show/devices).")
	}
}

// Goroutine that batches and sends data points to TDengine
func (sim *Simulator) startSender() {
	const batchSize = 100
	buffer := make([]DataPoint, 0, batchSize)

	// Ticker helps to flush the buffer every certain time
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	// Function to send the data in batch to TDengine
	flush := func() {
		if len(buffer) == 0 {
			return
		}

		// Build the insert query
		query := "INSERT INTO netlabs.temp_devices (tbname, ts, lat, lon, quadrant, val) VALUES "
		args := make([]interface{}, 0, len(buffer)*6) // 6 columns per data point

		// Build the query values and args
		for i, dp := range buffer {
			if i > 0 {
				query += ", "
			}
			query += "('?', '?', ?, ?, ?, ?)"
			args = append(args, dp.Tbname, dp.Ts, dp.Lat, dp.Lon, dp.Quadrant, dp.Val)
		}

		// Execute the batch insert
		_, err := sim.TDengine.Exec(query, args...)
		if err != nil {
			fmt.Printf("[ERROR]: sending batch: %v\n", err)
		} else if sim.debugDevice {
			fmt.Printf("Inserted batch of %d data points.\n", len(buffer))
		}

		buffer = buffer[:0] // Clearing the buffer
	}

	// Loop to receive data points and batch them
	for {
		select {
		case dp := <-sim.dataChan:
			buffer = append(buffer, dp)
			if len(buffer) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// Starts the simulator app
// Handles connection to DBs, starts handler and loop over user inputs
func (sim *Simulator) Start() {

	sim.deviceCounter = 0
	sim.dataChan = make(chan DataPoint, 200)

	// Connect to both DBs
	// TDengine:
	fmt.Println("Connecting to TDengine...")
	sim.connectToTDengine()
	defer sim.TDengine.Close()
	// PostgreSQL:
	fmt.Println("Connecting to PostgreSQL...")
	sim.connectToPostgreSQL()
	defer sim.PostgreSQL.Close()

	// Load to the app the devices already created, they're extracted from the PostgreSQL
	count, err := sim.loadDevicesFromDB()
	if err != nil {
		log.Fatal()
	}
	fmt.Printf("Loaded %d devices from PostgreSQL\n", count)

	// Goroutine to batch send data points
	go sim.startSender()

	// Read the stdin
	fmt.Println("Use 'new' to create a new device...")
	reader := bufio.NewReader(os.Stdin)
	for {
		// Read input
		fmt.Print("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		// Operations over each input
		switch text {
		case "new\n": // --- NEW ---
			fmt.Println("Creating new device...")
			dev, err := sim.createNewDevice()
			if err != nil {
				fmt.Printf("[ERROR]: creating the device... -> %v\n", err)
				continue
			}

			go sim.startDevice(dev)
			fmt.Printf("New device created. %d devices running...\n", sim.deviceCounter)

		case "mass new\n": // --- MASS NEW ---
			fmt.Println("Creating 100 devices...")
			for range 100 {
				dev, err := sim.createNewDevice()
				if err != nil {
					fmt.Printf("[ERROR]: creating the device... -> %v\n", err)
					continue
				}
				go sim.startDevice(dev)
			}
			fmt.Printf("Created 100 devices. %d devices running...\n", sim.deviceCounter)

		case "debug\n": // --- DEBUG ---
			sim.debugDevice = !sim.debugDevice
			fmt.Printf("Now debug is: %v\n", sim.debugDevice)

		case "forecast\n": // --- FORECAST ---
			fmt.Print("- Enter day for forecast (YYYY-MM-DD): ")
			day, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading day")
				continue
			}
			day = day[:len(day)-1] // removing newline

			sim.saveForecast(day)

		case "analytics\n": // --- ANALYTICS ---

			quadrant, time, err := sim.getAnalyticsInput()
			if err != nil {
				fmt.Printf("[ERROR]: getting analytics input -> %v\n", err)
				continue
			}

			err = sim.processAnalytics(time, quadrant)
			if err != nil {
				fmt.Printf("[ERROR]: processing analytics -> %v\n", err)
			}

		case "quadrants\n": // --- QUADRANTS ---
			fmt.Print("(create/delete/show/devices): ")
			action, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading action")
				continue
			}
			action = action[:len(action)-1] // removing newline

			sim.quadrantAction(action)
		}
	}
}

// Main function
func main() {
	sim := &Simulator{}

	fmt.Println("Starting simulator...")
	sim.Start()
}
