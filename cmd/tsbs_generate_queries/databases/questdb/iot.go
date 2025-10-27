package questdb

import (
	"fmt"
	"strings"
	"time"

	"github.com/taosdata/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/taosdata/tsbs/pkg/query"
)

const (
	iotReadingsTable    = "readings"
	iotDiagnosticsTable = "diagnostics"
	questdbTimeFmt      = "2006-01-02T15:04:05.000000Z"
)

// IoT produces QuestDB-specific queries for all the iot query types.
type IoT struct {
	*iot.Core
	*BaseGenerator
}

// getTrucksWhereWithNames creates a WHERE clause for the given truck names
func (i *IoT) getTrucksWhereWithNames(names []string) string {
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("name IN (%s)", strings.Join(nameClauses, ","))
}

// getTruckWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (i *IoT) getTruckWhereString(nTrucks int) string {
	names, err := i.GetRandomTrucks(nTrucks)
	if err != nil {
		panic(err.Error())
	}
	return i.getTrucksWhereWithNames(names)
}

// LastLocByTruck finds the truck location for nTrucks.
func (i *IoT) LastLocByTruck(qi query.Query, nTrucks int) {
	sql := fmt.Sprintf(`SELECT name, driver, longitude, latitude
		FROM %s
		WHERE %s
		LATEST ON timestamp PARTITION BY name`,
		iotReadingsTable,
		i.getTruckWhereString(nTrucks))

	humanLabel := "QuestDB last location by specific truck"
	humanDesc := fmt.Sprintf("%s: random %4d trucks", humanLabel, nTrucks)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// LastLocPerTruck finds all the truck locations along with truck and driver names.
func (i *IoT) LastLocPerTruck(qi query.Query) {
	sql := fmt.Sprintf(`SELECT name, driver, longitude, latitude
		FROM %s
		WHERE fleet = '%s'
		LATEST ON timestamp PARTITION BY name`,
		iotReadingsTable,
		i.GetRandomFleet())

	humanLabel := "QuestDB last location per truck"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLowFuel finds all trucks with low fuel (less than 10%).
func (i *IoT) TrucksWithLowFuel(qi query.Query) {
	sql := fmt.Sprintf(`SELECT name, driver, fuel_state
		FROM %s
		WHERE fleet = '%s' AND fuel_state < 0.1
		LATEST ON timestamp PARTITION BY name`,
		iotDiagnosticsTable,
		i.GetRandomFleet())

	humanLabel := "QuestDB trucks with low fuel"
	humanDesc := fmt.Sprintf("%s: under 10 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithHighLoad finds all trucks that have load over 90%.
func (i *IoT) TrucksWithHighLoad(qi query.Query) {
	sql := fmt.Sprintf(`SELECT r.name, r.driver, d.current_load, r.load_capacity
		FROM (
			SELECT name, driver, fleet, load_capacity
			FROM %s
			LATEST ON timestamp PARTITION BY name
		) r
		ASOF JOIN (
			SELECT name, current_load, timestamp
			FROM %s
		) d
		WHERE r.fleet = '%s' AND d.current_load / r.load_capacity > 0.9`,
		iotReadingsTable,
		iotDiagnosticsTable,
		i.GetRandomFleet())

	humanLabel := "QuestDB trucks with high load"
	humanDesc := fmt.Sprintf("%s: over 90 percent", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// StationaryTrucks finds all trucks that have low average velocity in a time window.
func (i *IoT) StationaryTrucks(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.StationaryDuration)
	sql := fmt.Sprintf(`WITH velocity_stats AS (
		SELECT name, driver, avg(velocity) AS avg_velocity
		FROM %s
		WHERE timestamp >= '%s' AND timestamp < '%s' AND fleet = '%s'
		GROUP BY name, driver
	)
	SELECT name, driver
	FROM velocity_stats
	WHERE avg_velocity < 1`,
		iotReadingsTable,
		interval.Start().Format(questdbTimeFmt),
		interval.End().Format(questdbTimeFmt),
		i.GetRandomFleet())

	humanLabel := "QuestDB stationary trucks"
	humanDesc := fmt.Sprintf("%s: with low avg velocity in last 10 minutes", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDrivingSessions finds all trucks that have not stopped at least 20 mins in the last 4 hours.
func (i *IoT) TrucksWithLongDrivingSessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.LongDrivingSessionDuration)
	sql := fmt.Sprintf(`WITH driving_intervals AS (
		SELECT 
			name, 
			driver, 
			timestamp_floor('10m', timestamp) AS ten_minutes,
			avg(velocity) AS avg_velocity
		FROM %s
		WHERE timestamp >= '%s' AND timestamp < '%s' AND fleet = '%s'
		GROUP BY name, driver, ten_minutes
	),
	filtered_intervals AS (
		SELECT 
			name, 
			driver, 
			ten_minutes
		FROM driving_intervals
		WHERE avg_velocity > 1
	),
	driver_counts AS (
		SELECT 
			name, 
			driver,
			count(*) AS interval_count
		FROM filtered_intervals
		GROUP BY name, driver
	)
	SELECT 
		name, 
		driver
	FROM driver_counts
	WHERE interval_count > %d`,
		iotReadingsTable,
		interval.Start().Format(questdbTimeFmt),
		interval.End().Format(questdbTimeFmt),
		i.GetRandomFleet(),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 5 mins per hour.
		tenMinutePeriods(5, iot.LongDrivingSessionDuration))

	humanLabel := "QuestDB trucks with longer driving sessions"
	humanDesc := fmt.Sprintf("%s: stopped less than 20 mins in 4 hour period", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TrucksWithLongDailySessions finds all trucks that have driven more than 10 hours in the last 24 hours.
func (i *IoT) TrucksWithLongDailySessions(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.DailyDrivingDuration)
	sql := fmt.Sprintf(`WITH driving_intervals AS (
		SELECT 
			name, 
			driver, 
			timestamp_floor('10m', timestamp) AS ten_minutes,
			avg(velocity) AS avg_velocity
		FROM %s
		WHERE timestamp >= '%s' AND timestamp < '%s' AND fleet = '%s'
		GROUP BY name, driver, ten_minutes
	),
	filtered_intervals AS (
		SELECT 
			name, 
			driver, 
			ten_minutes
		FROM driving_intervals
		WHERE avg_velocity > 1
	),
	driver_counts AS (
		SELECT 
			name, 
			driver,
			count(*) AS interval_count
		FROM filtered_intervals
		GROUP BY name, driver
	)
	SELECT 
		name, 
		driver
	FROM driver_counts
	WHERE interval_count > %d`,
		iotReadingsTable,
		interval.Start().Format(questdbTimeFmt),
		interval.End().Format(questdbTimeFmt),
		i.GetRandomFleet(),
		// Calculate number of 10 min intervals that is the max driving duration for the session if we rest 35 mins per hour.
		tenMinutePeriods(35, iot.DailyDrivingDuration))

	humanLabel := "QuestDB trucks with longer daily sessions"
	humanDesc := fmt.Sprintf("%s: drove more than 10 hours in the last 24 hours", humanLabel)

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgVsProjectedFuelConsumption calculates average and projected fuel consumption per fleet.
func (i *IoT) AvgVsProjectedFuelConsumption(qi query.Query) {
	sql := `SELECT fleet, avg(fuel_consumption) AS avg_fuel_consumption, avg(nominal_fuel_consumption) AS nominal_fuel_consumption
		FROM readings
		WHERE velocity > 1
		GROUP BY fleet`

	humanLabel := "QuestDB average vs projected fuel consumption per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingDuration finds the average driving duration per driver.
func (i *IoT) AvgDailyDrivingDuration(qi query.Query) {
	sql := `WITH driving_intervals AS (
		SELECT 
			name, 
			driver, 
			fleet, 
			timestamp_floor('10m', timestamp) AS ten_minutes,
			avg(velocity) AS avg_velocity
		FROM readings
		GROUP BY name, driver, fleet, ten_minutes
	),
	filtered_intervals AS (
		SELECT 
			name, 
			driver, 
			fleet, 
			ten_minutes
		FROM driving_intervals
		WHERE avg_velocity > 1
	),
	daily_driving AS (
		SELECT 
			name, 
			driver, 
			fleet, 
			timestamp_floor('24h', ten_minutes) AS day, 
			count(*) / 6.0 AS hours
		FROM filtered_intervals
		GROUP BY name, driver, fleet, day
	)
	SELECT 
		fleet, 
		name, 
		driver, 
		avg(hours) AS avg_daily_hours
	FROM daily_driving
	GROUP BY fleet, name, driver;`

	humanLabel := "QuestDB average driver driving duration per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgDailyDrivingSession finds the average driving session without stopping per driver per day.
func (i *IoT) AvgDailyDrivingSession(qi query.Query) {
	// This is a simplified version as QuestDB doesn't have the same window functions as PostgreSQL
	sql := `WITH driving_status AS (
		SELECT 
			name, 
			timestamp_floor('10m', timestamp) AS ten_minutes, 
			cast(avg(velocity) > 5 as int) AS driving
		FROM readings
		GROUP BY name, ten_minutes
	),
	sessions AS (
		SELECT 
			name, 
			timestamp_floor('4h', ten_minutes) AS session_group,
			min(ten_minutes) AS session_start, 
			max(ten_minutes) AS session_end, 
			(max(ten_minutes) - min(ten_minutes)) / 60000000.0 AS session_length
		FROM driving_status
		WHERE driving = 1
		GROUP BY name, timestamp_floor('4h', ten_minutes)
	)
	SELECT 
		name, 
		timestamp_floor('24h', session_start) AS day, 
		avg(session_length) AS avg_session_length
	FROM sessions
	GROUP BY name, timestamp_floor('24h', session_start);`

	humanLabel := "QuestDB average driver driving session without stopping per day"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// AvgLoad finds the average load per truck model per fleet.
func (i *IoT) AvgLoad(qi query.Query) {
	interval := i.Interval.MustRandWindow(iot.AvgLoadDuration)
	sql := fmt.Sprintf(`WITH load_data AS (
		SELECT r.fleet, r.model, r.load_capacity, d.current_load
		FROM (
			SELECT fleet, model, load_capacity, name, timestamp
			FROM %s
			WHERE timestamp >= '%s' AND timestamp < '%s'
		) r
		ASOF JOIN (
			SELECT name, current_load, timestamp
			FROM %s
			WHERE timestamp >= '%s' AND timestamp < '%s'
		) d
		ON r.name = d.name
	)
	SELECT fleet, model, load_capacity, avg(current_load / load_capacity) AS avg_load_percentage
	FROM load_data
	GROUP BY fleet, model, load_capacity;
	`,
		iotReadingsTable,
		interval.Start().Format(questdbTimeFmt),
		interval.End().Format(questdbTimeFmt),
		iotDiagnosticsTable,
		interval.Start().Format(questdbTimeFmt),
		interval.End().Format(questdbTimeFmt))

	humanLabel := "QuestDB average load per truck model per fleet"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// DailyTruckActivity returns the number of hours trucks has been active (not out-of-commission) per day per fleet per model.
func (i *IoT) DailyTruckActivity(qi query.Query) {
	sql := `WITH active_intervals AS (
		SELECT 
			fleet, 
			model, 
			timestamp_floor('10m', timestamp) AS ten_minutes, 
			timestamp_floor('24h', timestamp) AS day,
			avg(status) AS avg_status
		FROM diagnostics
		GROUP BY fleet, model, ten_minutes, day
	)
	SELECT 
		fleet, 
		model, 
		day, 
		count(*) / 144 AS daily_activity
	FROM active_intervals
	WHERE avg_status < 1
	GROUP BY fleet, model, day
	ORDER BY day;
	`

	humanLabel := "QuestDB daily truck activity per fleet per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// TruckBreakdownFrequency calculates the amount of times a truck model broke down in the last period.
func (i *IoT) TruckBreakdownFrequency(qi query.Query) {
	// Simplified version as QuestDB doesn't have lead/lag functions
	sql := `WITH breakdown_status AS (
		SELECT 
			model, 
			timestamp_floor('10m', timestamp) AS ten_minutes, 
			CASE WHEN avg(status) < 0.5 THEN 1 ELSE 0 END AS broken_down
		FROM diagnostics
		GROUP BY model, ten_minutes
	),
	transitions AS (
		SELECT 
			model, 
			ten_minutes, 
			broken_down,
			lag(broken_down) OVER (PARTITION BY model ORDER BY ten_minutes) AS prev_broken_down
		FROM breakdown_status
	)
	SELECT 
		model, 
		count(*) AS breakdown_count
	FROM transitions
	WHERE broken_down = 1 AND (prev_broken_down = 0 OR prev_broken_down IS NULL)
	GROUP BY model;
	`

	humanLabel := "QuestDB truck breakdown frequency per model"
	humanDesc := humanLabel

	i.fillInQuery(qi, humanLabel, humanDesc, sql)
}

// tenMinutePeriods calculates the number of 10 minute periods that can fit in
// the time duration if we subtract the minutes specified by minutesPerHour value.
// E.g.: 4 hours - 5 minutes per hour = 3 hours and 40 minutes = 22 ten minute periods
func tenMinutePeriods(minutesPerHour float64, duration time.Duration) int {
	durationMinutes := duration.Minutes()
	leftover := minutesPerHour * duration.Hours()
	return int((durationMinutes - leftover) / 10)
}
