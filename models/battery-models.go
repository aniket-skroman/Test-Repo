package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BatteryTemperatureAlert struct {
	ID             primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	BMSID          string             `json:"bms_id" bson:"bms_id"`
	TotalAttempt   int64              `json:"total_attempt" bson:"total_attempt"`
	Temperature    []int              `json:"battery_temperature:" bson:"battery_temperature:"`
	CreatedAt      primitive.DateTime `json:"created_at" bson:"created_at"`
	UpdateAt       primitive.DateTime `json:"updated_at" bson:"updated_at"`
	LocalTimeStamp interface{}        `json:"local_timestamp" bson:"local_timestamp"`
}

type BatteryHardwareMain struct {
	Id                        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Type                      int                `json:"type" bson:"type"`
	BmsID                     string             `json:"bms_id" bson:"bms_id"`
	GsmSignalStrength         int                `json:"gsm_signal_strength" bson:"gsm_signal_strength"`
	GpsSignalStrength         int                `json:"gps_signal_strength" bson:"gps_signal_strength"`
	GpsSatelliteInViewCount   int                `json:"gps_satellite_in_view_count" bson:"gps_satellite_in_view_count"`
	GnssSatelliteUsedCount    int                `json:"gnss_satellite_used_count" bson:"gnss_satellite_used_count"`
	GpsStatus                 int                `json:"gps_status" bson:"gps_status"`
	LocationLongitude         int                `json:"location_longitude" bson:"location_longitude"`
	LocationLatitude          int                `json:"location_latitude" bson:"location_latitude"`
	LocationSpeed             int                `json:"location_speed" bson:"location_speed"`
	LocationAngle             int                `json:"location_angle" bson:"location_angle"`
	IotTemperature            int                `json:"iot_temperature" bson:"iot_temperature"`
	GprsTotalDataUsed         int                `json:"gprs_total_data_used" bson:"gprs_total_data_used"`
	SoftwareVersion           string             `json:"software_version" bson:"software_version"`
	BmsSoftwareVersion        string             `json:"bms_software_version" bson:"bms_software_version"`
	Iccid                     string             `json:"iccid" bson:"iccid"`
	Imei                      string             `json:"imei" bson:"imei"`
	GprsApn                   string             `json:"gprs_apn" bson:"gprs_apn"`
	BatteryVoltage            int                `json:"battery_voltage" bson:"battery_voltage"`
	BatteryCurrent            int                `json:"battery_current" bson:"battery_current"`
	BatterySoc                int                `json:"battery_soc" bson:"battery_soc"`
	BatteryTemperature        interface{}        `json:"battery_temperature" bson:"battery_temperature"`
	BatteryRemainingCapacity  int                `json:"battery_remaining_capacity" bson:"battery_remaining_capacity"`
	BatteryFullChargeCapacity int                `json:"battery_full_charge_capacity" bson:"battery_full_charge_capacity"`
	BatteryCycleCount         int                `json:"battery_cycle_count" bson:"battery_cycle_count"`
	BatteryRatedCapacity      int                `json:"battery_rated_capacity" bson:"battery_rated_capacity"`
	BatteryRatedVoltage       int                `json:"battery_rated_voltage" bson:"battery_rated_voltage"`
	BatteryVersion            string             `json:"battery_version" bson:"battery_version"`
	BatteryManufactureDate    interface{}        `json:"battery_manufacture_date" bson:"battery_manufacture_date"`
	BatteryManufactureName    string             `json:"battery_manufacture_name" bson:"battery_manufacture_name"`
	BatteryName               string             `json:"battery_name" bson:"battery_name"`
	BatteryChemID             string             `json:"battery_chem_id" bson:"battery_chem_id"`
	BmsBarCode                string             `json:"bms_bar_code" bson:"bms_bar_code"`
	CellVoltageList0          interface{}        `json:"cell_voltage_list_0" bson:"cell_voltage_list_0"`
	CellVoltageList1          interface{}        `json:"cell_voltage_list_1" bson:"cell_voltage_list_1"`
	History                   interface{}        `json:"history" bson:"history"`
	ErrorCount                interface{}        `json:"error_count" bson:"error_count"`
	Status                    interface{}        `json:"status" bson:"status"`
	BatteryStatus             string             `json:"battery_status" bson:"battery_status"`
	IsFirstFill               bool               `json:"is_first_fill" bson:"is_first_fill"`
	IsSecondFill              bool               `json:"is_second_fill" bson:"is_second_fill"`
	IsThirdFill               bool               `json:"is_third_fill" bson:"is_third_fill"`
	CreatedAt                 primitive.DateTime `json:"created_at" bson:"created_at"`
	UpdatedAt                 primitive.DateTime `json:"updated_at" bson:"updated_at"`
	LocalDate                 string             `json:"local_date" bson:"local_d"`
	LocalTime                 string             `json:"local_time" bson:"local_t"`
	ISOTimeStamp              string             `json:"iso_timestamp" bson:"iso_timestamp"`
	OldCycleCount             int                `json:"old_cycle_count" bson:"old_cycle_count"`
	OldBatteryCurrent         int                `json:"old_battery_current" bson:"old_battery_current"`
	MinMaxSoc                 []int              `json:"min_max_soc" bson:"min_max_soc"`
	SpeedCal                  []int              `json:"speed_cal" bson:"speed_cal"`
	ODOMeter                  float64            `json:"odo_meter" bson:"odo_meter"`
}

func (batteryMain *BatteryHardwareMain) FormatSpeed() string {
	speed := batteryMain.LocationSpeed

	if speed > 0 {
		return "Moving"
	} else {
		return "Idle"
	}
}

func (batteryMain *BatteryHardwareMain) GetBatteryCurrentStatus() string {
	current := batteryMain.BatteryCurrent

	if current >= 0 {
		return "Discharge"
	} else {
		return "Charge"
	}
}

func (batteryMain *BatteryHardwareMain) GetBatterySOCStatus() string {
	batterySOC := batteryMain.BatterySoc

	switch {
	case batterySOC <= 25:
		return "Critical"
	case batterySOC > 25 && batterySOC <= 40:
		return "Ok"
	case batterySOC > 40 && batterySOC <= 60:
		return "Good"
	default:
		return "Very Good"
	}
}

func (batteryMain *BatteryHardwareMain) FormatByThousand(formatValue int) int {
	return formatValue / 1000
}

func (batteryMain *BatteryHardwareMain) FormatByHundred(formatValue int) int {
	return formatValue / 100
}

type BMSIdList struct {
	BMSID []string `json:"bms_id" bson:"bms_id"`
}

type LocationData struct {
	Latitude  int `json:"latitude" bson:"latitude"`
	Longitude int `json:"longitude" bson:"longitude"`
}

type BatteryDistanceTravelled struct {
	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	BMSID     string             `json:"bms_id" bson:"bms_id"`
	Location  []LocationData     `json:"location" bson:"location"`
	CreatedAt primitive.DateTime `json:"created_at" bson:"created_at"`
}

type UpdateBatteryDistanceTravelled struct {
	BMSID             string
	DistanceTravelled float64
}

type CreateCycleBasedReport struct {
	ID          primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	BMSID       string             `json:"bms_id" bson:"bms_id"`
	Asset       string             `json:"asset" bson:"asset"`
	CycleNo     int                `json:"cycle_no" bson:"cycle_no"`
	StartTime   primitive.DateTime `json:"start_time" bson:"start_time"`
	EndTime     primitive.DateTime `json:"end_time" bson:"end_time"`
	KMTravelled float64            `json:"km_travelled" bson:"km_travelled"`
	MinSoc      int                `json:"min_soc" bson:"min_soc"`
	MaxSoc      int                `json:"max_soc" bson:"max_soc"`
	AvgSpeed    int                `json:"avg_speed" bson:"avg_speed"`
	TopSpeed    int                `json:"top_speed" bson:"top_speed"`
	LowestSpeed int                `json:"lowest_speed" bson:"lowest_speed"`
	IsStart     bool               `json:"is_start" bson:"is_start"`
	IsEnd       bool               `json:"is_end" bson:"is_end"`
	DOD         string             `json:"dod" bson:"dod"`
	StartODO    float64            `json:"start_odo" bson:"start_odo"`
	EndODO      float64            `json:"end_odo" bson:"end_odo"`
	CreatedAt   primitive.DateTime `json:"created_at" bson:"created_at"`
}

type LastSevenHourUnreported struct {
	ID              primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Time            string             `json:"time" bson:"time"`
	UTCTime         primitive.DateTime `json:"utc_time" bson:"utc_time"`
	UnreportedCount int64              `json:"unreported_count" bson:"unreported_count"`
	CreatedAt       primitive.DateTime `json:"created_at" bson:"created_at"`
}

func (lst7Un *LastSevenHourUnreported) SetLastSevenHourUnreported(total int64) {
	cal := total - lst7Un.UnreportedCount
	lst7Un.UnreportedCount = cal
}

type Last24HourUnreported struct {
	ID               primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	Time             string             `json:"time" bson:"time"`
	UTCTime          primitive.DateTime `json:"utc_time" bson:"utc_time"`
	UnreportedCount  int64              `json:"unreported_count" bson:"unreported_count"`
	IndependentCount int64              `json:"independent_total_count" bson:"independent_total_count"`
	CreatedAt        primitive.DateTime `json:"created_at" bson:"created_at"`
}

type Last24HourUnreportedSpecificData struct {
	Time    string             `json:"time" bson:"time"`
	UTCTime primitive.DateTime `json:"utc_time" bson:"utc_time"`
	Data    []bson.M           `json:"data" bson:"data"`
}

type StartChargingReport struct {
	ID        primitive.ObjectID `json:"id" bson:"_id"`
	BMSID     string             `json:"bms_id" bson:"bms_id"`
	Asset     string             `json:"asset" bson:"asset"`
	IMEI      string             `json:"imei" bson:"imei"`
	StartTime primitive.DateTime `json:"start_time" bson:"start_time"`
	StartSOC  int                `json:"start_soc" bson:"start_soc"`
	IsStart   bool               `json:"is_start" bson:"is_start"`
}

func (startCharge *StartChargingReport) SetStartChargingReport(moduleData BatteryHardwareMain) StartChargingReport {
	newStartCharging := new(StartChargingReport)

	newStartCharging.BMSID = moduleData.BmsID
	newStartCharging.Asset = moduleData.BmsID
	newStartCharging.IMEI = moduleData.Imei
	newStartCharging.StartTime = primitive.NewDateTimeFromTime(time.Now())
	newStartCharging.StartSOC = moduleData.BatterySoc
	newStartCharging.IsStart = true

	return *newStartCharging
}

type EndChargingReport struct {
	BMSID   string             `json:"bms_id" bson:"bms_id"`
	EndTime primitive.DateTime `json:"end_time" bson:"end_time"`
	EndSOC  int                `json:"end_soc" bson:"end_soc"`
	IsEnd   bool               `json:"is_end" bson:"is_end"`
}

func (endCharge *EndChargingReport) SetEndChargingReport(moduleData BatteryHardwareMain) EndChargingReport {
	endChargeReport := new(EndChargingReport)

	endChargeReport.BMSID = moduleData.BmsID
	endChargeReport.EndTime = primitive.NewDateTimeFromTime(time.Now())
	endChargeReport.EndSOC = moduleData.BatterySoc
	endChargeReport.IsEnd = true

	return *endChargeReport
}

type ChargingReport struct {
	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	BMSID     string             `json:"bms_id" bson:"bms_id"`
	Asset     string             `json:"asset" bson:"asset"`
	IMEI      string             `json:"imei" bson:"imei"`
	StartTime primitive.DateTime `json:"start_time" bson:"start_time"`
	EndTime   primitive.DateTime `json:"end_time" bson:"end_time"`
	StartSOC  int                `json:"start_soc" bson:"start_soc"`
	EndSOC    int                `json:"end_soc" bson:"end_soc"`
	IsStart   bool               `json:"is_start" bson:"is_start"`
	IsEnd     bool               `json:"is_end" bson:"is_end"`
	CreatedAt primitive.DateTime `json:"created_at" bson:"created_at"`
}

func (chargeReport *ChargingReport) SetChargingReport() ChargingReport {
	charegingReport := new(ChargingReport)
	charegingReport.CreatedAt = primitive.NewDateTimeFromTime(time.Now())
	return *charegingReport
}

// to update a old battery current in battery main
type UpdateOldCurrent struct {
	BMSID      string `json:"bms_id" bson:"bms_id"`
	OldCurrent int    `json:"old_battery_current" bson:"old_battery_current"`
}

func (oldCurrent *UpdateOldCurrent) SetUpdateOldCurrent(moduleData []BatteryHardwareMain) []UpdateOldCurrent {
	newUpdateOldCurrent := []UpdateOldCurrent{}

	for i := range moduleData {
		temp := UpdateOldCurrent{
			BMSID:      moduleData[i].BmsID,
			OldCurrent: moduleData[i].BatteryCurrent,
		}

		newUpdateOldCurrent = append(newUpdateOldCurrent, temp)
	}

	return newUpdateOldCurrent
}

// to update a old battery cycle count in battery main
type UpdateOldCycleCount struct {
	BmsID         string `json:"bms_id" bson:"bms_id"`
	OldCycleCount int    `json:"old_cycle_count" bson:"old_cycle_count"`
}

func (oldCurrent *UpdateOldCycleCount) SetUpdateOldCycleCount(moduleData []BatteryHardwareMain) []UpdateOldCycleCount {
	newUpdateOldCurrent := []UpdateOldCycleCount{}

	for i := range moduleData {
		temp := UpdateOldCycleCount{
			BmsID:         moduleData[i].BmsID,
			OldCycleCount: moduleData[i].BatteryCycleCount,
		}

		newUpdateOldCurrent = append(newUpdateOldCurrent, temp)
	}

	return newUpdateOldCurrent
}
