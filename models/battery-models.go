package models

import "go.mongodb.org/mongo-driver/bson/primitive"

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
	BatteryManufactureDate    string             `json:"battery_manufacture_date" bson:"battery_manufacture_date"`
	BatteryManufactureName    string             `json:"battery_manufacture_name" bson:"battery_manufacture_name"`
	BatteryName               string             `json:"battery_name" bson:"battery_name"`
	BatteryChemID             string             `json:"battery_chem_id" bson:"battery_chem_id"`
	BmsBarCode                string             `json:"bms_bar_code" bson:"bms_bar_code"`
	CellVoltageList0          interface{}        `json:"cell_voltage_list_0" bson:"cell_voltage_list_0"`
	CellVoltageList1          interface{}        `json:"cell_voltage_list_1" bson:"cell_voltage_list_1"`
	History                   interface{}        `json:"history" bson:"history"`
	ErrorCount                interface{}        `json:"error_count" bson:"error_count"`
	BatteryStatus             interface{}        `json:"battery_status" bson:"status"`
	IsFirstFill               bool               `json:"is_first_fill" bson:"is_first_fill"`
	IsSecondFill              bool               `json:"is_second_fill" bson:"is_second_fill"`
	IsThirdFill               bool               `json:"is_third_fill" bson:"is_third_fill"`
	CreatedAt                 primitive.DateTime `json:"created_at" bson:"created_at"`
	UpdatedAt                 primitive.DateTime `json:"updated_at" bson:"updated_at"`
	LocalDate                 string             `json:"local_date" bson:"local_d"`
	LocalTime                 string             `json:"local_time" bson:"local_t"`
	ISOTimeStamp              string             `json:"iso_timestamp" bson:"iso_timestamp"`
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
