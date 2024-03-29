package repositories

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"time"

	dbconfig "github.com/aniket0951/testproject/db-config"
	"github.com/aniket0951/testproject/helper"
	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/proxyapis"
	"github.com/mashingan/smapping"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var vehicleCollection *mongo.Collection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "vehicle_info")
var vehicleLocationCollection *mongo.Collection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "vehicles")
var vehicleAlertCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "vehicle_alerts")
var vehicleAlertHistoryCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "alert_history")
var alertMasterConfigCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "alert_config")
var vehicleFallAlertCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "vehicle_fall_alerts")
var testCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "test_collection")
var vehicleDistanceTravelCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "vehicle_distance_travel")
var batteryTempCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_temp")
var batteryMainCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_main")
var batteryReportingCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_reporting")
var bmsTempCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "bms_temperature_alert")
var batteryDistanceTravelledCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_distance_travelled")
var batteryCycleTempReportCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_cycle_temp_report")
var batteryCycleHistoryCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_cycle_history")
var batteryCycleLocationCollection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_cycle_location")

var Mclient *mongo.Client

type VehicleRepository interface {
	GetAllVehicles() ([]models.VehiclesData, error)
	AddUpdateVehicleInformation(vehicleInfo models.VehiclesData)
	RefreshVehicleData() ([]models.VehiclesData, error)
	UpdateVehicleData(vehicle models.VehiclesData) error
	AddVehicleLocationData(vehicleLocation models.VehicleLocationData)
	GetVehicleAlertById(vehicleId string) (models.VehicleAlerts, error)
	GetVehicleFallAlertById(vehicleId string) (models.VehicleFallAlerts, error)
	GetOverSpeedAlerts() ([]models.VehicleAlerts, error)
	GetAllVehicleFallAlerts() ([]models.VehicleFallAlerts, error)

	TrackVehicleAlert() ([]models.VehiclesData, error)
	UpdateVehicleAlert(vehicleData models.VehicleAlerts) error
	UpdateVehicleFallAlert(vehicleData models.VehicleFallAlerts) error
	CreateOverSpeedAlertHistory(vehicleAlerts []models.VehicleAlerts) error
	CreateVehicleFallAlertHistory(vehicleAlerts []models.VehicleFallAlerts) error
	CreateDistanceTravelHistory(vehicleData []models.VehiclesData) error
	CreateBatteryTemperatureHistory(batteryTemperature []models.BatteryTemperatureAlert) error
	GetBatteryTemperatureData() ([]models.BatteryTemperatureAlert, error)

	// ResetDistanceTravel()
	DeleteTodayAlert(alertId primitive.ObjectID) error
	DeleteTodayFallAlert(alertId primitive.ObjectID) error
	DeleteBatteryTemperatureAlert(batteryTempAlert []string) error

	GetAlertLimit(alertType string) (models.AlertConfig, error)

	BatteryTempToMain() error
	AddBatteryToMain(batteryData []models.BatteryHardwareMain) error
	DeleteBatteryTempData(batteryData []string) error
	UpdateBMSReporting(batteryData []string) error
	UpdateBMSDistanceTravelled([]models.BatteryHardwareMain) error

	//battery cycle
	CheckForBatteryCycle() ([]models.BatteryHardwareMain, error)
	UpdateBatteryCycle(batteryData []models.BatteryHardwareMain) error
	AddTestData() error
}

type vehiclerepository struct {
	vehicleCollection                  *mongo.Collection
	vehicleLocationConnection          *mongo.Collection
	vehicleAlertConnection             *mongo.Collection
	vehicleAlertHistoryConnection      *mongo.Collection
	alertConfigConnection              *mongo.Collection
	vehicleFallAlertsConnection        *mongo.Collection
	testConnection                     *mongo.Collection
	vehicleDistanceTravelConnection    *mongo.Collection
	batteryTempConnection              *mongo.Collection
	batteryMainConnection              *mongo.Collection
	batteryReportingConnection         *mongo.Collection
	batteryTemperatureConnection       *mongo.Collection
	batteryDistanceTravelledConnection *mongo.Collection
	batteryCycleTempReportConnection   *mongo.Collection
	batteryCycleHistoryConnection      *mongo.Collection
	batteryCycleLocationConnection     *mongo.Collection
}

func NewVehicleRepository() VehicleRepository {
	return &vehiclerepository{
		vehicleCollection:                  vehicleCollection,
		vehicleLocationConnection:          vehicleLocationCollection,
		vehicleAlertConnection:             vehicleAlertCollection,
		vehicleAlertHistoryConnection:      vehicleAlertHistoryCollection,
		alertConfigConnection:              alertMasterConfigCollection,
		vehicleFallAlertsConnection:        vehicleFallAlertCollection,
		testConnection:                     testCollection,
		vehicleDistanceTravelConnection:    vehicleDistanceTravelCollection,
		batteryTempConnection:              batteryTempCollection,
		batteryMainConnection:              batteryMainCollection,
		batteryReportingConnection:         batteryReportingCollection,
		batteryTemperatureConnection:       bmsTempCollection,
		batteryDistanceTravelledConnection: batteryDistanceTravelledCollection,
		batteryCycleTempReportConnection:   batteryCycleTempReportCollection,
		batteryCycleHistoryConnection:      batteryCycleHistoryCollection,
		batteryCycleLocationConnection:     batteryCycleLocationCollection,
	}
}

func (db *vehiclerepository) AddUpdateVehicleInformation(vehicleInfo models.VehiclesData) {
	filter := bson.D{
		bson.E{Key: "vehicleno", Value: vehicleInfo.VehicleNo},
	}
	opt := options.FindOneAndReplace().SetUpsert(true)

	_ = db.vehicleCollection.FindOneAndReplace(context.TODO(), filter, vehicleInfo, opt)

}

func (db *vehiclerepository) AddVehicleLocationData(vehicleLocation models.VehicleLocationData) {

	_, _ = db.vehicleLocationConnection.InsertOne(context.Background(), vehicleLocation)

}

func (db *vehiclerepository) RefreshVehicleData() ([]models.VehiclesData, error) {
	reqURL := "http://fusioniot.mobilogix.com/webservice?token=getLiveData&user=skroman@mautoafrica.com&pass=Mauto@777"

	requestChannel := make(chan models.AutoGenerated)
	go proxyapis.GetAllVehicels(reqURL, requestChannel)
	responseData := <-requestChannel

	if len(responseData.Root.VehicleData) <= 1 {
		return nil, errors.New("API time limit exceed")
	}

	var vehicleData []models.VehiclesData

	for i := range responseData.Root.VehicleData {
		temp := models.VehiclesData{}
		_ = smapping.FillStruct(&temp, smapping.MapFields(responseData.Root.VehicleData[i]))
		vehicleData = append(vehicleData, temp)
	}

	return vehicleData, nil
}

func (db *vehiclerepository) UpdateVehicleData(vehicle models.VehiclesData) error {
	opt := options.FindOneAndReplace().SetUpsert(true)

	filter := bson.D{
		bson.E{Key: "vehicleno", Value: vehicle.VehicleNo},
	}

	result := models.VehiclesData{}
	err := db.vehicleCollection.FindOne(context.TODO(), filter).Decode(&result)

	if err != nil {
		return err
	}

	if (result != models.VehiclesData{}) {
		prevLatitude := helper.ConvertStrToFloat(result.Latitude)
		prevLongitude := helper.ConvertStrToFloat(result.Longitude)

		currentLatitude := helper.ConvertStrToFloat(vehicle.Latitude)
		currentLongitude := helper.ConvertStrToFloat(vehicle.Longitude)

		pointA := helper.Coordinates{Latitude: prevLatitude, Longitude: prevLongitude}
		pointB := helper.Coordinates{Latitude: currentLatitude, Longitude: currentLongitude}
		distance := pointA.Distance(pointB)

		vehicle.DistanceTraveled = result.DistanceTraveled + distance
	}

	vehicle.CreatedAt = primitive.NewDateTimeFromTime(time.Now())
	vehicle.UpdatedAt = primitive.NewDateTimeFromTime(time.Now())
	vehicle.TimeStamp = primitive.NewDateTimeFromTime(time.Now())

	res := db.vehicleCollection.FindOneAndReplace(context.TODO(), filter, &vehicle, opt)
	return res.Err()
}

func (db *vehiclerepository) GetVehicleAlertById(vehicleId string) (models.VehicleAlerts, error) {
	filter := bson.D{
		bson.E{Key: "bike_no", Value: vehicleId},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var vehicleData models.VehicleAlerts

	_ = db.vehicleAlertConnection.FindOne(ctx, filter).Decode(&vehicleData)

	return vehicleData, nil
}

func (db *vehiclerepository) GetVehicleFallAlertById(vehicleId string) (models.VehicleFallAlerts, error) {
	filter := bson.D{
		bson.E{Key: "bike_no", Value: vehicleId},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var vehicleData models.VehicleFallAlerts

	_ = db.vehicleFallAlertsConnection.FindOne(ctx, filter).Decode(&vehicleData)

	return vehicleData, nil
}

func (db *vehiclerepository) TrackVehicleAlert() ([]models.VehiclesData, error) {
	reqURL := "http://fusioniot.mobilogix.com/webservice?token=getLiveData&user=skroman@mautoafrica.com&pass=Mauto@777"

	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println("URL heat error => ", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(resp.Body)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Read all error =>", err)
	}
	content := string(body)
	var jsonMap models.AutoGenerated
	_ = json.Unmarshal([]byte(content), &jsonMap)
	vehicleData := []models.VehiclesData{}

	for i := range jsonMap.Root.VehicleData {
		temp := models.VehiclesData{}
		_ = smapping.FillStruct(&temp, smapping.MapFields(jsonMap.Root.VehicleData[i]))

		vehicleData = append(vehicleData, temp)
	}

	return vehicleData, nil
}

func (db *vehiclerepository) UpdateVehicleAlert(vehicleData models.VehicleAlerts) error {

	vehicleData.CreateAt = primitive.NewDateTimeFromTime(time.Now())
	vehicleData.UpdateAt = primitive.NewDateTimeFromTime(time.Now())

	istTime := helper.ConvertUTCToIndia()
	vehicleData.ISTTimeStamp = istTime[0] + istTime[1]

	filter := bson.D{
		bson.E{Key: "bike_no", Value: vehicleData.BikeNo},
	}
	opts := options.FindOneAndReplace().SetUpsert(true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = db.vehicleAlertConnection.FindOneAndReplace(ctx, filter, vehicleData, opts)

	return nil
}

func (db *vehiclerepository) UpdateVehicleFallAlert(vehicleData models.VehicleFallAlerts) error {

	vehicleData.CreateAt = primitive.NewDateTimeFromTime(time.Now())
	vehicleData.UpdateAt = primitive.NewDateTimeFromTime(time.Now())

	istTime := helper.ConvertUTCToIndia()
	vehicleData.ISTTimeStamp = istTime[0] + istTime[1]

	filter := bson.D{
		bson.E{Key: "bike_no", Value: vehicleData.BikeNo},
	}
	opts := options.FindOneAndReplace().SetUpsert(true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = db.vehicleFallAlertsConnection.FindOneAndReplace(ctx, filter, vehicleData, opts)

	return nil
}

func (db *vehiclerepository) GetOverSpeedAlerts() ([]models.VehicleAlerts, error) {

	filter := []bson.M{
		{"$match": bson.M{
			"create_at": bson.M{
				"$lte": primitive.NewDateTimeFromTime(time.Now()),
			},
		},
		},
	}

	cursor, curErr := db.vehicleAlertConnection.Aggregate(context.TODO(), filter)

	if curErr != nil {
		return nil, curErr
	}

	var vehicleAlerts []models.VehicleAlerts

	if err := cursor.All(context.TODO(), &vehicleAlerts); err != nil {
		return nil, err
	}

	return vehicleAlerts, nil
}

func (db *vehiclerepository) GetAllVehicleFallAlerts() ([]models.VehicleFallAlerts, error) {
	filter := []bson.M{
		{"$match": bson.M{
			"create_at": bson.M{
				"$lte": primitive.NewDateTimeFromTime(time.Now()),
			},
		},
		},
	}

	cursor, curErr := db.vehicleFallAlertsConnection.Aggregate(context.TODO(), filter)

	if curErr != nil {
		return nil, curErr
	}

	var vehicleAlerts []models.VehicleFallAlerts

	if err := cursor.All(context.TODO(), &vehicleAlerts); err != nil {
		return nil, err
	}

	return vehicleAlerts, nil
}

func (db *vehiclerepository) CreateOverSpeedAlertHistory(vehicleAlerts []models.VehicleAlerts) error {

	for i := range vehicleAlerts {
		if (reflect.DeepEqual(models.VehicleAlerts{}, vehicleAlerts[i])) {
			continue
		} else {

			temp := models.VehicleAlertHistory{}
			_ = smapping.FillStruct(&temp, smapping.MapFields(vehicleAlerts[i]))

			temp.Id = primitive.NewObjectID()
			temp.HistoryTimestamp = primitive.NewDateTimeFromTime(time.Now())
			temp.AlertType = "overspeed"

			_, err := db.vehicleAlertHistoryConnection.InsertOne(context.TODO(), temp)

			if err != nil {
				return err
			}

			_ = db.DeleteTodayAlert(vehicleAlerts[i].Id)
		}

	}

	return nil
}

func (db *vehiclerepository) CreateVehicleFallAlertHistory(vehicleAlerts []models.VehicleFallAlerts) error {

	for i := range vehicleAlerts {
		if (reflect.DeepEqual(models.VehicleFallAlerts{}, vehicleAlerts[i])) {
			continue
		} else {

			temp := models.VehicleFallAlertHistory{}
			_ = smapping.FillStruct(&temp, smapping.MapFields(vehicleAlerts[i]))

			temp.Id = primitive.NewObjectID()
			temp.HistoryTimestamp = primitive.NewDateTimeFromTime(time.Now())
			temp.AlertType = "fall"

			_, err := db.vehicleAlertHistoryConnection.InsertOne(context.TODO(), temp)

			if err != nil {
				return err
			}

			_ = db.DeleteTodayFallAlert(vehicleAlerts[i].Id)
		}

	}

	return nil
}

func (db *vehiclerepository) DeleteTodayAlert(alertId primitive.ObjectID) error {

	filter := bson.D{
		bson.E{Key: "_id", Value: alertId},
	}

	_, err := db.vehicleAlertConnection.DeleteOne(context.TODO(), filter)

	return err
}

func (db *vehiclerepository) DeleteTodayFallAlert(alertId primitive.ObjectID) error {
	filter := bson.D{
		bson.E{Key: "_id", Value: alertId},
	}

	_, err := db.vehicleFallAlertsConnection.DeleteOne(context.TODO(), filter)

	return err
}

func (db *vehiclerepository) GetAlertLimit(alertType string) (models.AlertConfig, error) {
	filter := bson.D{
		bson.E{Key: "alert_type", Value: alertType},
	}

	alertConfig := models.AlertConfig{}

	res := db.alertConfigConnection.FindOne(context.TODO(), filter).Decode(&alertConfig)
	if res != nil {
		return models.AlertConfig{}, res
	}

	return alertConfig, nil
}

func (db *vehiclerepository) AddTestData() error {
	// filter := bson.D{
	// 	bson.E{Key: "test", Value: "test2"},
	// }

	// t := time.Now()
	// isoDate := t.Format(time.RFC3339)

	// update := bson.D{
	// 	bson.E{Key: "$push", Value: bson.D{
	// 		bson.E{Key: "data", Value: 13},
	// 	}},
	// 	bson.E{Key: "$inc", Value: bson.D{
	// 		bson.E{Key: "count", Value: 2},
	// 	}},
	// 	bson.E{Key: "$set", Value: bson.D{
	// 		bson.E{Key: "isoTime", Value: isoDate},
	// 	}},
	// }

	// opts := options.Update().SetUpsert(true)

	// // bson.M{"$push": bson.M{"data": 12}}

	// res, err := db.testConnection.UpdateOne(context.TODO(), filter, update, opts)
	// if err != nil {
	// 	return err
	// }
	// return nil

	bmsTempCollection := dbconfig.GetCollection(dbconfig.ResolveClientDB(), "bms_temperature_alert")

	cursor, curErr := bmsTempCollection.Find(context.TODO(), bson.M{})

	if curErr != nil {
		return curErr
	}

	var data []models.BatteryTemperatureAlert

	if err := cursor.All(context.TODO(), &data); err != nil {
		return err
	}

	for i := range data {
		temp := reflect.TypeOf(data[i].LocalTimeStamp)
		if temp.Name() != "string" {
			filter := bson.D{
				bson.E{Key: "bms_id", Value: data[i].BMSID},
			}
			_, _ = bmsTempCollection.DeleteOne(context.TODO(), filter)

		}
	}

	return nil
}

func (db *vehiclerepository) GetAllVehicles() ([]models.VehiclesData, error) {
	cursor, curErr := db.vehicleCollection.Find(context.TODO(), bson.M{})

	if curErr != nil {
		return nil, curErr
	}

	vehiclesData := []models.VehiclesData{}

	if err := cursor.All(context.TODO(), &vehiclesData); err != nil {
		return nil, err
	}

	return vehiclesData, nil
}

func (db *vehiclerepository) CreateDistanceTravelHistory(vehicleData []models.VehiclesData) error {
	for i := range vehicleData {
		temp := models.VehicleFallAlertHistory{}

		temp.Id = primitive.NewObjectID()
		temp.HistoryTimestamp = primitive.NewDateTimeFromTime(time.Now())
		temp.AlertType = "travel_distance"
		temp.BikeNo = vehicleData[i].VehicleNo
		temp.CreateAt = primitive.NewDateTimeFromTime(time.Now())
		temp.DistanceTraveled = vehicleData[i].DistanceTraveled

		_, err := db.vehicleAlertHistoryConnection.InsertOne(context.TODO(), temp)

		if err != nil {
			return err
		}
	}

	return nil
}

func (db *vehiclerepository) BatteryTempToMain() error {
	fmt.Println("battery temp to main run from repo..")
	filter := bson.D{
		bson.E{Key: "is_first_fill", Value: true},
		bson.E{Key: "is_second_fill", Value: true},
		bson.E{Key: "is_third_fill", Value: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cursor, curErr := db.batteryTempConnection.Find(ctx, filter)

	if curErr != nil {
		return curErr
	}

	var batteryData []models.BatteryHardwareMain

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return err
	}

	dataToDelete := []string{}

	for i := range batteryData {
		dataToDelete = append(dataToDelete, batteryData[i].BmsID)
	}

	go db.CreateMBMSRawAndSOCData(batteryData)
	db.UpdateBatteryCycleStartParamsInMain(batteryData)
	db.UpdateBatteryLocationForCycle(batteryData)
	db.DeleteBatteryTempData(dataToDelete)
	db.AddBatteryToMain(batteryData)
	db.UpdateBMSDistanceTravelled(batteryData)
	// db.UpdateBMSReporting(dataToDelete)
	return nil
}

func (db *vehiclerepository) DeleteBatteryTempData(batteryData []string) error {
	fmt.Println("Delete battery temp data run successfully... with data :  ", len(batteryData))
	filter := bson.D{
		bson.E{Key: "bms_id", Value: bson.D{
			bson.E{Key: "$in", Value: batteryData},
		}},
	}

	res, err := db.batteryTempConnection.DeleteMany(context.TODO(), filter)
	fmt.Println("Result data from delete : ", res.DeletedCount)
	return err
}

func (db *vehiclerepository) DeleteBatteryTemperatureAlert(batteryTempAlert []string) error {
	filter := bson.D{
		bson.E{Key: "bms_id", Value: bson.D{
			bson.E{Key: "$in", Value: batteryTempAlert},
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.batteryTemperatureConnection.DeleteMany(ctx, filter)
	return err
}

func (db *vehiclerepository) AddBatteryToMain(batteryData []models.BatteryHardwareMain) error {
	var operations []mongo.WriteModel

	for i := range batteryData {
		optionsA := mongo.NewUpdateOneModel()
		optionsA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		LocalTimeStamp := helper.ConvertUTCToIndia()
		localDate := LocalTimeStamp[0]
		localTime := LocalTimeStamp[1]

		var batteryStatus, chargingStatus, batterySOCData string

		batteryStatus = batteryData[i].FormatSpeed()
		chargingStatus = batteryData[i].GetBatteryCurrentStatus()
		batterySOCData = batteryData[i].GetBatterySOCStatus()
		locationAngel := batteryData[i].FormatLocationAngle()
		fullChargeCapacity := batteryData[i].FormatByThousand(batteryData[i].BatteryFullChargeCapacity)
		ratedCapacity := batteryData[i].FormatByThousand(batteryData[i].BatteryRatedCapacity)
		ratedVoltage := batteryData[i].FormatByThousand(batteryData[i].BatteryRatedVoltage)
		voltage := batteryData[i].FormatByThousand(batteryData[i].BatteryVoltage)

		iotTemp := batteryData[i].FormatByHundred(batteryData[i].IotTemperature)

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "type", Value: batteryData[i].Type},
				bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
				bson.E{Key: "gsm_signal_strength", Value: batteryData[i].GsmSignalStrength},
				bson.E{Key: "gps_signal_strength", Value: batteryData[i].GpsSignalStrength},
				bson.E{Key: "gps_satellite_in_view_count", Value: batteryData[i].GpsSatelliteInViewCount},
				bson.E{Key: "gnss_satellite_used_count", Value: batteryData[i].GnssSatelliteUsedCount},
				bson.E{Key: "gps_status", Value: batteryData[i].GpsStatus},
				bson.E{Key: "location_longitude", Value: batteryData[i].LocationLongitude},
				bson.E{Key: "location_latitude", Value: batteryData[i].LocationLatitude},
				bson.E{Key: "location_speed", Value: batteryData[i].LocationSpeed},
				bson.E{Key: "location_angle", Value: locationAngel},
				bson.E{Key: "iot_temperature", Value: iotTemp},
				bson.E{Key: "gprs_total_data_used", Value: batteryData[i].GprsTotalDataUsed},
				bson.E{Key: "software_version", Value: batteryData[i].SoftwareVersion},
				bson.E{Key: "bms_software_version", Value: batteryData[i].BmsSoftwareVersion},
				bson.E{Key: "iccid", Value: batteryData[i].Iccid},
				bson.E{Key: "imei", Value: batteryData[i].Imei},
				bson.E{Key: "gprs_apn", Value: batteryData[i].GprsApn},
				bson.E{Key: "is_first_fill", Value: true},
				bson.E{Key: "battery_voltage", Value: voltage},
				bson.E{Key: "battery_current", Value: batteryData[i].BatteryCurrent},
				bson.E{Key: "battery_soc", Value: batteryData[i].BatterySoc},
				bson.E{Key: "battery_temperature", Value: batteryData[i].BatteryTemperature},
				bson.E{Key: "battery_remaining_capacity", Value: batteryData[i].BatteryRemainingCapacity},
				bson.E{Key: "battery_full_charge_capacity", Value: fullChargeCapacity},
				bson.E{Key: "battery_cycle_count", Value: batteryData[i].BatteryCycleCount},
				bson.E{Key: "battery_rated_capacity", Value: ratedCapacity},
				bson.E{Key: "battery_rated_voltage", Value: ratedVoltage},
				bson.E{Key: "battery_version", Value: batteryData[i].BatteryVersion},
				bson.E{Key: "battery_manufacture_date", Value: batteryData[i].BatteryManufactureDate},
				bson.E{Key: "battery_manufacture_name", Value: batteryData[i].BatteryManufactureName},
				bson.E{Key: "battery_name", Value: batteryData[i].BatteryName},
				bson.E{Key: "battery_chem_id", Value: batteryData[i].BatteryChemID},
				bson.E{Key: "bms_bar_code", Value: batteryData[i].BmsBarCode},
				bson.E{Key: "is_second_fill", Value: true},
				bson.E{Key: "cell_voltage_list_0", Value: batteryData[i].CellVoltageList0},
				bson.E{Key: "cell_voltage_list_1", Value: batteryData[i].CellVoltageList1},
				bson.E{Key: "history", Value: batteryData[i].History},
				bson.E{Key: "error_count", Value: batteryData[i].ErrorCount},
				bson.E{Key: "status", Value: batteryData[i].Status},
				bson.E{Key: "battery_status", Value: batteryStatus},
				bson.E{Key: "battery_charge_status", Value: chargingStatus},
				bson.E{Key: "battery_soc_status", Value: batterySOCData},
				bson.E{Key: "is_third_fill", Value: true},
				bson.E{Key: "created_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "updated_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "local_d", Value: localDate},
				bson.E{Key: "local_t", Value: localTime},
			}},
		}

		optionsA.SetUpdate(update)
		optionsA.SetUpsert(true)
		operations = append(operations, optionsA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := db.batteryMainConnection.BulkWrite(ctx, operations)
	fmt.Println("Add battery result : ", res)
	return err
}

func (db *vehiclerepository) UpdateBMSReporting(batteryData []string) error {
	var operations []mongo.WriteModel

	for i := range batteryData {
		option := mongo.NewUpdateOneModel()
		option.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i]},
		})

		LocalTimeStamp := helper.ConvertUTCToIndia()
		localDate := LocalTimeStamp[0]
		localTime := LocalTimeStamp[1]

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "created_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "updated_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "local_timestamp", Value: localDate + " " + localTime},
			}},
		}

		option.SetUpdate(&update)
		option.SetUpsert(true)
		operations = append(operations, option)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := db.batteryReportingConnection.BulkWrite(ctx, operations)

	return err
}

func (db *vehiclerepository) CreateMBMSRawAndSOCData(hardWareData []models.BatteryHardwareMain) error {
	Mclient = ConnectToMDB()
	var remote = "telematics"
	rawDataCollection := Mclient.Database(remote).Collection("bms_rawdata")
	socDataCollection := Mclient.Database(remote).Collection("bms_soc_updated_data")

	currentTime := time.Now()
	isoDateTime := currentTime.Format(time.RFC3339)

	for i := range hardWareData {
		LocalTimeStamp := helper.ConvertUTCToIndia()
		localDate := LocalTimeStamp[0]
		localTime := LocalTimeStamp[1]
		hardWareData[i].LocalDate = localDate
		hardWareData[i].LocalTime = localTime
		hardWareData[i].ISOTimeStamp = isoDateTime
		hardWareData[i].CreatedAt = primitive.NewDateTimeFromTime(time.Now())
		hardWareData[i].UpdatedAt = primitive.NewDateTimeFromTime(time.Now())
		hardWareData[i].Id = primitive.NewObjectID()

		rawDataCollection.InsertOne(context.TODO(), hardWareData[i])
		// if err != nil {
		// 	fmt.Println("Error from raw collection => ", err)
		// } else {
		// 	fmt.Println("Inserted Ids from raw : ", res.InsertedID)
		// }

		hardWareData[i].LocalDate = isoDateTime

		opts := options.Update().SetUpsert(true)
		filter := bson.D{
			bson.E{Key: "bms_id", Value: hardWareData[i].BmsID},
		}

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "type", Value: hardWareData[i].Type},
				bson.E{Key: "bms_id", Value: hardWareData[i].BmsID},
				bson.E{Key: "gsm_signal_strength", Value: hardWareData[i].GsmSignalStrength},
				bson.E{Key: "gps_signal_strength", Value: hardWareData[i].GpsSignalStrength},
				bson.E{Key: "gps_satellite_in_view_count", Value: hardWareData[i].GpsSatelliteInViewCount},
				bson.E{Key: "gnss_satellite_used_count", Value: hardWareData[i].GnssSatelliteUsedCount},
				bson.E{Key: "gps_status", Value: hardWareData[i].GpsStatus},
				bson.E{Key: "location_longitude", Value: hardWareData[i].LocationLongitude},
				bson.E{Key: "location_latitude", Value: hardWareData[i].LocationLatitude},
				bson.E{Key: "location_speed", Value: hardWareData[i].LocationSpeed},
				bson.E{Key: "location_angle", Value: hardWareData[i].LocationAngle},
				bson.E{Key: "iot_temperature", Value: hardWareData[i].IotTemperature},
				bson.E{Key: "gprs_total_data_used", Value: hardWareData[i].GprsTotalDataUsed},
				bson.E{Key: "software_version", Value: hardWareData[i].SoftwareVersion},
				bson.E{Key: "bms_software_version", Value: hardWareData[i].BmsSoftwareVersion},
				bson.E{Key: "iccid", Value: hardWareData[i].Iccid},
				bson.E{Key: "imei", Value: hardWareData[i].Imei},
				bson.E{Key: "gprs_apn", Value: hardWareData[i].GprsApn},
				bson.E{Key: "is_first_fill", Value: true},
				bson.E{Key: "battery_voltage", Value: hardWareData[i].BatteryVoltage},
				bson.E{Key: "battery_current", Value: hardWareData[i].BatteryCurrent},
				bson.E{Key: "battery_soc", Value: hardWareData[i].BatterySoc},
				bson.E{Key: "battery_temperature", Value: hardWareData[i].BatteryTemperature},
				bson.E{Key: "battery_remaining_capacity", Value: hardWareData[i].BatteryRemainingCapacity},
				bson.E{Key: "battery_full_charge_capacity", Value: hardWareData[i].BatteryFullChargeCapacity},
				bson.E{Key: "battery_cycle_count", Value: hardWareData[i].BatteryCycleCount},
				bson.E{Key: "battery_rated_capacity", Value: hardWareData[i].BatteryRatedCapacity},
				bson.E{Key: "battery_rated_voltage", Value: hardWareData[i].BatteryRatedVoltage},
				bson.E{Key: "battery_version", Value: hardWareData[i].BatteryVersion},
				bson.E{Key: "battery_manufacture_date", Value: hardWareData[i].BatteryManufactureDate},
				bson.E{Key: "battery_manufacture_name", Value: hardWareData[i].BatteryManufactureName},
				bson.E{Key: "battery_name", Value: hardWareData[i].BatteryName},
				bson.E{Key: "battery_chem_id", Value: hardWareData[i].BatteryChemID},
				bson.E{Key: "bms_bar_code", Value: hardWareData[i].BmsBarCode},
				bson.E{Key: "is_second_fill", Value: true},
				bson.E{Key: "cell_voltage_list_0", Value: hardWareData[i].CellVoltageList0},
				bson.E{Key: "cell_voltage_list_1", Value: hardWareData[i].CellVoltageList1},
				bson.E{Key: "history", Value: hardWareData[i].History},
				bson.E{Key: "error_count", Value: hardWareData[i].ErrorCount},
				bson.E{Key: "status", Value: hardWareData[i].BatteryStatus},
				bson.E{Key: "is_third_fill", Value: true},
				bson.E{Key: "created_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "updated_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				bson.E{Key: "local_d", Value: isoDateTime},
				bson.E{Key: "local_t", Value: localTime},
				bson.E{Key: "iso_timestamp", Value: isoDateTime},
			}},
		}

		socDataCollection.UpdateOne(context.TODO(), filter, &update, opts)
		// if err != nil {
		// 	fmt.Println("Error from raw collection => ", err)
		// } else {
		// 	fmt.Println("Inserted Ids from raw : ", res.UpsertedCount, res.MatchedCount, res.ModifiedCount)
		// }
	}

	return nil
}

func (db *vehiclerepository) CreateBatteryTemperatureHistory(batteryTemperatureData []models.BatteryTemperatureAlert) error {
	dataToDelete := []string{}

	for i := range batteryTemperatureData {
		currentTime := primitive.NewDateTimeFromTime(time.Now())
		temp := models.BatteryTemperatureAlertHistory{}
		temp.AlertCount = batteryTemperatureData[i].TotalAttempt
		temp.BMSID = batteryTemperatureData[i].BMSID
		temp.BatteryTemperature = batteryTemperatureData[i].Temperature
		temp.HistoryTimestamp = currentTime
		temp.CreateAt = currentTime
		temp.UpdateAt = currentTime
		temp.AlertType = "battery_temperature"

		dataToDelete = append(dataToDelete, batteryTemperatureData[i].BMSID)

		ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
		defer cancel()

		_, _ = db.vehicleAlertHistoryConnection.InsertOne(ctx, temp)
	}

	err := db.DeleteBatteryTemperatureAlert(dataToDelete)

	return err

}

func (db *vehiclerepository) GetBatteryTemperatureData() ([]models.BatteryTemperatureAlert, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	filter := []bson.M{
		{"$match": bson.M{
			"created_at": bson.M{
				"$lte": primitive.NewDateTimeFromTime(time.Now()),
			},
		},
		},
	}

	cursor, curErr := db.batteryTemperatureConnection.Aggregate(ctx, filter)
	if curErr != nil {
		return nil, curErr
	}

	batteryData := []models.BatteryTemperatureAlert{}

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	return batteryData, nil

}

func ConnectToMDB() *mongo.Client {
	// if Mclient == nil {
	// 	var err error
	// 	clientOptions := options.Client().ApplyURI(dbconfig.MongoURI())
	// 	Mclient, err = mongo.Connect(context.Background(), clientOptions)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	// check the connection
	// 	err = Mclient.Ping(context.Background(), nil)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	var err error
	//TODO add to your .env.yml or .config.yml MONGODB_URI: mongodb://localhost:27017
	clientOptions := options.Client().ApplyURI(dbconfig.MongoURI())
	Mclient, err = mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// check the connection
	err = Mclient.Ping(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}

	// TODO optional you can log your connected MongoDB client
	fmt.Println("Client DB Connection established...", time.Now())
	return Mclient
}

func (db *vehiclerepository) UpdateBMSDistanceTravelled(batteryData []models.BatteryHardwareMain) error {
	// ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	// defer cancel()
	var operations []mongo.WriteModel

	for i := range batteryData {
		optionsA := mongo.NewUpdateOneModel()
		optionsA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		locationData := new(models.LocationData)
		locationData.Latitude = batteryData[i].LocationLatitude
		locationData.Longitude = batteryData[i].LocationLongitude

		update := bson.D{
			bson.E{Key: "$push", Value: bson.D{
				bson.E{Key: "location", Value: locationData},
			}},
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "created_at", Value: primitive.NewDateTimeFromTime(time.Now())},
			}},
		}

		optionsA.SetUpdate(update)
		optionsA.SetUpsert(true)
		operations = append(operations, optionsA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	_, err := db.batteryDistanceTravelledConnection.BulkWrite(context.TODO(), operations)
	return err

}

func (db *vehiclerepository) CheckForBatteryCycle() ([]models.BatteryHardwareMain, error) {
	fmt.Println("start fetching all battery data...")
	opts := options.Find().SetProjection(
		bson.D{
			bson.E{Key: "bms_id", Value: 1},
			bson.E{Key: "battery_cycle_count", Value: 1},
			bson.E{Key: "old_cycle_count", Value: 1},
			bson.E{Key: "location_latitude", Value: 1},
			bson.E{Key: "location_longitude", Value: 1},
			bson.E{Key: "old_battery_current", Value: 1},
			bson.E{Key: "battery_current", Value: 1},
			bson.E{Key: "battery_soc", Value: 1},
			bson.E{Key: "imei", Value: 1},
			bson.E{Key: "min_max_soc", Value: 1},
			bson.E{Key: "speed_cal", Value: 1},
			bson.E{Key: "odo_meter", Value: 1},
		},
	)
	cursor, curErr := db.batteryMainConnection.Find(context.TODO(), bson.M{}, opts)
	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.BatteryHardwareMain

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	fmt.Println("Returning from repo.....")
	return batteryData, nil
}

//update current cycle count to old count

func (db *vehiclerepository) UpdateCycleOldCycleCount(batteryData []models.BatteryHardwareMain) error {
	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()
		optionA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "old_cycle_count", Value: batteryData[i].BatteryCycleCount},
			}},
		}

		optionA.SetUpdate(update)

		operation = append(operation, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryMainConnection.BulkWrite(context.TODO(), operation)
	fmt.Println(res)
	return err
}

func (db *vehiclerepository) UpdateBatteryCycle(batteryData []models.BatteryHardwareMain) error {
	var bmsIDS []string
	var cycleStartOperation, batteryDistanceOperation []mongo.WriteModel
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// update battery old cycle count every time when ever cycle get started and ended
		temp := new(models.UpdateOldCycleCount).SetUpdateOldCycleCount(batteryData)
		err := db.UpdateBatteryCycleOldCount(temp)
		fmt.Println(err)
	}()

	for i := range batteryData {

		filter := bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		}

		var batteryCycle models.CreateCycleBasedReport

		ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
		defer cancel()

		db.batteryCycleTempReportConnection.FindOne(ctx, filter).Decode(&batteryCycle)

		if (batteryCycle == models.CreateCycleBasedReport{}) {
			fmt.Println("Preparing to start a cycle for : ", batteryData[i].BmsID)
			cycleStartOption := mongo.NewUpdateOneModel()

			update := bson.D{
				bson.E{Key: "$set", Value: bson.D{
					bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
					bson.E{Key: "asset", Value: batteryData[i].BmsID},
					bson.E{Key: "is_start", Value: true},
					bson.E{Key: "start_time", Value: primitive.NewDateTimeFromTime(time.Now())},
					bson.E{Key: "cycle_no", Value: batteryData[i].BatteryCycleCount},
					bson.E{Key: "start_odo", Value: batteryData[i].ODOMeter},
				}},
			}

			cycleStartOption.SetFilter(filter)
			cycleStartOption.SetUpdate(update)
			cycleStartOption.SetUpsert(true)

			batteryDistanceOption := mongo.NewUpdateOneModel()

			locationData := []models.LocationData{
				{
					Latitude:  batteryData[i].LocationLatitude,
					Longitude: batteryData[i].LocationLongitude,
				},
			}

			distanceUpdate := bson.D{
				bson.E{Key: "$set", Value: bson.D{
					bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
					bson.E{Key: "location", Value: locationData},
					bson.E{Key: "created_at", Value: primitive.NewDateTimeFromTime(time.Now())},
				}},
			}

			batteryDistanceOption.SetFilter(filter)
			batteryDistanceOption.SetUpdate(distanceUpdate)
			batteryDistanceOption.SetUpsert(true)

			cycleStartOperation = append(cycleStartOperation, cycleStartOption)
			batteryDistanceOperation = append(batteryDistanceOperation, batteryDistanceOption)

		} else {
			fmt.Println("Preparing to end a cycle for : ", batteryData[i].BmsID)
			var totalSpeed int
			var avgSpeed int
			var topSpeed int = -100000000
			var lowSpeed int = 10000000000

			var minSoc int = 100000000
			var maxSoc int = -100000000

			var topSpeedChanged, lowSpeedChanged, minSocChanged, maxSocChanged bool

			for j := 0; j < len(batteryData[i].SpeedCal); j++ {
				totalSpeed += batteryData[i].SpeedCal[j]
				if topSpeed < batteryData[i].SpeedCal[j] {
					topSpeed = batteryData[i].SpeedCal[j]
					topSpeedChanged = true
				}
				if lowSpeed > batteryData[i].SpeedCal[j] {
					lowSpeed = batteryData[i].SpeedCal[j]
					lowSpeedChanged = true
				}
			}

			for j := 0; j < len(batteryData[i].MinMaxSoc); j++ {
				if minSoc > batteryData[i].MinMaxSoc[j] {
					minSoc = batteryData[i].MinMaxSoc[j]
					minSocChanged = true
				}

				if maxSoc < batteryData[i].MinMaxSoc[j] {
					maxSoc = batteryData[i].MinMaxSoc[j]
					maxSocChanged = true
				}

			}

			if totalSpeed > 0 && len(batteryData[i].SpeedCal) > 0 {
				avgSpeed = totalSpeed / len(batteryData[i].SpeedCal)
			}

			// km calculater
			if topSpeedChanged && lowSpeedChanged && minSocChanged && maxSocChanged {
				fmt.Println("Preparing a for create cycle history...")
				kmT, _ := db.GetBatteryCycleLocations(batteryCycle.BMSID)
				batteryCycle.KMTravelled = kmT
				batteryCycle.MinSoc = minSoc
				batteryCycle.MaxSoc = maxSoc
				batteryCycle.AvgSpeed = avgSpeed
				batteryCycle.TopSpeed = topSpeed
				batteryCycle.LowestSpeed = lowSpeed
				batteryCycle.EndTime = primitive.NewDateTimeFromTime(time.Now())
				batteryCycle.IsEnd = true

				// set value to end odo meter take current ODOMeter
				if batteryData[i].ODOMeter > 0 {
					endODO := batteryData[i].ODOMeter / 1000
					endODORes := endODO - kmT
					batteryCycle.EndODO = endODORes
				}

				// set value to DOD using SOC
				strSoc := strconv.Itoa(batteryData[i].BatterySoc)
				batteryCycle.DOD = strSoc + "%"

				// create cycle history
				res, err := db.batteryCycleHistoryConnection.InsertOne(context.TODO(), batteryCycle)
				fmt.Println("Error from history created : ", err, " and result : ", res.InsertedID)
				// remove cycle temp data
				db.RemoveCycleTempData(batteryCycle.BMSID)

				bmsIDS = append(bmsIDS, batteryData[i].BmsID)
			} else {
				fmt.Println("Failed to prepare a to create a history with bms id : ", batteryData[i].BmsID)
			}
		}
	}

	fmt.Println("Sending a bulk data for start the cycle...")
	cycleStartErr := db.StartNewBatteryCycle(cycleStartOperation)
	fmt.Println("Cycle start Error : ", cycleStartErr)

	fmt.Println("Sending a bulk data for storing a battery location....")
	createLocationErr := db.CreateBatteryLocationData(batteryDistanceOperation)
	fmt.Println("Cycle location stored error : ", createLocationErr)

	// updating a min max soc array and speed cal array after ending the cycle
	fmt.Println("Sending data to set a empty array to min max soc and speed cal array....")
	upMainErr := db.UpdateBatteryCycleDataInBatteryMain(bmsIDS)
	fmt.Println("Update min max soc and speed cal array error : ", upMainErr)

	wg.Wait()
	return nil

}

// create a new battery cycle start
func (db *vehiclerepository) StartNewBatteryCycle(operations []mongo.WriteModel) error {
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryCycleTempReportConnection.BulkWrite(context.TODO(), operations)
	fmt.Println("Start new battery cycle inserted count : ", res.InsertedCount)
	return err
}

// create  a battery location data to find a KM for all cycle
func (db *vehiclerepository) CreateBatteryLocationData(operations []mongo.WriteModel) error {
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryCycleLocationConnection.BulkWrite(context.TODO(), operations)
	fmt.Println("Create battery location data inserted count : ", res.InsertedCount)
	return err
}

func (db *vehiclerepository) GetBatteryCycleLocations(bmsID string) (float64, error) {
	filter := []bson.M{
		{
			"$match": bson.M{
				"bms_id": bmsID,
			},
		},
	}

	batteryData := models.BatteryDistanceTravelled{}
	res := db.batteryCycleLocationConnection.FindOne(context.TODO(), filter).Decode(&batteryData)

	var totalKM float64
	if len(batteryData.Location) > 0 {
		prev := helper.Coordinates{
			Latitude:  float64(batteryData.Location[0].Latitude),
			Longitude: float64(batteryData.Location[0].Longitude),
		}

		for i := 1; i < len(batteryData.Location); i++ {
			current := helper.Coordinates{
				Latitude:  float64(batteryData.Location[i].Latitude),
				Longitude: float64(batteryData.Location[i].Longitude),
			}

			dis := prev.Distance(current)
			totalKM += dis

			prev = current
		}
	}
	//delete location after cycle completed
	if res == nil {
		db.batteryCycleLocationConnection.DeleteOne(context.TODO(), filter)
	}
	return totalKM / 1000, res
}

func (db *vehiclerepository) RemoveCycleTempData(bmsID string) error {
	filter := bson.D{
		bson.E{Key: "bms_id", Value: bmsID},
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	db.batteryCycleTempReportConnection.DeleteOne(ctx, filter)
	return nil
}

// update and set empty array to min max soc and speed cal in main
func (db *vehiclerepository) UpdateBatteryCycleDataInBatteryMain(bmsIDS []string) error {

	operations := []mongo.WriteModel{}

	for i := range bmsIDS {
		optionsA := mongo.NewUpdateOneModel()

		filter := bson.D{
			bson.E{Key: "bms_id", Value: bmsIDS[i]},
		}
		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "min_max_soc", Value: []int{}},
				bson.E{Key: "speed_cal", Value: []int{}},
			}},
		}

		optionsA.SetFilter(filter)
		optionsA.SetUpdate(update)

		operations = append(operations, optionsA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	_, err := db.batteryMainConnection.BulkWrite(context.TODO(), operations)
	fmt.Println("Update Battery Cycle Data In Battery Main Inserted Count : ")
	return err
}

// update old battery count in main to refer start / end cycle
func (db *vehiclerepository) UpdateBatteryCycleOldCount(batteryData []models.UpdateOldCycleCount) error {
	operations := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()

		filter := bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		}

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "old_cycle_count", Value: batteryData[i].OldCycleCount},
			}},
		}

		optionA.SetFilter(filter)
		optionA.SetUpdate(update)
		optionA.SetUpsert(true)

		operations = append(operations, optionA)
	}

	bulkWriter := options.BulkWriteOptions{}
	bulkWriter.SetOrdered(true)

	res, err := db.batteryMainConnection.BulkWrite(context.TODO(), operations)
	fmt.Println("Update Old Cycle Count Error : ", err, " and result : ", res)
	return err
}

// update battery location
func (db *vehiclerepository) UpdateBatteryLocationForCycle(batteryData []models.BatteryHardwareMain) error {
	operations := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()
		optionA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})
		locationData := models.LocationData{
			Latitude:  batteryData[i].LocationLatitude,
			Longitude: batteryData[i].LocationLongitude,
		}
		update := bson.D{
			bson.E{Key: "$push", Value: bson.D{
				bson.E{Key: "location", Value: locationData},
			}},
		}

		optionA.SetUpdate(update)
		operations = append(operations, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryCycleLocationConnection.BulkWrite(context.TODO(), operations)
	fmt.Println(res)
	return err
}

// store soc, speed  for battery cycle start
func (db *vehiclerepository) UpdateBatteryCycleStartParamsInMain(batteryData []models.BatteryHardwareMain) error {
	operations := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()
		optionA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		update := bson.D{
			bson.E{Key: "$push", Value: bson.D{
				bson.E{Key: "min_max_soc", Value: batteryData[i].BatterySoc},
				bson.E{Key: "speed_cal", Value: batteryData[i].LocationSpeed},
			}},
		}

		optionA.SetUpdate(update)
		operations = append(operations, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryMainConnection.BulkWrite(context.TODO(), operations)
	fmt.Println(res)
	return err
}
