package repositories

import (
	"context"

	"fmt"

	"time"

	dbconfig "github.com/aniket0951/testproject/db-config"
	"github.com/aniket0951/testproject/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	batterySevenHourUnreportedConnection = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_seven_hour_unreported")
	batteryS24HourUnreportedConnection   = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "battery_twenty_four_hour_unreported")
	chargingReportTempConnection         = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "charging_temp_report")
	chargingReportHistoryConnection      = dbconfig.GetCollection(dbconfig.ResolveClientDB(), "charging_report_history")
)

type BatteryRepository interface {
	Init() (context.Context, context.CancelFunc)
	GetOfflineBattery() ([]models.BatteryHardwareMain, error)
	GetIdleBattery() ([]models.BatteryHardwareMain, error)
	GetMoveBattery() ([]models.BatteryHardwareMain, error)
	GetBatteryCount() (int64, error)

	UpdateBatteryOfflineStatus([]models.BatteryHardwareMain) error
	UpdateBatteryIdleStatus([]models.BatteryHardwareMain) error
	UpdateBatteryMoveStatus([]models.BatteryHardwareMain) error

	// battery distance calculater or ODO meter
	GetBatteryDistanceTravelled() ([]models.BatteryDistanceTravelled, error)
	UpdateBatteryDistanceTravelled([]models.UpdateBatteryDistanceTravelled) error
	DeleteTodayDistanceTravelled() error

	// hourly reported and unreported count
	GetLastSevenHourUnreported() ([]models.LastSevenHourUnreported, error)
	GetLast1hoursUnreportedData() (map[string]int64, error)
	GetLast7hoursUnreportedData() ([]models.LastSevenHourUnreported, error)
	GetLast24hoursUnreportedData() ([]models.Last24HourUnreportedSpecificData, error)
	InsertLastSevenHourUnreported(data models.LastSevenHourUnreported) error
	DeleteLastSevenHourUnreported() error
	InsertLast24HourUnreported(models.Last24HourUnreported) error
	DeleteAllLast24HourUnreported() error

	//Charging Reports
	CheckChargingCycleStartOrNot(bmsId string) models.StartChargingReport
	StartChargingReport([]models.StartChargingReport) error
	EndChargingReport([]models.EndChargingReport) error
	GetCurrentCycleEnd() ([]models.ChargingReport, error)
	CreateChargingReportHistory(batteryData []models.ChargingReport) error
	UpdateBatteryCurrentInMain(oldCurrentData []models.UpdateOldCurrent) error
	DeleteChargingTempReport(bmsIDs []string) error
}

type batteryRepository struct {
	batteryMainConnection                *mongo.Collection
	batteryReportingConnection           *mongo.Collection
	batteryDistanceTravelledConnection   *mongo.Collection
	batterySevenHourUnreportedCollection *mongo.Collection
	battery24HourUnreportedCollection    *mongo.Collection
	chargingReportTempCollection         *mongo.Collection
	chargingReportHistoryCollection      *mongo.Collection
}

func NewBatteryRepository() BatteryRepository {
	return &batteryRepository{
		batteryMainConnection:                batteryMainCollection,
		batteryReportingConnection:           batteryReportingCollection,
		batteryDistanceTravelledConnection:   batteryDistanceTravelledCollection,
		batterySevenHourUnreportedCollection: batterySevenHourUnreportedConnection,
		battery24HourUnreportedCollection:    batteryS24HourUnreportedConnection,
		chargingReportTempCollection:         chargingReportTempConnection,
		chargingReportHistoryCollection:      chargingReportHistoryConnection,
	}
}

func (db *batteryRepository) Init() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
	return ctx, cancel
}

func (db *batteryRepository) GetOfflineBattery() ([]models.BatteryHardwareMain, error) {
	currentTime := time.Now()
	last30Min := currentTime.Add(-30 * time.Minute)

	filter := []bson.M{
		{
			"$match": bson.M{
				"updated_at": bson.M{
					"$lte": primitive.NewDateTimeFromTime(last30Min),
				},
			},
		},
	}

	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.batteryMainConnection.Aggregate(ctx, filter)

	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.BatteryHardwareMain

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}
	return batteryData, nil
}

func (db *batteryRepository) UpdateBatteryOfflineStatus(batteryData []models.BatteryHardwareMain) error {
	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionsA := mongo.NewUpdateOneModel()
		optionsA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "battery_status", Value: "Offline"},
			}},
		}

		optionsA.SetUpdate(update)
		operation = append(operation, optionsA)

	}
	if len(operation) > 0 {
		bulkOption := options.BulkWriteOptions{}
		bulkOption.SetOrdered(true)

		ctx, cancel := db.Init()
		defer cancel()

		_, err := db.batteryMainConnection.BulkWrite(ctx, operation)
		//fmt.Println("Offline update result : ", res)
		return err
	}
	return nil
}

func (db *batteryRepository) GetIdleBattery() ([]models.BatteryHardwareMain, error) {
	currentTime := primitive.NewDateTimeFromTime(time.Now())
	last30Min := currentTime.Time().Add(-30 * time.Minute)

	filter := []bson.M{
		{
			"$match": bson.M{
				"updated_at": bson.M{
					"$gte": last30Min,
				},
				"location_speed": 0,
			},
		},
	}

	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.batteryMainConnection.Aggregate(ctx, filter)

	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.BatteryHardwareMain

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	//fmt.Println("Battery Data : ", batteryData)

	return batteryData, nil
}

func (db *batteryRepository) UpdateBatteryIdleStatus(batteryData []models.BatteryHardwareMain) error {
	ctx, cancel := db.Init()
	defer cancel()

	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()
		optionA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
		})

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "battery_status", Value: "Idle"},
			}},
		}
		optionA.SetUpdate(update)
		operation = append(operation, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	res, err := db.batteryMainConnection.BulkWrite(ctx, operation)
	fmt.Println("Update Idle Result : ", res)

	return err
}
func (db *batteryRepository) GetMoveBattery() ([]models.BatteryHardwareMain, error) {
	currentTime := primitive.NewDateTimeFromTime(time.Now())
	last30Min := currentTime.Time().Add(-30 * time.Minute)

	filter := []bson.M{
		{
			"$match": bson.M{
				"updated_at": bson.M{
					"$lte": last30Min,
				},
				"location_speed": bson.M{
					"$gt": 0,
				},
				"battery_status": "idle",
			},
		},
	}

	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.batteryMainConnection.Aggregate(ctx, filter)

	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.BatteryHardwareMain
	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	return batteryData, nil
}

func (db *batteryRepository) UpdateBatteryMoveStatus(batteryData []models.BatteryHardwareMain) error {
	if len(batteryData) > 0 {
		operation := []mongo.WriteModel{}

		for i := range batteryData {
			optionA := mongo.NewUpdateOneModel()
			optionA.SetFilter(bson.D{
				bson.E{Key: "bms_id", Value: batteryData[i].BmsID},
			})

			update := bson.D{
				bson.E{Key: "$set", Value: bson.D{
					bson.E{Key: "battery_status", Value: "Moving"},
				}},
			}

			optionA.SetUpdate(update)
			operation = append(operation, optionA)
		}

		bulkOption := options.BulkWriteOptions{}
		bulkOption.SetOrdered(true)

		ctx, cancel := db.Init()
		defer cancel()

		res, err := db.batteryMainConnection.BulkWrite(ctx, operation)
		fmt.Println("Move update result : ", res)
		return err
	}
	return nil
}

func (db *batteryRepository) GetBatteryDistanceTravelled() ([]models.BatteryDistanceTravelled, error) {
	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.batteryDistanceTravelledConnection.Find(ctx, bson.M{})

	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.BatteryDistanceTravelled

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	return batteryData, nil
}

func (db *batteryRepository) UpdateBatteryDistanceTravelled(batteryData []models.UpdateBatteryDistanceTravelled) error {
	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionsA := mongo.NewUpdateOneModel()
		optionsA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BMSID},
		})

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "odo_meter", Value: batteryData[i].DistanceTravelled},
			}},
		}

		optionsA.SetUpdate(update)
		operation = append(operation, optionsA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.batteryMainConnection.BulkWrite(ctx, operation)
	_, reportingErr := db.batteryReportingConnection.BulkWrite(ctx, operation)
	if reportingErr != nil {
		return reportingErr
	}
	return err
}

func (db *batteryRepository) DeleteTodayDistanceTravelled() error {
	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.batteryDistanceTravelledConnection.DeleteMany(ctx, bson.M{})
	return err
}

func (db *batteryRepository) GetLastSevenHourUnreported() ([]models.LastSevenHourUnreported, error) {

	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.batterySevenHourUnreportedCollection.Find(ctx, bson.M{})

	if curErr != nil {
		return nil, curErr
	}

	var batteryData []models.LastSevenHourUnreported

	if err := cursor.All(context.TODO(), &batteryData); err != nil {
		return nil, err
	}

	return batteryData, nil
}

// delete last record for only maintain a 7 hour
func (db *batteryRepository) DeleteLastSevenHourUnreported() error {

	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.batterySevenHourUnreportedCollection.DeleteMany(ctx, bson.M{})
	return err
}

func (db *batteryRepository) InsertLastSevenHourUnreported(data models.LastSevenHourUnreported) error {
	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.batterySevenHourUnreportedCollection.InsertOne(ctx, data)
	return err
}

func (db *batteryRepository) GetLast1hoursUnreportedData() (map[string]int64, error) {
	ConnectToMDB()
	var remote = "telematics"
	rawDataCollection := Mclient.Database(remote).Collection("bms_rawdata")
	ref := 1
	mp := map[string]int64{}
	currentTime := time.Now()

	from := currentTime.Add(time.Hour * time.Duration(-ref))
	data, _ := QueryHelper(from, currentTime, rawDataCollection)
	ref++
	hourFormat := currentTime.Format("15:04:05")
	mp[hourFormat] = data

	return mp, nil
}

func (db *batteryRepository) GetLast7hoursUnreportedData() ([]models.LastSevenHourUnreported, error) {
	ConnectToMDB()
	var remote = "telematics"
	rawDataCollection := Mclient.Database(remote).Collection("bms_rawdata")
	ref := 1
	//mp := map[string]int64{}
	batteryData := []models.LastSevenHourUnreported{}
	currentTime := time.Now().UTC()
	for ref <= 7 {
		if ref == 1 {
			from := currentTime.Add(time.Hour * time.Duration(-ref))
			data, _ := QueryHelper(from, currentTime, rawDataCollection)
			ref++
			hourFormat := currentTime.Format("15:04:05")
			temp := models.LastSevenHourUnreported{
				Time:            hourFormat,
				UnreportedCount: data,
				UTCTime:         primitive.NewDateTimeFromTime(currentTime),
				CreatedAt:       primitive.NewDateTimeFromTime(time.Now()),
			}
			batteryData = append(batteryData, temp)
		} else {
			toint := ref - 1
			from := currentTime.Add(time.Duration(-ref) * time.Hour)
			to := currentTime.Add(time.Duration(-toint) * time.Hour)
			data, _ := QueryHelper(from, to, rawDataCollection)
			hourFormat := to.Format("15:04:05")
			temp := models.LastSevenHourUnreported{
				Time:            hourFormat,
				UnreportedCount: data,
				UTCTime:         primitive.NewDateTimeFromTime(to),
				CreatedAt:       primitive.NewDateTimeFromTime(time.Now()),
			}
			batteryData = append(batteryData, temp)
			ref++
		}
	}

	return batteryData, nil
}

func QueryHelper(from, to time.Time, rawDataCollection *mongo.Collection) (int64, error) {

	filter := []bson.M{
		{
			"$match": bson.M{
				"created_at": bson.M{
					"$gt":  from,
					"$lte": to,
				},
			},
		},
		{
			"$group": bson.M{
				"_id": "$bms_id",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
	}

	cursor, curErr := rawDataCollection.Aggregate(context.TODO(), filter)
	if curErr != nil {
		return 0, curErr
	}

	var bdata []bson.M

	if err := cursor.All(context.TODO(), &bdata); err != nil {
		return 0, err
	}

	return int64(len(bdata)), nil
}

func (db *batteryRepository) GetLast24hoursUnreportedData() ([]models.Last24HourUnreportedSpecificData, error) {
	ConnectToMDB()
	var remote = "telematics"
	rawDataCollection := Mclient.Database(remote).Collection("bms_rawdata")
	ref := 1
	batteryData := []models.Last24HourUnreportedSpecificData{}
	currentTime := time.Now().UTC()
	for ref <= 24 {
		if ref == 1 {
			from := currentTime.Add(time.Hour * time.Duration(-ref))
			data, _ := QueryHelperFor24HourUnreported(from, currentTime, rawDataCollection)
			ref++
			hourFormat := currentTime.Format("15:04:05")
			temp := models.Last24HourUnreportedSpecificData{
				Time:    hourFormat,
				UTCTime: primitive.NewDateTimeFromTime(currentTime),
				Data:    data,
			}

			batteryData = append(batteryData, temp)

		} else {
			toint := ref - 1
			from := currentTime.Add(time.Duration(-ref) * time.Hour)
			to := currentTime.Add(time.Duration(-toint) * time.Hour)
			data, _ := QueryHelperFor24HourUnreported(from, to, rawDataCollection)
			hourFormat := to.Format("15:04:05")
			temp := models.Last24HourUnreportedSpecificData{
				Time:    hourFormat,
				UTCTime: primitive.NewDateTimeFromTime(to),
				Data:    data,
			}

			batteryData = append(batteryData, temp)
			ref++

		}
	}

	return batteryData, nil
}

func QueryHelperFor24HourUnreported(from, to time.Time, rawDataCollection *mongo.Collection) ([]bson.M, error) {

	filter := []bson.M{
		{
			"$match": bson.M{
				"created_at": bson.M{
					"$gt":  from,
					"$lte": to,
				},
			},
		},
		{
			"$group": bson.M{
				"_id": "$bms_id",
				"count": bson.M{
					"$sum": 1,
				},
			},
		},
	}

	cursor, curErr := rawDataCollection.Aggregate(context.TODO(), filter)
	if curErr != nil {
		return nil, curErr
	}

	var bdata []bson.M

	if err := cursor.All(context.TODO(), &bdata); err != nil {
		return nil, err
	}

	return bdata, nil
}

func (db *batteryRepository) GetBatteryCount() (int64, error) {
	ctx, cancel := db.Init()
	defer cancel()
	return db.batteryMainConnection.EstimatedDocumentCount(ctx)
}

func (db *batteryRepository) InsertLast24HourUnreported(data models.Last24HourUnreported) error {
	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.battery24HourUnreportedCollection.InsertOne(ctx, data)
	return err
}

func (db *batteryRepository) DeleteAllLast24HourUnreported() error {
	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.battery24HourUnreportedCollection.DeleteMany(ctx, bson.M{})
	return err
}

// charging reports

// check charging cycle already started for bmsID
func (db *batteryRepository) CheckChargingCycleStartOrNot(bmsId string) models.StartChargingReport {
	filter := bson.D{
		bson.E{Key: "bms_id", Value: bmsId},
	}

	ctx, cancel := db.Init()
	defer cancel()
	var startChargingReport models.StartChargingReport
	db.chargingReportTempCollection.FindOne(ctx, filter).Decode(&startChargingReport)
	return startChargingReport
}

// get all end cycle from temp
func (db *batteryRepository) GetCurrentCycleEnd() ([]models.ChargingReport, error) {

	filter := bson.D{
		bson.E{Key: "is_start", Value: true},
		bson.E{Key: "is_end", Value: true},
	}

	ctx, cancel := db.Init()
	defer cancel()

	cursor, curErr := db.chargingReportTempCollection.Find(ctx, filter)

	if curErr != nil {
		return nil, curErr
	}

	var chargingReports []models.ChargingReport

	if err := cursor.All(context.TODO(), &chargingReports); err != nil {
		return nil, err
	}

	return chargingReports, nil
}

// making a start charging report
func (db *batteryRepository) StartChargingReport(batteryData []models.StartChargingReport) error {
	var operations []mongo.WriteModel

	for i := range batteryData {
		optionsA := mongo.NewUpdateOneModel()

		optionsA.SetFilter(bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BMSID},
		})

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "asset", Value: batteryData[i].Asset},
				bson.E{Key: "imei", Value: batteryData[i].IMEI},
				bson.E{Key: "start_time", Value: batteryData[i].StartTime},
				bson.E{Key: "start_soc", Value: batteryData[i].StartSOC},
				bson.E{Key: "is_start", Value: batteryData[i].IsStart},
			}},
		}

		optionsA.SetUpdate(update)
		optionsA.SetUpsert(true)

		operations = append(operations, optionsA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	_, err := db.chargingReportTempCollection.BulkWrite(context.TODO(), operations)
	return err
}

// making a end charging report
func (db *batteryRepository) EndChargingReport(batteryData []models.EndChargingReport) error {
	ctx, cancel := db.Init()
	defer cancel()

	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewUpdateOneModel()

		filter := bson.D{
			bson.E{Key: "bms_id", Value: batteryData[i].BMSID},
		}

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "end_time", Value: batteryData[i].EndTime},
				bson.E{Key: "end_soc", Value: batteryData[i].EndSOC},
				bson.E{Key: "is_end", Value: batteryData[i].IsEnd},
			}},
		}

		optionA.SetFilter(filter)
		optionA.SetUpdate(update)

		operation = append(operation, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	_, err := db.chargingReportTempCollection.BulkWrite(ctx, operation)
	return err
}

// delete all charging temp report
func (db *batteryRepository) DeleteChargingTempReport(bmsIDs []string) error {
	filter := bson.D{
		bson.E{Key: "bms_id", Value: bson.D{
			bson.E{Key: "$in", Value: bmsIDs},
		}},
	}

	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.chargingReportTempCollection.DeleteMany(ctx, filter)
	return err
}

// create a charging report history after complete one cycle temp to history
func (db *batteryRepository) CreateChargingReportHistory(batteryData []models.ChargingReport) error {
	operation := []mongo.WriteModel{}

	for i := range batteryData {
		optionA := mongo.NewInsertOneModel()
		batteryData[i].CreatedAt = primitive.NewDateTimeFromTime(time.Now())
		optionA.SetDocument(batteryData[i])

		operation = append(operation, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.chargingReportHistoryCollection.BulkWrite(ctx, operation)
	return err
}

// update old battery current in battery main to refer a start or end cycle
func (db *batteryRepository) UpdateBatteryCurrentInMain(oldCurrentData []models.UpdateOldCurrent) error {
	operation := []mongo.WriteModel{}

	for i := range oldCurrentData {
		optionA := mongo.NewUpdateOneModel()

		filter := bson.D{
			bson.E{Key: "bms_id", Value: oldCurrentData[i].BMSID},
		}

		update := bson.D{
			bson.E{Key: "$set", Value: bson.D{
				bson.E{Key: "old_battery_current", Value: oldCurrentData[i].OldCurrent},
			}},
		}

		optionA.SetFilter(filter)
		optionA.SetUpdate(update)
		optionA.SetUpsert(true)

		operation = append(operation, optionA)
	}

	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	ctx, cancel := db.Init()
	defer cancel()

	_, err := db.batteryMainConnection.BulkWrite(ctx, operation)

	return err
}
