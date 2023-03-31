package repositories

import (
	"context"
	"fmt"

	"time"

	"github.com/aniket0951/testproject/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BatteryRepository interface {
	Init() (context.Context, context.CancelFunc)
	GetOfflineBattery() ([]models.BatteryHardwareMain, error)
	GetIdleBattery() ([]models.BatteryHardwareMain, error)
	GetMoveBattery() ([]models.BatteryHardwareMain, error)

	UpdateBatteryOfflineStatus([]models.BatteryHardwareMain) error
	UpdateBatteryIdleStatus([]models.BatteryHardwareMain) error
	UpdateBatteryMoveStatus([]models.BatteryHardwareMain) error
}

type batteryRepository struct {
	batteryMainConnection *mongo.Collection
}

func NewBatteryRepository() BatteryRepository {
	return &batteryRepository{
		batteryMainConnection: batteryMainCollection,
	}
}

func (db *batteryRepository) Init() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.TODO(), 20*time.Second)
	return ctx, cancel
}

func (db *batteryRepository) GetOfflineBattery() ([]models.BatteryHardwareMain, error) {
	currentTime := primitive.NewDateTimeFromTime(time.Now())
	last30Min := currentTime.Time().Add(-30 * time.Minute)

	filter := []bson.M{
		{
			"$match": bson.M{
				"updated_at": bson.M{
					"$lte": last30Min,
				},
				"battery_status": bson.M{
					"$ne": "offline",
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
				bson.E{Key: "battery_status", Value: "offline"},
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

		res, err := db.batteryMainConnection.BulkWrite(ctx, operation)
		fmt.Println("Offline update result : ", res)
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
					"$lte": last30Min,
				},
				"battery_status": bson.M{
					"$ne": "idle",
				},
				"location_speed": bson.M{
					"$lte": 0,
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
				bson.E{Key: "battery_status", Value: "idle"},
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
				"battery_status":"idle",
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
					bson.E{Key: "battery_status", Value: "moving"},
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
