package services

import (
	"context"
	"fmt"

	"time"

	"github.com/aniket0951/testproject/helper"
	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BatteryService interface {
	UpdateBatteryStatus() error

	GetBatteryDistanceTravelled() ([]models.BatteryDistanceTravelled, error)
	CalculateDistanceForLatLng(batteryData models.BatteryDistanceTravelled) (float64, error)
	UpdateBatteryDistanceTravelled() error

	UpdateLastSevenHourUnReported() error
}

type batteryService struct {
	batteryRepo repositories.BatteryRepository
}

func NewBatteryService(repo repositories.BatteryRepository) BatteryService {
	return &batteryService{
		batteryRepo: repo,
	}
}
func (ser *batteryService) UpdateBatteryStatus() error {
	_, offlineCancel := context.WithTimeout(context.Background(), 10*time.Second)

	go func(cancel context.CancelFunc) {

		defer cancel()
		batteryData, _ := ser.batteryRepo.GetOfflineBattery()

		_ = ser.batteryRepo.UpdateBatteryOfflineStatus(batteryData)

	}(offlineCancel)
	return nil
}

func (ser *batteryService) GetBatteryDistanceTravelled() ([]models.BatteryDistanceTravelled, error) {
	return ser.batteryRepo.GetBatteryDistanceTravelled()
}

func (ser *batteryService) CalculateDistanceForLatLng(batteryData models.BatteryDistanceTravelled) (float64, error) {
	var total float64

	prevLatLng := helper.Coordinates{}

	if len(batteryData.Location) > 0 {
		prevLatLng.Latitude = float64(batteryData.Location[0].Latitude)
		prevLatLng.Longitude = float64(batteryData.Location[0].Longitude)
	}

	for i := 1; i < len(batteryData.Location); i++ {
		currentLatLng := helper.Coordinates{
			Latitude:  float64(batteryData.Location[i].Latitude),
			Longitude: float64(batteryData.Location[i].Longitude),
		}

		distance := prevLatLng.Distance(currentLatLng)
		total += distance

		prevLatLng.Latitude = currentLatLng.Latitude
		prevLatLng.Longitude = currentLatLng.Longitude
	}

	return total, nil
}

func (db *batteryService) UpdateBatteryDistanceTravelled() error {
	res, err := db.GetBatteryDistanceTravelled()

	if err != nil {
		return err
	}

	var batteryData []models.UpdateBatteryDistanceTravelled

	for i := range res {
		distance, _ := db.CalculateDistanceForLatLng(res[i])
		temp := models.UpdateBatteryDistanceTravelled{
			BMSID:             res[i].BMSID,
			DistanceTravelled: distance,
		}

		batteryData = append(batteryData, temp)
	}

	_ = db.batteryRepo.UpdateBatteryDistanceTravelled(batteryData)
	delErr := db.batteryRepo.DeleteTodayDistanceTravelled()
	return delErr

}

func (ser *batteryService) UpdateLastSevenHourUnReported() error {
	// fetching old seven records
	data, err := ser.GetUnreportedForSevenHour()
	if err != nil {
		return err
	}

	// delete all previous  records...
	delErr := ser.batteryRepo.DeleteLastSevenHourUnreported()
	fmt.Println("Delete Error : ", delErr)

	for k, v := range data {
		temp := models.LastSevenHourUnreported{
			Time:            k,
			UnreportedCount: v,
			CreatedAt:       primitive.NewDateTimeFromTime(time.Now()),
		}
		_ = ser.batteryRepo.InsertLastSevenHourUnreported(temp)
	}

	return nil
}

func (ser *batteryService) GetLastSevenHourUnreported() ([]models.LastSevenHourUnreported, error) {
	return ser.batteryRepo.GetLastSevenHourUnreported()
}

func (ser *batteryService) GetUnreportedForSevenHour() (map[string]int64, error) {
	allBattery := make(chan int64)

	go func() {
		count, _ := ser.batteryRepo.GetBatteryCount()
		allBattery <- count
	}()

	res, err := ser.batteryRepo.GetLast7hoursUnreportedData()
	if err != nil {
		return map[string]int64{}, err
	}

	total := <-allBattery
	for k, v := range res {
		temp := total - v
		res[k] = temp
	}
	close(allBattery)
	return res, nil
}

func (ser *batteryService) GetUnreportedForOneHour() (map[string]int64, error) {
	allBattery := make(chan int64)

	go func() {
		count, _ := ser.batteryRepo.GetBatteryCount()
		allBattery <- count
	}()

	res, err := ser.batteryRepo.GetLast1hoursUnreportedData()
	if err != nil {
		return map[string]int64{}, err
	}

	total := <-allBattery
	for k, v := range res {
		temp := total - v
		res[k] = temp
	}
	close(allBattery)
	return res, nil
}
