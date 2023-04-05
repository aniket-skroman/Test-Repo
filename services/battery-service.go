package services

import (
	"context"

	"time"

	"github.com/aniket0951/testproject/helper"
	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/repositories"
)

type BatteryService interface {
	UpdateBatteryStatus() error

	GetBatteryDistanceTravelled() ([]models.BatteryDistanceTravelled, error)
	CalculateDistanceForLatLng(batteryData models.BatteryDistanceTravelled) (float64, error)
	UpdateBatteryDistanceTravelled() error
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
