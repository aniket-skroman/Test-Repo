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
	UpdateLast24HourUnreported() error
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

	for i := range data {
		_ = ser.batteryRepo.InsertLastSevenHourUnreported(data[i])
	}

	return nil
}

func (ser *batteryService) UpdateLast24HourUnreported() error {
	data, err := ser.batteryRepo.GetLast24hoursUnreportedData()

	if err != nil {
		return err
	}

	// delete all last counts
	delErr := ser.batteryRepo.DeleteAllLast24HourUnreported()
	fmt.Println("Delete All 24 hours Counts Error : ", delErr)

	// fetch total battery
	totalBatteryChan := make(chan int64)

	go func() {
		count, _ := ser.batteryRepo.GetBatteryCount()
		totalBatteryChan <- count
	}()
	totalCount := <-totalBatteryChan
	for i := range data {
		var ans int32
		for j := range data[i].Data {
			currentCount := data[i].Data[j]["count"]
			ans += currentCount.(int32)
		}

		unreportCount := totalCount - int64(len(data[i].Data))
		temp := models.Last24HourUnreported{
			Time:             data[i].Time,
			UnreportedCount:  int64(unreportCount),
			UTCTime:          data[i].UTCTime,
			IndependentCount: int64(ans),
			CreatedAt:        primitive.NewDateTimeFromTime(time.Now()),
		}

		err := ser.batteryRepo.InsertLast24HourUnreported(temp)
		fmt.Println("New data inserted error : ", err)
	}

	// defer close(totalBatteryChan)

	return nil
}

func (ser *batteryService) GetLastSevenHourUnreported() ([]models.LastSevenHourUnreported, error) {
	return ser.batteryRepo.GetLastSevenHourUnreported()
}

func (ser *batteryService) GetUnreportedForSevenHour() ([]models.LastSevenHourUnreported, error) {
	allBattery := make(chan int64)

	go func() {
		count, _ := ser.batteryRepo.GetBatteryCount()
		allBattery <- count
	}()

	res, err := ser.batteryRepo.GetLast7hoursUnreportedData()
	if err != nil {
		return nil, err
	}

	total := <-allBattery

	for i := range res {
		res[i].SetLastSevenHourUnreported(total)
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
