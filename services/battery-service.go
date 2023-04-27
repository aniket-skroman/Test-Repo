package services

import (
	"context"
	"fmt"
	"sync"

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

	CheckForBatteryChargingReport([]models.BatteryHardwareMain) error
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
	_ = ser.batteryRepo.DeleteAllLast24HourUnreported()

	for i := range data {
		var ans int32
		for j := range data[i].Data {
			currentCount := data[i].Data[j]["count"]
			ans += currentCount.(int32)
		}
		temp := models.Last24HourUnreported{
			Time:             data[i].Time,
			UnreportedCount:  int64(len(data[i].Data)),
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

// battery charging report check
func (ser *batteryService) CheckForBatteryChargingReport(batteryData []models.BatteryHardwareMain) error {
	// check old and current battery current and store it another model list for next operations
	var newBatteryData []models.BatteryHardwareMain

	for i := range batteryData {
		var oldCurrent, latestCurrent int
		oldCurrent = batteryData[i].OldBatteryCurrent
		latestCurrent = batteryData[i].BatteryCurrent
		if (oldCurrent < 0 && latestCurrent < 0) && (oldCurrent > 0 && latestCurrent > 0) {
			continue
		} else if oldCurrent != latestCurrent {
			newBatteryData = append(newBatteryData, batteryData[i])
		}
	}

	// check for trip is start or end do implementation accordingly
	err := ser.CheckBatteryCurrentCycleStartOrEnd(newBatteryData)

	return err
}

// check for cycle started or ended with the help of temp collection
func (ser *batteryService) CheckBatteryCurrentCycleStartOrEnd(batteryData []models.BatteryHardwareMain) error {
	var startCycleBattery, endCycleBattery, updateOldBatteryCurrent = []models.StartChargingReport{}, []models.EndChargingReport{}, []models.UpdateOldCurrent{}
	var wg sync.WaitGroup
	wg.Add(1)

	// prepare to update old current in battery main
	go func() {
		defer wg.Done()
		temp := new(models.UpdateOldCurrent).SetUpdateOldCurrent(batteryData)
		updateOldBatteryCurrent = append(updateOldBatteryCurrent, temp...)
	}()

	// checking all bmsid one by one
	for i := range batteryData {
		startChargingReport := ser.batteryRepo.CheckChargingCycleStartOrNot(batteryData[i].BmsID)

		// check for start
		if (startChargingReport == models.StartChargingReport{}) {
			newStartChargingReport := new(models.StartChargingReport).SetStartChargingReport(batteryData[i])
			// store all battery for start current cycle
			startCycleBattery = append(startCycleBattery, newStartChargingReport)

		} else {
			endChargingReport := new(models.EndChargingReport).SetEndChargingReport(batteryData[i])

			// store all battery for end current cycle
			endCycleBattery = append(endCycleBattery, endChargingReport)
		}
	}

	// prepare and do a start current cycle
	_ = ser.batteryRepo.StartChargingReport(startCycleBattery)
	// go func() {
	// 	defer wg.Done()
	// 	fmt.Println("Send data to start charging Report....")
	// 	err := ser.batteryRepo.StartChargingReport(startCycleBattery)
	// 	fmt.Println("Cycle Start Error : ", err)
	// }()

	// prepare and do a end current cycle

	// end the cycle first in temp c
	endErr := ser.batteryRepo.EndChargingReport(endCycleBattery)
	fmt.Println("Cycle End Error : ", endErr)

	// get all cycle end cycle battery
	chargingReport, fetErr := ser.batteryRepo.GetCurrentCycleEnd()
	fmt.Println("Fetch all end cycle error : ", fetErr)

	// create a current cycle history
	hisErr := ser.batteryRepo.CreateChargingReportHistory(chargingReport)
	fmt.Println("Create current cycle history error : ", hisErr)

	// store only all bms id for remove temp data
	bmsIDS := []string{}
	for i := range chargingReport {
		bmsIDS = append(bmsIDS, chargingReport[i].BMSID)
	}

	// once history created remove all data from temp
	delErr := ser.batteryRepo.DeleteChargingTempReport(bmsIDS)
	fmt.Println("Delete current cycle temp data error : ", delErr)

	// go func() {
	// 	defer wg.Done()
	// 	fmt.Println("Send data to end charging Report.....")

	// 	// end the cycle first in temp c
	// 	fmt.Println("ending the cycle first...")
	// 	err := ser.batteryRepo.EndChargingReport(endCycleBattery)
	// 	fmt.Println("Cycle End Error : ", err)
	// 	fmt.Println("Cycle has been ended...")

	// 	// get all cycle end cycle battery
	// 	fmt.Println("fetching all end cycle battery..")
	// 	chargingReport, fetErr := ser.batteryRepo.GetCurrentCycleEnd()
	// 	fmt.Println("Fetch all end cycle error : ", fetErr)
	// 	fmt.Println("fetched all end cycle battery..")

	// 	// create a current cycle history
	// 	fmt.Println("Send data to create a current cycle history....")
	// 	hisErr := ser.batteryRepo.CreateChargingReportHistory(chargingReport)
	// 	fmt.Println("Create current cycle history error : ", hisErr)
	// 	fmt.Println("Current cycle history has been created....")

	// 	// store only all bms id for remove temp data
	// 	fmt.Println("Storing all bms id's started....")
	// 	bmsIDS := []string{}
	// 	for i := range chargingReport {
	// 		bmsIDS = append(bmsIDS, chargingReport[i].BMSID)
	// 	}
	// 	fmt.Println("Storing all bms id's ended....")

	// 	// once history created remove all data from temp
	// 	fmt.Println("Send data to delete all data from temp started....")
	// 	delErr := ser.batteryRepo.DeleteChargingTempReport(bmsIDS)
	// 	fmt.Println("Delete current cycle temp data error : ", delErr)
	// 	fmt.Println("Send data to delete all data from temp ended....")

	// }()
	fmt.Println("Waiting for goroutine done")
	wg.Wait()

	// for every start or end we have to update battery old current with latest battery current
	upErr := ser.batteryRepo.UpdateBatteryCurrentInMain(updateOldBatteryCurrent)
	fmt.Println("Update old battery current in main error : ", upErr)

	return nil
}
