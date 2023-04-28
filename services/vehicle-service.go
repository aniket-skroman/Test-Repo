package services

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var wg sync.WaitGroup
var overSpeedLimitChannel int
var maxAngleLimit int
var minAngleLimit int

type VehicleServices interface {
	AddUpdateVehicleInformation(vehicleInfo []models.VehiclesData) bool
	RefreshVehicleData() error
	AddVehicleLocationData(vehicleLocation []models.VehicleLocationData)

	TrackVehicleAlert(vehicleData []models.VehiclesData) error
	VerifyVehicleForAlert(vehicleData []models.VehiclesData) error
	UpdateVehicleAlert(vehicleData models.VehicleAlerts) error
	UpdateVehicleFallAlert(vehicleAlert models.VehicleFallAlerts) error
	CreateVehicleAlertHistory() error
	CreateDistanceTravelHistory() error
	CreateBatteryTemperatureHistory() error

	BatteryTempToMain() error
	CheckForBatteryCycle() error

	AddTestData() error
}

type vehicleservice struct {
	vehicleRepository repositories.VehicleRepository
	batteryService    BatteryService
}

func NewVehicleService(repo repositories.VehicleRepository, batteryService BatteryService) VehicleServices {
	return &vehicleservice{
		vehicleRepository: repo,
		batteryService:    batteryService,
	}
}

func (ser *vehicleservice) AddUpdateVehicleInformation(vehicleInfo []models.VehiclesData) bool {
	for i := range vehicleInfo {
		ser.vehicleRepository.AddUpdateVehicleInformation(vehicleInfo[i])
	}
	return true
}

func (ser *vehicleservice) AddVehicleLocationData(vehicleLocation []models.VehicleLocationData) {

	for i := range vehicleLocation {
		ser.vehicleRepository.AddVehicleLocationData(vehicleLocation[i])
		break
	}
}

func (s *vehicleservice) RefreshVehicleData() error {
	vehicleData, err := s.vehicleRepository.RefreshVehicleData()

	if err != nil {
		return err
	}
	vehicleDataForAlerts := []models.VehiclesData{}

	for i := range vehicleData {

		go func() {
			if vehicleData[i].Status == "RUNNING" {
				vehicleDataForAlerts = append(vehicleDataForAlerts, vehicleData[i])
			}
		}()

		vehicleData[i].TimeStamp = primitive.NewDateTimeFromTime(time.Now())
		insErr := s.vehicleRepository.UpdateVehicleData(vehicleData[i])

		if insErr != nil {
			return insErr
		}

	}

	serr := s.TrackVehicleAlert(vehicleDataForAlerts)

	return serr
}

func (s *vehicleservice) TrackVehicleAlert(vehicleData []models.VehiclesData) error {
	var res models.AlertConfig
	var err error
	res, err = s.vehicleRepository.GetAlertLimit("overspeed")

	if err != nil {
		overSpeedLimitChannel = 60
	} else {
		overSpeedLimitChannel = int(res.MaxLimit)
	}

	fallLimit, err := s.vehicleRepository.GetAlertLimit("fall")

	if err != nil {
		maxAngleLimit = 135
		minAngleLimit = 45
	} else {
		maxAngleLimit = int(fallLimit.MaxLimit)
		minAngleLimit = int(fallLimit.MinLimit)
	}

	// res, err := s.vehicleRepository.TrackVehicleAlert()

	// if err != nil {
	// 	fmt.Println("Error occur from TrackVehicleAlert", err)
	// 	return err
	// }

	verErr := s.VerifyVehicleForAlert(vehicleData)
	return verErr
}

func (s *vehicleservice) VerifyVehicleForAlert(vehicleData []models.VehiclesData) error {
	for i := range vehicleData {
		speed, _ := strconv.Atoi(vehicleData[i].Speed)

		angel, _ := strconv.Atoi(vehicleData[i].Angle)

		if speed >= overSpeedLimitChannel {
			vehicleAlertData, _ := s.vehicleRepository.GetVehicleAlertById(vehicleData[i].VehicleNo)

			if reflect.DeepEqual(vehicleAlertData, models.VehicleAlerts{}) {
				vehicleAlertData.BikeNo = vehicleData[i].VehicleNo
			}

			vehicleAlertData.BikeSpeed = append(vehicleAlertData.BikeSpeed, speed)
			_ = s.UpdateVehicleAlert(vehicleAlertData)

		}

		if angel < 0 || angel > maxAngleLimit {
			vehicleAlertData, _ := s.vehicleRepository.GetVehicleFallAlertById(vehicleData[i].VehicleNo)

			if reflect.DeepEqual(vehicleAlertData, models.VehicleFallAlerts{}) {
				vehicleAlertData.BikeNo = vehicleData[i].VehicleNo
			}

			vehicleAlertData.BikeAngle = append(vehicleAlertData.BikeAngle, angel)

			_ = s.UpdateVehicleFallAlert(vehicleAlertData)

		}

	}
	return nil
}

func (s *vehicleservice) UpdateVehicleAlert(vehicleAlert models.VehicleAlerts) error {

	if (reflect.DeepEqual(vehicleAlert, models.VehicleAlerts{})) {
		vehicleAlert.AlertCount = 0
	} else {
		vehicleAlert.AlertCount = vehicleAlert.AlertCount + 1
	}

	err := s.vehicleRepository.UpdateVehicleAlert(vehicleAlert)

	return err
}

func (s *vehicleservice) UpdateVehicleFallAlert(vehicleAlert models.VehicleFallAlerts) error {

	if (reflect.DeepEqual(vehicleAlert, models.VehicleFallAlerts{})) {
		vehicleAlert.AlertCount = 0
	} else {
		vehicleAlert.AlertCount = vehicleAlert.AlertCount + 1
	}

	err := s.vehicleRepository.UpdateVehicleFallAlert(vehicleAlert)

	return err
}

func (s *vehicleservice) CreateVehicleAlertHistory() error {
	res, err := s.vehicleRepository.GetOverSpeedAlerts()

	if err != nil {
		return err
	}

	s.vehicleRepository.CreateOverSpeedAlertHistory(res)

	fallAlerts, fallErr := s.vehicleRepository.GetAllVehicleFallAlerts()

	if fallErr != nil {
		return fallErr
	} else {
		err = s.vehicleRepository.CreateVehicleFallAlertHistory(fallAlerts)
	}

	return err
}

func (s *vehicleservice) CreateDistanceTravelHistory() error {
	vehicleData, err := s.vehicleRepository.GetAllVehicles()

	if err != nil {
		return err
	}

	requiredData := []models.VehiclesData{}

	for i := range vehicleData {
		if vehicleData[i].DistanceTraveled >= 1 {
			requiredData = append(requiredData, vehicleData[i])
		}
	}

	return s.vehicleRepository.CreateDistanceTravelHistory(requiredData)
}

func (s *vehicleservice) BatteryTempToMain() error {
	return s.vehicleRepository.BatteryTempToMain()
}

func (s *vehicleservice) CreateBatteryTemperatureHistory() error {
	res, err := s.vehicleRepository.GetBatteryTemperatureData()
	if err != nil {
		return err
	}

	if err := s.vehicleRepository.CreateBatteryTemperatureHistory(res); err != nil {
		return err
	}

	return nil
}

func (s *vehicleservice) AddTestData() error {
	return s.vehicleRepository.AddTestData()
}

func (s *vehicleservice) CheckForBatteryCycle() error {
	// fetch all data from main
	fmt.Println("Fetching data from main...")
	batteryData, err := s.vehicleRepository.CheckForBatteryCycle()
	if err != nil {
		return err
	}
	fmt.Println("len of battery data : ", len(batteryData))
	var wg sync.WaitGroup
	wg.Add(1)

	//work for battery charge report
	go func() {
		defer wg.Done()
		s.batteryService.CheckForBatteryChargingReport(batteryData)
	}()

	// prepare for Cycle based report, Charging Report
	var newCycleReport = []models.BatteryHardwareMain{}

	for i := range batteryData {
		if batteryData[i].BatteryCycleCount != batteryData[i].OldCycleCount {
			newCycleReport = append(newCycleReport, batteryData[i])
		}

	}

	upErr := s.vehicleRepository.UpdateBatteryCycle(newCycleReport)
	wg.Wait()
	return upErr
}
