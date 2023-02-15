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

	AddTestData() error
}

type vehicleservice struct {
	vehicleRepository repositories.VehicleRepository
}

func NewVehicleService(repo repositories.VehicleRepository) VehicleServices {
	return &vehicleservice{
		vehicleRepository: repo,
	}
}

func (ser *vehicleservice) AddUpdateVehicleInformation(vehicleInfo []models.VehiclesData) bool {
	fmt.Println("service get called")
	for i := range vehicleInfo {
		fmt.Println("data from service to repo => ", vehicleInfo[i])
		ser.vehicleRepository.AddUpdateVehicleInformation(vehicleInfo[i])

	}

	fmt.Println("service get return")

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

	fmt.Println("proxy data len => ", len(vehicleData))
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
			fmt.Println("Insert error => ", insErr)
		}

	}

	serr := s.TrackVehicleAlert(vehicleDataForAlerts)
	if serr != nil {
		fmt.Println("Error trackVehicleAlert from background thread =>", serr)
	}
	return nil
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
	fmt.Println("Limit for over speed & angle => ", overSpeedLimitChannel, maxAngleLimit, minAngleLimit)
	for i := range vehicleData {
		speed, _ := strconv.Atoi(vehicleData[i].Speed)

		angel, _ := strconv.Atoi(vehicleData[i].Angle)

		if speed >= overSpeedLimitChannel {
			vehicleAlertData, _ := s.vehicleRepository.GetVehicleAlertById(vehicleData[i].VehicleNo)

			if reflect.DeepEqual(vehicleAlertData, models.VehicleAlerts{}) {
				vehicleAlertData.BikeNo = vehicleData[i].VehicleNo
			}

			fmt.Println("Vehicle detect with high speed", vehicleAlertData, speed)
			vehicleAlertData.BikeSpeed = append(vehicleAlertData.BikeSpeed, speed)
			upErr := s.UpdateVehicleAlert(vehicleAlertData)
			if upErr != nil {
				fmt.Println("Error occur for call update vehicle alert call", upErr)
			}
		}

		if angel < 0 || angel > maxAngleLimit {
			vehicleAlertData, _ := s.vehicleRepository.GetVehicleFallAlertById(vehicleData[i].VehicleNo)

			if reflect.DeepEqual(vehicleAlertData, models.VehicleFallAlerts{}) {
				vehicleAlertData.BikeNo = vehicleData[i].VehicleNo
			}

			vehicleAlertData.BikeAngle = append(vehicleAlertData.BikeAngle, angel)

			fmt.Println("Vehicle detect with fall", reflect.TypeOf(vehicleAlertData.BikeAngle), angel)
			upErr := s.UpdateVehicleFallAlert(vehicleAlertData)
			if upErr != nil {
				fmt.Println("Error occur for call update vehicle alert call", upErr)
			}
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
		fmt.Println("Vehicle Alert History from Service", err)
		return err
	}

	s.vehicleRepository.CreateOverSpeedAlertHistory(res)

	fallAlerts, fallErr := s.vehicleRepository.GetAllVehicleFallAlerts()

	if fallErr != nil {
		fmt.Println("Error from get a vehicle fall history =>", fallErr)
	} else {
		err = s.vehicleRepository.CreateVehicleFallAlertHistory(fallAlerts)
	}

	return err
}

func (s *vehicleservice) AddTestData() error {
	return s.vehicleRepository.AddTestData()
}
