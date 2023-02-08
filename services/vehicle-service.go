package services

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/repositories"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var wg sync.WaitGroup

type VehicleServices interface {
	AddUpdateVehicleInformation(vehicleInfo []models.VehiclesData) bool
	RefreshVehicleData() error
	AddVehicleLocationData(vehicleLocation []models.VehicleLocationData)

	TrackVehicleAlert() error
	VerifyVehicleForAlert(vehicleData []models.VehiclesData) error
	UpdateVehicleAlert(vehicleData models.VehicleAlerts) error
	CreateVehicleAlertHistory() error
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
	go func() {
		for i := range vehicleData {
			vehicleData[i].TimeStamp = primitive.NewDateTimeFromTime(time.Now())
			insErr := s.vehicleRepository.UpdateVehicleData(vehicleData[i])
			if insErr != nil {
				fmt.Println("Insert error => ", insErr)
			}
		}
	}()

	return nil
}

func (s *vehicleservice) TrackVehicleAlert() error {

	res, err := s.vehicleRepository.TrackVehicleAlert()

	if err != nil {
		fmt.Println("Error occur from TrackVehicleAlert", err)
		return err
	}

	verErr := s.VerifyVehicleForAlert(res)
	return verErr
}

func (s *vehicleservice) VerifyVehicleForAlert(vehicleData []models.VehiclesData) error {

	for i := range vehicleData {
		speed, _ := strconv.Atoi(vehicleData[i].Speed)

		if speed >= 40 {
			vehicleAlertData, _ := s.vehicleRepository.GetVehicleAlertById(vehicleData[i].VehicleNo)

			if (vehicleAlertData == models.VehicleAlerts{}) {
				vehicleAlertData.BikeNo = vehicleData[i].VehicleNo
				vehicleAlertData.BikeSpeed = int64(speed)

			}
			upErr := s.UpdateVehicleAlert(vehicleAlertData)
			if upErr != nil {
				fmt.Println("Error occur for call update vehicle alert call", upErr)
			}
		}
	}
	return nil
}

func (s *vehicleservice) UpdateVehicleAlert(vehicleAlert models.VehicleAlerts) error {

	if (vehicleAlert == models.VehicleAlerts{}) {
		vehicleAlert.AlertCount = 0
	} else {
		vehicleAlert.AlertCount = vehicleAlert.AlertCount + 1
	}

	err := s.vehicleRepository.UpdateVehicleAlert(vehicleAlert)

	return err
}

func (s *vehicleservice) CreateVehicleAlertHistory() error {
	res, err := s.vehicleRepository.GetAllVehicleAlerts()

	if err != nil {
		fmt.Println("Vehicle Alert History from Service", err)
		return err
	}

	err = s.vehicleRepository.CreateVehicleAlertHistory(res)

	return err
}
