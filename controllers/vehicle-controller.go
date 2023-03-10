package controllers

import (
	"time"

	"github.com/aniket0951/testproject/models"
	"github.com/aniket0951/testproject/proxyapis"
	"github.com/aniket0951/testproject/services"
	"github.com/mashingan/smapping"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type VehicleController interface {
	AddUpdateVehicleInformation()
	AddVehicleLocationData()
	TrackVehicleAlert()
}

type vehiclecontroller struct {
	vehicleService services.VehicleServices
}

func NewVehicleController(service services.VehicleServices) VehicleController {
	return &vehiclecontroller{
		vehicleService: service,
	}
}

func (c *vehiclecontroller) AddUpdateVehicleInformation() {
	reqURL := "http://fusioniot.mobilogix.com/webservice?token=getLiveData&user=skroman@mautoafrica.com&pass=Mauto@777"

	requestChannel := make(chan models.AutoGenerated)
	proxyapis.GetAllVehicels(reqURL, requestChannel)

	if len(requestChannel) <= 0 {
		return
	}

	var vehicleInfo []models.VehiclesData

	for i := range requestChannel {
		for ik := range i.Root.VehicleData {

			singleVehicleInfo := models.VehiclesData{}

			_ = smapping.FillStruct(&singleVehicleInfo, smapping.MapFields(&i.Root.VehicleData[ik]))

			singleVehicleInfo.TimeStamp = primitive.NewDateTimeFromTime(time.Now())
			singleVehicleInfo.CreatedAt = primitive.NewDateTimeFromTime(time.Now())
			singleVehicleInfo.UpdatedAt = primitive.NewDateTimeFromTime(time.Now())

			vehicleInfo = append(vehicleInfo, singleVehicleInfo)
		}
	}
	_ = c.vehicleService.AddUpdateVehicleInformation(vehicleInfo)

}

func (c *vehiclecontroller) AddVehicleLocationData() {
	reqURL := "http://fusioniot.mobilogix.com/webservice?token=getLiveData&user=skroman@mautoafrica.com&pass=Mauto@777"

	requestChannel := make(chan models.AutoGenerated)
	go proxyapis.GetAllVehicels(reqURL, requestChannel)

	vehicleLocation := []models.VehicleLocationData{}

	for i := range requestChannel {
		for ik := range i.Root.VehicleData {

			vehicleLocationToCreate := models.VehicleLocationData{}

			vehicleLocationToCreate.Id = primitive.NewObjectID()
			vehicleLocationToCreate.CreatedAt = time.Now()
			vehicleLocationToCreate.UpdatedAt = time.Now()
			vehicleLocationToCreate.Latitude = i.Root.VehicleData[ik].Latitude
			vehicleLocationToCreate.Longitude = i.Root.VehicleData[ik].Longitude
			vehicleLocationToCreate.Location = i.Root.VehicleData[ik].Location
			vehicleLocationToCreate.VehicleNo = i.Root.VehicleData[ik].VehicleNo

			vehicleLocation = append(vehicleLocation, vehicleLocationToCreate)
		}
	}
	c.vehicleService.AddVehicleLocationData(vehicleLocation)
}

func (c *vehiclecontroller) TrackVehicleAlert() {
	// err := c.vehicleService.TrackVehicleAlert()

	// fmt.Println("err =>", err)
}
