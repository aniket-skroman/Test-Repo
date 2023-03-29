package services

import (
	"context"

	"time"

	"github.com/aniket0951/testproject/repositories"
)

type BatteryService interface {
	UpdateBatteryStatus() error
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
	_, idleCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, moveCancel := context.WithTimeout(context.Background(), 10*time.Second)

	
	go func(cancel context.CancelFunc) {
		
		defer cancel()
		batteryData, _ := ser.batteryRepo.GetOfflineBattery()
		

		_ = ser.batteryRepo.UpdateBatteryOfflineStatus(batteryData)

	}(offlineCancel)

	go func(cancel context.CancelFunc) {
		
		defer idleCancel()
		batteryData, _ := ser.batteryRepo.GetIdleBattery()

		_ = ser.batteryRepo.UpdateBatteryIdleStatus(batteryData)

	}(idleCancel)

	go func(cancel context.CancelFunc) {
		defer cancel()
		batteryData, _ := ser.batteryRepo.GetMoveBattery()
		_ = ser.batteryRepo.UpdateBatteryMoveStatus(batteryData)

	}(moveCancel)

	return nil
}
