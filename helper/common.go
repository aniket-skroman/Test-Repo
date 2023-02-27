package helper

import (
	"math"
	"strconv"
	"strings"
	"time"
)

func ConvertUTCToIndia() []string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Now().In(loc)
	f_date := (now.Format("01-02-2006 15:04:05"))

	indiaDate := strings.SplitAfter(f_date, " ")

	return indiaDate
}

func MakeDateTimeFormat() time.Time {
	currentStr := time.Now().String()

	if strings.Contains(currentStr, "T") {
		currentStr = strings.Replace(currentStr, "Z", "", -1)
	}
	if strings.Contains(currentStr, "00:00") {
		currentStr = strings.ReplaceAll(currentStr, "00:00", "")
	}

	layout := "2006-01-02T15:04:05.000"
	str := currentStr
	t, _ := time.Parse(layout, str)

	return t
}

func ConvertStrToFloat(str string) float64 {
	ans, _ := strconv.ParseFloat(str, 64)

	return ans
}

type Coordinates struct {
	Latitude  float64
	Longitude float64
}

const radius = 6371

func degrees2radians(degrees float64) float64 {
	return degrees * math.Pi / 180
}

func (origin Coordinates) Distance(destination Coordinates) float64 {
	degreesLat := degrees2radians(destination.Latitude - origin.Latitude)
	degreesLong := degrees2radians(destination.Longitude - origin.Longitude)
	a := (math.Sin(degreesLat/2)*math.Sin(degreesLat/2) +
		math.Cos(degrees2radians(origin.Latitude))*
			math.Cos(degrees2radians(destination.Latitude))*math.Sin(degreesLong/2)*
			math.Sin(degreesLong/2))
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	d := radius * c

	return d
}
