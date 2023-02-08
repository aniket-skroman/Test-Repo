package helper

import (
	"fmt"
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
	t, err := time.Parse(layout, str)

	if err != nil {
		fmt.Println(err)
	}
	return t
}
