package collector

import "time"

type Quality string

const (
	QualityGood      Quality = "good"
	QualityBad       Quality = "bad"
	QualityCommError Quality = "communication_error"
)

type Sample struct {
	EventID   string
	DeviceID  string
	Model     string
	PointCode string
	Timestamp time.Time
	Value     any
	Quality   Quality
	Error     string
}
