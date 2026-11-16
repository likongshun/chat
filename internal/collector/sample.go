package collector

import "time"

type Quality string

const (
	QualityGood      Quality = "good"
	QualityBad       Quality = "bad"
	QualityCommError Quality = "communication_error"
)

type Sample struct {
	EventID   string    `json:"event_id"`
	DeviceID  string    `json:"device_id"`
	Model     string    `json:"model"`
	PointCode string    `json:"point_code"`
	Unit      string    `json:"unit,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Value     any       `json:"value,omitempty"`
	Quality   Quality   `json:"quality"`
	Error     string    `json:"error,omitempty"`
}
