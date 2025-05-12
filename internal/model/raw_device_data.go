package model

import "time"

type RawDeviceData struct {
	TenantID  string                 `json:"tenant_id"`
	DeviceID  string                 `json:"device_id"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}
