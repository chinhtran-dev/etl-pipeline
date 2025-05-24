package extract

import (
	"encoding/json"
	"errors"
	"etl-pipeline/internal/model"
	"time"

	"github.com/segmentio/kafka-go"
)

type Identity struct {
	TenantId string
	DeviceId string
}

type HonoExtractor interface {
	Extracter(msg kafka.Message) (Identity, interface{}, time.Time, error)
}

func (e *extract) Extracter(msg kafka.Message) (Identity, interface{}, time.Time, error) {
	identity := Identity{}

	var action, tenantId, deviceId string
	for _, header := range msg.Headers {
		switch header.Key {
		case "action":
			action = string(header.Value)
		case "tenant_id":
			tenantId = string(header.Value)
		case "device_id":
			deviceId = string(header.Value)
		}
	}

	if action != "modified" {
		return identity, nil, time.Time{}, nil
	}

	var value model.KafkaMessageValue
	if err := json.Unmarshal(msg.Value, &value); err != nil {
		return identity, nil, time.Time{}, errors.New("failed to unmarshal KafkaMessageValue: " + err.Error())
	}

	if value.Path != "/features" {
		return identity, nil, time.Time{}, nil
	}

	identity.TenantId = tenantId
	identity.DeviceId = deviceId
	return identity, value.Value, value.Timestamp, nil
}

func NewHonoExtractor() HonoExtractor {
	return &extract{}
}
