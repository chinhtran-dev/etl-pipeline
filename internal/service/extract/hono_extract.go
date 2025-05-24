package extract

import (
	"encoding/json"
	"errors"
	"etl-pipeline/internal/model"
	"time"
)

type Identity struct {
	TenantId string
	DeviceId string
}

type HonoExtractor interface {
	Extracter(data []byte) (Identity, interface{}, time.Time, error)
}

func (e *extract) Extracter(data []byte) (Identity, interface{}, time.Time, error) {
	identity := Identity{}

	var value model.KafkaMessageValue
	if err := json.Unmarshal(data, &value); err != nil {
		return identity, nil, time.Time{}, errors.New("failed to unmarshal KafkaMessageValue: " + err.Error())
	}

	tenantId := value.Headers["tenant_id"]
	deviceId := value.Headers["device_id"]

	identity.TenantId = tenantId.(string)
	identity.DeviceId = deviceId.(string)
	return identity, value.Value, value.Timestamp, nil
}

func NewHonoExtractor() HonoExtractor {
	return &extract{}
}
