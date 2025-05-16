package transform

import (
	"encoding/json"
	"errors"
	"etl-pipeline/pkg/util"
	"time"
)

type rawData struct {
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

type transform struct {
}

// transform implements Transform.
func (t *transform) Transform(input []byte) (time.Time, map[string]interface{}, error) {
	var raw rawData
	if err := json.Unmarshal(input, &raw); err != nil {
		return time.Time{}, nil, errors.New("failed to unmarshal RawData: " + err.Error())
	}

	if raw.Timestamp.IsZero() {
		raw.Timestamp = time.Now().UTC()
	}

	if len(raw.Data) == 0 {
		return time.Time{}, nil, errors.New("missing or empty 'data' field")
	}

	var data map[string]interface{}
	if err := json.Unmarshal(raw.Data, &data); err != nil {
		return time.Time{}, nil, errors.New("failed to unmarshal 'data' field: " + err.Error())
	}

	result := make(map[string]interface{})
	for k, v := range data {
		if v == nil {
			continue
		}
		result[util.ToCamelCase(k)] = v
	}

	if len(result) == 0 {
		return time.Time{}, nil, errors.New("all fields in 'data' are null or empty")
	}

	return raw.Timestamp, result, nil
}

type Transform interface {
	Transform(input []byte) (time.Time, map[string]interface{}, error)
}

func NewTransform() Transform {
	return &transform{}
}
