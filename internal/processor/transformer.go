package processor

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type RawData struct {
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

func toCamelCase(s string) string {
	parts := strings.Split(s, "_")
	for i := 1; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

func transform(input []byte) (time.Time, map[string]interface{}, error) {
	var raw RawData
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
		result[toCamelCase(k)] = v
	}

	if len(result) == 0 {
		return time.Time{}, nil, errors.New("all fields in 'data' are null or empty")
	}

	return raw.Timestamp, result, nil
}
