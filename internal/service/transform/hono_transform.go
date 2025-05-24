package transform

import "errors"

type HonoTransformer interface {
	HonoTransform(input interface{}) (map[string]interface{}, error)
}

func NewHonoTransformer() HonoTransformer {
	return &transform{}
}

func (t *transform) HonoTransform(input interface{}) (map[string]interface{}, error) {
	var data map[string]interface{}
	data, ok := input.(map[string]interface{})
	if !ok {
		return nil, errors.New("input is not a map[string]interface{}")
	}
	return data, nil
}
