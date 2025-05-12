package repository

const (
	InsertRawDeviceData = `
	INSERT INTO raw_device_data (tenant_id, device_id, timestamp, data)
	VALUES ($1, $2, $3, $4)
	`
)
