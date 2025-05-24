package kafka

import (
	"etl-pipeline/config"
	"time"

	"github.com/segmentio/kafka-go"
)

func createSecureDialer(cfg *config.Config) (*kafka.Dialer, error) {
	// mechanism, err := scram.Mechanism(scram.SHA512, cfg.Kafka.User, cfg.Kafka.Password)
	// if err != nil {
	// 	return nil, err
	// }

	// Optional: load CA cert if needed (replace "ca.pem" with your actual cert path)
	// Uncomment the below if you want secure TLS
	/*
		caCert, err := ioutil.ReadFile("ca.pem")
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
	*/

	return &kafka.Dialer{
		Timeout:   30 * time.Second,
		DualStack: true,
		// TLS:       &tls.Config{InsecureSkipVerify: true}, // <- Replace for production
		// SASLMechanism: mechanism,
		ClientID: "etl-pipeline-client",
	}, nil
}
