.PHONY: swagger-init swagger-build build run test install-tools lint format init clean help

MOCK_OUTPUT_DIR=internal/mock
MOCK_CASE=snake

help:
	@echo "Available commands:"
	@echo "  install-tools    - Install required tools"
	@echo "  swagger-init     - Initialize Swagger documentation"
	@echo "  swagger-build    - Build Swagger documentation"
	@echo "  init             - Initialize the project"
	@echo "  build            - Build the application"
	@echo "  run              - Run the application"
	@echo "  test             - Run tests"
	@echo "  fmt              - Format code"
	@echo "  lint             - Lint code"
	@echo "  generate-mocks   - Generate mocks"
	@echo "  clean            - Clean build artifacts"

install-tools:
	@echo "Installing required tools..."
	@go install github.com/swaggo/swag/cmd/swag@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@go install github.com/vektra/mockery/v2@latest
	@echo "Tools installed successfully!"

swagger-init:
	swag init -g cmd/app/main.go -o docs/swagger

swagger-build:
	swag fmt
	swag init -g cmd/app/main.go -o docs/swagger

init:
	go mod tidy
	go mod download

build:
	go build -o bin/app ./cmd/app/main.go

run:
	bin/app api

test:
	go test -v ./...

fmt:
	goimports -w .

lint:
	golangci-lint run ./...

generate-mocks:
	mockery

clean:
	rm -rf bin/