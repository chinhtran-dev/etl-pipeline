# ETL Pipeline

## Overview
This project is an ETL (Extract, Transform, Load) pipeline built in Go. It processes messages from Kafka, transforms the data, and loads it into a database.

## Prerequisites
- Go 1.24 or higher
- Kafka
- PostgreSQL (or TimescaleDB)

## Installation
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd etl-pipeline
   ```
2. Install dependencies:
   ```bash
   go mod download
   ```
3. Install required tools:
   ```bash
   make install-tools
   ```

## Usage
1. Configure the application by setting environment variables or updating the configuration file.
2. Build the application:
   ```bash
   make build
   ```
3. Run the application:
   ```bash
   make run
   ```

## Testing
Run the tests using:
```bash
make test
```

## Contributing
1. Fork the repository.
2. Create a new branch for your feature.
3. Commit your changes.
4. Push to the branch.
5. Create a Pull Request. 