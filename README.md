# Crystal - Distributed Key-Value Store

<p align="center">
  <img src="logo.png" alt="Crystal Logo" width="200">
</p>

Crystal is a simple distributed key-value store using Log-Structured Merge Trees (LSM Trees) and RAFT consensus algorithm, written in Golang.

## Features

### Planned Features

- **Distributed Architecture**: Multiple nodes working together to form a consistent cluster
- **RAFT Consensus**: Strong consistency model using RAFT algorithm for leader election and log replication
- **LSM Tree Storage**: Efficient storage engine with optimized write performance
- **Automatic Sharding**: Data distribution across multiple nodes
- **Consistency over Availability**: Crystal is an CP not an AP store

### Current Capabilities

- Basic HTTP API with GET and SET operations
- Thread-safe key-value storage using RWMutex
- JSON-based request/response format
- In-memory data storage

## Getting Started

### Prerequisites

- Go 1.24.3 or higher

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/OmarAbdo/Crystal.git
   cd Crystal
   ```

2. Run the application:
   ```bash
   go run main.go
   ```

The server will start on port 8080.

### Testing the API

Here are some curl commands to test the API:

**Set a value (should throw an error due to incorrect JSON value type):**

```bash
curl -i -X POST -H "Content-Type: application/json" -d "{\"key\": \"test\", \"value\": 12345}" http://localhost:8080/set
```

**Set a value (correct JSON format):**

```bash
curl -i -X POST -H "Content-Type: application/json" -d "{\"key\": \"test\", \"value\": \"12345\"}" http://localhost:8080/set
```

**Get a value:**

```bash
curl -i -X GET "http://localhost:8080/get?key=test"
```

## Project Structure

```
Crystal/
├── main.go          # Main application code
├── go.mod           # Go module file
├── logo.png         # Project logo
└── README.md        # This file
```

### main.go Components

- **CrystalStore**: The core key-value store implementation with thread-safe operations
- **NewCrystalStore**: Initializes a new store instance
- **Put**: Inserts or updates a key-value pair
- **Get**: Retrieves a value by key
- **HTTP Handlers**:
  - `/set` endpoint for setting key-value pairs
  - `/get` endpoint for retrieving values

## Contributing

We welcome contributions to the Crystal project! Here's how you can help:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please make sure to follow the existing code style and add tests for new functionality.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

- Repository: [https://github.com/OmarAbdo/Crystal](https://github.com/OmarAbdo/Crystal)
- Issues: [Report bugs or request features](https://github.com/OmarAbdo/Crystal/issues)
