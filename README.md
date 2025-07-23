# Kafka Protobuf CLI Tool

This CLI tool allows you to **produce** and **consume** messages in Apache Kafka using **Protocol Buffers** for serialization. The tool reads JSON files, converts them to Protobuf messages, sends them to Kafka topics, and also consumes and decodes Protobuf messages from Kafka topics.

---

## Prerequisites

- **Java 17+ / Java 21 recommended**
- **Apache Maven 3.6+**
- **Docker & Docker Compose** (optional, for running Kafka locally)

---

## Project Structure

- `src/main/proto/person.proto`: Protocol Buffers schema for `Person` message.
- `src/main/resources/person.json`: Sample JSON file representing a `Person`.
- `com.aymene.producer`: Kafka producer code that converts JSON → Protobuf → Kafka.
- `com.aymene.consumer`: Kafka consumer code that consumes Protobuf messages and decodes them.
- `com.aymene.config`: Kafka producer and consumer configuration utilities.
- `docker/docker-compose.yml`: Kafka and Zookeeper setup for local testing.

---

## Build Instructions

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd kafka-protobuf-cli
   ```

2. **Compile the project and generate Protobuf sources**

This command runs the Maven protobuf plugin, generates Java sources from .proto files, and builds the executable jar:
   ```bash
   mvn clean package
   ```

3. **Locate the jar**

The compiled executable jar will be at:
   ```bash
   target/kafka-protobuf-cli-1.0.0.jar
   ```


## Running the Tool

The CLI tool has two modes: **produce** and **consume**.

---

### 1. Produce Mode

This mode reads a JSON file containing a person, converts it to a Protobuf message, and sends it to a Kafka topic.

**Usage:**

```bash
java -jar target/kafka-protobuf-cli-1.0.0.jar produce <json-file-path> <topic> <bootstrap-server>
```
### 2. Consume Mode

This mode subscribes to a Kafka topic, continuously consumes messages, decodes the Protobuf payload, and prints the person details to the console.

**Usage:**

```bash
java -jar target/kafka-protobuf-cli-1.0.0.jar consume <topic> <bootstrap-server>
```

```bash
java -jar target/kafka-protobuf-cli-1.0.0.jar consume persons-topic localhost:9092
```

## Message Format
* Kafka messages use Protocol Buffers for serialization.
* The `.proto` schema is defined in `src/main/proto/person.proto`.
* The `Person` message has fields:
  * `int32 id`
  * `string name`
  * `string email`


# Docker Setup for Kafka (Optional)

You can start Kafka and Zookeeper locally using Docker Compose.

From the project root run:

```bash
docker-compose -f docker/docker-compose.yml up -d
```
**This will start:**
* Zookeeper at `localhost:2181`
* Kafka broker at `localhost:9092`

**Make sure Kafka is running before producing or consuming messages.**