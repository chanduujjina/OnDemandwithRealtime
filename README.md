# OnDemandwithRealtime


# DrugInfoService & DrugDealer — Specification Document

## 1. Architecture Overview

Two independent Spring Boot microservices communicate via REST (client-facing ingestion) and Kafka (asynchronous event propagation).

**Real-time flow:**

1. An external caller sends `POST /saveDrugInfo` to **DrugInfoService**.
2. DrugInfoService validates the request, persists it to **MySQL**, and receives the database-generated `drugId`.
3. DrugInfoService returns `{ "drugId": ... }` synchronously to the caller.
4. Asynchronously (same request lifecycle, non-blocking), DrugInfoService's Kafka producer publishes a `drug.info.requested` event to topic **`/drugInfo`**.
5. **DrugDealer**'s Kafka consumer, subscribed to `/drugInfo`, receives the event.
6. DrugDealer deserializes the event envelope and persists it into **Cassandra** as a consumed-event record.
7. Independently, DrugInfoService also performs **outbound HttpClient calls** (read and write) to a downstream drug-data service, separate from the Kafka path (see Section 6).

```
Client
  │  POST /saveDrugInfo
  ▼
DrugInfoService ──(JPA)──► MySQL
  │
  ├──(Kafka Producer)──► topic: /drugInfo ──► DrugDealer (Kafka Consumer) ──(Cassandra Data)──► Cassandra
  │
  └──(HttpClient)──► External Drug Data API (read/write)
```

Both services run independently and can be deployed, scaled, and restarted separately. Kafka is the sole coupling mechanism between them — there is no direct REST call from DrugInfoService to DrugDealer.

---

## 2. DrugInfoService Data Model (MySQL)

**Table:** `drug_info`

| Column         | Type            | Constraints                          |
|----------------|-----------------|---------------------------------------|
| `drug_id`      | `BIGINT`        | Primary Key, `AUTO_INCREMENT`         |
| `drug_name`    | `VARCHAR(255)`  | `NOT NULL`                            |
| `ndc_code`     | `VARCHAR(50)`   | `NOT NULL`                            |
| `strength`     | `VARCHAR(50)`   | `NOT NULL`                            |
| `dosage_form`  | `VARCHAR(50)`   | `NOT NULL`                            |
| `quantity`     | `INT`           | `NOT NULL`                            |
| `created_at`   | `TIMESTAMP`     | `NOT NULL DEFAULT CURRENT_TIMESTAMP`  |

- `drug_id` is **database-generated** (`IDENTITY`/`AUTO_INCREMENT`) and maps to JPA field `drugId` via `@GeneratedValue(strategy = GenerationType.IDENTITY)`.
- Entity class: `DrugInfo` (suggested), mapped via Spring Data JPA `@Entity` + `@Table(name = "drug_info")`.
- Repository: `DrugInfoRepository extends JpaRepository<DrugInfo, Long>`.

---

## 3. DrugDealer Data Model (Cassandra)

**Keyspace:** `drugdealer_ks` (placeholder name — configurable)

**Table:** `consumed_drug_events`

| Column           | Type        | Role              |
|------------------|-------------|-------------------|
| `event_type`     | `text`      | **Partition Key** |
| `event_id`       | `text`      | **Clustering Key** (`ASC`) |
| `event_version`  | `text`      |                   |
| `event_timestamp`| `timestamp` |                   |
| `source`         | `text`      |                   |
| `drug_name`      | `text`      |                   |
| `ndc_code`       | `text`      |                   |
| `strength`       | `text`      |                   |
| `dosage_form`    | `text`      |                   |
| `quantity`       | `int`       |                   |
| `consumed_at`    | `timestamp` | server-set on write |

**Design rationale:**
- Partitioning by `event_type` groups all events of the same kind (e.g., all `drug.info.requested` events) onto the same partition, which supports efficient range queries per event type.
- Clustering by `event_id` (ascending) ensures uniqueness within a partition and a deterministic ordering.

```sql
CREATE TABLE consumed_drug_events (
    event_type text,
    event_id text,
    event_version text,
    event_timestamp timestamp,
    source text,
    drug_name text,
    ndc_code text,
    strength text,
    dosage_form text,
    quantity int,
    consumed_at timestamp,
    PRIMARY KEY ((event_type), event_id)
) WITH CLUSTERING ORDER BY (event_id ASC);
```

- Entity class: `ConsumedDrugEvent` (suggested), mapped via Spring Data Cassandra `@Table("consumed_drug_events")`, `@PrimaryKeyClass` with `@PrimaryKeyColumn` annotations for partition/clustering roles.
- Repository: `ConsumedDrugEventRepository extends CassandraRepository<ConsumedDrugEvent, ConsumedDrugEventKey>`.

---

## 4. REST API Contract — `POST /saveDrugInfo`

**Service:** DrugInfoService
**Method:** `POST`
**Path:** `/saveDrugInfo`
**Content-Type:** `application/json`

### Request Body

| Field         | Type    | Required | Notes                        |
|---------------|---------|----------|-------------------------------|
| `drugName`    | String  | Yes      | Non-blank                     |
| `ndcCode`     | String  | Yes      | Non-blank                     |
| `strength`    | String  | Yes      | e.g. `"200mg"`                |
| `dosageForm`  | String  | Yes      | e.g. `"Tablet"`                |
| `quantity`    | Integer | Yes      | Must be `> 0`                  |

```json
{
  "drugName": "Ibuprofen",
  "ndcCode": "00573-0164-40",
  "strength": "200mg",
  "dosageForm": "Tablet",
  "quantity": 1
}
```

### Response Body

| Field     | Type  | Notes                              |
|-----------|-------|-------------------------------------|
| `drugId`  | Long  | Database-generated primary key      |

```json
{ "drugId": 1 }
```

**Status codes:**
- `201 Created` — successful save (Kafka publish is fire-and-forget; REST response does not wait on Kafka ack).
- `400 Bad Request` — validation failure (missing/blank fields, `quantity <= 0`).
- `500 Internal Server Error` — persistence failure.

**Response header (on 201):** `Location: /saveDrugInfo/{drugId}` (optional, recommended for REST convention).

---

## 5. Kafka Contract — Topic `/drugInfo`

> Note: Kafka topic names conventionally avoid leading slashes (`/`), but this specification preserves the exact literal topic name `drugInfo` as given, referenced here as `/drugInfo` per the requirement. Implementers should configure the topic name as `drugInfo` (no slash) if the target Kafka broker rejects slash-prefixed topic names — call out this discrepancy in Phase 1 setup and confirm before implementation.

**Topic name (placeholder-configurable):** `${KAFKA_TOPIC_DRUG_INFO}` → default value `drugInfo`

### Producer — DrugInfoService

- Triggered immediately after a successful MySQL persist within the `/saveDrugInfo` request flow.
- Uses `KafkaTemplate<String, DrugInfoEvent>` (JSON serialization via `JsonSerializer`).
- **Message key:** `eventId` (String) — ensures partition affinity per event.
- **Delivery semantics:** at-least-once (producer acks = `all`, retries enabled).
- Failure to publish does **not** roll back the MySQL write or fail the REST response; publish failures are logged and (optionally) routed to a dead-letter mechanism in a later phase.

### Consumer — DrugDealer

- `@KafkaListener` bound to topic `${KAFKA_TOPIC_DRUG_INFO}`, consumer group `${KAFKA_CONSUMER_GROUP_DRUGDEALER}` (e.g. `drugdealer-group`).
- Deserializes JSON payload into `DrugInfoEvent` (via `JsonDeserializer`, trusted packages configured explicitly — no wildcard trust).
- On successful deserialization, maps envelope + nested `data` fields into `ConsumedDrugEvent` and persists via `ConsumedDrugEventRepository.save(...)`.
- Manual or auto ack per configured `AckMode` (recommend `MANUAL_IMMEDIATE` for phase where reliability is emphasized; `RECORD` acceptable for initial phases).
- Malformed/unparseable messages are routed to error handling (log + optional DLT topic `drugInfo.DLT`), not silently dropped.

### Event Envelope Schema

```json
{
  "eventId": "evt-7f3a9c12-88b1-4e2d-9a6f-3c1d8e5b0a44",
  "eventType": "drug.info.requested",
  "eventVersion": "1.0",
  "timestamp": "2026-09-18T14:45:00Z",
  "source": "pharmacy-service",
  "data": {
    "drugName": "Ibuprofen",
    "ndcCode": "00573-0164-40",
    "strength": "200mg",
    "dosageForm": "Tablet",
    "quantity": 1
  }
}
```

| Field                | Type            | Notes                                          |
|----------------------|-----------------|--------------------------------------------------|
| `eventId`             | String (UUID-prefixed) | Unique per event; used as Kafka message key |
| `eventType`           | String          | Fixed value `"drug.info.requested"` for this contract |
| `eventVersion`        | String          | Semantic-style version, e.g. `"1.0"`            |
| `timestamp`           | String (ISO-8601, UTC) | Event creation time                       |
| `source`              | String          | Fixed value `"pharmacy-service"` (or configurable placeholder) |
| `data.drugName`       | String          | Mirrors REST request field                      |
| `data.ndcCode`        | String          | Mirrors REST request field                      |
| `data.strength`       | String          | Mirrors REST request field                      |
| `data.dosageForm`     | String          | Mirrors REST request field                      |
| `data.quantity`       | Integer         | Mirrors REST request field                      |

**Java model shape (interfaces only, no implementation):**

```java
public class DrugInfoEvent {
    private String eventId;
    private String eventType;
    private String eventVersion;
    private String timestamp;
    private String source;
    private DrugInfoData data;
    // getters/setters
}

public class DrugInfoData {
    private String drugName;
    private String ndcCode;
    private String strength;
    private String dosageForm;
    private Integer quantity;
    // getters/setters
}
```

---

## 6. HttpClient Integration (DrugInfoService)

DrugInfoService makes outbound calls to an external/downstream **Drug Data API** using Java's `java.net.http.HttpClient` (or Spring's `RestClient`/`WebClient` — implementer's choice, specified here at the contract level).

### Outbound READ call

- **Purpose:** Enrich or validate incoming drug data before persistence (e.g., verify NDC code exists).
- **Endpoint:** `${DRUG_DATA_API_BASE_URL}/drugs/{ndcCode}` (placeholder base URL, configured via environment variable — never hard-coded).
- **Method:** `GET`
- **Request:** path parameter `ndcCode`, no body.
- **Response (expected shape):**
```json
{
  "ndcCode": "00573-0164-40",
  "verified": true,
  "manufacturer": "Pfizer"
}
```
- **Error handling:**
  - Non-2xx response → log warning, proceed with save using unverified data (non-blocking enrichment), OR reject with `422 Unprocessable Entity` — **decision placeholder**, to be confirmed in Phase 3.
  - Connection timeout / `IOException` → caught, logged, treated as enrichment failure (does not fail the primary save flow) unless configured otherwise.
  - Configured timeouts: connect timeout `${HTTP_CONNECT_TIMEOUT_MS}`, request timeout `${HTTP_REQUEST_TIMEOUT_MS}`.

### Outbound WRITE call

- **Purpose:** Notify/sync the external Drug Data API of the newly saved drug record.
- **Endpoint:** `${DRUG_DATA_API_BASE_URL}/drugs`
- **Method:** `POST`
- **Request body:**
```json
{
  "drugId": 1,
  "drugName": "Ibuprofen",
  "ndcCode": "00573-0164-40",
  "strength": "200mg",
  "dosageForm": "Tablet",
  "quantity": 1
}
```
- **Response:** `202 Accepted` expected, body ignored.
- **Error handling:**
  - Non-2xx → logged as a warning; retried up to `${HTTP_WRITE_MAX_RETRIES}` times with exponential backoff; does not roll back the MySQL transaction or block the REST response to the original caller (this call is asynchronous/best-effort, executed off the request thread — e.g., via `@Async` or a dedicated executor).
  - Persistent failure after retries exhausted → logged as an error for manual follow-up (no dead-letter store required at this phase).

---

## 7. Technology Stack & Runtime Configuration

### Stack

| Concern             | Technology                          |
|----------------------|--------------------------------------|
| REST layer            | Spring Web (Spring MVC)             |
| DrugInfoService persistence | Spring Data JPA + MySQL Driver |
| DrugDealer persistence | Spring Data Cassandra              |
| Messaging              | Spring Kafka                       |
| Outbound HTTP           | `java.net.http.HttpClient` (or Spring `RestClient`) |
| Build tool              | Maven or Gradle (implementer's choice) |
| Containerization         | Docker + Docker Compose            |

### Base Package

`[INSERT BASE PACKAGE, ex: com.example.druginfo]` — applies to both services, with service-specific sub-packages, e.g.:
- `com.example.druginfo` (DrugInfoService)
- `com.example.drugdealer` (DrugDealer)

### DrugInfoService `application.yml` (placeholders only)

```yaml
spring:
  datasource:
    url: ${MYSQL_URL}
    username: ${MYSQL_USERNAME}
    password: ${MYSQL_PASSWORD}
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:
    hibernate:
      ddl-auto: ${JPA_DDL_AUTO:validate}
    show-sql: false
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all
      retries: 3

drug-data-api:
  base-url: ${DRUG_DATA_API_BASE_URL}
  connect-timeout-ms: ${HTTP_CONNECT_TIMEOUT_MS:3000}
  request-timeout-ms: ${HTTP_REQUEST_TIMEOUT_MS:5000}
  write-max-retries: ${HTTP_WRITE_MAX_RETRIES:3}

kafka:
  topic:
    drug-info: ${KAFKA_TOPIC_DRUG_INFO:drugInfo}
```

### DrugDealer `application.yml` (placeholders only)

```yaml
spring:
  data:
    cassandra:
      contact-points: ${CASSANDRA_CONTACT_POINTS}
      port: ${CASSANDRA_PORT:9042}
      keyspace-name: ${CASSANDRA_KEYSPACE:drugdealer_ks}
      local-datacenter: ${CASSANDRA_DATACENTER:datacenter1}
      username: ${CASSANDRA_USERNAME}
      password: ${CASSANDRA_PASSWORD}
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}
    consumer:
      group-id: ${KAFKA_CONSUMER_GROUP_DRUGDEALER:drugdealer-group}
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: com.example.drugdealer.model

kafka:
  topic:
    drug-info: ${KAFKA_TOPIC_DRUG_INFO:drugInfo}
```

### Docker Compose Services (placeholders only)

```yaml
version: "3.9"
services:
  mysql:
    image: mysql:8
    environment:
      MYSQL_ROOT_PASSWORD: ${MYSQL_ROOT_PASSWORD}
      MYSQL_DATABASE: ${MYSQL_DATABASE}
      MYSQL_USER: ${MYSQL_USERNAME}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
    ports:
      - "${MYSQL_HOST_PORT:-3306}:3306"

  cassandra:
    image: cassandra:4
    environment:
      CASSANDRA_CLUSTER_NAME: ${CASSANDRA_CLUSTER_NAME}
    ports:
      - "${CASSANDRA_HOST_PORT:-9042}:9042"

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: ${ZOOKEEPER_CLIENT_PORT:-2181}

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:${ZOOKEEPER_CLIENT_PORT:-2181}
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:${KAFKA_BROKER_PORT:-9092}
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "${KAFKA_HOST_PORT:-9092}:9092"

  druginfoservice:
    build: ./druginfoservice
    depends_on:
      - mysql
      - kafka
    environment:
      MYSQL_URL: ${MYSQL_URL}
      MYSQL_USERNAME: ${MYSQL_USERNAME}
      MYSQL_PASSWORD: ${MYSQL_PASSWORD}
      KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}
      DRUG_DATA_API_BASE_URL: ${DRUG_DATA_API_BASE_URL}
    ports:
      - "${DRUGINFOSERVICE_PORT:-8081}:8081"

  drugdealer:
    build: ./drugdealer
    depends_on:
      - cassandra
      - kafka
    environment:
      CASSANDRA_CONTACT_POINTS: ${CASSANDRA_CONTACT_POINTS}
      CASSANDRA_USERNAME: ${CASSANDRA_USERNAME}
      CASSANDRA_PASSWORD: ${CASSANDRA_PASSWORD}
      KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP_SERVERS}
    ports:
      - "${DRUGDEALER_PORT:-8082}:8082"
```

No credentials, hostnames, or connection strings are hard-coded anywhere; all are supplied via environment variables / `.env` file, injected at container or application runtime.

---

# Phase-by-Phase Build Plan

### Phase 1 — Scaffolding & Infrastructure

- **Goal:** Both Spring Boot applications exist as independently buildable projects, and all infrastructure dependencies (MySQL, Cassandra, Kafka) run via Docker Compose.
- **Objective:**
  - Generate two Spring Boot projects: `druginfoservice` (Spring Web, Spring Data JPA, Spring Kafka, MySQL driver) and `drugdealer` (Spring Web not required unless health endpoints desired, Spring Data Cassandra, Spring Kafka).
  - Set base package structure per Section 7.
  - Write `docker-compose.yml` with MySQL, Cassandra, Zookeeper, Kafka services and placeholder-driven environment variables (`.env.example` file, no real secrets committed).
  - Confirm Kafka topic naming convention (`drugInfo` vs `/drugInfo`) with the team before Phase 4.
  - Add empty `application.yml` files with placeholder property keys (no values) for both services.
- **Acceptance criteria:**
  - `docker-compose up` starts MySQL, Cassandra, Zookeeper, and Kafka containers without errors.
  - Both Spring Boot projects build successfully (`mvn clean install` / `gradle build`) with no compilation errors, even with empty business logic.
  - Each service exposes a working `/actuator/health` (or equivalent) endpoint returning `200 OK` once started.

---

### Phase 2 — DrugInfoService: MySQL Schema & JPA Persistence

- **Goal:** DrugInfoService can persist a drug info record to MySQL and retrieve a generated `drugId`.
- **Objective:**
  - Create `drug_info` table (via migration script or `ddl-auto` for dev) per Section 2 schema.
  - Implement `DrugInfo` JPA entity with `@GeneratedValue(strategy = GenerationType.IDENTITY)` on `drugId`.
  - Implement `DrugInfoRepository extends JpaRepository<DrugInfo, Long>`.
  - Implement a service-layer method that accepts the request DTO, maps to entity, saves, and returns the generated ID.
- **Acceptance criteria:**
  - Calling the repository's save method with a valid `DrugInfo` object persists a row in MySQL and returns a non-null, auto-incremented `drugId`.
  - Table schema in MySQL exactly matches Section 2 (verified via `DESCRIBE drug_info;`).
  - Duplicate saves produce distinct, sequential `drugId` values.

---

### Phase 3 — DrugInfoService: REST Endpoint

- **Goal:** `POST /saveDrugInfo` is fully functional end-to-end against MySQL, matching the exact request/response contract.
- **Objective:**
  - Implement `DrugInfoController` with `POST /saveDrugInfo` mapped to the service layer from Phase 2.
  - Implement request DTO validation (`@NotBlank`, `@Positive`, etc.) matching Section 4 field rules.
  - Map persisted entity's `drugId` into the `{ "drugId": ... }` response DTO.
  - Implement global exception handling for validation errors (`400`) and persistence errors (`500`).
- **Acceptance criteria:**
  - `POST /saveDrugInfo` with the exact sample payload from Section 4 returns `201 Created` with body `{ "drugId": <int> }` matching a row present in MySQL.
  - Submitting a payload with a missing/blank required field returns `400 Bad Request`.
  - Submitting `quantity: 0` or negative returns `400 Bad Request`.

---

### Phase 4 — DrugInfoService: Kafka Producer

- **Goal:** Every successful `/saveDrugInfo` call publishes a correctly formed event to topic `drugInfo`.
- **Objective:**
  - Configure `KafkaTemplate<String, DrugInfoEvent>` with `JsonSerializer`.
  - Implement `DrugInfoEvent` / `DrugInfoData` model classes matching Section 5 schema exactly.
  - After successful MySQL save, construct and publish the event (keyed by `eventId`) to `${KAFKA_TOPIC_DRUG_INFO}`.
  - Add logging for publish success/failure; ensure publish failure does not affect REST response.
- **Acceptance criteria:**
  - After a successful `POST /saveDrugInfo` call, a message is observable on topic `drugInfo` (verified via CLI consumer or test harness) whose JSON body matches the Section 5 schema field-for-field, with `data` populated from the original request.
  - Simulated Kafka broker unavailability does not cause `/saveDrugInfo` to return a non-2xx response (verified via test with broker stopped).

---

### Phase 5 — DrugDealer: Kafka Consumer & Cassandra Persistence

- **Goal:** DrugDealer reliably consumes events from `drugInfo` and persists them to Cassandra.
- **Objective:**
  - Create Cassandra keyspace and `consumed_drug_events` table per Section 3 DDL.
  - Implement `ConsumedDrugEvent` entity with composite primary key class defining `event_type` as partition key and `event_id` as clustering key.
  - Implement `ConsumedDrugEventRepository extends CassandraRepository<...>`.
  - Implement `@KafkaListener` bound to `drugInfo` topic with `JsonDeserializer` configured with explicit trusted packages.
  - Map incoming `DrugInfoEvent` to `ConsumedDrugEvent`, setting `consumed_at` to current server time, and save via repository.
  - Implement basic error handling for deserialization failures (log + skip, or route to DLT topic).
- **Acceptance criteria:**
  - Publishing a valid event to `drugInfo` (matching Section 5 schema) results in a corresponding row appearing in `consumed_drug_events` within a few seconds, with all fields correctly mapped.
  - Querying Cassandra by `event_type = 'drug.info.requested'` returns the row, confirming partition key behavior.
  - A malformed (non-JSON or schema-mismatched) message on the topic does not crash the consumer process; the consumer continues processing subsequent valid messages.

---

### Phase 6 — DrugInfoService: HttpClient Integration

- **Goal:** DrugInfoService performs the outbound read (enrichment) and write (notification) HTTP calls described in Section 6.
- **Objective:**
  - Implement an `HttpClient`-based (or `RestClient`-based) component configured with `${DRUG_DATA_API_BASE_URL}` and configurable timeouts.
  - Implement the READ call (`GET /drugs/{ndcCode}`) invoked prior to/alongside the save flow; finalize and implement the decided error-handling behavior (block vs. proceed) from Section 6.
  - Implement the WRITE call (`POST /drugs`) invoked asynchronously after a successful save, with retry-with-backoff logic up to `${HTTP_WRITE_MAX_RETRIES}`.
  - Add logging/metrics for both call outcomes (success, timeout, non-2xx, retries exhausted).
- **Acceptance criteria:**
  - With a mock/stub Drug Data API returning `200 OK` for the read call, `/saveDrugInfo` completes successfully and the read call is observably invoked (via mock server request log).
  - With the mock Drug Data API returning `202 Accepted` for the write call, the write call is observably invoked after the save completes, without blocking or delaying the REST response beyond an acceptable threshold (e.g., response returns before write-call completion).
  - With the mock Drug Data API simulating repeated failures on the write call, retries occur up to the configured max, and the failure is logged without crashing the service or affecting the original REST response.

---

### Phase 7 — End-to-End Verification

- **Goal:** The full system — REST ingestion, MySQL persistence, Kafka propagation, Cassandra consumption, and HttpClient integration — works together correctly under a realistic Docker Compose deployment.
- **Objective:**
  - Run the full stack via `docker-compose up` (all 6 services from Section 7).
  - Execute an end-to-end test: `POST /saveDrugInfo` with the sample payload from this specification.
  - Verify data consistency across all three data stores/systems: MySQL row, Kafka message, Cassandra row, and (via mock or real endpoint) the outbound HttpClient calls.
  - Document the verification steps and expected observable outcomes in a runbook.
- **Acceptance criteria:**
  - A single `POST /saveDrugInfo` call results in: (1) a new row in MySQL `drug_info` with a generated `drugId`; (2) a `201 Created` response with matching `drugId`; (3) a corresponding Kafka message on `drugInfo` matching the envelope schema; (4) a corresponding row in Cassandra `consumed_drug_events` with matching `data` fields; (5) observable outbound HTTP read and write calls (via mock server or logs).
  - Restarting the DrugDealer service mid-flow does not lose messages published while it was down, once Kafka consumer offset behavior is verified (message re-delivered/consumed on restart, assuming default retention and no offset commit before crash).
  - All acceptance criteria from Phases 1–6 remain independently verifiable/passing in the composed environment.
