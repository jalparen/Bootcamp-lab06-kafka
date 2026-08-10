# BankOps - Gestión de Reemplazo de Tarjetas Bancarias (Lab 06)

Sistema reactivo end-to-end basado en eventos (EDA) con Java 17 + Spring Boot 3:

- **card-ops-producer** (puerto 8085): recibe `POST /api/card-replacements`, valida, verifica en Redis si es intento 1 o 2, guarda snapshot del evento en Redis y publica el evento **Avro** al tópico Kafka `bank.card.replacements.v1`.
- **dispatch-consumer** (puerto 8086): consume el tópico con **Reactor Kafka**, deserializa Avro (Schema Registry), persiste en **MongoDB** reactiva (`DISPATCHED` / `DISPATCHED_CACHE` usando el snapshot de Redis) y redirige mensajes inválidos al tópico **DLT**.

## Stack

| Componente      | Puerto | Notas                                   |
|-----------------|--------|-----------------------------------------|
| zookeeper       | 2181   | confluentinc/cp-zookeeper:7.6.1         |
| kafka           | 9092 / 29092 | cp-kafka:7.6.1 (KRaft vía ZK)    |
| schema-registry | 8081   | cp-schema-registry:7.6.1                |
| redis           | 6379   | redis:7-alpine                          |
| mongo           | 27018 (host) -> 27017 | mongo:7                      |
| card-ops-producer | 8085 | Spring Boot 3.2.5                      |
| dispatch-consumer | 8086 | Spring Boot 3.2.5                      |
| prometheus      | 9090   | scrapea `/actuator/prometheus`          |
| grafana         | 3000   | http://localhost:3000 (admin/admin)     |

## Cómo levantar todo

```bash
docker compose up -d --build
```

> IMPORTANTE: si ya tienes otro `docker compose` corriendo con Kafka/Redis/Schema Registry
> (por ejemplo `docker-compose.yaml` del bootcamp), detenlo primero para evitar conflictos
> de puertos (9092/29092/8081/6379):
> ```bash
> docker compose -f <ese-archivo> down
> ```

Verificar salud:

```bash
curl http://localhost:8086/api/health
```

## Flujo de demostración

### Paso 1 — Intento 1 (Attempt 1)

```bash
curl -X POST http://localhost:8085/api/card-replacements \
  -H "Content-Type: application/json" \
  -d '{
    "requestId": "REQ-1001",
    "customerId": "CUST-8899",
    "cardPANMasked": "4111********1111",
    "reasonCode": "LOST",
    "priority": "HIGH",
    "branchCode": "BR-001",
    "deliveryAddress": "Av. Javier Prado 1234, Lima",
    "correlationId": "CORR-001",
    "status": "REQUESTED"
  }'
```

Respuesta: `202` con el `eventId`.

### Paso 2 — Validar en MongoDB

```bash
curl -X GET "http://localhost:8086/api/events?requestId=REQ-1001"
```

Respuesta esperada: `status = DISPATCHED`, `attemptNumber = 1`.

### Paso 3 — Intento 2 (Attempt 2, usa snapshot de Redis)

Repite el mismo POST de arriba (mismo `requestId`).

### Paso 4 — Validar actualización (upsert + enriquecimiento)

```bash
curl -X GET "http://localhost:8086/api/events?requestId=REQ-1001"
```

Respuesta esperada: `status = DISPATCHED_CACHE`, `attemptNumber = 2`.

### Paso 5 (opcional) — Evento inválido a DLT

Enviar un payload sin `requestId` (o consumir directamente al tópico) y verificar en
Offset Explorer que el mensaje llega a `bank.card.replacements.v1.DLT`.

## Verificación manual

- **Redis**: `redis-cli` → `GET card:req:REQ-1001`, `GET card:event:REQ-1001`
- **Mongo**: `mongosh "mongodb://localhost:27018/cardsdb"` → `db.card_replacements.find()`
- **Kafka**: Offset Explorer (Kafka Tool) → brokers `localhost:29092`, tópico `bank.card.replacements.v1`
- **Schema Registry**: `curl http://localhost:8081/subjects`
- **Métricas**: `curl http://localhost:8085/actuator/prometheus`, `curl http://localhost:8086/actuator/prometheus`
- **Grafana**: http://localhost:3000 (admin/admin), add data source Prometheus `http://prometheus:9090`

## Postman

Importa la colección `postman/Lab06 - BankOps.postman_collection.json`.

## Ejecutar en local (sin Docker)

1. Tener Kafka + Schema Registry + Redis corriendo (y MongoDB local).
2. `card-ops-producer`: `mvn spring-boot:run` en `card-ops-producer/`
3. `dispatch-consumer`: `mvn spring-boot:run` en `dispatch-consumer/`

Por defecto conectan a `localhost:29092` (Kafka), `http://localhost:8081` (Registry), `localhost:6379` (Redis) y `mongodb://localhost:27017/cardsdb` (Mongo). Todo es configurable por variables de entorno: `KAFKA_BOOTSTRAP_SERVERS`, `SCHEMA_REGISTRY_URL`, `REDIS_HOST`, `REDIS_PORT`, `MONGO_URI`.

## Arquitectura

```
POST /api/card-replacements (8085)
        │
        ▼
card-ops-producer (Clean/Hexagonal + RxJava3)
  ├─ AttemptPolicyOrchestrator → Redis card:req:{id} → attempt 1 | 2
  ├─ EventMapper → CardReplacementEvent (Avro)
  ├─ Snapshot en Redis card:event:{id} (TTL 15 min)
  └─ ResilientPublisher (CircuitBreaker + Retry) → Kafka
        │
        ▼  bank.card.replacements.v1 (Avro + Schema Registry)
dispatch-consumer (Clean/Hexagonal + Reactor Kafka + RxJava3)
  ├─ KafkaRxConsumer → EntityMapper (GenericRecord → dominio)
  ├─ ProcessingService:
  │    ├─ attempt 1 → persistir Mongo → DISPATCHED
  │    ├─ attempt 2 → snapshot Redis + upsert Mongo → DISPATCHED_CACHE
  │    └─ inválido/sin requestId → bank.card.replacements.v1.DLT
  └─ QueryController: GET /api/events?requestId=... | GET /api/health
```
