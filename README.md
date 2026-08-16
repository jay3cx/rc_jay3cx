# notifyd

`notifyd` is an internal service that accepts a fully formed outbound HTTP notification, stores it in SQLite, and returns `202 Accepted`. Workers deliver the stored request independently of the caller.

This service only handles delivery. It does not translate business events, manage vendor credentials, authenticate internal callers, or promise exactly-once delivery. See [DESIGN.md](DESIGN.md) for the design decisions and [AI_USAGE.md](AI_USAGE.md) for how I used AI while working on the exercise.

## Run

Go 1.23 or later is required.

```sh
go test ./...
go run ./cmd/mockvendor
```

In another terminal:

```sh
NOTIFYD_ALLOW_PRIVATE_NETWORKS=true go run .
```

The private-network override is only for the local mock. It is off by default.

The same setup is available with containers:

```sh
docker compose up --build
```

## API

Create a notification:

```sh
curl -i http://localhost:8080/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: payment-7429-crm' \
  -d '{
    "url": "http://localhost:8081/flaky",
    "method": "POST",
    "headers": {"Content-Type": "application/json", "X-Demo-Token": "secret"},
    "body": {"contact_id": "c-104", "status": "customer"}
  }'
```

When using Docker Compose, use `http://mockvendor:8081/flaky` as the target URL. The mock also provides `/ok` and `/bad`.

A successful enqueue returns an ID. Reusing the same `Idempotency-Key` with the same request returns that ID; reusing it with a different request returns `409 Conflict`.

```text
GET  /v1/notifications/{id}
POST /v1/notifications/{id}/replay
GET  /healthz
```

The query response contains the current status and attempt history. Replay accepts only a notification in `dead` state. Target methods are limited to `POST`, `PUT`, `PATCH`, and `DELETE`. The complete create request, including its headers and JSON body, is limited to 1 MiB.

## Configuration

| Environment variable | Default |
| --- | --- |
| `NOTIFYD_ADDR` | `127.0.0.1:8080` |
| `NOTIFYD_DB_PATH` | `notifyd.db` |
| `NOTIFYD_WORKERS` | `8` |
| `NOTIFYD_PER_HOST_CONCURRENCY` | `2` |
| `NOTIFYD_MAX_ATTEMPTS` | `12` |
| `NOTIFYD_REQUEST_TIMEOUT` | `10s` |
| `NOTIFYD_BASE_BACKOFF` | `5s` |
| `NOTIFYD_MAX_BACKOFF` | `15m` |
| `NOTIFYD_LEASE_DURATION` | `30s` |
| `NOTIFYD_POLL_INTERVAL` | `250ms` |
| `NOTIFYD_ALLOW_PRIVATE_NETWORKS` | `false` |
