# Design

## 1. System boundary

The input is a fully formed outbound request: URL, method, headers, and a JSON body. `notifyd` durably accepts it, schedules delivery, applies a retry policy, records attempts, and exposes dead-letter replay. The caller stops waiting once the request is stored.

This is the same delivery shape as notifying a CRM to update a Contact after a successful payment. The service remains vendor-neutral.

The first version deliberately does not:

- translate domain events into vendor schemas;
- implement one adapter per vendor;
- acquire or refresh vendor credentials;
- guarantee ordering or exactly-once effects;
- provide multi-region availability;
- authenticate callers.

Those concerns have different owners and failure modes. In production this service should sit behind the caller's existing internal authentication and authorization. Vendor credentials may be supplied in the stored headers, so the database needs normal platform encryption and access controls. Headers and bodies are never written to logs or returned by the status API.

## 2. Architecture and state

One Go process contains the HTTP API and a fixed worker pool. SQLite in WAL mode is both the durable store and the queue.

```text
pending ──claim──> delivering ──2xx──────────────> succeeded
   ^                    │
   │                    ├──retryable failure─────> retrying
   │                    └──permanent/exhausted───> dead
   │                                                   │
   └────────────────────manual replay──────────────────┘

delivering with an expired lease ──claim again──> delivering
```

The create handler inserts the notification before returning `202`. Workers atomically claim due rows by changing them to `delivering` and attaching a random lease token. They renew the lease while waiting for a per-host slot or performing I/O. Only the current token may finish an attempt. If the process stops, another worker can reclaim the row after lease expiry.

SQLite is enough for an MVP running as one process. It gives transactions, uniqueness for idempotency keys, crash recovery, and an inspectable dead-letter record without operating a broker. An in-memory queue would lose work after `202`; a custom file queue would recreate transaction and recovery logic. Kafka or Redis would add deployment and consistency work before there is throughput evidence that needs it.

## 3. Reliability and failure handling

Delivery is **at least once**. There is an unavoidable window where a vendor accepts a request and the process stops before recording success. The lease later expires and the request is sent again. The internal `Idempotency-Key` prevents duplicate enqueue calls; it cannot make the vendor's side effect idempotent. When a vendor supports its own idempotency token, the caller should include it in the outbound headers or body.

The result policy is small and explicit:

| Result | Action |
| --- | --- |
| `2xx` | Mark `succeeded` |
| timeout or network error | Retry |
| `408`, `429`, or `5xx` | Retry |
| other `4xx` | Mark `dead` |
| other final status | Mark `dead` |

Retries use capped exponential backoff with jitter. The defaults allow 12 attempts, starting around five seconds and capping at 15 minutes. Once the budget is exhausted, the row moves to `dead`. Its original request and attempt history stay in SQLite. An operator can inspect the status and replay it after correcting the external problem; replay starts a new retry budget without deleting previous attempt records.

Workers share a per-target-host concurrency cap. One slow or failing supplier therefore cannot consume every outbound connection or receive an unbounded burst.

## 4. SSRF and data handling

Arbitrary destination URLs make SSRF part of the service contract. HTTP and HTTPS are the only accepted schemes. Validation runs when accepting the request, immediately before delivery, on every redirect, and again inside the dialer. The dialer connects to the validated IP rather than resolving the hostname a second time in the transport. Environment proxies are disabled because a proxy would move DNS resolution outside this check.

Loopback, private, link-local, multicast, unspecified, and known metadata addresses are denied by default. `NOTIFYD_ALLOW_PRIVATE_NETWORKS=true` permits loopback and private addresses for the local demonstration, but link-local and metadata addresses remain denied.

Response bodies are discarded. Logs contain notification IDs, attempt numbers, outcome categories, and state only. They do not contain target URLs, headers, request bodies, or response bodies.

## 5. Technology choices and tradeoffs

I chose Go because this work is mostly timed I/O and bounded concurrency, and a single binary keeps it easy to run and test. The state machine and table design are not tied to Go and can be implemented in another language.

I kept four things out of the first version:

- Kafka: no measured volume or independent scaling need justifies operating it.
- exactly-once delivery: HTTP cannot close the post-acceptance crash window without vendor cooperation.
- vendor-specific adapters: callers already know the required URL, headers, and body; adapters duplicate business ownership here.
- a full Prometheus, OpenTelemetry, and dashboard stack: the MVP logs only IDs and outcome categories, and has a health endpoint and query API. Production telemetry should follow the deployment standard rather than be invented in this repository.

## 6. Evolution

The next step depends on the observed limit. If SQLite write contention becomes material, I would move the same schema to PostgreSQL and claim rows with `SKIP LOCKED`. If API and delivery load need independent scaling, I would split those processes and use a managed queue, retaining a durable request record and dead-letter policy. Higher supplier cardinality would add persistent rate limits and circuit breakers. Stronger security would replace raw secret headers with references to a secret store and add a destination allowlist per tenant.

These changes preserve the API and delivery states. They are migrations driven by measured load or security requirements, not prerequisites for the MVP.
