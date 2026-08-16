# AI use

I used Codex mostly as a second pair of eyes. It helped list failure cases, check the lease transitions, and review the redirect, DNS, and proxy handling around SSRF. It also helped with some routine Go/SQLite code and an initial integration test. I read through and simplified those changes before running the tests locally.

I did not follow its suggestions to use Kafka or Redis, promise exactly-once delivery, add one adapter per vendor, or build a full Prometheus and OpenTelemetry stack. For a single-process MVP, those choices either add infrastructure without a demonstrated need or claim guarantees that generic HTTP cannot provide.

I made the final calls to persist each request before returning `202` so accepted work survives a restart, and to use expiring token-bound leases so stuck deliveries can be reclaimed safely. Most `4xx` responses are permanent failures because repeating the same request will not fix them. The per-host concurrency cap stops one slow vendor from occupying every worker, and URLs, headers, and bodies stay out of logs because they may contain credentials or customer data.
