# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A teaching/demo project for the **Transactional Outbox Pattern** (Spring Boot 3.5, Java 17, Maven) using an e-commerce **Order** domain, built to accompany the presentation `Outbox Pattern.pdf` (30 pages, Turkish). The PDF is an export of a Slides artifact (https://claude.ai/artifact/Vzcsk2pNBDosDujEqneF91, private to the user); its code snippets are taken from this project, so if `OrderService`, `OutboxRelay`, `OrderEventsListener`, `schema.sql` or the Kafka settings in `application.yml` change, the matching slides go stale. The user asked for the deck to follow the original presentation's structure — don't add slides unasked. Code comments, log messages, README and UI text are in **Turkish** — keep new code consistent with that.

## Commands

Windows is the primary environment; use `mvnw.cmd` (or `./mvnw` in bash). No Maven install required; no Kafka/Docker required.

```powershell
.\mvnw.cmd spring-boot:run                                   # run app (embedded Kafka + in-memory H2) -> http://localhost:8080
.\mvnw.cmd test                                              # all tests
.\mvnw.cmd test -Dtest=OutboxFlowIntegrationTest#naiveYontemdeHayaletEventOlusur   # single test
.\mvnw.cmd clean test                                        # use after renaming/removing resources (stale copies in target/)
docker compose up -d; .\mvnw.cmd spring-boot:run "-Dspring-boot.run.profiles=docker"   # optional: real Kafka on 9092, Kafka UI on 8090
```

No linter/formatter is configured.

**PKIX certificate errors** when Maven downloads dependencies on this machine: set `$env:MAVEN_OPTS='-Djavax.net.ssl.trustStoreType=Windows-ROOT'` first.

**Port 8080 in use** usually means the user already has the app running — don't kill it without asking.

## Architecture

Single Spring Boot app simulating two services that communicate only via Kafka:

- **`order`** (producer side): `OrderService` writes `orders`/`order_items` **and** an `outbox` row in one `@Transactional` method via `OutboxWriter` (`Propagation.MANDATORY` — throws if called outside a transaction). It never touches Kafka. `NaiveOrderService` is the intentionally wrong dual-write version (publishes to Kafka inside the DB transaction) kept for comparison.
- **`outbox`**: `OutboxRelay` is a `@Scheduled` + `@SchedulerLock` (ShedLock) polling publisher. It runs one `TransactionTemplate` per batch: native `FOR UPDATE SKIP LOCKED` query → `KafkaEventPublisher.publish(...)` (sync, waits for ack) → `markProcessed()`. Topic is derived from `aggregate_type` (`Order` → `order-events`), key = `aggregate_id` (orderId), headers `eventId`/`eventType`. On a send failure it calls `registerFailure` and **stops the batch** (preserves ordering); after `outbox.relay.max-retries` the row becomes `FAILED` and stays there until requeued via `POST /api/outbox/{id}/retry` (no backoff — a known demo limitation documented in README). Also `OutboxCleanupJob` (one unbatched `DELETE` of `PROCESSED` rows older than `outbox.cleanup.retention`; never touches `PENDING`/`FAILED`) and `OutboxMetrics` (Micrometer gauges). There is no cleanup for the `inbox` table — if one is added, its retention must exceed the window in which duplicates can arrive (≈ Kafka retention).
- **`invoice`** (consumer side, "Fatura Servisi"): `OrderEventsListener` (`@KafkaListener`, group `invoice-service`, `@Transactional`) checks the `inbox` table by `eventId` header; if new, writes `invoices` + `inbox` in the same transaction (idempotent consumer). It deliberately does **not** import producer classes — it has its own `OrderCreatedMessage` DTO and never reads order tables. Keep that separation.
- **`demo`**: `DemoSwitches` (relay paused, broker down, crash-after-send-once) drive failure scenarios. `KafkaEventPublisher` checks `brokerDown`; `OutboxRelay` checks the other two. Exposed via `/api/demo/*` and the static dashboard `src/main/resources/static/index.html` (plain HTML/JS, polls REST endpoints every second, no build step).
- **`config`**: `KafkaConfig` (3-partition topic, `DefaultErrorHandler` → `.DLT`), `ShedLockConfig`, `EmbeddedKafkaConfig`.

Delivery guarantee is at-least-once by design: the "crash after send" scenario produces a duplicate that the inbox must swallow.

## Non-obvious constraints

- **Schema is owned by `src/main/resources/schema.sql`** (`ddl-auto: none`, `sql.init.mode: always`). Entity changes require matching DDL there. H2 is in-memory (`jdbc:h2:mem:orderdb`), reset on every start.
- **Embedded Kafka uses a random port** (the KRaft `EmbeddedKafkaKraftBroker` cannot use fixed ports). A `BeanPostProcessor` in `EmbeddedKafkaConfig` injects the broker address into `KafkaProperties`; the `bootstrap-servers` value in `application.yml` only matters for the `docker` profile. `spring-kafka-test` is intentionally in compile scope for this.
- **Turkish locale breaks Kafka** (`"classic".toUpperCase()` → `CLASSİC`). `main()` calls `Locale.setDefault(Locale.ENGLISH)` and surefire runs with `-Duser.language=en`. Don't remove either; any new entry point/JVM launch needs the same.
- **YAML keys containing dots** (Kafka client properties, logger names) must be written as `"[enable.idempotence]"` so Spring Boot doesn't split them.
- **Tests** (`OutboxFlowIntegrationTest`) share one `@SpringBootTest` context with embedded Kafka and a running relay; they use unique customer IDs, Awaitility, and reset `DemoSwitches` in `@AfterEach`. Rows from earlier tests remain in the DB, so assertions filter by the test's own orderId.
- Not a git repository.
