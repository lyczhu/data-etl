# AGENTS.md — data-etl

Operating notes for coding agents working in this repository. Only non-obvious, decision-changing facts are listed;
human-oriented background lives in `README.md` and `docs/architecture/`.

## Build & test

- Full reactor build: `mvn clean install`
- Focused module (parent reactor must be built too, so keep `-am`):
  `mvn -pl task-management-service -am clean test`
- Full suite: `mvn test` — this is the verification gate every slice must pass before it is considered done.
- Integration tests (`orchestrator-design.md` §12.1) live in `task-management-service/src/test/java/.../it/`, are tagged `it`, and are **excluded** from `mvn test`.
  Run them with `mvn -pl task-management-service -am test -Pit` (or `-Dtest=*IT` for only the ITs). They need
  `docker compose up -d postgres redis kafka`; when infra is absent the cases **skip, not fail** (so a green `mvn test`
  proves nothing about them — always check `Skipped: 0` in the `-Pit` output). Any Redis answering on `localhost:6379`
  works, since the ITs only use it for Redisson locks — if another local stack already holds that port, the repo's own
  `redis` container is unnecessary (compose failing to bind it is harmless for the ITs). They use a dedicated
  database `etl_it` (auto-created, schema applied from `db/schema/02|05|06`) and per-JVM Kafka topic names, so they never
  touch the `etl_system` database used by the e2e runbook.
- `FullChainE2EIT` boots the real `etl-worker` Spring context in the same test JVM (test-scope dependency on `etl-worker`);
  that is why its repackage goal publishes an `-exec` classifier jar and keeps the plain jar as the main artifact.
- Java target is **17** (`pom.xml` → `<java.version>` / `<maven.compiler.release>`). `README.md` "JDK 11+" is stale;
  trust the parent pom. Records and `switch` expressions are used in production code.
- Before repackaging, stop any JVM still holding `target/*.jar`, otherwise `spring-boot:repackage` silently produces a
  non-executable jar. On Windows, find the PID by listening port:
  `Get-NetTCPConnection -LocalPort 8084 -State Listen`

## Local run (dependency order matters)

- Infra: `docker compose up -d postgres redis kafka` — PostgreSQL `5433`, Redis `6379`, Kafka `9092`.
- Start services strictly in this order or they fail at boot:
  `registry` (8761) → `config-server` (8050) → `data-source` (8081), `task-management` (8082), `data-extraction` (8083),
  `data-transformation` (8084), `data-storage` (8085); `gateway-service` (8080).
- The pipeline/business services read Kafka / Redis / datasource / topic config **only from config-server**; their local
  `application.yml` holds just port + actuator. If config-server is not up first, they cannot start.
- Launch each service through its `start-service.bat` / `start-service.sh` — those read `service.conf` (JVM heap, etc.).
  `java -jar ...` started by hand does **not** read `service.conf`.

## Debugging route

- Correlate by **`taskId`** in logs (`[trace:,span:,task:<taskId>]`). Sleuth traceId/spanId are NOT propagated across the
  hand-written custom Kafka listener factories, so do not assume an end-to-end trace chain exists.
- Per-service metrics: `http://localhost:8085/actuator/prometheus` (filter `^etl_`). Full runbook and prior fixes:
  `docs/architecture/e2e-runbook.md`.

## Repository boundaries

- `e2e-logs/` is gitignored (run logs, local-only). `docs/` **is** versioned since 2026-10 — architecture docs, `.dot`,
  and `.svg` are meant to land in PRs together with the code they describe.
- Diagram rendering: Graphviz is not on PATH. Regenerate the SVG whenever a `.dot` changes:
  `& 'D:\Programs\Graphviz\bin\dot.exe' -Tsvg target-lifecycle-states.dot -o target-lifecycle-states.svg`
- A service that ships its own `logback-spring.xml` **ignores** `logging.pattern.*` from `application.yml` / config-repo;
  change log format by editing `logback-spring.xml`, not the properties.

## Domain invariants (do not violate)

- ETL status state machine (`task-management-service`, Phase 1): `OrchestratorService.transition(runId, from, to, ctx)` is
  the **only** writer of `etl_task_run.status`. Never scatter `entity.setStatus("literal")`. Legal edges are enumerated in
  `AllowedTransitions` (**exactly 32**, locked by `AllowedTransitionsTest`); a new/changed transition must be applied to
  `AllowedTransitions`, the §5.1 table in `orchestrator-design.md`, and `target-lifecycle-states.dot` (then re-render SVG)
  together.
- `EtlTask.status` stays a `String` (front-end + `findByStatus` contract) and is a read-only projection of the current
  run's status via `refreshDerivedStatus` (INV-6). Never write it from outside the transition path.
- Storage batch consumer (`data-storage-service`): a batch listener must **record-and-skip** unparseable messages (Poison
  Guard) and must never throw to retry the whole batch — that causes an infinite poison loop. Real DLT isolation is a
  later phase.
- Kafka listeners are **not** transactional. Any Spring Data `@Modifying` query they call therefore needs `@Transactional`
  on the repository method itself (`EtlTaskRunRepository.touchProgress`, `EtlTaskCheckpointRepository.touch`); without it
  the update throws `TransactionRequiredException`, which escapes the listener and re-triggers delivery forever.
- `attempt_no` restarts at 1 for every new run (§5.1 T1 / D-22). `ux_run_task_attempt` is a **partial** index
  (`WHERE is_current`) for exactly that reason — do not make `attempt_no` task-monotonic to "fix" a duplicate-key error.
- `etl_task_run` counter columns (`shard_succeeded/failed/skipped`, `rows_*`) are an **absolute-value mirror** of the
  checkpoint aggregate (`RunCounterProjector`), never an increment — at-least-once delivery projects the same aggregate
  repeatedly. They deliberately exclude `shard_total`, which is the T2 *plan* count and the basis of G-4/convergence;
  projecting over it would zero it while no shards exist. Without the mirror those columns stay 0 and §12.2 check 2
  passes vacuously (`rows_out + rows_dropped + rows_deduped <> rows_in` is false when all four are 0).
- G-4 / INV-4 balance is **four-term**: `rows_out + rows_filtered + rows_failed + rows_deduped = rows_in`. A crash after
  the target commit but before the checkpoint write replays the same rows, so `rows_in` and `rows_deduped` both grow by
  the same amount. Do not exclude `rows_deduped` — that turns a correct idempotent replay into `NEEDS_ATTENTION`.
- Checkpoint fencing is **dual-key**: every worker write must match `shard_attempt` *and* `lease_owner` (see
  `CheckpointWriter`). `tryAcquireLease` advances `shard_attempt` with `GREATEST(...)`; `/resume` and `/retry` clear leases
  via the `__reclaimed__` sentinel. Dropping either condition lets a late write from a superseded worker overwrite the new
  attempt (Gate 2).
- Global dispatch permits come from the **database**: `COUNT(etl_task_checkpoint.status IN ('ASSIGNED','RUNNING'))` against
  `etl.dispatch.max-concurrent-shards`, read fresh on every call (`TokenBucket`). A JVM `Semaphore` is not a global
  authority and must not be reintroduced. `DispatchService.dispatchOne` runs under
  `pg_advisory_xact_lock(hashtext('etl:dispatch:permits'))` so multiple orchestrators cannot overshoot the same free slot.
- Target-table DDL belongs to **V-3** (`TargetTableManager`): one-time create plus `idempotency_key` UNIQUE under a target-DB
  advisory lock. Workers must not call `ensureTargetTable` per shard; a failed V-3 becomes `FAILED(TARGET_UNWRITABLE)`.
- `etl.routing.new-worker` stays **false** until all Gate 2 evidence exists (multi-JVM orchestrator, real alert firing,
  rollback drill). Phase 1B code being green is not sufficient to flip it.

## Toolchain

- Local dev host is Windows / PowerShell 7: `grep`, `tail`, and Unix `find` are unavailable — use `Select-String`,
  `Get-Content`, `Get-ChildItem`. The repo itself is cross-platform (each module ships both `.bat` and `.sh` launchers).
- Lombok is processed by Maven; IDE incremental compilation sometimes reports phantom "cannot find getter" errors on
  `@Data` classes. Trust `mvn compile`, not the IDE marker.
