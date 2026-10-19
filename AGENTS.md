# AGENTS.md — data-etl

Operating notes for coding agents working in this repository. Only non-obvious, decision-changing facts are listed;
human-oriented background lives in `README.md` and `docs/architecture/`.

## Build & test

- Full reactor build: `mvn clean install`
- Focused module (parent reactor must be built too, so keep `-am`):
  `mvn -pl task-management-service -am clean test`
- Full suite: `mvn test` — this is the verification gate every slice must pass before it is considered done.
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

- `docs/` and `e2e-logs/` are gitignored (`.gitignore` lines 68 and 46). Architecture docs, `.dot`, and `.svg` are
  **local-only and never committed** — do not `git add -f` them or expect them in a PR. Only code, `db/schema/*.sql`,
  `pom.xml`, and config under `config-server` are versioned.
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

## Toolchain

- Local dev host is Windows / PowerShell 7: `grep`, `tail`, and Unix `find` are unavailable — use `Select-String`,
  `Get-Content`, `Get-ChildItem`. The repo itself is cross-platform (each module ships both `.bat` and `.sh` launchers).
- Lombok is processed by Maven; IDE incremental compilation sometimes reports phantom "cannot find getter" errors on
  `@Data` classes. Trust `mvn compile`, not the IDE marker.
