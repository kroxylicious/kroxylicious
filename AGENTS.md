# Kroxylicious Proxy — AI Agent Guidelines

This file provides repository-specific guidance for AI coding tools working on the
Kroxylicious proxy. It supplements the [organisation-level AGENTS.md](https://github.com/kroxylicious/.github/blob/main/AGENTS.md);
refer to that file first for contribution process, commit discipline, and PR standards.

## Build System

Kroxylicious uses Java 21 and Apache Maven with ~30 Maven modules.

```bash
# Full build with all tests
mvn clean verify

# Fast build, no tests (useful for compilation checks)
mvn clean package -Dquick

# Build distribution artifacts (required to run the proxy locally)
mvn clean package -Pdist -Dquick

# Code quality checks (Checkstyle, SpotBugs, formatting)
mvn -Pqa clean verify

# Format code and sort imports (run before committing)
mvn process-sources

# Add missing Apache 2.0 licence headers
mvn org.commonjava.maven.plugins:directory-maven-plugin:highest-basedir@resolve-rootdir license:format
```

CI enforces formatting. Always run `mvn process-sources` before committing.

## Running Tests

```bash
# Skip specific test categories
mvn clean verify -DskipITs=true   # Skip integration tests
mvn clean verify -DskipUTs=true   # Skip unit tests
mvn clean verify -DskipKTs=true   # Skip container (Docker image) tests
mvn clean verify -DskipDTs=true   # Skip documentation tests

# Run a single test class
mvn test -Dtest=YourTestClass

# Integration test environment (default is CONTAINER; IN_VM is faster locally)
TEST_CLUSTER_EXECUTION_MODE=IN_VM mvn verify
TEST_CLUSTER_EXECUTION_MODE=CONTAINER mvn verify
```

### Test Categories

| Category | Flag | Description |
|----------|------|-------------|
| Unit (UT) | `-DskipUTs` | Component-level, no external dependencies |
| Integration (IT) | `-DskipITs` | Proxy + Kafka interaction |
| Container (KT) | `-DskipKTs` | Docker image validation |
| Documentation (DT) | `-DskipDTs` | Validates code snippets in docs |
| System (ST) | `-DskipSTs` | Full end-to-end on Kubernetes (requires a cluster) |

## Architecture Overview

Kroxylicious is a Kafka protocol proxy built with Netty. Requests and responses flow through a configurable filter chain:

```
Client → Network Handler → Filter Chain → Broker
         (Request)         F1 → F2 → F3
         (Response)           ← ← ←
```

### Key Interfaces

- **`FilterFactory<C, I>`** — filter lifecycle: `initialize(context, config)` validates config and returns init state; `createFilter(context, initData)` creates per-connection instances (must be thread-safe).
- **`RequestFilter` / `ResponseFilter`** — intercept Kafka RPCs; return a `FilterResult` (forward, drop, or close connection).
- **`FilterContext`** — runtime context: channel access, SASL callbacks, config injection.

### Threading Model

Filters run on Netty event loop threads. **Do not block.** Use `CompletableFuture` for any async work. `FilterFactory.initialize()` and `createFilter()` run on different threads.

### Module Layering

Checkstyle enforces module boundaries — the build will fail if they are violated:
- API modules may not depend on runtime internals.
- Filter modules may not depend on runtime implementations.
- No circular dependencies.

### Configuration

YAML-based, deserialised with Jackson. Durations use Go-style format: `30s`, `5m`, `1h30m` (not ISO-8601). Duration units: `d`, `h`, `m`, `s`, `ms`, `μs`/`us`, `ns`.

## Coding Conventions

### Code Quality (CI-enforced)

- **Checkstyle**: style and module layering — config in `etc/module-checkstyle.xml`
- **SpotBugs**: static analysis at max effort / low threshold
- **Formatter**: Eclipse formatter — config in `etc/eclipse-formatter-config.xml`
- **Import ordering**: `impsort-maven-plugin` (enforced by `mvn process-sources`)
- **Licence headers**: Apache 2.0 required on all source files
- **API compatibility**: JAPICMP validates public API stability

### Logging

Avoid logging in hot paths (per-message operations). Use the SLF4J fluent API to
conditionally attach stack traces only at DEBUG level:

```java
LOGGER.atWarn()
    .setCause(LOGGER.isDebugEnabled() ? exception : null)
    .log("Error message: {}. Enable DEBUG for stack trace", exception.getMessage());
```

### Design Principles

- Aim for single responsibility: high cohesion, loose coupling.
- Validate configuration in `FilterFactory.initialize()`, not constructors.
- Keep filter instances as stateless as possible.
- Register filters via SPI (`META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`).

## Preferred Development Workflow

For new features and bug fixes, the preferred approach is test-driven development.
Work in small increments: write a test before the code it exercises. Commit working,
tested changes — a test and the code that makes it pass can land in a single commit
if they are small enough. Refactoring should be its own commit. Target ~100–200 lines
per commit; treat 300 lines as a ceiling.

### Tests Constrain Behaviour — Not the Other Way Around

Tests exist to constrain behaviour, not to be adjusted to fit the implementation.
If code changes cause test failures, fix the code — not the test.

Tests should only be modified when they are genuinely wrong (incorrect assertion,
testing the wrong thing) or overly coupled to implementation detail. In those cases,
explain why the test is being changed in the commit message.

## Important Files and Locations

| Path | Purpose |
|------|---------|
| `etc/module-checkstyle.xml` | Module layering rules |
| `etc/eclipse-formatter-config.xml` | Code style |
| `proxy-config.yaml` | Example proxy configuration |
| `compose/kafka-compose.yaml` | Local Kafka for integration testing |
| `DEV_GUIDE.md` | Comprehensive development guide |
| `RELEASING.md` | Release process |
| `PERFORMANCE.md` | Benchmarking instructions |

## Key Dependencies

| Library | Role |
|---------|------|
| Netty | Async networking (epoll, KQueue, io_uring) |
| Apache Kafka | Protocol definitions and client libraries |
| Jackson | YAML/JSON configuration |
| Log4j | Logging with async appenders |
| Micrometer | Prometheus metrics |
| JUnit 5 | Testing framework |
| Testcontainers | Docker-based integration tests |
