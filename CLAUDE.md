# Kroxylicious - Claude Context

This is Kroxylicious, a Layer 7 proxy for the Kafka protocol. It's written using Netty and uses the same `*RequestData` and `*ResponseData` classes which Apache Kafka does to represent protocol messages.

## Primary Documentation

See [README.md](README.md) for comprehensive project documentation including:
- Architecture and design (Netty pipeline, filter system, protocol contracts)
- User personas
- Security model
- Deployment considerations
- Configuration
- API vs implementation distinctions
- Testing guidance

## Build and Development

See [DEV_GUIDE.md](DEV_GUIDE.md) for:
- Build commands and Maven properties/profiles
- Maven profiles: `qa` (quality checks), `ci`, `dist`, `quick`, `systemtest`, `errorprone-jdk-compatible`
- IDE setup and debugging
- Running integration/system tests locally
- Container image building and pushing
- Continuous integration workflows

### Running specific tests

- **Unit tests** (surefire): `mvn test -pl <module> -Dtest="TestClassA,TestClassB"`
- **Integration tests** (failsafe): `mvn verify -pl <module> -Dit.test="SomeIT,OtherIT"`

IMPORTANT: Integration test classes (named `*IT`) are run by the failsafe plugin, NOT surefire. You **must** use `-Dit.test` (not `-Dtest`) to select them. Using `-Dtest` will silently run zero integration tests while still running all ITs via failsafe.

IMPORTANT: If you use `verify` with `-pl`, `-am` and `-Dit.test` at the same time then you must also add `-Dfailsafe.failIfNoSpecifiedTests=false`, otherwise a dependency's integration tests will most likely fail owing to the absence of the named test in that module.

IMPORTANT: Our builds and tests can take a long time. To minimize time spent re-testing **do not** using shell pipelines which discard output like `mvn ... | head -20` or `mvn ... | grep ... | head -20`. Instead, use `mvn test ... | tee /tmp/claude-kroxylicious/test.log | head -20` or `mvn test ... | tee /tmp/claude-kroxylicious/test.log | grep ... | head -20`. This way, if the test fails, you have already captured all the test output for analysis so you don't need to re-exec the slow `mvn` command.

## Coding Rules

When writing code, follow these prescriptive rules:
- [API Changes](.claude/rules/api-changes.md) - When proposals are required
- [Performance](.claude/rules/performance.md) - Performance-sensitive patterns
- [Security](.claude/rules/security-patterns.md) - Security coding requirements
- [Logging](.claude/rules/logging.md) - Logging conventions
- [Documentation](.claude/rules/documentation-requirements.md) - When to write docs
- [Pull Requests](.claude/rules/pull-requests.md) - PR checklist requirements

## Commit Conventions

When creating commits, follow these conventions:

**Commit Message Format:**
- Use Conventional Commits: `<type>(<scope>): <description>`
- Types: `feat`, `fix`, `docs`, `build`, `chore`, `refactor`, `perf`, `test`, `ci`
- Keep subject line under 72 characters

**AI Disclosure Requirement:**
When AI assists with code changes, add an `Assisted-by:` trailer:
- Format: `Assisted-by: Claude <model-name> <noreply@anthropic.com>`
- Placement: After commit body, before `Signed-off-by:` trailer
- Model name examples: "Claude Sonnet 4.5", "Claude Opus 4.6"

**DCO Signoff:**
All commits require `Signed-off-by:` trailer (auto-added by git hook).

**IMPORTANT for Claude Code:** Use `Assisted-by:` NOT `Co-Authored-By:`. Format:

```
<type>(<scope>): <subject>

<body>

Assisted-by: Claude <model-name> <noreply@anthropic.com>
Signed-off-by: <name> <email>
```

**Example:**
```
feat(filters): add request throttling filter

Implements configurable rate limiting at the filter level with
per-client quotas and burst handling.

Assisted-by: Claude Sonnet 4.5 <noreply@anthropic.com>
Signed-off-by: Jane Developer <jane@example.com>
```

## Module-Specific Context

See module README.md files for detailed context:
- [kroxylicious-api/README.md](kroxylicious-api/README.md) - Filter API
- [kroxylicious-runtime/README.md](kroxylicious-runtime/README.md) - Proxy runtime
- [kroxylicious-filters/README.md](kroxylicious-filters/README.md) - End-user filters
- [kroxylicious-kms/README.md](kroxylicious-kms/README.md) - KMS API
- [kroxylicious-authorizer-api/README.md](kroxylicious-authorizer-api/README.md) - Authoriser API
- [kroxylicious-kubernetes/kroxylicious-kubernetes-api/README.md](kroxylicious-kubernetes/kroxylicious-kubernetes-api/README.md) - Kubernetes CRDs documentation)
- [kroxylicious-kubernetes/kroxylicious-operator/README.md](kroxylicious-kubernetes/kroxylicious-operator/README.md) - Kubernetes operator
- [kroxylicious-kubernetes/kroxylicious-admission/README.md](kroxylicious-kubernetes/kroxylicious-admission/README.md) - Kubernetes mutating admission webhook for sidecar injection
- [kroxylicious-docs/README.md](kroxylicious-docs/README.md) - Documentation

## Kubernetes

There is a Kubernetes operator for the proxy in `kroxylicious-operator`. The end user defines a number of custom resources, such as `KafkaProxy`, `KafkaProxyIngress`, and `KafkaProtocolFilter`, and the operator observes ("reconciles") these and creates/updates a Kubernetes `Deployment` to run a number of proxy instances as containers within `Pods`.

## Kafka Protocol and Version Negotiation

The Kafka wire protocol is versioned per API key. Each request type (PRODUCE, FETCH, METADATA, etc.) has an independent version range. The authoritative schema for each request/response lives in the Apache Kafka source tree at `clients/src/main/resources/common/message/<ApiKey>Request.json`. These JSON IDL files define which fields exist at which versions — a field with `"versions": "0-12"` is absent from the wire format at v13+.

**Version negotiation** works as follows:
1. The client sends `API_VERSIONS` to discover the server's supported version ranges.
2. The proxy's `ApiVersionsIntersectFilter` intersects the backend's ranges with the proxy's own maximums, and returns this intersection to the client.
3. The client then uses the highest mutually-supported version for each API key.
4. `ApiVersionsServiceImpl` accepts an override map (`Map<ApiKeys, Short>`) to cap specific API keys below their natural maximum.

**Key protocol evolution to be aware of:**
- **PRODUCE v13** (KIP-516): Replaces `Name` (topic name string) with `TopicId` (UUID). Topic IDs are cluster-specific — the same topic on two independent clusters has different UUIDs. This means any feature that routes PRODUCE requests between independent clusters must cap the advertised PRODUCE version at v12 to force name-based addressing.
- The pattern for capping versions is demonstrated by `MultiTenantFilter`, which uses `ApiVersionsResponseTransformers.limitMaxVersionForApiKeys(Map.of(ApiKeys.PRODUCE, (short) 12))` on the `API_VERSIONS` response.
- **FETCH v7+** (KIP-227): Introduces incremental fetch sessions. The client and server negotiate a session (sessionId/epoch) so that subsequent fetches only carry changed partitions, reducing wire overhead. Key session semantics: `sessionId=0, epoch=-1` means sessionless (full fetch); `sessionId=0, epoch=0` creates a new session; `sessionId=N, epoch=M` (M>0) is an incremental fetch; `sessionId=N, epoch=-1` closes the session. The `TopicPartitionRouter` manages bidirectional sessions via `FetchSessionManager`: it acts as a session **server** for the downstream client and as a session **client** for each upstream backend route. These two sides are independent — a pre-v7 client (no session support) can still benefit from server-side sessions, and vice versa. When a backend sends an incremental response the proxy reconstructs a full response before merging across routes.

**Implications for routers:**
- A `Router` that sends requests to multiple independent backend clusters must intercept `API_VERSIONS` (make it dynamically routed rather than static) and cap any API key whose wire format uses cluster-specific identifiers (e.g. topic IDs).
- The proxy preserves the client's original `apiVersion` when creating `DecodedRequestFrame` for the backend (`RoutingContextImpl` line 81). The backend encoder serialises at that version. There is no automatic version translation between client and backend.
- The Kafka producer's default configuration (`enable.idempotence=true` in Kafka 3.0+) can cause retries that appear as additional requests to the proxy. Tests that count requests per API key should disable idempotence and retries, and set `batch.size=0` to ensure one PRODUCE request per record.
- Pre-v7 FETCH requests cannot carry session fields on the wire, so server-side sessions are structurally impossible for pre-v7 clients regardless of proxy logic.
- **Nested routers** (a route targeting another router) are supported. Each level has its own `NodeIdMapping`. Virtual node IDs are translated between levels: `outerVirtual = outerMapping.toVirtual(outerRoute, nestedVirtual)`. This composition guarantees no collisions with the outer level's IDs. The CCSM address resolver sees only outermost virtual IDs; each router level sees its own virtual space in responses. The translation happens in the nested level's forwarder callbacks and `MetadataAddressCacher`.
- **METADATA must be statically routed** when a router sends PRODUCE to multiple independent clusters. `BrokerAddressFilter` only translates addresses for responses on the static/filter path, not the dynamic `PendingResponse` path. Routers that need client-addressable brokers from METADATA should declare METADATA as a static route to a fixed default route.
- **Per-route filters** use Netty local transport (`LocalChannel`/`LocalServerChannel`) with a dedicated `DefaultEventLoopGroup` because platform I/O event loops (epoll, kqueue) do not support local channels. Filter handlers are bound to the client connection's event loop via `pipeline.addLast(clientEventLoop, handler)` so that `ctx.executor()` returns the correct event loop and deferred filter work completes on the connection's thread. Pipeline creation is asynchronous (completes on the next event loop tick); callers must use `thenAcceptAsync(callback, clientEventLoop)` when chaining on the creation stage.
- **`ResponseSequencer` and nested routers**: nested `RouterContextImpl` instances must not allocate sequence numbers from the shared `ResponseSequencer` — nested responses propagate back through `dispatchToNestedRouter`'s `thenCompose` chain, not `submitResponse`. A leaked sequence number creates a permanent gap that blocks all subsequent responses on the connection. The sequencer includes stall detection (20-second warning).

## End User Documentation

End user documentation lives in `kroxylicious-docs`.

