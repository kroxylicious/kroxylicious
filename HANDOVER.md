# Handover: PLAN2.md kafka-vendoring migration, in progress

Branch: `kafka-vendoring-phase-d` (created fresh off `main` at `0ff2dcf0b`, previous
attempt `kafka-vendoring-phase-c` was abandoned — do not merge from it).

This file is the continuation point for a new session. Read `PLAN2.md` first (the
governing plan), then this file (what actually happened vs. the plan, and exact next
steps). Delete this file once the migration is complete and merged.

## Current state: what compiles

- `kroxylicious-api` — **DONE, verified.** Compiles (`mvn -o -pl kroxylicious-api compile`
  and `test-compile`) with **zero** `org.apache.kafka.*` imports in `src/main` or `src/test`
  outside `io/kroxylicious/kafka/common/`. `kafka-clients` removed from the pom entirely
  (main scope). Vendored: full `errors.*` package (150 files), `ApiKeys`, `Errors`,
  `CoordinatorType` (see note below — not where the plan said), `TopicPartition`.
- `kroxylicious-kafka-message-json` — **DONE, new module created and building.** Generates
  190 `*DataJsonConverter` classes + `KafkaApiMessageConverter`
  (`io.kroxylicious.kafka.common.message.json.KafkaApiMessageConverter`). Wired into
  `kroxylicious-proxy-core/pom.xml` modules list and `kroxylicious-bom`. All import sites of
  the old `io.kroxylicious.testing.filter.requestresponsetestdef.KafkaApiMessageConverter`
  across the whole reactor (9 files, not the 3 the plan predicted — see below) rewritten to
  the new package.
- `kroxylicious-filter-test-support` — **DONE, verified.** `mvn -o -pl
  kroxylicious-filter-test-support compile` and `test-compile` both clean. `kafka-clients`
  correctly remains a dependency (this module keeps the coexistence model, unlike
  `kroxylicious-api`). Reinstalled into `~/.m2`
  (`mvn -o -pl kroxylicious-filter-test-support install -DskipTests`) so downstream modules
  pick up the vendored jar instead of a stale pre-migration one. Confinement check: only
  `ResponseAssert.java` still touches kafka-clients internals outside the vendored tree (see
  below — intentional, same pattern as `KafkaProxyExceptionMapper`).
- `kroxylicious-filters/kroxylicious-protocol-logger` — pom updated (main-scope dep swapped
  from `kroxylicious-filter-test-support` to `kroxylicious-kafka-message-json`, test-scope
  `kroxylicious-filter-test-support` dep added back for its test). `MessageFormatter.java`
  import fixed to new package. **Not fully migrated** — still uses kafka-clients `ApiKeys` at
  the call site (`apiKey.messageType`), which will now mismatch the vendored-typed converter.
  Expected, deferred to Phase B (§7 step 7).
- `kroxylicious-runtime/src/main` — **DONE, verified.** §8.6 (`KafkaProxyExceptionMapper`)
  finished this session using the byte-round-trip adapter described below (was the only
  remaining broken file). `mvn -o -pl kroxylicious-runtime compile` is clean. Confinement
  check passes: the only `src/main` file left referencing `org.apache.kafka.common.*` outside
  the vendored tree is `KafkaProxyExceptionMapper.java` itself (plus two dead Javadoc/comment
  mentions in `BareSaslRequest.java` and `KafkaMessageEncoder.java`, not real dependencies).
  One incidental fix needed: `BodyDecoder.ftl`'s `decodeResponse` method had to be made
  `public` (was package-private) — `KafkaProxyExceptionMapper` lives in
  `io.kroxylicious.proxy.internal`, a different package from generated `BodyDecoder`
  (`io.kroxylicious.proxy.internal.codec`), so it couldn't call the package-private method.
  `decodeRequest` was left package-private (only ever called from within the codec package).
  Regenerate after this template change: `rm -rf kroxylicious-runtime/target/generated-sources/krpc`.
- `kroxylicious-runtime/src/test` — **DONE, verified.** Ran the §9 bulk script (40 files
  rewritten), then hand-fixed two categories of fallout:
  - `EagerMetadataLearnerTest.java` built test fixtures via kafka-clients `AbstractRequest`
    subclasses just to get a `data()`/`apiKey()`/`version()` triple — replaced with direct
    vendored `ApiKeys`/`*RequestData` construction, no kafka-clients dependency needed.
  - `KafkaRequestDecoderTest.java` built raw wire bytes via kafka-clients `ProduceRequest`/
    `RequestHeader` (`.serializeWithHeader(...)`) mixed with vendored `ApiKeys`/`ProduceRequestData`
    — the two no longer type-check together (vendored and kafka-clients `*Data` are unrelated
    classes now). Rewritten to serialize purely with vendored types (`RequestHeaderData`/
    `ProduceRequestData` + vendored `ByteBufferAccessor`, mirroring the pattern already used by
    `AbstractCodecTest.serializeUsingKafkaApis`), dropping the kafka-clients dependency entirely.
  - `KafkaProxyExceptionMapperTest.java` updated to the new `ApiMessage`-returning
    `KafkaProxyExceptionMapper` API and `ResponseAssert`'s new `(actual, apiVersion)` signature
    (see below).
  All 5 files that were blocked on `kroxylicious-filter-test-support` (see prior handover
  revision) are now fixed — 3 resolved for free once `filter-test-support` was migrated and
  reinstalled (`RequestFilterResultBuilderTest.java`, `KafkaProxyFrontendHandlerTest.java`,
  `BrokerAddressFilterTest.java`), 2 needed the hand-fixes above.
  `mvn -o -pl kroxylicious-runtime test-compile` is clean.
- Everything else in Phase B (§7 steps 6, 7, 9–11: `kroxylicious-kafka-message-tools`, all
  `kroxylicious-filters/*` submodules, `kroxylicious-integration-test-support`,
  `kroxylicious-runtime-plugins`, `kroxylicious-microbenchmarks`, `kroxylicious-app`,
  `kroxylicious-integration-tests`, `kroxylicious-krpc-plugin`) — **not started.**

## Corrections to PLAN2.md found this session (real repo state wins over the plan text)

1. **§5 / §3a: `CoordinatorType` is not `org.apache.kafka.common.protocol.CoordinatorType`.**
   No such top-level class exists in Kafka 4.3.0. The real thing is a nested enum,
   `org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType` — inside a KEEP
   class (`requests.*` stays kafka-clients everywhere outside `kroxylicious-api`). Fix applied:
   extracted the nested enum's body verbatim into a new standalone
   `io.kroxylicious.kafka.common.protocol.CoordinatorType` (kroxylicious-api). This is safe —
   real usages (`kroxylicious-authorization`, `kroxylicious-entity-isolation`,
   `kroxylicious-integration-tests`) only ever use the enum's values/`forId`/`id()`, never a
   `FindCoordinatorRequest` reference alongside it. When Phase B reaches those filter modules,
   their imports of `org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType`
   need a **hand fix** to `io.kroxylicious.kafka.common.protocol.CoordinatorType` — the bulk
   §9 script's `SRC.protocol.CoordinatorType` rule will **not** catch this because the source
   FQN differs. Grep for `FindCoordinatorRequest.CoordinatorType` specifically when doing those
   modules.

2. **§4a's file audit was incomplete.** The plan claimed exactly 4 hand-written files in
   `kroxylicious-api/src/main` had a live `org.apache.kafka.*` dependency. Actual count: 13
   files (`FilterResultBuilder`, `FilterContext`, `TopLevelMetadataErrorException`,
   `ResponseFilterResultBuilder`, `FilterResult`, `TopologyService`,
   `TopicLevelMetadataErrorException`, `TopicNameMappingException`, `RouterResponse`,
   `RouterContext`, `package-info.java`, plus the original `RequestFilter`/`ResponseFilter`/
   `Router`/`RequestFilterResultBuilder`). All were standard §3a REWRITE cases (`ApiMessage`,
   `Uuid`, `RequestHeaderData`/`ResponseHeaderData`, `Errors`, `errors.ApiException`,
   `utils.ByteBufferOutputStream`) — no new KEEP-class surprises. Fixed by running the §9
   script over the whole `kroxylicious-api/src/main` tree in one pass instead of the plan's
   named 4-file list. **Lesson for the rest of the migration: don't trust the plan's per-module
   file lists at face value — always `grep -rlE "org\.apache\.kafka\.common\." --include=*.java
   <module>/src/main | grep -v '/io/kroxylicious/kafka/common/'` fresh for each module and
   run the bulk script over the whole tree, not a hand-picked subset.**

3. **§2d's "3 test files" for the `KafkaApiMessageConverter` package move was also
   incomplete.** Actual reference count across the reactor: 9 files (added
   `MultiTenantFilterTest.java`, `AuthzIT.java`, `KafkaDriver.java`,
   `BrokerAddressFilterTest.java` on top of the plan's `MockUpstream.java` ×2,
   `AuthorizationFilterTest.java`, `EntityIsolationFilterTest.java`). All fixed with a single
   reactor-wide `sed` pass on the fully-qualified name — see the "New module" section of the
   diff for the sed command if you need to repeat the pattern elsewhere.

4. **japicmp will fail `mvn install`/`verify` on `kroxylicious-api`.** Once `RequestFilter`/
   `ResponseFilter`'s public `onRequest`/`onResponse` parameter types change from
   `org.apache.kafka.common.protocol.ApiKeys`/`ApiMessage` to the vendored equivalents, the
   `japicmp-maven-plugin` (bound to `verify`, active by default via the `qa` profile,
   `-Dquick` skips it) reports it as a binary/source incompatibility against the 0.23.0
   reference version. This is real and unavoidable — vendoring these types into public filter
   API signatures **is** a breaking API change. PLAN2.md doesn't mention this. Options for
   whoever finishes this migration: bump `ApiCompatability.ReferenceVersion` and accept the
   major-version-zero break, or add explicit `<excludes>` entries (the pom already has a
   precedent block for the 0.19→0.24 four-arg/five-arg `onRequest`/`onResponse` rename, see
   `kroxylicious-api/pom.xml` around line 317). Not resolved this session — used `-Dquick` to
   skip it and keep moving. **This is a real decision the maintainers need to make, not a
   mechanical fix — flag it, don't just silently exclude it.**

5. All three `kroxylicious-runtime/src/main/templates/*.ftl` files were already fully vendored
   before this branch started (see "already vendored" note above) — §8.2/§8.3 of the plan are
   already satisfied on `main`. Don't redo.

6. **The §9 bulk script's "rewrite `header.*` wholesale" rule (§3a) is too blunt where a
   kafka-clients KEEP class (§6) constructs `Header`/`Headers` directly.**
   `kroxylicious-filter-test-support`'s `ConsumerRecordAssert.java` builds an AssertJ assertion
   over kafka-clients `org.apache.kafka.clients.consumer.ConsumerRecord` (admin/consumer client,
   KEEP, never vendored) — its `.headers()` returns kafka-clients
   `org.apache.kafka.common.header.Headers`, which the bulk script had blindly rewritten to the
   vendored type, breaking the bridge. Fixed by converting kafka-clients headers into vendored
   `RecordHeaders` at the assertion boundary (iterate and copy key/value pairs) rather than
   trying to type the whole class against kafka-clients. `ConsumerRecordAssertTest.java`
   similarly needed its `TimestampType`/`RecordHeaders` construction reverted to kafka-clients
   types (it builds a literal kafka-clients `ConsumerRecord`). **Lesson: any file touching
   `org.apache.kafka.clients.consumer.ConsumerRecord` (or other client-side KEEP classes with
   `Header`/`Headers` in their signature) needs hand review after the bulk script — grep
   `ConsumerRecord<` in a module before trusting a clean bulk-script pass.**

7. **`kroxylicious-filter-test-support`'s generated `KafkaApiAssertJConditions.ftl` template had
   dead code that broke once `Errors`/`ApiKeys` (and by extension the request wrapper classes)
   were vendored.** Each generated `*Condition` class had two branches: one matching a bare
   vendored `*Data` instance, and a dead legacy branch matching a kafka-clients
   `requests.*Request` wrapper and calling `.data()` on it — a holdover from before `*Data`
   values were ever passed around unwrapped. Since `frame.body()` is always the bare vendored
   `*Data` now, the wrapper-matching branch is unreachable dead code, and it also doesn't
   compile (`.data()` returns kafka-clients-typed `*Data`, conflicting with the vendored import
   of the same simple name). Fixed by deleting the wrapper-matching branch and its
   `org.apache.kafka.common.requests.*` import from the template entirely — see the template
   diff for the exact before/after.

8. **`ResponseAssert.hasErrorCount(...)` has no vendored equivalent to fall back on.**
   kafka-clients' `AbstractResponse.errorCounts()` is a hand-written abstract method overridden
   per concrete `*Response` subclass, walking each response's nested per-partition/per-topic
   error fields — it is **not** something the message-spec-driven generator produces for the
   vendored bare `*ResponseData` classes (checked: no `errorCounts()` method exists anywhere in
   generated output). Since this method has exactly one real caller in the whole reactor
   (`KafkaProxyExceptionMapperTest.java` — verified by grep), the fix was to retype
   `ResponseAssert<T extends ApiMessage>` (constructed with `(actual, apiVersion)` now, since
   `apiVersion` is needed to serialize) and reuse the exact byte-round-trip pattern from §8.6:
   serialize the vendored response, parse it into a kafka-clients `AbstractResponse` via
   `AbstractResponse.parseResponse(kafkaApiKey, readable, apiVersion)`, then call
   `errorCounts()` on that. This is why `ResponseAssert.java` is a second, intentional entry in
   the confinement-check exception list (§11 step 9) alongside `KafkaProxyExceptionMapper.java`
   — if a future module needs the same "does this vendored response have error X" check outside
   a test context, this pattern is the answer, not a fresh vendored `errorCounts()` generator
   feature (not worth building for one caller).

## §8.6 `KafkaProxyExceptionMapper.java` — DONE this session

Implemented the byte-round-trip adapter exactly as planned: `buildErrorResponse(ApiKeys
vendoredApiKey, ApiMessage reqBody, short apiVersion, Throwable error)` serializes the
vendored request body (`MessageUtil.toByteBufferAccessor`), parses it as a kafka-clients
`AbstractRequest` (`AbstractRequest.parseRequest`, translating `ApiKeys` via `.id`/`.forId`),
builds the kafka-clients error response (`getErrorResponse`, translating `Errors` via
`.code()`/`.forCode(code).exception()` — passing a vendored exception straight into
`getErrorResponse` silently degrades to `UNKNOWN_SERVER_ERROR`, must translate first),
serializes that, and decodes it back into a vendored `ApiMessage` via
`BodyDecoder.decodeResponse`. One incidental fix required: `BodyDecoder.ftl`'s
`decodeResponse` had to become `public` (see note above) since it's called cross-package.
The ~200-line per-API-key `switch` and `toLeaveGroupBuilder` helper are gone. Both call sites
(`RequestFilterResultBuilderImpl.java`, `RouterDispatchHandler.java`) updated to the new
`ApiMessage`-returning signature; `KafkaProxyFrontendHandler.java` needed no change (already
expected `ApiMessage`).

## `kroxylicious-kafka-message-tools` — DONE this session

Fully vendored, **and turns out fully kafka-clients-free** (main + test) — every import in this
module was a §3a/§3b REWRITE case (`message.*`, `protocol.ApiKeys`, `record.*` → `record.internal`
except `TimestampType`, `header.Header`, `utils.ByteBufferOutputStream`, `compress.Compression`),
so after the bulk script ran there were zero `org.apache.kafka.*` references left anywhere in the
module. This wasn't anticipated by PLAN2.md (§7 step 6 only asked for vendoring, not kafka-clients
removal), but it fell out naturally once `ApiKeys`/`Errors`/records were vendored — same situation
as `kroxylicious-api` in miniature. Changes beyond the mechanical rewrite:
- `pom.xml`: replaced the `kafka-clients` dependency (was `provided` scope) with a plain
  dependency on `kroxylicious-api` (where the vendored classes live). Verified with
  `dependency:tree` that `kafka-clients` no longer appears at all, even transitively.
  Description text updated to stop claiming a kafka-clients-only dependency.
- `etc/module-layering.xml` (the `ImportControl` checkstyle config that enforces this module's
  declared isolation from the rest of Kroxylicious): swapped `<allow pkg="org.apache.kafka"/>`
  for `<allow pkg="io.kroxylicious.kafka.common"/>`. Verified with
  `mvn -o -pl kroxylicious-kafka-message-tools checkstyle:check@layering` (the bound execution
  has no explicit phase, so plain `checkstyle:check` runs the wrong, default `sun_checks.xml`
  ruleset instead — use the `@layering` execution-id suffix to run the real one).
- `BatchAwareMemoryRecordsBuilderTest.java`'s `controlRecord()` helper called `.sizeOf()`/
  `.writeTo()` on `ControlRecordType.ABORT.recordKey()`, which used to return a kafka-clients
  `Struct`. The vendored `ControlRecordType` (copied from Kafka 4.3.0 source, per PLAN2.md §5)
  returns a plain `ByteBuffer` instead — Kafka's own API for this changed between whatever
  kafka-clients version this repo pins (`kafka.version=4.2.0` in the root pom) and 4.3.0. Fixed
  by reading the `ByteBuffer` into a `byte[]` directly instead of calling `Struct` methods on it.
  **This is a live version-skew signal worth flagging to maintainers**: the vendored source was
  copied from 4.3.0 while the repo's own kafka-clients dependency is pinned to 4.2.0 — most
  vendored classes are self-contained enough that this doesn't matter, but any future vendored
  class whose *shape* changed between 4.2.0 and 4.3.0 could surface the same kind of surprise.

## Next step: rest of Phase B (§7 steps 7, 9–11), untouched

`kroxylicious-api`, `kroxylicious-kafka-message-json`, `kroxylicious-runtime` (main + test),
`kroxylicious-filter-test-support` (main + test), and `kroxylicious-kafka-message-tools`
(main + test) are all DONE and verified this session. Everything else in Phase B has not been
started.

Work through each `kroxylicious-filters/*` submodule, `kroxylicious-integration-test-support`,
`kroxylicious-runtime-plugins`, `kroxylicious-microbenchmarks`, `kroxylicious-app`,
`kroxylicious-integration-tests`, `kroxylicious-krpc-plugin`. Same recipe each time: fresh grep
for `org\.apache\.kafka\.common\.` in that module's `src/main` (don't trust the plan's file
counts, see correction #2/#3 above), run the §9 script over the whole module tree, compile,
hand-fix what's left. Watch specifically for:
- The `FindCoordinatorRequest.CoordinatorType` gotcha (correction #1) in
  `kroxylicious-authorization` and `kroxylicious-entity-isolation`.
- The record→`record.internal` bridge (§8.7) in `kroxylicious-kafka-message-tools` and the
  record-touching filters (`kroxylicious-record-encryption`, `kroxylicious-authorization`'s
  `RequestDataUtils`).
- `OauthBearerValidationFilter.java` — check per §8.6 whether it needs the same
  `requests.*`-adjacent treatment as `KafkaProxyExceptionMapper`.
- The `ConsumerRecord<` / client-side `Header`/`Headers` gotcha (correction #6) — grep for
  `ConsumerRecord<` in each module before trusting a clean bulk-script pass; the record-encryption
  and integration-test-support modules are the most likely to hit this (they use kafka-clients
  producer/consumer APIs directly, unlike the wire-protocol path).
- The confinement check in PLAN2.md §11 step 9 once everything compiles.

## Housekeeping

- `/tmp/migrate.sh` on this machine has the §9 bulk-rewrite script, already used and working
  correctly (matches PLAN2.md §9 exactly). Re-create it from the plan if working on a
  different machine.
- The japicmp issue (correction #4) needs a maintainer decision before this can go through
  normal CI (`mvn install`/`verify` without `-Dquick`).
