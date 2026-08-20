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
- `kroxylicious-filter-test-support` — pom wiring done (dependency added, old krpc execution
  + template removed) but **not migrated**, still on `kafka-clients` main dependency, still
  fails to compile (see below). This is expected — plan explicitly scopes its full migration
  to Phase B (§7 step 8) and says doing the pom wiring early is fine.
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
- `kroxylicious-runtime/src/test` — **mostly done, 5 files blocked on Phase B.** Ran the §9
  bulk script (40 files rewritten), then hand-fixed `EagerMetadataLearnerTest.java` (it built
  test fixtures via kafka-clients `AbstractRequest` subclasses just to get a `data()`/`apiKey()`/
  `version()` triple — replaced with direct vendored `ApiKeys`/`*RequestData` construction,
  no kafka-clients dependency needed). `mvn -o -pl kroxylicious-runtime test-compile` now fails
  in exactly 5 files, **all genuinely blocked on `kroxylicious-filter-test-support` being
  unmigrated** (confirmed: `mvn -o -pl kroxylicious-filter-test-support compile` fails with 82
  errors today — Maven falls back to a stale pre-migration jar from `~/.m2` for
  `kroxylicious-runtime`'s test-scope dependency, so these tests see kafka-clients-typed
  `ApiKeys`/`ApiMessage` where they need vendored ones):
  - `KafkaProxyExceptionMapperTest.java` — uses `RequestFactory.apiMessageFor(...)` and
    `io.kroxylicious.testing.filter.assertj.ResponseAssert`.
  - `RequestFilterResultBuilderTest.java` — uses `RequestFactory.apiMessageFor(...)`.
  - `KafkaProxyFrontendHandlerTest.java` — uses `RequestFactory`.
  - `BrokerAddressFilterTest.java` — uses `RequestResponseTestDef`/`ApiMessageTestDef` and a
    static import of `io.kroxylicious.kafka.common.message.json.KafkaApiMessageConverter`
    (doesn't exist as a test dependency of `kroxylicious-runtime` yet — would need adding once
    `filter-test-support` is migrated and re-exports it, or a direct test dep on
    `kroxylicious-kafka-message-json`).
  - `KafkaRequestDecoderTest.java` — uses `io.kroxylicious.testing.filter.record.RecordTestUtils`.

  **Do not try to hand-fix these 5 without migrating `kroxylicious-filter-test-support` first**
  (Phase B, §7 step 8) — the type mismatches are a direct symptom of that module's stale jar,
  not a runtime-side bug. Migrating `filter-test-support` will very likely fix most/all of these
  for free once `kroxylicious-runtime` builds against a freshly-installed, vendored jar
  (`mvn -o -pl kroxylicious-filter-test-support install -DskipTests` after migrating it).
- Everything else in Phase B (§7 steps 6–11: `kroxylicious-kafka-message-tools`, all
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

## Next step: migrate `kroxylicious-filter-test-support` (Phase B, §7 step 8)

Pulled forward ahead of the rest of Phase B because it's now the hard blocker for finishing
`kroxylicious-runtime`'s test-compile (5 files, see above) — not just its own module. Do this
next, then re-run `kroxylicious-runtime` test-compile before moving to the remaining Phase B
modules; most/all of those 5 files likely resolve for free once `filter-test-support` installs
a freshly-vendored jar.

Standard recipe: fresh grep for `org\.apache\.kafka\.common\.` in
`kroxylicious-filter-test-support/src/{main,test}` (don't trust old file counts), run the §9
bulk script over the whole module tree, compile, hand-fix what's left. Known gotchas specific
to this module:
- It already has pom wiring done this/last session (§2d): `kroxylicious-krpc-plugin`
  `generate-converters` execution removed, `KafkaApiMessageConverter.ftl` template removed,
  dependency on `kroxylicious-kafka-message-json` added. Don't redo that part.
- `RequestFactory`, `RecordTestUtils`, `ResponseAssert` (all in
  `io.kroxylicious.testing.filter.*`) are the classes `kroxylicious-runtime`'s blocked tests
  depend on — prioritize getting these three compiling/vendored correctly.
- After migrating, reinstall so downstream modules pick it up:
  `mvn -q -o -pl kroxylicious-filter-test-support install -DskipTests`, then re-run
  `mvn -q -o -pl kroxylicious-runtime test-compile` and check the 5-file blocker list shrinks.

## Then: rest of Phase B (§7 steps 6, 7, 9–11), untouched

Work through `kroxylicious-kafka-message-tools`, each `kroxylicious-filters/*` submodule,
`kroxylicious-integration-test-support`, `kroxylicious-runtime-plugins`,
`kroxylicious-microbenchmarks`, `kroxylicious-app`, `kroxylicious-integration-tests`,
`kroxylicious-krpc-plugin`. Same recipe each time: fresh grep for
`org\.apache\.kafka\.common\.` in that module's `src/main` (don't trust the plan's file
counts, see correction #2/#3 above), run the §9 script over the whole module tree, compile,
hand-fix what's left. Watch specifically for:
- The `FindCoordinatorRequest.CoordinatorType` gotcha (correction #1) in
  `kroxylicious-authorization` and `kroxylicious-entity-isolation`.
- The record→`record.internal` bridge (§8.7) in `kroxylicious-kafka-message-tools` and the
  record-touching filters (`kroxylicious-record-encryption`, `kroxylicious-authorization`'s
  `RequestDataUtils`).
- `OauthBearerValidationFilter.java` — check per §8.6 whether it needs the same
  `requests.*`-adjacent treatment as `KafkaProxyExceptionMapper`.
- The confinement check in PLAN2.md §11 step 9 once everything compiles.

## Housekeeping

- `/tmp/migrate.sh` on this machine has the §9 bulk-rewrite script, already used and working
  correctly (matches PLAN2.md §9 exactly). Re-create it from the plan if working on a
  different machine.
- The japicmp issue (correction #4) needs a maintainer decision before this can go through
  normal CI (`mvn install`/`verify` without `-Dquick`).
