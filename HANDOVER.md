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
- `kroxylicious-runtime/src/main` — **bulk mechanical rewrite applied and verified narrow.**
  Ran the §9 script across the whole tree (49 files rewritten). Result: **every file compiles
  except exactly 3**, all belonging to the single known hot spot (§8.6):
  - `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/KafkaProxyExceptionMapper.java`
  - `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/filter/RequestFilterResultBuilderImpl.java`
  - `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/routing/RouterDispatchHandler.java`

  This is much better than PLAN2.md's framing suggested — §8.1 (`ByteBufAccessor`/
  `ByteBufAccessorImpl`) and §8.2/§8.3 (all three FreeMarker templates: `BodyDecoder.ftl`,
  `FilterInvoker.ftl`, `SpecificFilterArrayInvoker.ftl`) were **already vendored on `main`**
  before this branch was created (landed by earlier unrelated commits, e.g. `f5b01e647`).
  Do not redo that work.
- `kroxylicious-runtime/src/test` — **not touched yet.** ~40 files still reference
  `org.apache.kafka.common.*` outside the vendored tree. Not attempted this session.
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

## Exact next step: finish `KafkaProxyExceptionMapper.java` (§8.6)

This is the only remaining broken file in `kroxylicious-runtime/src/main`. A backup of the
pre-migration original is at `/tmp/KafkaProxyExceptionMapper.java.orig` (only valid on this
machine/session — copy it into the repo somewhere durable if you need it, e.g. `git show
HEAD:kroxylicious-runtime/.../KafkaProxyExceptionMapper.java` after the commit this session
made, since the committed version is the *broken*, half-migrated state — imports rewritten,
body not).

Verified API shapes (checked against Kafka 4.3.0 source at
`/home/robeyoun/development/upstream/kafka`) to build the byte-round-trip adapter:

- Vendored `io.kroxylicious.kafka.common.protocol.MessageUtil.toByteBufferAccessor(Message
  message, short version)` returns a **vendored** `ByteBufferAccessor` with a `.buffer()`
  getter (already flipped, ready to read). Use this to serialize the vendored request body.
- kafka-clients `org.apache.kafka.common.protocol.ByteBufferAccessor` implements both
  `Readable` and `Writable` — wrap the serialized `ByteBuffer` in one of these to feed
  `AbstractRequest.parseRequest`.
- `org.apache.kafka.common.requests.AbstractRequest.parseRequest(ApiKeys apiKey, short
  apiVersion, Readable readable)` returns `RequestAndSize` (`.request` is the built
  `AbstractRequest`).
- Error translation: **do not** pass a vendored exception directly into
  `AbstractRequest#getErrorResponse(Throwable)` — its internal `Errors.forException`
  does a `getClass()`-keyed lookup against kafka-clients' own exception hierarchy and won't
  recognize a vendored exception instance, silently degrading everything to
  `UNKNOWN_SERVER_ERROR`. Instead: vendored `Errors.forException(error)` → `.code()` →
  `org.apache.kafka.common.protocol.Errors.forCode(code)` → `.exception()` (a fresh
  kafka-clients exception instance for that code) → pass that into `getErrorResponse`.
- `AbstractResponse` has no direct `serialize(short)`; use `.data()` (returns kafka-clients
  `ApiMessage`) then kafka-clients `MessageUtil.toByteBufferAccessor(data, apiVersion)`
  (same shape as the vendored one) to get bytes back out.
- Decode the response bytes back into a **vendored** `ApiMessage` via the generated
  `io.kroxylicious.proxy.internal.codec.BodyDecoder.decodeResponse(ApiKeys apiKey, short
  apiVersion, ByteBufAccessor accessor)` — note this takes the *project's*
  `io.kroxylicious.proxy.frame.ByteBufAccessor` interface (Netty-based, more methods than
  vendored `Readable`/`Writable`), not the vendored `ByteBufferAccessor` directly. Reuse the
  existing `io.kroxylicious.proxy.internal.codec.ByteBufAccessorImpl` wrapping
  `io.netty.buffer.Unpooled.wrappedBuffer(responseBytes)` for this final decode step.

Skeleton (fill in, don't blindly paste — verify field/method names against the actual
generated `BodyDecoder` and vendored `MessageUtil`/`Errors`/`ApiKeys` before compiling):

```java
private static ApiMessage buildErrorResponse(ApiKeys vendoredApiKey, ApiMessage reqBody, short apiVersion, Throwable error) {
    var kafkaApiKey = org.apache.kafka.common.protocol.ApiKeys.forId(vendoredApiKey.id);
    ByteBuffer requestBytes = io.kroxylicious.kafka.common.protocol.MessageUtil.toByteBufferAccessor(reqBody, apiVersion).buffer();
    var ras = AbstractRequest.parseRequest(kafkaApiKey, apiVersion,
            new org.apache.kafka.common.protocol.ByteBufferAccessor(requestBytes));
    short code = Errors.forException(error).code(); // vendored Errors
    var kafkaException = org.apache.kafka.common.protocol.Errors.forCode(code).exception();
    AbstractResponse kafkaResponse = ras.request.getErrorResponse(kafkaException);
    ByteBuffer responseBytes = org.apache.kafka.common.protocol.MessageUtil
            .toByteBufferAccessor(kafkaResponse.data(), apiVersion).buffer();
    return BodyDecoder.decodeResponse(vendoredApiKey, apiVersion,
            new ByteBufAccessorImpl(io.netty.buffer.Unpooled.wrappedBuffer(responseBytes)));
}
```

This one method replaces the entire ~200-line `switch` in the current file (the whole
`private static AbstractRequest errorResponse(ApiKeys apiKey, ApiMessage reqBody, short
apiVersion)` method and `toLeaveGroupBuilder` helper go away — they exist only to build a
`kafka-clients` request object per-API-key so `.getErrorResponse()` can be called on it; the
round-trip through `AbstractRequest.parseRequest` does that generically for every API key).

Also update:
- `errorResponseForMessage(...)`: change return type `AbstractResponse` → vendored
  `ApiMessage`, and its body to call `buildErrorResponse(...)` instead of
  `errorResponse(ApiKeys.forId(apiKey), message, apiVersion).getErrorResponse(apiException)`.
- `errorResponse(DecodedRequestFrame<?>, Throwable)` (package-private,
  `@VisibleForTesting`): change return type `AbstractResponse` → vendored `ApiMessage`,
  body calls `buildErrorResponse(...)` and returns it directly (no more `.getErrorResponse()`
  call at this level — that's now inside `buildErrorResponse`).
- `errorResponseMessage(...)`: simplifies to `return errorResponse(frame, error);` (no more
  `.data()` — `errorResponse` now returns the data directly).
- `newListConfigResourcesV0ErrorResponse`: change to build and return a **vendored**
  `ListConfigResourcesResponseData` directly (no `ListConfigResourcesResponse` wrapper needed
  since we're returning `ApiMessage`, not `AbstractResponse`) — this special case does not
  need the round-trip at all, it's already a straight vendored-data build in the current code,
  just drop the `org.apache.kafka.common.requests.ListConfigResourcesResponse` wrapper.
- Drop now-unused imports: all ~90 `org.apache.kafka.common.requests.*Request` imports, `acl.*`,
  `resource.*`, `security.auth.KafkaPrincipal`, `IsolationLevel`, `ElectionType` (all were only
  used inside the deleted switch). Keep `org.apache.kafka.common.requests.AbstractRequest`,
  `AbstractResponse` (used internally now), add
  `org.apache.kafka.common.protocol.ApiKeys`/`Errors`/`ByteBufferAccessor` as
  fully-qualified or aliased imports (there will be a name collision with the vendored
  `ApiKeys`/`Errors`/`ByteBufferAccessor` already imported — use fully-qualified references
  for the kafka-clients side, as sketched above, rather than importing both under the same
  simple name).

Then fix the two call sites (both currently do `KafkaProxyExceptionMapper
.errorResponseForMessage(...)` then `.data()` — once the return type is the vendored
`ApiMessage`, just drop the `.data()` and retype the local variable, and drop each file's now-
unused `import org.apache.kafka.common.requests.AbstractResponse;`):
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/filter/RequestFilterResultBuilderImpl.java`
  (currently line ~67, method `errorResponse(RequestHeaderData, ApiMessage, ApiException)`)
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/routing/RouterDispatchHandler.java`
  (currently line ~369, the `RespondWithError` switch case)

`KafkaProxyFrontendHandler.java:456` calls `errorResponseMessage(...)` and already expects
`ApiMessage` back — no change needed there, it was already correct against the target shape.

After that, recompile:
```bash
mvn -q -o -pl kroxylicious-runtime compile > /tmp/krox-mig.log 2>&1; echo "EXIT=$?"
grep -E 'ERROR' /tmp/krox-mig.log | head -60
```
Expect **zero** errors in `src/main` at that point (verified this session that nothing else
in the module is broken). Then move to `kroxylicious-runtime/src/test` (~40 files, not yet
touched — run the §9 bulk script first, then hand-fix whatever's left, same pattern as
`kroxylicious-api`).

## Then: Phase B (§7 steps 6–11), untouched

Work through `kroxylicious-kafka-message-tools`, then each `kroxylicious-filters/*`
submodule, `kroxylicious-filter-test-support` (finish what was pom-wired this session),
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
