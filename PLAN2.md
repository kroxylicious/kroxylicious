# PLAN2: Migrate core + filters to vendored `io.kroxylicious.kafka.*` (revised)

Supersedes `PLAN.md`. This is not a patch on top of it — it restarts the design from the
decisions that PLAN.md got wrong, discovered while executing it on `spike-that-shit` and
`kafka-vendoring-phase-c` (see `PROGRESS.md` for the raw session log this plan is distilled
from).

Same umbrella issues: **#4577** (proposal), **#4581** (migrate core), **#4582** (migrate
filters + test support).

**Goal, unchanged from PLAN.md:** get the whole project to **compile** against the vendored
Kafka protocol classes under `io.kroxylicious.kafka.*`. Out of scope: wire-level fidelity
tests, byte-for-byte verification, making existing unit/integration test suites pass. We need
`mvn -am compile` (and ideally `test-compile`) to succeed. Runtime correctness is a later phase.

**Three corrections from PLAN.md, decided up front this time:**

1. `ApiKeys`, `Errors`, and `CoordinatorType` are **vendored from the start**, not kept on
   `kafka-clients`. PLAN.md's original call to keep them was reversed mid-way through the
   Phase C session once `kroxylicious-api` itself needed to stop depending on `kafka-clients`
   (see §3). Reversing that decision after ~85% of the mechanical rewrite was done forced a
   second, uncoordinated sed pass across ~64 files and is the direct cause of Phase C's
   unfinished state. Deciding it here, before any file is touched, avoids paying that cost twice.
2. **`kroxylicious-api` loses its `kafka-clients` main-scope dependency entirely.** Every
   hand-written class in `kroxylicious-api` will compile against `io.kroxylicious.kafka.*`
   only. `kafka-clients` may remain a **test-scope** dependency if a test genuinely needs it
   (current audit found none — see §4).
3. **The `*DataJsonConverter` classes are generated into their own new module**,
   `kroxylicious-kafka-message-json`, not into `kroxylicious-api`. This corrects a misplacement
   PLAN.md explicitly deferred ("known limitation accepted for this prototype"). The new module
   also absorbs the `KafkaApiMessageConverter` registry class, which today is generated into
   `kroxylicious-filter-test-support` — a **test-support artifact that `kroxylicious-protocol-logger`
   (a main-scope filter) currently depends on**, which is itself a pre-existing wart this plan
   removes as a side effect.

This file is self-contained. It does not assume you have read PLAN.md or PROGRESS.md.

---

## 0. Background you must know before touching anything

Kroxylicious is a Layer-7 Kafka proxy. It decodes Kafka protocol frames into the Kafka
`*Data` message classes (e.g. `ProduceRequestData`), lets filters mutate them, and re-encodes.
Historically those `*Data` classes and their supporting protocol/record/util classes came from
the `kafka-clients` jar (`org.apache.kafka.common.*`). Proposal 116 (#4577) moves ownership of
those classes into Kroxylicious under `io.kroxylicious.kafka.*`.

**The single most important architectural fact, unchanged from PLAN.md:**

> `kafka-clients` (`org.apache.kafka.*`) **stays on the classpath for the reactor as a whole**.
> The vendored `io.kroxylicious.kafka.*` classes and the `org.apache.kafka.*` classes
> **coexist** in `kroxylicious-runtime` and the filters. The migration there is: switch imports
> to the vendored classes everywhere the code touches the `*Data` / codec world, and repair the
> few type-bridge points where a vendored value meets a `kafka-clients`-typed API.

**What's new in this plan:** `kroxylicious-api` is the one module where coexistence ends.
Its hand-written code (filter interfaces, `Router`, exception hierarchy) must compile with
zero `org.apache.kafka.*` imports in `src/main`. This is achievable because `kroxylicious-api`'s
hand-written surface is small and self-contained — the audit in §4 found exactly four files
with a live dependency, all trivially vendorable.

`kroxylicious-runtime` and the filters are **not** targeted for kafka-clients removal by this
plan — they keep the coexistence model from PLAN.md (KEEP list in §3, hot spots in §6).
Removing `kafka-clients` from those modules is future scope; don't attempt it here.

---

## 1. Prerequisite: (re)build the generator, then regenerate `kroxylicious-api` and the new JSON module

The `exec-maven-plugin` in `kroxylicious-api` (and, after §2 below, in
`kroxylicious-kafka-message-json`) resolves the generator as a **plugin dependency from the
local Maven repo** (`~/.m2`), *not* from source. If a stale generator jar is installed, the
generated `*Data` files will carry `org.apache.kafka.*` imports and nothing downstream will
compile.

Always run this first (and again any time you change the generator):

```bash
cd /home/robeyoun/development/upstream/kroxylicious
mvn -q -pl kroxylicious-kafka-message-generator install -DskipTests
rm -rf kroxylicious-api/target/generated-sources/kafka-messages
rm -rf kroxylicious-kafka-message-json/target/generated-sources/kafka-messages
mvn -q -pl kroxylicious-api,kroxylicious-kafka-message-json generate-sources
# sanity check: zero org.apache.kafka imports in EITHER generated tree
grep -rhoE '^import org\.apache\.kafka\.[^;]*;' \
  kroxylicious-api/target/generated-sources/kafka-messages/ \
  kroxylicious-kafka-message-json/target/generated-sources/kafka-messages/ | sort -u
#    -> expected output: (empty)
```

Use `-o` (offline) once dependencies are cached. Per repo convention, pipe noisy maven output
to a file: `mvn ... > /tmp/krox-build.log 2>&1` and grep the file for `ERROR`.

---

## 2. New module: `kroxylicious-kafka-message-json`

### 2a. Why it exists

The forked generator (`kroxylicious-kafka-message-generator`) has three independent output
modes selected by its `-m` argument: `MessageDataGenerator` (the `*Data` classes),
`ApiMessageTypeGenerator` (`ApiMessageType`), and `JsonConverterGenerator` (`*DataJsonConverter`
per message, ~190 files). PLAN.md ran all three into `kroxylicious-api` because that was the
path of least resistance, and left fixing it as future work.

The `*DataJsonConverter` classes are only needed by:
- test-support code that builds fixtures from JSON (`kroxylicious-filter-test-support`,
  and transitively the authorization / entity-isolation test suites),
- `kroxylicious-protocol-logger`'s `MessageFormatter`, which serializes live traffic to JSON
  for logging — this is **main-scope production code**, not a test.

Neither of those is `kroxylicious-api`. Generating them there just because it was convenient
means every consumer of `kroxylicious-api` drags Jackson-based JSON conversion machinery it
doesn't need. Splitting them into their own module fixes that and, as a side effect, removes
`kroxylicious-protocol-logger`'s current main-scope dependency on `kroxylicious-filter-test-support`
(a test-support artifact) — which is where its `KafkaApiMessageConverter` import comes from
today. That dependency direction is backwards for a production filter; this plan corrects it
by giving both consumers a proper shared module instead.

### 2b. What it contains

1. **`*DataJsonConverter` classes** (generated), one per `*Data` message, in package
   `io.kroxylicious.kafka.common.message` — the **same package name** as the `*Data` classes
   in `kroxylicious-api`, but a different jar. This is a deliberate split package. Accepted
   for now per explicit decision; do not try to avoid it by renaming the package (that would
   diverge from the upstream Kafka generator's package assumptions for no benefit).
2. **`KafkaApiMessageConverter`** (generated via the `kroxylicious-krpc-plugin`'s
   `KafkaApiMessageConverter.ftl` template, moved here from `kroxylicious-filter-test-support`),
   in package `io.kroxylicious.kafka.common.message.json`. This package is deliberately
   *not* `io.kroxylicious.kafka.common.message` — it signals "generator-support plumbing", not
   "protocol model", and keeps the split-package surface confined to the `*DataJsonConverter`
   classes alone.
3. No hand-written code. This module is pure generated output plus its own `pom.xml`.

Not a public API: no `japicmp`/`revapi` compatibility check is configured for this module
(matching the existing convention for `kroxylicious-kafka-message-tools`, which also carries
no compatibility gate). Treat any change to its generated shape as free — nothing depends on
binary compatibility here across releases.

### 2c. `pom.xml` shape

Modeled on `kroxylicious-api`'s existing generation setup plus `kroxylicious-filter-test-support`'s
existing `kroxylicious-krpc-plugin` usage (both patterns already exist in the reactor —
this module recombines them, it invents nothing new):

```xml
<dependencies>
    <dependency>
        <groupId>io.kroxylicious</groupId>
        <artifactId>kroxylicious-api</artifactId>
    </dependency>
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
    </dependency>
</dependencies>
<build>
    <plugins>
        <!-- unpack-message-specs: same maven-dependency-plugin execution pattern as
             kroxylicious-api/kroxylicious-filter-test-support. This references
             org.apache.kafka:kafka-clients as a build-time artifact to unpack its
             common/message/*.json specs -- it is NOT a project dependency, does not
             appear on any compile/runtime classpath, and does not conflict with this
             module (or kroxylicious-api's) freedom from kafka-clients at the dependency
             level. -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-dependency-plugin</artifactId>
            <executions>
                <execution>
                    <id>unpack-message-specs</id>
                    <phase>generate-sources</phase>
                </execution>
            </executions>
        </plugin>
        <!-- generate *DataJsonConverter classes only -- MessageDataGenerator and
             ApiMessageTypeGenerator are deliberately omitted; those classes already
             exist in kroxylicious-api and this module depends on that jar. -->
        <plugin>
            <groupId>org.codehaus.mojo</groupId>
            <artifactId>exec-maven-plugin</artifactId>
            <executions>
                <execution>
                    <id>generate-json-converters</id>
                    <phase>generate-sources</phase>
                    <goals><goal>java</goal></goals>
                    <configuration>
                        <mainClass>io.kroxylicious.kafka.message.MessageGenerator</mainClass>
                        <includePluginDependencies>true</includePluginDependencies>
                        <arguments>
                            <argument>-p</argument>
                            <argument>io.kroxylicious.kafka.common.message</argument>
                            <argument>-i</argument>
                            <argument>${project.build.directory}/message-specs/common/message</argument>
                            <argument>-o</argument>
                            <argument>${project.build.directory}/generated-sources/kafka-messages/io/kroxylicious/kafka/common/message</argument>
                            <argument>-m</argument>
                            <argument>JsonConverterGenerator</argument>
                        </arguments>
                    </configuration>
                </execution>
            </executions>
            <dependencies>
                <dependency>
                    <groupId>io.kroxylicious</groupId>
                    <artifactId>kroxylicious-kafka-message-generator</artifactId>
                    <version>${project.version}</version>
                </dependency>
            </dependencies>
        </plugin>
        <!-- generate KafkaApiMessageConverter -- krpc-plugin usage moved verbatim from
             kroxylicious-filter-test-support's pom.xml, only outputPackage changes. -->
        <plugin>
            <groupId>io.kroxylicious</groupId>
            <artifactId>kroxylicious-krpc-plugin</artifactId>
            <executions>
                <execution>
                    <id>generate-converters</id>
                    <goals><goal>generate-multi</goal></goals>
                    <phase>process-sources</phase>
                    <configuration>
                        <messageSpecDirectory>${project.build.directory}/message-specs/common/message</messageSpecDirectory>
                        <messageSpecFilter>*{Request,Response}.json</messageSpecFilter>
                        <templateDirectory>${project.basedir}/src/main/templates</templateDirectory>
                        <templateNames>KafkaApiMessageConverter.ftl</templateNames>
                        <outputFilePattern>${templateName}.java</outputFilePattern>
                        <outputPackage>io.kroxylicious.kafka.common.message.json</outputPackage>
                        <outputDirectory>${project.build.directory}/generated-sources/krpc</outputDirectory>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

Move `KafkaApiMessageConverter.ftl` from
`kroxylicious-filter-test-support/src/main/templates/` to
`kroxylicious-kafka-message-json/src/main/templates/`, updating only the `import` lines to
reflect the new `outputPackage` for the class itself (the `*DataJsonConverter` imports inside
the template stay `io.kroxylicious.kafka.common.message.*` — no change needed there).

### 2d. Reactor wiring

Add the module to `kroxylicious-proxy-core/pom.xml`'s `<modules>` list, immediately after
`../kroxylicious-api` (it depends on `kroxylicious-api`, so must come after it in read order —
Maven's reactor sorts by dependency regardless, but keep the file readable):

```xml
<module>../kroxylicious-api</module>
<module>../kroxylicious-kafka-message-json</module>
```

Update consumers:
- `kroxylicious-filter-test-support/pom.xml` — **remove** its `kroxylicious-krpc-plugin`
  `generate-converters` execution and the `KafkaApiMessageConverter.ftl` template file (moved
  away in §2c), **add** a dependency on `kroxylicious-kafka-message-json`. Update the two test
  classes that reference `io.kroxylicious.testing.filter.requestresponsetestdef.KafkaApiMessageConverter`
  (`MockUpstream.java`, `AuthorizationFilterTest.java`, `EntityIsolationFilterTest.java`'s
  `MockUpstream.java` — grep confirmed these three files) to the new package
  `io.kroxylicious.kafka.common.message.json.KafkaApiMessageConverter`.
- `kroxylicious-filters/kroxylicious-protocol-logger/pom.xml` — **replace** its transitive
  reliance on `kroxylicious-filter-test-support` (if declared as a real dependency; verify —
  it may currently be resolving only because `filter-test-support` happens to be on some
  shared classpath) with an explicit dependency on `kroxylicious-kafka-message-json`. Update
  `MessageFormatter.java`'s import of `KafkaApiMessageConverter` to the new package.

---

## 3. The canonical import mapping (mechanical rewrite rules)

Apply these rewrites to `import` statements **and** any fully-qualified references in code.
`SRC` below means `org.apache.kafka.common`, `DST` means `io.kroxylicious.kafka.common`.

### 3a. REWRITE — vendored, safe to swap

| From (`SRC.` = `org.apache.kafka.common.`)                 | To (`DST.` = `io.kroxylicious.kafka.common.`) |
|-----------------------------------------------------------|-----------------------------------------------|
| `SRC.message.*` (every `*Data`, plus `ApiMessageType`)    | `DST.message.*`                               |
| `SRC.protocol.types.*` (all)                              | `DST.protocol.types.*`                        |
| `SRC.protocol.ApiMessage`                                 | `DST.protocol.ApiMessage`                     |
| `SRC.protocol.Message`                                    | `DST.protocol.Message`                        |
| `SRC.protocol.MessageUtil`                                | `DST.protocol.MessageUtil`                    |
| `SRC.protocol.MessageSizeAccumulator`                     | `DST.protocol.MessageSizeAccumulator`        |
| `SRC.protocol.ObjectSerializationCache`                   | `DST.protocol.ObjectSerializationCache`      |
| `SRC.protocol.Readable`                                   | `DST.protocol.Readable`                       |
| `SRC.protocol.Writable`                                   | `DST.protocol.Writable`                       |
| `SRC.protocol.ByteBufferAccessor`                         | `DST.protocol.ByteBufferAccessor`            |
| `SRC.protocol.ApiKeys`                                    | `DST.protocol.ApiKeys`  **(vendored — corrected decision, §5)** |
| `SRC.protocol.Errors`                                     | `DST.protocol.Errors`  **(vendored — corrected decision, §5)** |
| `SRC.protocol.CoordinatorType`                            | `DST.protocol.CoordinatorType`  **(vendored — corrected decision, §5)** |
| `SRC.header.*` and `SRC.header.internals.*`               | `DST.header.*` / `DST.header.internals.*`    |
| `SRC.compress.*`                                          | `DST.compress.*`                             |
| `SRC.utils.{AbstractIterator,BufferSupplier,ByteBufferInputStream,ByteBufferOutputStream,Bytes,ByteUtils,Checksums,ChunkedBytesStream,CloseableIterator,Crc32C,ImplicitLinkedHashCollection,ImplicitLinkedHashMultiCollection,OperatingSystem,Utils}` | `DST.utils.<same>` |
| `SRC.errors.*` (**all** exception classes, ~149 — vendored wholesale, §5) | `DST.errors.<same>` |
| `SRC.TopicPartition`                                       | `DST.TopicPartition`  **(vendored — corrected decision, §5)** |
| `SRC.Uuid`                                                | `DST.Uuid`                                    |
| `SRC.KafkaException`                                      | `DST.KafkaException`                          |
| `SRC.InvalidRecordException`                              | `DST.InvalidRecordException`                  |

### 3b. REWRITE with re-packaging — the `record` special case

Record classes moved into a `.internal` sub-package during vendoring, **except `TimestampType`**:

| From                                                | To                                                    |
|-----------------------------------------------------|-------------------------------------------------------|
| `SRC.record.TimestampType`                          | `DST.record.TimestampType`  (**not** `.internal`)     |
| `SRC.record.<AnyOther>` (MemoryRecords, RecordBatch, Record, Records, BaseRecords, DefaultRecord, DefaultRecordBatch, MemoryRecordsBuilder, MutableRecordBatch, SimpleRecord, ControlRecordType, EndTransactionMarker, CompressionType, …) | `DST.record.internal.<AnyOther>` |

### 3c. Ordering rule for the rewrite script

1. `SRC.record.TimestampType` → `DST.record.TimestampType`
2. `SRC.record.internal.` → `DST.record.internal.`  (idempotency guard)
3. `SRC.record.` → `DST.record.internal.`  (catches remaining flat record classes)
4. All the §3a exact-class rules, including the now-vendored `ApiKeys`/`Errors`/`CoordinatorType`/`TopicPartition`.

A ready-to-run script is in §8.

---

## 4. `kroxylicious-api`: the kafka-clients-free module

### 4a. Audit (verified against the code today, not against PLAN.md's assumptions)

Grepping `kroxylicious-api/src/main` for `org.apache.kafka.` outside the already-vendored
`io/kroxylicious/kafka/common/` tree turns up exactly **four files**:

| File | What it imports | Fix |
|------|-----------------|-----|
| `src/main/java/io/kroxylicious/proxy/filter/RequestFilter.java` | `protocol.ApiKeys` | Rewrite per §3a (now vendored). |
| `src/main/java/io/kroxylicious/proxy/filter/ResponseFilter.java` | `protocol.ApiKeys` | Rewrite per §3a. |
| `src/main/java/io/kroxylicious/proxy/router/Router.java` | `protocol.ApiKeys` | Rewrite per §3a. |
| `src/main/java/io/kroxylicious/proxy/filter/RequestFilterResultBuilder.java` | `{@link org.apache.kafka.common.requests.ApiError}` / `{@link ...Errors#forException}` in a Javadoc comment only, no import | Rewrite the Javadoc to reference the vendored `Errors`, or drop to `{@code}` if the vendored class doesn't have an equivalent `ApiError` type — check before choosing. |

Plus one file already inside the vendored tree that still imports live `kafka-clients` types:

| File | What it imports | Fix |
|------|-----------------|-----|
| `src/main/java/io/kroxylicious/kafka/common/errors/RecordTooLargeException.java` | `org.apache.kafka.common.TopicPartition` | Vendor `TopicPartition` itself (§3a, §5). It is a small, self-contained value class (confirmed footprint note carried over from PLAN.md: "trivial, zero drag") and `RecordTooLargeException` is not constructed anywhere else in the reactor today — this is a pure copy, no ripple. |

**`kroxylicious-api/src/test` has zero `org.apache.kafka.*` references today** (verified by
grep, excluding the vendored tree). No test-scope `kafka-clients` dependency is currently
needed. If a future test genuinely requires a `kafka-clients` type (e.g. to assert
interoperability with an upstream client), add `kafka-clients` back with `<scope>test</scope>`
at that point — don't add it speculatively now.

### 4b. `pom.xml` changes

Remove the main-scope `kafka-clients` dependency:

```xml
<!-- DELETE -->
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-clients</artifactId>
</dependency>
```

**Do not touch** the `unpack-message-specs` execution — it references `kafka-clients` as a
build-time artifact to unpack (`common/message/*.json` spec files), not as a project
dependency. It doesn't add `kafka-clients` to any classpath and is orthogonal to this goal.

After deletion, `jackson-databind`, `jackson-core`, `slf4j-api`, and the native codec
dependencies that the existing pom comment says "kafka-clients brings ... transitively at
runtime only" **must be checked**: with `kafka-clients` gone, nothing else may provide them
transitively. The existing pom comment already declares them explicitly for exactly this
reason (it predates this plan, written when `kafka-clients` was still present as a compile
dependency but its transitive resolution couldn't be relied on for jackson-core specifically).
Re-verify after removal with:

```bash
mvn -q -o -pl kroxylicious-api dependency:tree > /tmp/krox-api-deptree.log 2>&1
grep -E 'jackson|slf4j|lz4|snappy|zstd' /tmp/krox-api-deptree.log
```

Confirm every compress/record class that needs a native codec (lz4, snappy, zstd — used by the
vendored `io.kroxylicious.kafka.common.compress.*`) still resolves. Add explicit dependencies
for anything that drops off the classpath.

### 4c. Verification

```bash
mvn -q -o -pl kroxylicious-api compile > /tmp/krox-api-compile.log 2>&1; echo "EXIT=$?"
grep -rlE "org\.apache\.kafka\." --include=*.java kroxylicious-api/src/main \
  | grep -v '/io/kroxylicious/kafka/common/'
# expected: empty (zero files)
grep -rl "org.apache.kafka" kroxylicious-api/pom.xml
# expected: no <dependency> block, only the unpack-message-specs <artifactItem> reference
```

---

## 5. Vendoring `ApiKeys`, `Errors`, `CoordinatorType`, `TopicPartition` — do this before any other rewrite

This is the corrected starting decision. Do these four vendoring steps **first**, before
running any mechanical import rewrite elsewhere in the reactor, so every other module's
migration sees a stable target from the start.

1. **`ApiKeys`** → `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/protocol/ApiKeys.java`.
   Copy from Kafka 4.3.0 source (`/home/robeyoun/development/upstream/kafka`), rewrite its
   package and internal imports per §3. It references `ApiMessageType` (already generated) and
   `protocol.types.Schema`/`Type` (already vendored) — no new drag.
2. **`Errors`** → `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/protocol/Errors.java`.
   Copy and rewrite. This is the one with real footprint: it references every exception class
   in `org.apache.kafka.common.errors.*` (~149 classes). **Vendor the entire exception
   hierarchy wholesale** rather than the small allowlist PLAN.md originally kept — Phase C
   already did this (149 files copied into
   `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/errors/`) and it is the
   correct call once `Errors` itself is vendored: a partial allowlist just means `Errors`
   can't compile without the exceptions it maps to. Copy the whole `errors` package from Kafka
   4.3.0, rewrite imports per §3. The **only** cross-package import expected to remain inside
   this tree after rewrite is `TopicPartition` in `RecordTooLargeException` — fixed by vendoring
   `TopicPartition` (step 4 below), not by special-casing that file.
3. **`CoordinatorType`** → `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/protocol/CoordinatorType.java`.
   Standalone enum, no dependencies. Copy as-is with package rewrite.
4. **`TopicPartition`** → `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/TopicPartition.java`.
   Copy as-is with package rewrite. Self-contained value class (`topic` + `partition` fields,
   `equals`/`hashCode`/`toString`), no further drag.

After these four are in place, run:

```bash
grep -rhoE '^import org\.apache\.kafka\.[^;]*;' \
  kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/{protocol,errors}/*.java \
  kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/TopicPartition.java | sort -u
# expected: (empty)
```

Then proceed to §4 (`kroxylicious-api` hand-written code) and §6/§7 (runtime + filters).

**Consequence for the KEEP list (§6):** `ApiKeys`, `Errors`, `CoordinatorType`, and
`TopicPartition` move from PLAN.md's KEEP list to the REWRITE list (§3a) everywhere in the
reactor — `kroxylicious-runtime` and the filters get the vendored versions too, for
consistency. There is no module where it's correct to use the `kafka-clients` `ApiKeys`/`Errors`
and the vendored ones side by side for the same value — pick one per compilation unit and it
should always be the vendored one, per §3a.

---

## 6. KEEP list — classes that MUST stay `org.apache.kafka.*` (outside `kroxylicious-api`)

This list applies to `kroxylicious-runtime` and the filters, which keep the coexistence model.
It does **not** apply inside `kroxylicious-api`, which has none of these left after §4/§5.

**Never rewrite these.** They are not vendored; `kafka-clients` provides them and they do not
type-bridge into the vendored `*Data` world.

| Kept class(es)                                        | Why it stays / notes |
|------------------------------------------------------|----------------------|
| `org.apache.kafka.common.requests.*`                 | `AbstractRequest`, `AbstractResponse`, and all `*Request`/`*Response` wrapper classes. NOT vendored. Only genuinely needed inside `KafkaProxyExceptionMapper` (see §7.6). |
| `org.apache.kafka.common.*` other top-level value types | `TopicCollection`, `Node`, `KafkaFuture`, `TopicPartitionInfo`, `IsolationLevel`, `GroupState`, `GroupType`, `Cluster`, `MetricName`, `Metric`, `ElectionType`. (`TopicPartition` itself is now vendored, §5 — everything else here stays.) |
| `org.apache.kafka.common.errors.*` beyond what's vendored | N/A now — §5 vendors the whole hierarchy. This row exists only as a reminder: if the compiler surfaces a Kafka exception class *not* found under `io.kroxylicious.kafka.common.errors`, that's a gap in the copy (§9 step 5c), not a KEEP case. |
| `org.apache.kafka.clients.*`, `org.apache.kafka.server.*`, `org.apache.kafka.coordinator.*`, etc. | Admin/producer/consumer client classes, only used in tests/integration. Not vendored. |
| `org.apache.kafka.common.acl.*`, `config.*`, `security.*`, `serialization.*`, `header.*`(client-side) | Not part of the protocol-message vendoring. Keep. |

> If, after migrating, the compiler complains that a KEPT class is incompatible with a vendored
> value (a real type bridge), that call site is a **hot spot** — handle it per §7, do **not**
> "fix" it by rewriting the KEEP class.

`org.apache.kafka.common.protocol.SendBuilder` appears only in a Javadoc `{@link}` and a code
comment (the send subsystem was deleted). Not a real dependency — if doclint fails on the dead
`{@link SendBuilder}` in `io/kroxylicious/kafka/common/protocol/MessageSizeAccumulator.java`,
delete that `@link` reference. No functional change.

---

## 7. Module scope and compile order

Compile in reactor dependency order. Migrate + verify one module before moving on.

**Phase A — core (#4581):**
1. **`kroxylicious-api`** — vendor `ApiKeys`/`Errors`/`CoordinatorType`/`TopicPartition` (§5)
   first, then migrate the 4 hand-written files (§4), then remove the `kafka-clients`
   dependency (§4b). Generated code (`*Data`/`ApiMessageType`) is already correct.
2. **`kroxylicious-kafka-message-json`** — new module (§2). Build it right after
   `kroxylicious-api` so `kroxylicious-filter-test-support` and `kroxylicious-protocol-logger`
   have it available when their turn comes.
3. `kroxylicious-kafka-message-generator` templates already done — no change.
4. `kroxylicious-runtime` (codec, frames, exception mapper, templates) — **the hard module**.
5. `kroxylicious-krpc-plugin` (FreeMarker templates + model, if referenced).

**Phase B — filters + support (#4582):**
6. `kroxylicious-kafka-message-tools` (record transform utilities — `RecordStream`, `BatchAwareMemoryRecordsBuilder`).
7. `kroxylicious-filters/*` submodules (record-encryption, multitenant, record-validation,
   simple-transform, authorization, sasl-*, oauthbearer-validation, connection-expiration,
   entity-isolation, protocol-logger). **`kroxylicious-protocol-logger` additionally needs its
   new `kroxylicious-kafka-message-json` dependency wired in here (§2d).**
8. `kroxylicious-filter-test-support` — **also needs the `kroxylicious-kafka-message-json`
   dependency + `KafkaApiMessageConverter` package-reference update (§2d)** in addition to the
   standard §3 rewrite.
9. `kroxylicious-integration-test-support`.
10. `kroxylicious-runtime-plugins`, `kroxylicious-microbenchmarks`, `kroxylicious-app` (mostly test/support).
11. `kroxylicious-integration-tests` (test-compile only; heavy `org.apache.kafka.clients.*`
    usage that mostly stays).

Note: `kroxylicious-filters` and several others are **aggregator POMs** — the code lives in
submodules; recurse into them.

---

## 8. Hot spots — the type-bridge points that need manual work

These apply to `kroxylicious-runtime` and the filters (§6/§7 scope), not `kroxylicious-api`
(handled in §4/§5). Unchanged from PLAN.md except where the `ApiKeys`/`Errors` vendoring
decision ripples through — flagged inline.

### 8.1 `ByteBufAccessor` / `ByteBufAccessorImpl` — the central codec adapter
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/frame/ByteBufAccessor.java`
  currently `extends org.apache.kafka.common.protocol.{Readable,Writable}`.
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/codec/ByteBufAccessorImpl.java`
  implements it; its `slice()` returns `org.apache.kafka...Readable`.
- **Action:** re-point both `Readable`/`Writable` to `io.kroxylicious.kafka.common.protocol.*`.
  The vendored `*Data` constructors require the vendored `Readable`/`Writable`, so this adapter
  MUST implement the vendored interfaces. This one change unblocks the entire codec.
- Verify the vendored `Readable`/`Writable` method set matches what `ByteBufAccessor` overrides.
  `readRecords(int)` returns the vendored `record.internal.MemoryRecords`;
  `writeRecords(BaseRecords)` takes the vendored `record.internal.BaseRecords`. Update override
  signatures accordingly.

### 8.2 `BodyDecoder.ftl` — the `apiKey → new *Data(...)` switch (generated at build time)
- Template: `kroxylicious-runtime/src/main/templates/BodyDecoder.ftl`.
- Change the `*Data` import line to `import io.kroxylicious.kafka.common.message.${inputSpec.name}Data;`.
- Change `import org.apache.kafka.common.protocol.ApiMessage;` → `io.kroxylicious.kafka.common.protocol.ApiMessage;`.
- Change `import org.apache.kafka.common.protocol.ApiKeys;` → `io.kroxylicious.kafka.common.protocol.ApiKeys;`
  **(this is now a REWRITE, not a KEEP — different from PLAN.md, since `ApiKeys` is vendored
  from the start per §5)**. The `switch (apiKey)` still matches on the vendored `ApiKeys` enum
  constants — same `int` ids, same enum ordering, generated from the same specs.
- `accessor` param is `ByteBufAccessor`, which after §8.1 extends the vendored `Readable`, so
  `new XxxData(accessor, apiVersion)` resolves against the vendored constructor. No other change.
- Regenerate and inspect `kroxylicious-runtime/target/generated-sources/**/BodyDecoder.java`
  after editing.

### 8.3 Other runtime FreeMarker templates
- `kroxylicious-runtime/src/main/templates/`: `FilterInvoker.ftl`, `SpecificFilterArrayInvoker.ftl`.
- `SpecificFilterArrayInvoker.ftl` uses `ApiMessageType.values()` / `ApiMessageType::apiKey` for
  `int` array indexing. Swap the import to `io.kroxylicious.kafka.common.message.ApiMessageType`
  (generated) for consistency with the rest of the vendored stack.
- `FilterInvoker.ftl` and any per-message templates that import `*Data` / `ApiMessage`: apply §3a swaps.
- Grep every `.ftl` under the repo for `org.apache.kafka.common.` and apply §3/§6 rules:
  ```bash
  grep -rl 'org\.apache\.kafka\.common\.' --include=*.ftl kroxylicious-runtime kroxylicious-krpc-plugin
  ```

### 8.4 Frame classes + codec
- `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/frame/`:
  `DecodedFrame.java`, `DecodedRequestFrame.java`, `DecodedResponseFrame.java`, `RequestFrame`,
  `Frame`, `OpaqueRequestFrame`, `InternalRequestFrame`, `InternalResponseFrame`. Generic over
  `ApiMessage`, use `RequestHeaderData`/`ResponseHeaderData` → swap those to vendored (§3a).
  `DecodedResponseFrame:25` `ApiKeys.forId(id).messageType.responseHeaderVersion(v)` now
  resolves entirely against vendored `ApiKeys`/`ApiMessageType` — no bridge, just an import swap
  **(previously a KEEP no-op in PLAN.md; now an active rewrite, still a no-op semantically)**.
- `kroxylicious-runtime/.../internal/codec/`: `KafkaRequestDecoder`, `KafkaResponseDecoder`,
  `KafkaRequestEncoder`, `KafkaResponseEncoder`, `KafkaMessageEncoder`, `KafkaMessageDecoder`,
  `CorrelationManager`. Swap `ApiMessage`, `Readable`, `RequestHeaderData`, `ApiKeys` to vendored.
  Header build `new RequestHeaderData(accessor, headerVersion)` resolves once `accessor` is the
  vendored-`Readable` adapter (§8.1).

### 8.5 `ApiMessage` generic bounds ripple
`org.apache.kafka.common.protocol.ApiMessage` is used ~482 times as a type bound / field / param.
Once `frame.body()` is a vendored `ApiMessage`, **every** signature that passes a body must use
the vendored `ApiMessage`. Swap wholesale (§3a) across `kroxylicious-runtime` and the filters
early, so the compiler surfaces the remaining true bridges.

### 8.6 `KafkaProxyExceptionMapper` — the kafka-clients island (biggest single break)
- File: `kroxylicious-runtime/src/main/java/io/kroxylicious/proxy/internal/KafkaProxyExceptionMapper.java` (~438 lines).
- **Problem:** it takes `frame.body()` (a vendored `ApiMessage`), needs to build a `kafka-clients`
  `requests.*Request`/`*Response` to call `.getErrorResponse(error)` (no vendored equivalent of
  the request/response wrapper classes exists — those stay KEEP, §6), then must return a
  vendored `*ResponseData` for the response frame.
- **Recommended fix (byte round-trip adapter — simplifies AND compiles):**
  1. Serialize the vendored request body to bytes at `apiVersion` using the vendored writer
     (`io.kroxylicious...MessageUtil` / `ObjectSerializationCache` + a vendored accessor over a
     `ByteBuffer`).
  2. `org.apache.kafka.common.requests.RequestAndSize ras =
     AbstractRequest.parseRequest(kafkaApiKeys, apiVersion, buffer);` — where `kafkaApiKeys` is
     obtained by translating the vendored `ApiKeys` value via `.id` →
     `org.apache.kafka.common.protocol.ApiKeys.forId(id)`. This translation is the one place in
     the whole reactor where vendored `ApiKeys` legitimately meets `kafka-clients` `ApiKeys`,
     because `AbstractRequest`/`AbstractResponse` (KEEP, §6) are typed against the latter.
  3. `AbstractResponse kresp = ras.request.getErrorResponse(error);` — `error` here is
     similarly translated: vendored `Errors` → `org.apache.kafka.common.protocol.Errors.forCode(vendoredErrors.code())`.
  4. Serialize `kresp` to bytes at `apiVersion` (`kafka-clients` serializer).
  5. Re-read into the **vendored** response `*Data` via the generated
     `BodyDecoder.decodeResponse(vendoredApiKeys, apiVersion, new ByteBufAccessorImpl(Unpooled.wrappedBuffer(respBytes)))`.
  6. Return the vendored `ApiMessage`.
  - This removes ~90 `org.apache.kafka.common.requests.*` imports and the whole monster switch.
  - Keep the `LIST_CONFIG_RESOURCES` v0 special case, building the response as a **vendored**
    `ListConfigResourcesResponseData` directly.
  - Change the public method return types from `AbstractResponse` to the vendored `ApiMessage`
    and update the 3 call sites:
    - `kroxylicious-runtime/.../internal/KafkaProxyFrontendHandler.java:449`
    - `kroxylicious-runtime/.../internal/filter/RequestFilterResultBuilderImpl.java:56`
    - `kroxylicious-runtime/.../internal/routing/RouterDispatchHandler.java:343`
  - Cost: an extra serialize/parse per **error** response (not a hot path). Acceptable for a prototype.
  - This file (plus, if the same reasoning applies, `OauthBearerValidationFilter.java` for its
    `SaslAuthenticationException` catch) is expected to be the **only** place in main sources
    with fully-qualified `org.apache.kafka.common.{protocol.ApiKeys,protocol.Errors,requests.*}`
    references. Confinement check in §9 step 5 verifies this stays true.
- **Alternative (if the round-trip is troublesome):** stub `errorResponse(...)` to build a
  minimal vendored `*ResponseData` for a handful of api keys and throw
  `UnsupportedOperationException` for the rest. Compiles, loses error fidelity — only if the
  round-trip blocks progress.

### 8.7 Record transform tools + record filters — the records bridge
- `kroxylicious-kafka-message-tools` (`io.kroxylicious.kafka.transform`): `RecordStream.java`,
  `BatchAwareMemoryRecordsBuilder.java`, `RecordMapper`, `RecordTransform`, `RecordConsumer`
  import `org.apache.kafka.common.record.MemoryRecords` etc. Apply the §3b record→`record.internal`
  remap so they operate on the same vendored record types that `*Data.records()` now returns.
  This module also gets the `ApiKeys`/`Errors` rewrite (§3a) for its `ApiVersionMaxVersionLimiter`,
  `ApiVersionRemover`, `ApiVersionsResponseTransformers` classes.
- Record-touching filters (`kroxylicious-filters/kroxylicious-record-encryption/**`:
  `RecordEncryptionFilter`, `InBandEncryptionManager`, `InBandDecryptionManager`,
  `EncryptionManager`, `DecryptionManager`, `RecordEncryptionUtil`; and
  `kroxylicious-filters/kroxylicious-authorization/**` `RequestDataUtils`) cast
  `(MemoryRecords) xxx.records()` and call `RecordStream.ofRecords(...)`. After migration
  `xxx.records()` returns `io.kroxylicious...record.internal.MemoryRecords`, so the casts, the
  `RecordStream` types, and `setRecords(...)` args must all use the vendored `record.internal`
  types. Apply §3b consistently across message-tools + these filters together (they're one
  connected type graph).

### 8.8 `kroxylicious-krpc-plugin`
- Model classes under `kroxylicious-krpc-plugin/src/main/java/io/kroxylicious/krpccodegen/**`
  and its own templates reference `org.apache.kafka.common.*`. Apply §3/§6, including the
  `ApiKeys`/`Errors` rewrite. Grep for `org.apache.kafka.common.` in this module and swap per
  the rules.

---

## 9. The mechanical rewrite script

For the bulk of files (everything that is NOT a §8 hot spot), this script applies §3 while
never touching the §6 KEEP list. Idempotent. Run it per module, review the diff, then compile.

```bash
#!/usr/bin/env bash
# usage: ./migrate.sh <dir1> [<dir2> ...]
set -euo pipefail
SRC='org\.apache\.kafka\.common'
DST='io.kroxylicious.kafka.common'

mapfile -t files < <(grep -rlE "org\.apache\.kafka\.common\." "$@" \
  --include='*.java' --include='*.ftl' 2>/dev/null \
  | grep -v '/io/kroxylicious/kafka/common/' || true)

for f in "${files[@]}"; do
  # 3b record: TimestampType first (escapes .internal), then flat record -> record.internal
  sed -i -E "s/${SRC}\.record\.TimestampType/${DST}.record.TimestampType/g" "$f"
  sed -i -E "s/${SRC}\.record\.internal\./${DST}.record.internal./g" "$f"
  sed -i -E "s/${SRC}\.record\.([A-Z])/${DST}.record.internal.\1/g" "$f"

  # 3a message + protocol.types (wholesale)
  sed -i -E "s/${SRC}\.message\./${DST}.message./g" "$f"
  sed -i -E "s/${SRC}\.protocol\.types\./${DST}.protocol.types./g" "$f"

  # 3a protocol infra, INCLUDING ApiKeys/Errors/CoordinatorType (vendored from the start)
  for c in ApiMessage Message MessageUtil MessageSizeAccumulator ObjectSerializationCache \
           Readable Writable ByteBufferAccessor ApiKeys Errors CoordinatorType; do
    sed -i -E "s/${SRC}\.protocol\.${c}([^A-Za-z0-9])/${DST}.protocol.${c}\1/g" "$f"
  done

  # 3a header + compress (wholesale)
  sed -i -E "s/${SRC}\.header\./${DST}.header./g" "$f"
  sed -i -E "s/${SRC}\.compress\./${DST}.compress./g" "$f"

  # 3a utils (allowlist)
  for c in AbstractIterator BufferSupplier ByteBufferInputStream ByteBufferOutputStream Bytes \
           ByteUtils Checksums ChunkedBytesStream CloseableIterator Crc32C \
           ImplicitLinkedHashCollection ImplicitLinkedHashMultiCollection OperatingSystem Utils; do
    sed -i -E "s/${SRC}\.utils\.${c}([^A-Za-z0-9])/${DST}.utils.${c}\1/g" "$f"
  done

  # 3a errors -- wholesale now (Errors.java references the whole hierarchy, §5)
  sed -i -E "s/${SRC}\.errors\./${DST}.errors./g" "$f"

  # 3a top-level (allowlist, now includes TopicPartition)
  for c in Uuid KafkaException InvalidRecordException TopicPartition; do
    sed -i -E "s/${SRC}\.${c}([^A-Za-z0-9])/${DST}.${c}\1/g" "$f"
  done
done
echo "rewrote ${#files[@]} files"
```

Notes:
- Unlike PLAN.md's script, this one rewrites `ApiKeys`, `Errors`, `CoordinatorType`, and
  `TopicPartition` **wholesale**, not by omission. There is no module-specific carve-out — every
  file gets the same treatment. This removes the entire class of bug that broke Phase C (a
  script run "fragmented across multiple ad-hoc invocations" because the KEEP/REWRITE boundary
  moved mid-flight).
- Two files are **excluded by hand**, not by the script, because they are genuine
  `kafka-clients` islands (§8.6): `KafkaProxyExceptionMapper.java` and, if the same
  `requests.*`-adjacent reasoning applies to it, `OauthBearerValidationFilter.java`. Run the
  script everywhere first, then manually verify these two still reference `kafka-clients` only
  where §8.6 requires it, reverting anything the blanket script incorrectly touched inside them.
- **Run this script exactly once per module, in one pass, immediately followed by a compile.**
  Do not run it ad hoc across a session in fragments — that's what left Phase C with 64 stale
  files. If a module doesn't compile after one script run, fix the remaining hot spots (§8) by
  hand; don't re-run the bulk script hoping it catches more — it's already applied everything
  it can.

---

## 10. Copying additional classes from Kafka (if the compiler asks)

Kafka 4.3.0 source: `/home/robeyoun/development/upstream/kafka` (mostly
`clients/src/main/java/org/apache/kafka/common/...`). If a needed class is missing from the
vendored set and is genuinely part of the protocol/message/record/error graph (not a §6 KEEP
class):

1. Copy the `.java` into the mirrored path under
   `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/...` (records go under
   `record/internal/`, errors go under `errors/`).
2. Keep the original ASF copyright header.
3. Change its `package` to `io.kroxylicious.kafka.common...` and rewrite its imports with §3/§9 rules.
4. Recompile.

---

## 11. Execution checklist

1. `git checkout -b kafka-vendoring-phase-d` (or continue on `kafka-vendoring-phase-c` — your call).
2. Vendor `ApiKeys`, `Errors`, `CoordinatorType`, `TopicPartition` into `kroxylicious-api` (§5).
   Compile `kroxylicious-api` in isolation; fix any gaps in the copied `errors` hierarchy.
3. Migrate `kroxylicious-api`'s 4 remaining hand-written files (§4a), remove the `kafka-clients`
   main dependency (§4b), verify (§4c).
4. Create `kroxylicious-kafka-message-json` (§2). Move `KafkaApiMessageConverter.ftl` and update
   `kroxylicious-filter-test-support`'s pom + the 3 test files that reference it (§2d — note:
   this step touches `filter-test-support`, which is Phase B in the compile order (§7); doing
   the pom wiring now and the full module migration later is fine, they're independent).
5. For each remaining module in §7 order: run the §9 script once, apply §8 hot spots where
   listed, compile:
   ```bash
   mvn -q -o -pl <module> -am compile > /tmp/krox-mig.log 2>&1; echo "EXIT=$?"
   grep -E 'ERROR|cannot find symbol|incompatible types|does not exist' /tmp/krox-mig.log | head -40
   ```
6. Read the errors. Each remaining error is one of:
   - **Import missed** by the script (fully-qualified inline reference, or a `.ftl`) → fix by hand.
   - **True type bridge** (vendored value meets a §6 KEEP `kafka-clients` API) → new hot spot,
     apply the §8.6-style pattern (adapt at the boundary), do NOT rewrite the KEEP class.
   - **Missing vendored class** → copy per §10, or confirm it's genuinely §6 KEEP.
7. Repeat until the module compiles. Move to the next module.
8. Finally: `mvn -q -o -DskipTests -Dmaven.test.skip=true install` from the root, then
   `test-compile` per module.
9. Confinement check — after everything compiles, exactly these files (and no others) may still
   reference `org.apache.kafka.common.{protocol.ApiKeys,protocol.Errors,errors.,requests.}`
   fully-qualified or via import, outside `io/kroxylicious/kafka/common/`:
   ```bash
   grep -rlE "org\.apache\.kafka\.common\.(protocol\.(ApiKeys|Errors)|errors\.|requests\.)" \
     --include=*.java \
     kroxylicious-api/src/main kroxylicious-runtime/src/main kroxylicious-filters/*/src/main \
     kroxylicious-filter-test-support/src/main kroxylicious-kafka-message-tools/src/main \
     kroxylicious-kafka-message-json/src/main \
     | grep -v '/io/kroxylicious/kafka/common/'
   # expected output: exactly
   #   kroxylicious-runtime/.../internal/KafkaProxyExceptionMapper.java
   #   kroxylicious-filters/kroxylicious-oauthbearer-validation/.../OauthBearerValidationFilter.java
   ```
   And separately, `kroxylicious-api` gets a stricter check — zero hits, no exceptions:
   ```bash
   grep -rlE "org\.apache\.kafka\." --include=*.java kroxylicious-api/src/main \
     | grep -v '/io/kroxylicious/kafka/common/'
   # expected output: (empty)
   grep -n "kafka-clients" kroxylicious-api/pom.xml
   # expected output: (empty, or only inside a comment referring to the unpack-message-specs artifactItem)
   ```
10. Commit with a conventional-commit message per repo convention, `Assisted-by:` trailer.

For `kroxylicious-integration-tests` and other test-heavy modules, only `test-compile` matters,
and most `org.apache.kafka.clients.*` imports there **stay** (KEEP) — expect far fewer changes.

---

## 12. Known limitations accepted for this prototype

- **No wire-level verification.** Not proving byte-for-byte compatibility — phase 2 (#4579).
- **Existing tests are not expected to pass**, only to *compile* (deferrable per module if it
  blocks the main-source goal).
- **`KafkaProxyExceptionMapper` error responses go through a byte round-trip** (§8.6) rather
  than native construction — correct output, extra copies on the error path only.
- **`kroxylicious-runtime` and the filters keep both `kafka-clients` and the vendored classes on
  the classpath.** Only `kroxylicious-api` becomes kafka-clients-free in this plan. Extending
  that to the rest of the reactor is separate future scope.
- **`kroxylicious-kafka-message-json` has a deliberate split package**
  (`io.kroxylicious.kafka.common.message`) with `kroxylicious-api`, for the generated
  `*DataJsonConverter` classes only. Accepted per explicit decision (§2b).
- **Pre-existing test-compile failures in `kroxylicious-integration-tests`** unrelated to this
  work (Uuid type mismatch in `AbstractFilterIT.java`/`AbstractTracingIT.java`/`ClusterPrepUtils.java`,
  `ProducerRecord`/`Header` mismatches in the record-encryption and JWS validation ITs) — file as
  separate issues, out of scope here.

---

## 13. Quick reference — verified facts (this session's audit, not carried over unchecked from PLAN.md)

- `kroxylicious-api/src/main` has exactly 4 hand-written files with a live `org.apache.kafka.*`
  dependency (all `ApiKeys`, one via Javadoc only), plus 1 vendored-tree file
  (`RecordTooLargeException.java`) needing `TopicPartition`. `kroxylicious-api/src/test` has
  zero such references today.
- `MessageFormatter.java` in `kroxylicious-protocol-logger` (main scope) already imports
  `io.kroxylicious.testing.filter.requestresponsetestdef.KafkaApiMessageConverter` from
  `kroxylicious-filter-test-support` — a main-scope filter depending on a test-support artifact.
  This plan's §2 fixes that as a side effect of relocating the JSON converter machinery.
- The forked generator's `-m` argument accepts multiple generator names independently
  (`MessageDataGenerator`, `ApiMessageTypeGenerator`, `JsonConverterGenerator`) — splitting them
  across two modules (§2) requires no generator code changes, only pom wiring.
- Generated `*Data` land in `io.kroxylicious.kafka.common.message` (381 files today incl.
  `ApiMessageType`); after this plan the ~190 `*DataJsonConverter` files move to
  `kroxylicious-kafka-message-json`, leaving `kroxylicious-api`'s generated tree ~191 files.
- Codec `*Data` construction happens in the generated `BodyDecoder` switch (keyed on `ApiKeys`,
  vendored per §5), NOT via `ApiMessageType.newRequest()`.
- The one central serialization adapter is `ByteBufAccessor(Impl)` (§8.1) — flipping its
  `Readable`/`Writable` to vendored unblocks the whole codec.
- The one genuinely hard class is `KafkaProxyExceptionMapper` (§8.6) — unchanged by the
  `ApiKeys`/`Errors` vendoring decision except that the translation now runs vendored→kafka-clients
  (via `.id`/`.code()`) instead of being able to use kafka-clients values natively.
- Record classes are vendored under `record.internal` (except `TimestampType`); the record
  bridge spans `kafka-message-tools` + record filters (§8.7).
