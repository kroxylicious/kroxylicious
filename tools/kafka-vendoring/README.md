<!--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
# Vendored Kafka protocol support classes and forked tests

`kroxylicious-api` exposes a copy of Apache Kafka's protocol surface under the
`io.kroxylicious.kafka.common.*` namespace, so that the API references no
`org.apache.kafka.*` types. That surface has three parts:

1. **Generated message classes** — the `*Data` / `*DataJsonConverter` classes,
   `ApiMessageType` and the `ApiKeys` enum (a veneer over `ApiMessageType`), produced on
   every build by the forked message generator (`kroxylicious-kafka-message-generator`)
   from the protocol JSON specs. These are **not** committed; they land in
   `target/generated-sources/kafka-messages`.
2. **Vendored support classes** — the ~90 non-generated classes the generated code
   needs at runtime (records, compression, protocol readers/writers, a handful of
   utils and errors). These **are** committed, under
   `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/**`.
3. **Forked unit tests** — Apache Kafka's own tests for the classes in (2), plus a
   trimmed copy of `org.apache.kafka.test.TestUtils` (a shared test-support helper a
   few of them depend on). Committed under
   `kroxylicious-api/src/test/java/io/kroxylicious/kafka/**`.

This directory holds the tooling that (re)produces the *second and third* parts. It is
a **reproducible process, not a one-off hand copy**: given a clean Apache Kafka
checkout at the pinned tag, `vendor.sh` regenerates the entire committed
vendored/forked tree deterministically.

## Pinned source

| | |
|---|---|
| Kafka tag | `4.3.0` (property `kafka.message-spec.version` in the root `pom.xml`) |
| Commit | `a9ce3221537b8653448750697915607dc7936cf3` |

The spec/source version is deliberately **decoupled** from `kafka.version` (the
`kafka-clients` we depend on, currently `4.2.0`): the vendored/generated protocol
surface can track a different Kafka release than the client jar. When bumping,
change `kafka.message-spec.version` and re-run `vendor.sh` against a matching
checkout.

## Files

| File | Purpose |
|---|---|
| `vendor.sh` | Orchestrator. `vendor.sh <path-to-apache-kafka-checkout>` re-vendors/re-forks the whole tree. |
| `main/vendored-files.txt` | The frozen, `java/`-source-root-relative list of files to copy for `src/main` (the closure — see below). |
| `main/edits.yaml` | Declarative content-based edits that cut the server/config edges we do not vendor. |
| `test/vendored-files.txt` | Same idea as `main/vendored-files.txt`, for `src/test`: Kafka's own unit tests for the vendored classes, plus `org/apache/kafka/test/TestUtils.java`. |
| `test/edits.yaml` | Edits for the test tree — cutting tests of already-stripped production methods, and trimming `TestUtils.java` down to the handful of methods actually used. |
| `apply-edits.py` | Generic engine that applies an `edits.yaml` against a freshly copied source root. |
| `requirements.txt` | Pinned Python dependency (`PyYAML`) needed to parse the `edits.yaml` files. |
| `rewrite.yml` | OpenRewrite recipe relocating `org.apache.kafka.*` → `io.kroxylicious.kafka.*`. |
| `rewrite-pom.xml` | Throwaway pom used to invoke the OpenRewrite recipe against the staging copy. |

Both `main/` and `test/` mirror Maven's own source-root split — the axis that actually
determines where output lands and how it's treated by the build (licenseSet, Checkstyle/
SpotBugs/ErrorProne exclusion, and so on), not which Kafka package a file happens to
come from. There's no module-level grouping above that: everything here comes from
Kafka's one `clients` module, so it collapses to plain `main/`/`test/`. Each list is
rooted at the Maven source root itself (`clients/src/{main,test}/java/`), so every entry
is the file's full package-qualified path (e.g. `org/apache/kafka/common/utils/Utils.java`,
`org/apache/kafka/test/TestUtils.java`) — `TestUtils.java` living in a different
top-level package (`org.apache.kafka.test`, not `org.apache.kafka.common`) needs no
special case anywhere in the tooling as a result.

## How `vendor.sh` works

For each of `main` and `test`:

1. **Copy** the `$root/vendored-files.txt` files from
   `<kafka>/clients/src/$root/java`.
2. **Edit** (`apply-edits.py` from `$root/edits.yaml`) — fails loudly if a target has
   moved rather than silently no-op-ing.

Then, once for the whole staged tree:

3. **Relocate** packages to `io.kroxylicious.kafka.*` with OpenRewrite.
4. **Sync** each root's result into `kroxylicious-api/src/$root/java`, wiping only the
   previously-vendored/forked tree first.
5. **Format** by running `mvn -pl kroxylicious-api -am process-test-sources`, so the
   build's `formatter-maven-plugin`/`impsort-maven-plugin` are applied. The raw
   vendored output is not what gets committed.

After running, rebuild and verify the module (`mvn -pl kroxylicious-api -am verify`)
and commit the changed tree.

## Deriving the file lists

`main/vendored-files.txt` was derived by compiling the generated message classes with
`kafka-clients` off the classpath: each "cannot find symbol" error names a file to
add and copy in, repeating until it compiles cleanly. Two things needed manual
handling on top of that iteration:

- **Two `javac` blind spots** meant missing-symbol errors alone would under-copy, so
  `compress`, `record`, `record/internal`, `header` and `header/internals` were
  copied wholesale up front instead — minus the four file/server classes.
  `record/internal/Record.java` is silently shadowed by the JDK's `java.lang.Record`
  (it never errors "missing"), and the compression impls (`GzipCompression`, …) are
  only referenced reflectively via a factory, so an import-driven closure misses them.
- **Kafka's `errors` package** has ~130 classes; only the 5 actually reachable
  (`ApiException`, `RetriableException`, `CorruptRecordException`,
  `InvalidConfigurationException`, `UnsupportedVersionException`) were kept, rather
  than copying the whole hierarchy.

The result compiles with **no** `org.apache.kafka.*` references remaining.

`test/vendored-files.txt` was derived the other way round: for each entry in
`main/vendored-files.txt`, checking whether Kafka's tree has a same-named `*Test.java`.
One test, `protocol/types/ProtocolSerializationTest.java`, exercises several vendored
`protocol/types` classes together and has no 1:1 production counterpart of its own, so
it's included too. One same-named test, `record/internal/MultiRecordsSendTest.java`, is
deliberately **not** included: its private test helper extends
`org.apache.kafka.common.requests.ByteBufferChannel`, and the whole `requests` package
is out of scope (never vendored) for the same reason the file/server classes are cut
from `main/edits.yaml`. `org/apache/kafka/test/TestUtils.java` is added because four of
the remaining tests call a handful of its methods — see "Trimming TestUtils" below.

## Edits — what is cut and why

### `main/edits.yaml`

Four files are edited to drop server-side / broker-config edges that are out of scope
for the proxy's protocol surface:

| File | Cut |
|---|---|
| `record/internal/DefaultRecordBatch` | nested `*FileChannelRecordBatch` + `FileLogInputStream`/`FileRecords` imports |
| `record/internal/AbstractLegacyRecordBatch` | nested `*FileChannelRecordBatch` + `FileLogInputStream`/`FileRecords` imports |
| `record/internal/CompressionType` | the 4 `levelValidator()` methods + config imports |
| `utils/Utils` | the 4 broker-config methods (`propsToMap`, `castToStringObjectMap`, `ensureConcreteSubclass`, `mergeConfigs`) + config imports |

The four file/server classes themselves are **not** copied: `FileRecords`,
`FileLogInputStream`, `UnalignedFileRecords`, `RemoteLogInputStream`.

### `test/edits.yaml`

Two test files are trimmed of tests that exercise the production methods `main/edits.yaml`
already strips:

| File | Cut |
|---|---|
| `compress/GzipCompressionTest` | `testLevelValidator` (tests the removed `levelValidator()` config-validator surface) |
| `utils/UtilsTest` | the 10 `testPropsToMap*`/`testCastToStringObjectMap*` methods, plus their shared `assertValue` helper (test the 4 removed broker-config methods) |

Three more files need a `qualifyNestedImports` entry (see "Qualifying nested-class
imports" below): `record/internal/AbstractLegacyRecordBatchTest`,
`record/internal/MemoryRecordsTest` and `utils/ImplicitLinkedHashMultiCollectionTest`.

### Qualifying nested-class imports

Some Kafka tests import a nested class from another type in the same package — e.g.
`MemoryRecordsTest` imports `MemoryRecords.RecordFilter`, both ending up in
`io.kroxylicious.kafka.common.record.internal`. OpenRewrite's `ChangePackage` treats
that import as now-redundant (same package) once it rewrites both files, and strips it —
correct for a top-level type, wrong for a nested one, which still needs either an import
or full qualification to be referenced by its simple name. Confirmed by running
`vendor.sh` and hitting "cannot find symbol" on the bare nested name. The
`qualifyNestedImports` directive works around it: given the fully-qualified nested name,
it drops the import and replaces every bare use of the simple name with the qualified
`Outer.Inner` form, so nothing depends on what OpenRewrite decides to do with the import.

### Trimming TestUtils

`org.apache.kafka.test.TestUtils` is a 60+ method grab-bag test helper (pulls in
`ConsumerConfig`, `Cluster`, `RequestHeader`, and other classes far outside anything we
vendor). The four forked tests that use it only call 9 of its methods in total —
`checkEquals` (2 overloads), `toList`, `randomString`, `tempFile` (2 overloads) and
`tempDirectory` (3 overloads, one calling into another) — plus the 4 private fields
those methods reference.

Rather than hand-writing a substitute helper (which wouldn't regenerate correctly if a
future test needs another `TestUtils` method, or if Kafka changes these ones),
`apply-edits.py` supports a **`preserveBlocks`** directive: the inverse of
`removeBlocks`/`stripImports` — instead of naming everything to cut, name the handful of
fields/methods and imports to *keep*, and everything else in the file is dropped. This
is self-correcting if Kafka adds more methods to `TestUtils` later (they're simply not
in the keep-list), and far less verbose than enumerating the ~50 unwanted methods by
hand.

One of the methods, `tempDirectory(Path, String)`, registers a JVM shutdown hook via
`org.apache.kafka.common.utils.Exit` (a class we don't vendor — it exists so tests can
swap out `System.exit()`/`Runtime.halt()` behaviour, orthogonal to what these tests
need). That registration is cut with an ordinary `removeBlocks` entry *before*
`preserveBlocks` runs, the same "cut an out-of-scope edge" treatment applied to the
production classes above.

## Build wiring (outside this directory)

- **Codec dependencies.** The vendored `compress` classes reference the native
  codecs (`zstd-jni`, `lz4-java`, `snappy-java`) at compile time. `kafka-clients`
  brings these transitively at *runtime* only, so `kroxylicious-api` declares them at
  compile scope, with versions managed in the root `pom.xml` alongside `kafka.version`.
- **jackson-core.** The vendored `MessageUtil` implements jackson-core's `TreeNode`
  directly (not just transitively through the already-declared `jackson-databind`), and
  the forked `MessageUtilTest` uses it too. It needs a `compile`-scope declaration, not
  `test` — an explicit scope on an otherwise-transitive dependency overrides Maven's
  scope mediation, so declaring it test-scoped would hide it from the main compile
  classpath even though `jackson-databind` already pulls it in transitively at compile
  scope. The dependency analyzer's bytecode scan only ever sees it referenced from test
  classes (implementing an interface with no method actually called elsewhere leaves no
  bytecode footprint for it to find), so it's listed in the root `pom.xml`'s
  `ignoredNonTestScopedDependencies` to stop it flagging that as an error.
- **Quality gates.** The vendored/forked tree is copied verbatim and does not follow
  this project's conventions, so `io.kroxylicious.kafka.*` (both `src/main` and
  `src/test`) is excluded from Checkstyle (`etc/checkstyle-suppressions.xml`), SpotBugs
  (`etc/spotbugs-exclude.xml`) and Error Prone (`-XepExcludedPaths` in the root
  `pom.xml`). Javadoc (`excludePackageNames` in the module `pom.xml`) only needs the
  `src/main` side, since the plugin doesn't process test sources by default. The
  generated message classes are emitted into their package directory so Javadoc can
  exclude them by package name.
