<!--

    Copyright Kroxylicious Authors.

    Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0

-->
# Vendored Kafka protocol support classes

`kroxylicious-api` exposes a copy of Apache Kafka's protocol surface under the
`io.kroxylicious.kafka.common.*` namespace, so that the API references no
`org.apache.kafka.*` types. That surface has two halves:

1. **Generated message classes** — the `*Data` / `*DataJsonConverter` classes and
   `ApiMessageType`, produced on every build by the forked message generator
   (`kroxylicious-kafka-message-generator`) from the protocol JSON specs. These are
   **not** committed; they land in `target/generated-sources/kafka-messages`.
2. **Vendored support classes** — the ~90 non-generated classes the generated code
   needs at runtime (records, compression, protocol readers/writers, a handful of
   utils and errors). These **are** committed, under
   `kroxylicious-api/src/main/java/io/kroxylicious/kafka/common/**`.

This directory holds the tooling that (re)produces the *second* half. It is a
**reproducible process, not a one-off hand copy**: given a clean Apache Kafka
checkout at the pinned tag, `vendor.sh` regenerates the entire committed vendored
tree deterministically.

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
| `vendor.sh` | Orchestrator. `vendor.sh <path-to-apache-kafka-checkout>` re-vendors the whole tree. |
| `vendored-files.txt` | The frozen, package-relative list of Kafka `common/**` files to copy (the closure — see below). |
| `edits.yaml` | Declarative content-based edits that cut the server/config edges we do not vendor. |
| `apply-edits.py` | Generic engine that applies `edits.yaml` against the freshly copied files. |
| `requirements.txt` | Pinned Python dependency (`PyYAML`) needed to parse `edits.yaml`. |
| `rewrite.yml` | OpenRewrite recipe relocating `org.apache.kafka.common.*` → `io.kroxylicious.kafka.common.*`. |
| `rewrite-pom.xml` | Throwaway pom used to invoke the OpenRewrite recipe against the staging copy. |

## How `vendor.sh` works

1. **Copy** the `vendored-files.txt` files from
   `<kafka>/clients/src/main/java/org/apache/kafka/common`.
2. **Edit** (`apply-edits.py` from `edits.yaml`) to cut the server/config edges
   we don't vendor (see below) — fails loudly if a target has moved rather than
   silently no-op-ing.
3. **Relocate** packages to `io.kroxylicious.kafka.common.*` with OpenRewrite.
4. **Sync** the result into `kroxylicious-api/src/main/java`, wiping only the
   previously-vendored tree first.
5. **Format** by running `mvn -pl kroxylicious-api -am process-sources`, so the
   build's `formatter-maven-plugin`/`impsort-maven-plugin` are applied. The raw
   vendored output is not what gets committed.

```console
$ ./vendor.sh /path/to/apache/kafka
==> copying 93 files from …/clients/src/main/java/org/apache/kafka/common
==> applying edits (edits.yaml)
==> relocating packages with OpenRewrite (org.apache.kafka.common -> io.kroxylicious.kafka.common)
==> syncing into …/kroxylicious-api/src/main/java/io/kroxylicious/kafka/common
==> formatting (mvn -pl kroxylicious-api -am process-sources)
==> done: 93 support files vendored
```

After running, rebuild and verify the module (`mvn -pl kroxylicious-api -am verify`)
and commit the changed tree.

## Deriving the file list

`vendored-files.txt` was derived by compiling the generated message classes with
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

## Edits — what is cut and why

Four files are edited (see `edits.yaml`) to drop server-side / broker-config edges
that are out of scope for the proxy's protocol surface:

| File | Cut |
|---|---|
| `record/internal/DefaultRecordBatch` | nested `*FileChannelRecordBatch` + `FileLogInputStream`/`FileRecords` imports |
| `record/internal/AbstractLegacyRecordBatch` | nested `*FileChannelRecordBatch` + `FileLogInputStream`/`FileRecords` imports |
| `record/internal/CompressionType` | the 4 `levelValidator()` methods + config imports |
| `utils/Utils` | the 4 broker-config methods (`propsToMap`, `castToStringObjectMap`, `ensureConcreteSubclass`, `mergeConfigs`) + config imports |

The four file/server classes themselves are **not** copied: `FileRecords`,
`FileLogInputStream`, `UnalignedFileRecords`, `RemoteLogInputStream`.

## Build wiring (outside this directory)

- **Codec dependencies.** The vendored `compress` classes reference the native
  codecs (`zstd-jni`, `lz4-java`, `snappy-java`) at compile time. `kafka-clients`
  brings these transitively at *runtime* only, so `kroxylicious-api` declares them at
  compile scope, with versions managed in the root `pom.xml` alongside `kafka.version`.
- **Quality gates.** The vendored tree is copied verbatim and does not follow this
  project's conventions, so it is excluded from Checkstyle
  (`etc/checkstyle-suppressions.xml`), SpotBugs (`etc/spotbugs-exclude.xml`),
  Error Prone (`-XepExcludedPaths` in the root `pom.xml`) and Javadoc
  (`excludePackageNames` in the module `pom.xml`). The generated message classes are
  emitted into their package directory so Javadoc can exclude them by package name.
