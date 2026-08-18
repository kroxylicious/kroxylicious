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
checkout at the pinned tag, `refresh.sh` regenerates the entire committed vendored
tree deterministically.

## Pinned source

| | |
|---|---|
| Kafka tag | `4.3.0` (property `kafka.message-spec.version` in the root `pom.xml`) |
| Commit | `a9ce3221537b8653448750697915607dc7936cf3` |

The spec/source version is deliberately **decoupled** from `kafka.version` (the
`kafka-clients` we depend on, currently `4.2.0`): the vendored/generated protocol
surface can track a different Kafka release than the client jar. When bumping,
change `kafka.message-spec.version` and re-run `refresh.sh` against a matching
checkout.

## Files

| File | Purpose |
|---|---|
| `refresh.sh` | Orchestrator. `refresh.sh <path-to-apache-kafka-checkout>` re-vendors the whole tree. |
| `closure.txt` | The frozen, package-relative list of Kafka `common/**` files to copy (the closure — see below). |
| `surgery.py` | Content-based edits that cut the server/config edges we do not vendor. |
| `rewrite.yml` | OpenRewrite recipe relocating `org.apache.kafka.common.*` → `io.kroxylicious.kafka.common.*`. |

## How `refresh.sh` works

1. **Copy** the `closure.txt` files from `<kafka>/clients/src/main/java/org/apache/kafka/common`.
2. **Surgery** (`surgery.py`) on 4 files to remove the file/server/config edges we
   don't take (see below). Matching is by signature + brace-balancing, so it
   survives line-number drift between Kafka releases; it fails loudly ("SURGERY
   MISS") rather than silently no-op if a target moves.
3. **Relocate** packages with OpenRewrite `ChangePackage` (`rewrite.yml`). This
   rewrites package declarations, imports and fully-qualified references, and moves
   the files onto the new package path. OpenRewrite honours `.gitignore`, so the
   staging area is its own throwaway git repo. `rewrite:run` forks the lifecycle
   through `compile`; the staged sources are not expected to compile there (they
   still name `org.apache.kafka`, and native codec libs are off the throwaway
   classpath), so compilation is made non-fatal — `ChangePackage` is textual and
   needs no type attribution.
4. **Sync** the result into `src/main/java`, wiping only the previously-vendored
   (non-`message`) tree first.

```console
$ ./refresh.sh /path/to/apache/kafka
==> copying 93 files from …/clients/src/main/java/org/apache/kafka/common
==> applying surgery
==> relocating packages with OpenRewrite (org.apache.kafka.common -> io.kroxylicious.kafka.common)
==> syncing into …/kroxylicious-api/src/main/java/io/kroxylicious/kafka/common
==> done: 93 support files vendored
```

After running, rebuild the module (`mvn -pl kroxylicious-api -am verify`) and commit
the changed tree.

## How the closure was computed

`closure.txt` is a **compiler-driven** closure. `javac` is used as the oracle with
`kafka-clients` off the classpath, so every Kafka symbol must resolve either from a
copied file or be flagged as an edge to cut. Starting from the generated message
classes, the hub packages (`compress`, `record`, `record/internal`, `header`,
`header/internals`) are seeded wholesale — minus the four file/server classes — and
the compiler fixpoint then pulls in the `protocol` / `utils` / `errors` leaves.
Seeding the hubs wholesale works around two `javac` blind spots:

- `record/internal/Record.java` is silently shadowed by the JDK's `java.lang.Record`
  (it never errors "missing"); and
- the compression impls (`GzipCompression`, …) are only referenced reflectively via
  a factory, so an import-closure misses them.

The result is a self-contained surface that compiles with **no** `org.apache.kafka.*`
references. Only `errors` is trimmed to the 5 classes actually reachable
(`ApiException`, `RetriableException`, `CorruptRecordException`,
`InvalidConfigurationException`, `UnsupportedVersionException`) rather than Kafka's
full ~130-class hierarchy.

## Surgery — what is cut and why

Four files are edited to drop server-side / broker-config edges that are out of
scope for the proxy's protocol surface:

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
