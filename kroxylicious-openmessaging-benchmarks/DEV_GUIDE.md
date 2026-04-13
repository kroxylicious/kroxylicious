# Benchmark Developer Guide

This guide is for contributors who want to modify, extend, or test the benchmark infrastructure.

For instructions on running benchmarks to measure performance, see [README.md](README.md) and
[QUICKSTART.md](QUICKSTART.md).

## Architecture

```
OpenMessaging Benchmark (Kafka driver)
        ↓
   bootstrap.servers config
        ↓
    Baseline Scenario → Kafka cluster directly
    Proxy Scenarios   → Kroxylicious → Kafka
```

The benchmark infrastructure is deployed via a single Helm chart (`helm/kroxylicious-benchmark`).
Scenarios are layered values files on top of the base chart. Scripts orchestrate
deploy → wait → run → collect → teardown for each scenario.

## Directory Structure

```
kroxylicious-openmessaging-benchmarks/
├── README.md
├── QUICKSTART.md
├── DEV_GUIDE.md (this file)
├── Containerfile
├── .dockerignore
├── scripts/
│   ├── setup-cluster.sh          # Install Strimzi + Kroxylicious operators (one-time)
│   ├── run-benchmark.sh          # Run one scenario end-to-end (deploy → benchmark → teardown)
│   ├── run-all-scenarios.sh      # Run baseline + proxy-no-filters and compare
│   ├── measure-overhead.sh       # Rate sweep (infra deployed once per scenario)
│   ├── compare-results.sh        # Compare two OMB result files (JBang wrapper)
│   ├── collect-results.sh        # Collect results and generate metadata (JBang wrapper)
│   ├── poll-proxy-metrics.sh     # Poll proxy /metrics during a run
│   └── poll-nmt.sh               # Poll OMB coordinator native memory during a run
├── src/main/java/.../results/
│   ├── OmbResult.java             # Jackson model for OMB result JSON
│   ├── ResultComparator.java      # Comparison logic: two OmbResults → formatted table
│   ├── CompareResults.java        # Picocli CLI entry point (JBang shebang)
│   ├── RunMetadata.java           # Generates run-metadata.json
│   └── CollectResults.java        # Picocli CLI entry point (JBang shebang)
├── helm/
│   └── kroxylicious-benchmark/
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── templates/
│       │   ├── _helpers.tpl
│       │   ├── kafka-strimzi.yaml
│       │   ├── kafka-nodepool.yaml
│       │   ├── kroxylicious-proxy.yaml
│       │   ├── kroxylicious-ingress.yaml
│       │   ├── kroxylicious-service.yaml
│       │   ├── kroxylicious-cluster.yaml
│       │   ├── omb-workers-statefulset.yaml
│       │   ├── omb-benchmark-job.yaml
│       │   └── configmaps/
│       │       ├── omb-driver-baseline.yaml
│       │       ├── workload-1topic-1kb.yaml
│       │       ├── workload-10topics-1kb.yaml
│       │       └── workload-100topics-1kb.yaml
│       └── scenarios/
│           ├── baseline-values.yaml
│           ├── smoke-values.yaml
│           └── proxy-no-filters-values.yaml
```

## OMB Container Image

The published `openmessaging/openmessaging-benchmark:latest` image ships Kafka client 1.0.0 on
Java 8 and is no longer suitable for benchmarking modern Kafka. We build our own image from the
current upstream source using the `Containerfile` in this directory.

### Image tag convention

Image tags follow the format `omb-<omb-sha7>-krox-<krox-sha7>-<build>`, for example
`omb-8559989-krox-a1b2c3d-42`. This encodes the upstream OMB commit, the Kroxylicious commit
used for build configuration, and a monotonically increasing build number.

The Helm chart's `omb.image` in `values.yaml` references a specific tag — never a floating tag
like `latest` — so builds are always reproducible.

### Building locally

```bash
podman build -f Containerfile -t kroxylicious-omb:test .
```

To build from a different upstream OMB commit:

```bash
podman build -f Containerfile \
  --build-arg OMB_COMMIT=<commit-sha> \
  -t kroxylicious-omb:test .
```

### CI workflow

The GitHub Actions workflow at `/.github/workflows/build-omb-image.yml` builds and pushes images
on demand via `workflow_dispatch`. It accepts two inputs:

| Input | Required | Description |
|-------|----------|-------------|
| `omb_ref` | Yes | Upstream OMB commit SHA, branch, or tag to build from |
| `kroxylicious_ref` | No (default: `main`) | Kroxylicious ref for build config |

The workflow resolves the OMB ref to a full SHA, computes the image tag, builds the image, and
pushes it to the registry configured via repository variables (`REGISTRY_SERVER`,
`REGISTRY_ORGANISATION`, `REGISTRY_USERNAME`, `REGISTRY_TOKEN`). When registry variables are not
configured (e.g. on forks), the image is built but not pushed.

### Renovate

A Renovate custom manager in `/.github/renovate.json` tracks the `omb.image` reference in
`values.yaml` and opens PRs when new image builds are pushed to the registry.

## Testing the Helm Chart

The project includes tests to verify the Helm chart renders correctly and stays working as
changes are made.

### Prerequisites

- Java 17+
- Maven 3.6+
- Helm 3.0+

### Running tests

```bash
# Run all tests
mvn clean test

# Run only template rendering tests
mvn test -Dtest=HelmTemplateRenderingTest

# Run only helm lint test
mvn test -Dtest=HelmLintTest
```

### Test coverage

**`HelmTemplateRenderingTest`** validates Helm template rendering:

- Templates render without errors and produce valid Kubernetes resources
- Strimzi Kafka CR has correct default replica count and version
- Configurable broker replica counts (1, 3, 5)
- Default durations are production-quality (15 min test, 5 min warmup)
- Smoke profile overrides durations and reduces infrastructure sizing
- Kroxylicious CRs are rendered/suppressed correctly based on `kroxylicious.enabled`
- Worker URLs and node ID ranges are generated correctly for each replica count

**`HelmLintTest`** validates that the Helm chart passes linting with no warnings or errors.

### Test implementation

1. **`HelmUtils`** — executes `helm template` and `helm lint` CLI commands and parses the
   resulting YAML into Fabric8 `GenericKubernetesResource` objects for type-safe assertions
2. Fluent assertions with AssertJ
3. JUnit 5 parameterized tests for multi-value configurations

## Profiling implementation

When a proxy pod is present, `run-benchmark.sh` patches the proxy deployment to start JFR and
async-profiler at JVM startup. This section covers the implementation details.

### How JFR is started and stopped

JFR is started via `JAVA_TOOL_OPTIONS` injected into the proxy container at startup:

```
-XX:StartFlightRecording=name=benchmark,disk=true,dumponexit=true,maxsize=64m
```

`disk=true` streams chunks continuously to `/tmp/benchmark.jfr` on the PVC-backed `/tmp` mount.
After the benchmark, `run-benchmark.sh` issues `jcmd JFR.stop` which finalises the recording.

### How async-profiler is started and stopped

async-profiler is loaded as a JVMTI agent at JVM startup via the `-agentpath` flag in
`ASYNC_PROFILER_FLAGS`, configured to write a CPU flamegraph to `/tmp/flamegraph.html`. After
the benchmark, `run-benchmark.sh` stops the profiler using `jcmd JVMTI.agent_load` with a stop
command — re-attaching the already-loaded agent to flush the output, with no `asprof` binary
required.

### Why both tools are started at JVM startup rather than mid-run

Starting either tool via `jcmd` after the JVM is already running caused the proxy pod to be
OOM-killed. Starting JFR via `jcmd JFR.start` allocates recording buffers (global + per-thread)
as a sudden mid-run spike; on a memory-limited pod this pushes the process over its cgroup limit.
async-profiler has the same risk when started mid-run.

Initialising both tools at JVM startup allows the JVM to account for the memory from the
beginning, avoiding the spike entirely. Stopping via `jcmd` is safe because no new buffers are
allocated — the stop command simply flushes and closes an already-running recording.

Both recordings are copied off the PVC by `collect-results.sh` using a short-lived debug pod.

## Result Scripts

JBang-based CLI tools for working with OMB result files. Requires
[JBang](https://www.jbang.dev/download/) and a Maven build
(`mvn process-sources -pl kroxylicious-openmessaging-benchmarks`) to resolve dependency versions.

### collect-results.sh

```bash
./scripts/collect-results.sh ./results/
```

Finds the benchmark Job pod, copies result JSON files from `/var/lib/omb/results`, copies JFR
and flamegraph files from the JFR PVC (via a short-lived debug pod), and generates
`run-metadata.json` with git commit, branch, and UTC timestamp. Set `NAMESPACE` to override the
default namespace (`kafka`).

### poll-proxy-metrics.sh

Started automatically by `run-benchmark.sh` for proxy scenarios. Polls the proxy management
endpoint (`/metrics` on port 9190) throughout the run, writing timestamped Prometheus snapshots
to `proxy-metrics.txt` in the output directory.

Each snapshot is preceded by a `benchmark_sample_timestamp_seconds` gauge containing the Unix
epoch of that sample, making it straightforward to align metrics to the benchmark timeline.
The polling interval defaults to 30 seconds and is controlled by `METRICS_INTERVAL`.

### poll-nmt.sh

Started automatically by `run-benchmark.sh`. Polls the OMB coordinator pod for native memory
(NMT) statistics throughout the run, writing snapshots to `nmt.txt`. Useful for diagnosing
native memory growth in the coordinator JVM. The polling interval is controlled by
`MEMORY_TRACKING_INTERVAL` (default: 10 seconds).

## Planned

- Encryption scenario (RecordEncryption filter + Vault)
- Encryption+Auth scenario (RecordEncryption + Authorization filters)
- On-cluster sweep coordinator ([#3493](https://github.com/kroxylicious/kroxylicious/issues/3493))
  — run `measure-overhead.sh` as a Kubernetes Job so long sweeps don't require a laptop to stay awake

## Contributing

This project follows Kroxylicious contribution guidelines:

- All commits must be signed off with DCO: `git commit -s`
- Follow the conventional commit format for PR titles
- Add `Assisted-by: Claude Sonnet 4.6 <noreply@anthropic.com>` to commits assisted by Claude
