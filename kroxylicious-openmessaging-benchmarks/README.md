# Kroxylicious OpenMessaging Benchmarks

Performance benchmarking for Kroxylicious using the [OpenMessaging Benchmark Framework](https://openmessaging.cloud/).

## Overview

This project provides Helm charts and automation scripts for benchmarking Kroxylicious performance against baseline Kafka. It uses OpenMessaging Benchmark's standard Kafka driver - no custom code required.

**Key Insight:** Kroxylicious is a transparent Kafka proxy. We simply change `bootstrap.servers` to point to Kroxylicious instead of Kafka directly.

## OMB Container Image

The published `openmessaging/openmessaging-benchmark:latest` image ships Kafka client 1.0.0 on Java 8 and is no longer suitable for benchmarking modern Kafka.
We build our own image from the current upstream source (Kafka 3.6.1, JDK 17) using the `Containerfile` in this directory.

### Image tag convention

Image tags follow the format `omb-<omb-sha7>-krox-<krox-sha7>-<build>`, for example `omb-8559989-krox-a1b2c3d-42`.
This encodes the upstream OMB commit, the Kroxylicious commit used for build configuration, and a monotonically increasing build number.
The Helm chart's `omb.image` in `values.yaml` references a specific tag — never a floating tag like `latest` — so builds are always reproducible.

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

The GitHub Actions workflow at `/.github/workflows/build-omb-image.yml` (in the repository root) builds and pushes images on demand via `workflow_dispatch`.
It accepts two inputs:

| Input | Required | Description |
|-------|----------|-------------|
| `omb_ref` | Yes | Upstream OMB commit SHA, branch, or tag to build from |
| `kroxylicious_ref` | No (default: `main`) | Kroxylicious ref (branch, tag, or SHA) for build config |

The workflow resolves the OMB ref to a full SHA, computes the image tag, builds the image, and pushes it to the registry configured via repository variables (`REGISTRY_SERVER`, `REGISTRY_ORGANISATION`, `REGISTRY_USERNAME`, `REGISTRY_TOKEN`).
When registry variables are not configured (e.g. on forks), the image is built but not pushed.

### Renovate

A Renovate custom manager in `/.github/renovate.json` (in the repository root) tracks the `omb.image` reference in `values.yaml` and opens PRs when new image builds are pushed to the registry.

## Current Status: Phase 4 Complete ✅

**Phase 1 (Baseline Scenario)** — complete:
- Helm chart foundation
- Kafka cluster (3 brokers in KRaft mode)
- OpenMessaging Benchmark workers and driver
- Baseline scenario (direct Kafka connection, no proxy)
- 3 workload configurations (1, 10, and 100 topics)

**Phase 2 (Proxy No-Filters Scenario)** — complete:
- Kroxylicious operator CRs (KafkaProxy, KafkaProxyIngress, KafkaService, VirtualKafkaCluster)
- Proxy no-filters scenario (Kroxylicious with empty filter chain)
- Automatic bootstrap routing through proxy when enabled

**Phase 3 (Automation)** — complete:
- `setup-cluster.sh` — one-time operator installation (Strimzi + Kroxylicious)
- `run-benchmark.sh` — end-to-end scenario execution with automatic teardown
- `run-all-scenarios.sh` — runs baseline and proxy-no-filters, produces comparison

**Phase 4 (Proxy Metrics Collection)** — complete:
- `poll-proxy-metrics.sh` — polls proxy management endpoint during benchmark runs
- Prometheus snapshots written to `proxy-metrics.txt` alongside OMB result JSON

## Architecture

```
OpenMessaging Benchmark (Kafka driver)
        ↓
   bootstrap.servers config
        ↓
    Baseline Scenario → Kafka cluster directly
    Proxy Scenarios   → Kroxylicious → Kafka
```

## Directory Structure

```
kroxylicious-openmessaging-benchmarks/
├── README.md (this file)
├── QUICKSTART.md
├── Containerfile
├── .dockerignore
├── scripts/
│   ├── setup-cluster.sh          # Install Strimzi + Kroxylicious operators (one-time)
│   ├── run-benchmark.sh          # Run one scenario end-to-end (deploy → benchmark → teardown)
│   ├── run-all-scenarios.sh      # Run baseline + proxy-no-filters and compare
│   ├── compare-results.sh        # Compare two OMB result files (JBang wrapper)
│   ├── collect-results.sh        # Collect results and generate metadata (JBang wrapper)
│   └── poll-proxy-metrics.sh     # Poll proxy /metrics during a run (started by run-benchmark.sh)
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
│       │   ├── omb-benchmark-deployment.yaml
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

## Prerequisites

- Kubernetes cluster (minikube, kind, or cloud provider)
- `kubectl` configured to access the cluster
- `helm` 3.0+
- `gh` (GitHub CLI) — used by `setup-cluster.sh` to download the Kroxylicious operator
- `jbang` — used by the result scripts (see [JBang installation](https://www.jbang.dev/download/))
- `mvn` — used to generate JBang source filters before comparing results
- Sufficient resources: 8 CPU cores, 16GB RAM recommended (4 CPU, 8GB RAM with smoke profile)

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for the full walkthrough. The short version:

### 1. Set up cluster operators (once per cluster)

```bash
cd kroxylicious-openmessaging-benchmarks
./scripts/setup-cluster.sh
```

This installs Strimzi and the Kroxylicious operator and waits for both to be ready.

### 2. Run the proxy overhead comparison

```bash
./scripts/run-all-scenarios.sh ./results/run-$(date +%Y%m%d-%H%M%S)/
```

This runs the `baseline` and `proxy-no-filters` scenarios across all workloads sequentially,
then prints a side-by-side comparison. Each run deploys fresh infrastructure and tears it
down afterwards, so no manual cleanup is needed.

### 3. Run a single scenario manually

```bash
./scripts/run-benchmark.sh baseline 1topic-1kb ./results/baseline/
./scripts/run-benchmark.sh proxy-no-filters 1topic-1kb ./results/proxy/
./scripts/compare-results.sh ./results/baseline/*.json ./results/proxy/*.json
```

## Configuration

### Kafka Cluster Configuration

The Kafka cluster uses Strimzi. Key settings (default: 3 brokers):

```yaml
kafka:
  version: "4.1.1"  # Kafka version
  replicas: 5  # Number of Kafka brokers
  replicationFactor: 5  # Should be <= replicas
  minInSyncReplicas: 3  # Should be < replicationFactor
```

Or via `--set`:
```bash
helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  --set kafka.version=4.1.1 \
  --set kafka.replicas=5 \
  --set kafka.replicationFactor=5 \
  --set kafka.minInSyncReplicas=3
```

### Workloads

Three pre-configured workloads are available:
- **1topic-1kb**: Single topic, 1KB messages, 50K msg/sec target
- **10topics-1kb**: 10 topics, 1KB messages, 5K msg/sec per topic
- **100topics-1kb**: 100 topics, 1KB messages, 500 msg/sec per topic

To change the workload, update `values.yaml`:

```yaml
omb:
  workload: 10topics-1kb  # or 100topics-1kb
```

### Benchmark Profiles

The chart defaults to **production-quality durations** (15 min test, 5 min warmup) with 3 Kafka brokers and 3 OMB workers.
At 50K msg/sec this produces ~45M samples, sufficient for reliable latency measurement up to p99.9.

For quick validation during development, layer the **smoke profile** on top of a scenario:

```bash
helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/smoke-values.yaml \
  -n kafka
```

| Setting | Production (default) | Smoke |
|---------|---------------------|-------|
| Test duration | 15 min | 1 min |
| Warmup duration | 5 min | 0 (disabled) |
| Kafka brokers | 3 | 1 |
| Replication factor | 3 | 1 |
| OMB workers | 3 | 2 |
| Kafka memory request | 2Gi | 1Gi |
| OMB memory request | 2Gi | 1Gi |

The smoke profile is **not suitable for performance measurement** — it exists only to verify
deployment, connectivity, and workload execution.

To override individual duration settings without using the smoke profile:

```bash
helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  --set benchmark.testDurationMinutes=30 \
  --set benchmark.warmupDurationMinutes=10 \
  -n kafka
```

## Testing

The project includes test coverage to ensure the Helm chart works correctly and stays working as changes are made.

### Running Tests Locally

Prerequisites:
- Java 17+
- Maven 3.6+
- Helm 3.0+

```bash
# Run all tests
mvn clean test

# Run only template rendering tests
mvn test -Dtest=HelmTemplateRenderingTest

# Run only helm lint test
mvn test -Dtest=HelmLintTest
```

### Test Coverage

#### Template Rendering Tests (`HelmTemplateRenderingTest`)
Validates that Helm templates render correctly:
- Templates render without errors
- Valid Kubernetes resources are produced
- Strimzi Kafka CR has correct default replica count (3)
- Configurable broker replica counts (1, 3, 5)
- Default durations are production-quality (15 min test, 5 min warmup)
- Smoke profile overrides durations and reduces infrastructure

#### Helm Lint Test (`HelmLintTest`)
Validates that the Helm chart passes linting with no warnings or errors.

### Test Implementation

Tests use the following approach:
1. **HelmUtils** - Utility class that executes `helm template` and `helm lint` CLI commands
2. **YAML Parsing** - Parses rendered templates into Kubernetes resource maps using Jackson
3. **AssertJ** - Fluent assertions for validating resource structure
4. **JUnit 5** - Parameterized tests for testing multiple configurations

## Result Scripts

JBang-based CLI tools for working with OMB result files. Requires [JBang](https://www.jbang.dev/download/) and a Maven build (`mvn process-sources -pl kroxylicious-openmessaging-benchmarks`) to resolve dependency versions.

### Compare Results

```bash
./kroxylicious-openmessaging-benchmarks/scripts/compare-results.sh baseline.json candidate.json
```

Outputs a table with Publish Latency, End-to-End Latency, and Throughput sections showing baseline vs candidate values with deltas and percentage changes.

### Proxy Metrics

For proxy scenarios, `run-benchmark.sh` automatically polls the proxy management endpoint
(`/metrics` on port 9190) throughout the benchmark run, writing timestamped Prometheus
snapshots to `proxy-metrics.txt` in the output directory. Baseline scenario runs produce
no metrics file (there is no proxy pod).

Each snapshot is preceded by a `benchmark_sample_timestamp_seconds` gauge metric containing
the Unix epoch of that sample, making it straightforward to align metrics to the benchmark
timeline or graph them with external tooling.

The polling interval defaults to 30 seconds and can be overridden with `METRICS_INTERVAL`:

```bash
METRICS_INTERVAL=10 ./scripts/run-benchmark.sh proxy-no-filters 1topic-1kb ./results/
```

### Collect Results

```bash
./kroxylicious-openmessaging-benchmarks/scripts/collect-results.sh ./results/
```

Finds the benchmark pod, copies result JSON files from `/var/lib/omb/results`, and generates `run-metadata.json` with git commit, branch, and UTC timestamp. Set `NAMESPACE` to override the default namespace (`kafka`).

## Planned

- Encryption scenario (RecordEncryption filter + Vault)
- Encryption+Auth scenario (RecordEncryption + Authorization filters)

## Troubleshooting

### Kafka pods not starting

Check Kafka logs:
```bash
kubectl logs kafka-0 -n kafka
```

Ensure sufficient resources are available:
```bash
kubectl describe pod kafka-0 -n kafka
```

### OMB workers not ready

Check worker logs:
```bash
kubectl logs -l app=omb-worker -n kafka
```

Verify worker endpoints:
```bash
kubectl get pods -l app=omb-worker -o wide -n kafka
```

### Benchmark fails

Check if Kafka is accessible from benchmark pod:
```bash
kubectl exec deploy/omb-benchmark -n kafka -- kafka-topics --bootstrap-server kafka-kafka-bootstrap:9092 --list
```

## Contributing

This project follows Kroxylicious contribution guidelines:
- All commits must be signed off with DCO: `git commit -s`
- Add: `Assisted-by: Claude Sonnet 4.5 <noreply@anthropic.com>` to commits

## License

Apache License 2.0
