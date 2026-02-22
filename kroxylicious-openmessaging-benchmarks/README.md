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

## Current Status: Phase 2 Complete ✅

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
│   ├── compare-results.sh        # Compare two OMB result files (JBang wrapper)
│   └── collect-results.sh        # Collect results and generate metadata (JBang wrapper)
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
- **Strimzi Kafka Operator** installed in the cluster
- **Kroxylicious operator** installed in the cluster (proxy scenarios only)
- Sufficient resources: 8 CPU cores, 16GB RAM recommended (4 CPU, 8GB RAM with smoke profile)

## Quick Start

### 1. Install Strimzi Operator

```bash
# Install Strimzi operator
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Wait for operator to be ready
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

### 2. Install Kroxylicious Operator (proxy scenarios only)

Skip this step for baseline scenarios. See [QUICKSTART.md](QUICKSTART.md) for detailed operator installation instructions.

```bash
gh release download v0.18.0 --repo kroxylicious/kroxylicious --pattern 'kroxylicious-operator-*.tar.gz'
tar xzf kroxylicious-operator-*.tar.gz
kubectl apply -f install/
kubectl wait --for=condition=ready pod -l app=kroxylicious -n kroxylicious-operator --timeout=300s
```

### 3. Install Benchmark Scenario

```bash
# Choose your scenario:
SCENARIO=baseline-values              # Direct Kafka, no proxy
# SCENARIO=proxy-no-filters-values    # Kroxylicious proxy, no filters

# Install Helm chart with the configured scenario (production durations: 15 min test + 5 min warmup)
helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml

# Or for quick validation, add the smoke profile (1 min test, 1 broker, 2 workers):
# helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
#   -n kafka \
#   -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml \
#   -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/smoke-values.yaml

# Wait for Kafka cluster to be ready
kubectl wait kafka/kafka --for=condition=Ready --timeout=300s -n kafka

# Wait for all pods to be ready
kubectl wait --for=condition=ready pod -l strimzi.io/cluster=kafka --timeout=300s -n kafka
kubectl wait --for=condition=ready pod -l app=omb-worker --timeout=300s -n kafka
kubectl wait --for=condition=ready pod -l app=omb-benchmark --timeout=300s -n kafka
```

### 4. Run Benchmark

The `omb-benchmark` deployment is ready for manual benchmark execution:

```bash
# Get list of OMB workers
WORKERS="omb-worker-0.omb-worker:8080,omb-worker-1.omb-worker:8080,omb-worker-2.omb-worker:8080"

# Run benchmark (1 topic workload)
kubectl exec -it deploy/omb-benchmark -n kafka -- \
  bin/benchmark \
  --drivers /config/driver-kafka.yaml \
  --workers $WORKERS \
  /workloads/workload.yaml

# Results will be printed to console
```

### 5. Cleanup

```bash
helm uninstall benchmark -n kafka
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka  # Clean up persistent volumes
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

### Collect Results

```bash
./kroxylicious-openmessaging-benchmarks/scripts/collect-results.sh ./results/
```

Finds the benchmark pod, copies result JSON files from `/var/lib/omb/results`, and generates `run-metadata.json` with git commit, branch, and UTC timestamp. Set `NAMESPACE` to override the default namespace (`kafka`).

## Next Phases (Planned)

**Phase 2: Proxy Scenarios**
- ✅ No-filters (empty filter chain) — complete
- Encryption (RecordEncryption filter)
- Encryption+Auth (RecordEncryption + Authorization filters)

**Phase 3: Automation**
- `run-benchmark.sh` - Automated execution
- `run-all-scenarios.sh` - Multi-scenario testing
- ✅ `compare-results.sh` - Performance comparison (JBang)
- ✅ `collect-results.sh` - Result collection (JBang)

**Phase 4: Documentation**
- Setup guide
- Running guide
- Results interpretation

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
