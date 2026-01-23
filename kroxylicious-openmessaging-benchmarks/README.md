# Kroxylicious OpenMessaging Benchmarks

Performance benchmarking for Kroxylicious using the [OpenMessaging Benchmark Framework](https://openmessaging.cloud/).

## Overview

This project provides Helm charts and automation scripts for benchmarking Kroxylicious performance against baseline Kafka. It uses OpenMessaging Benchmark's standard Kafka driver - no custom code required.

**Key Insight:** Kroxylicious is a transparent Kafka proxy. We simply change `bootstrap.servers` to point to Kroxylicious instead of Kafka directly.

## Current Status: Phase 1 Complete ✅

**Phase 1 (Baseline Scenario)** is implemented and ready for testing:
- Helm chart foundation
- Kafka cluster (3 brokers in KRaft mode)
- OpenMessaging Benchmark workers and driver
- Baseline scenario (direct Kafka connection, no proxy)
- 3 workload configurations (1, 10, and 100 topics)

## Architecture

```
OpenMessaging Benchmark (Kafka driver)
        ↓
   bootstrap.servers config
        ↓
    Baseline Scenario → Kafka cluster directly
    (Future: Proxy Scenarios → Kroxylicious → Kafka)
```

## Directory Structure

```
kroxylicious-openmessaging-benchmarks/
├── README.md (this file)
├── helm/
│   └── kroxylicious-benchmark/
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── templates/
│       │   ├── kafka-statefulset.yaml
│       │   ├── omb-workers-deployment.yaml
│       │   ├── omb-benchmark-pod.yaml
│       │   └── configmaps/
│       │       ├── omb-driver-baseline.yaml
│       │       ├── workload-1topic-1kb.yaml
│       │       ├── workload-10topics-1kb.yaml
│       │       └── workload-100topics-1kb.yaml
│       └── scenarios/
│           └── baseline-values.yaml
```

## Prerequisites

- Kubernetes cluster (minikube, kind, or cloud provider)
- `kubectl` configured to access the cluster
- `helm` 3.0+
- Sufficient resources: 8 CPU cores, 16GB RAM recommended

## Quick Start

### 1. Install Baseline Scenario

```bash
cd kroxylicious-openmessaging-benchmarks

# Install Helm chart with baseline scenario
helm install benchmark ./helm/kroxylicious-benchmark \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml

# Wait for all pods to be ready
kubectl wait --for=condition=ready pod -l app=kafka --timeout=300s
kubectl wait --for=condition=ready pod -l app=omb-worker --timeout=300s
kubectl wait --for=condition=ready pod -l app=omb-benchmark --timeout=300s
```

### 2. Run Benchmark

The `omb-benchmark` pod is ready for manual benchmark execution:

```bash
# Get list of OMB workers
WORKERS="omb-worker-0.omb-worker:8080,omb-worker-1.omb-worker:8080,omb-worker-2.omb-worker:8080"

# Run benchmark (1 topic workload)
kubectl exec -it omb-benchmark -- \
  bin/benchmark \
  --drivers /config/driver-kafka.yaml \
  --workers $WORKERS \
  /workloads/workload.yaml

# Results will be printed to console
```

### 3. Cleanup

```bash
helm uninstall benchmark
kubectl delete pvc -l app=kafka  # Clean up persistent volumes
```

## Configuration

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

### Benchmark Duration

Adjust test duration in `values.yaml`:

```yaml
benchmark:
  testDurationMinutes: 5
  warmupDurationMinutes: 1
```

## Validation

Helm chart passes linting:

```bash
helm lint helm/kroxylicious-benchmark
# 1 chart(s) linted, 0 chart(s) failed
```

## Next Phases (Planned)

**Phase 2: Proxy Scenarios**
- No-filters (empty filter chain)
- Encryption (RecordEncryption filter)
- Encryption+Auth (RecordEncryption + Authorization filters)

**Phase 3: Automation**
- `run-benchmark.sh` - Automated execution
- `run-all-scenarios.sh` - Multi-scenario testing
- `compare-results.sh` - Performance comparison

**Phase 4: Documentation**
- Setup guide
- Running guide
- Results interpretation

## Troubleshooting

### Kafka pods not starting

Check Kafka logs:
```bash
kubectl logs kafka-0
```

Ensure sufficient resources are available:
```bash
kubectl describe pod kafka-0
```

### OMB workers not ready

Check worker logs:
```bash
kubectl logs -l app=omb-worker
```

Verify worker endpoints:
```bash
kubectl get pods -l app=omb-worker -o wide
```

### Benchmark fails

Check if Kafka is accessible from benchmark pod:
```bash
kubectl exec omb-benchmark -- kafka-topics --bootstrap-server kafka-0.kafka:9092 --list
```

## Contributing

This project follows Kroxylicious contribution guidelines:
- All commits must be signed off with DCO: `git commit -s`
- Add: `Assisted-by: Claude Sonnet 4.5 <noreply@anthropic.com>` to commits

## License

Apache License 2.0
