# Running OpenMessaging Benchmarks - Quick Start

Benchmark Kroxylicious proxy overhead in two steps: set up the cluster operators once,
then run the benchmark suite.

## Prerequisites

- Kubernetes cluster (minikube, kind, or cloud provider) with 8 CPU cores and 16GB RAM
- `kubectl` configured to access the cluster
- `helm` 3.0+
- `gh` (GitHub CLI) — for downloading the Kroxylicious operator release
- `jbang` — for result comparison scripts ([install](https://www.jbang.dev/download/))
- `mvn` — for generating JBang source filters

### Start a local cluster (if needed)

```bash
# Minikube
minikube start --cpus 8 --memory 16384

# Or Kind
kind create cluster
```

## Step 1: Set up cluster operators (once per cluster)

```bash
cd kroxylicious-openmessaging-benchmarks
./scripts/setup-cluster.sh
```

This creates the `kafka` namespace and installs the Strimzi and Kroxylicious operators,
waiting for each to be ready before proceeding.

**Options:**

```bash
# Pin operator versions
STRIMZI_VERSION=0.45.0 KROXYLICIOUS_VERSION=0.18.0 ./scripts/setup-cluster.sh

# Baseline scenario only (no Kroxylicious operator needed)
./scripts/setup-cluster.sh --skip-kroxylicious
```

## Step 2: Run the proxy overhead comparison

```bash
./scripts/run-all-scenarios.sh ./results/run-$(date +%Y%m%d-%H%M%S)/
```

This runs `baseline` and `proxy-no-filters` across all three workloads (1-topic, 10-topic, 100-topic).
For each scenario/workload combination it:

1. Deploys Kafka and OMB infrastructure via Helm
2. Waits for Kafka and OMB pods to be ready
3. Executes the benchmark
4. Collects result JSON and run metadata
5. Tears down (helm uninstall + PVC cleanup) ready for the next run

After all runs complete it prints a side-by-side comparison for each workload.

### Running a single scenario

To run one scenario and workload manually:

```bash
./scripts/run-benchmark.sh baseline 1topic-1kb ./results/baseline/
./scripts/run-benchmark.sh proxy-no-filters 1topic-1kb ./results/proxy/

# Compare afterwards
./scripts/compare-results.sh ./results/baseline/*.json ./results/proxy/*.json
```

Available scenarios: `baseline`, `proxy-no-filters`
Available workloads: `1topic-1kb`, `10topics-1kb`, `100topics-1kb`

### Quick validation (smoke profile)

The smoke profile uses 1 broker, 2 workers, and a 1-minute test — not suitable for
performance measurement but useful to verify the cluster is working before a full run:

```bash
helm install benchmark ./helm/kroxylicious-benchmark \
  -n kafka \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  -f ./helm/kroxylicious-benchmark/scenarios/smoke-values.yaml

kubectl wait kafka/kafka --for=condition=Ready --timeout=600s -n kafka
kubectl wait --for=condition=ready pod -l app=omb-benchmark -n kafka --timeout=300s

kubectl exec -it deploy/omb-benchmark -n kafka -- \
  sh -c '/opt/benchmark/bin/benchmark --drivers /etc/omb/driver/driver-kafka.yaml --workers "$WORKERS" /etc/omb/workloads/workload.yaml'

helm uninstall benchmark -n kafka
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka
```

## Interpreting Results

`compare-results.sh` outputs a table with three sections:

- **Publish Latency** — producer-side latency percentiles (avg, 50th, 75th, 95th, 99th, 99.9th)
- **End-to-End Latency** — latency from produce to consume
- **Throughput** — publish and consume rates in msg/s and MB/s

Each row shows the baseline value, proxy value, absolute delta, and percentage change.
Negative latency delta = proxy is faster; positive = proxy adds latency.

## Troubleshooting

**Kafka not starting**

```bash
kubectl describe kafka kafka -n kafka
# Check Events section for errors
kubectl logs -l strimzi.io/cluster=kafka -n kafka --tail=50
```

**OMB workers failing**

```bash
kubectl logs -l app=omb-worker -n kafka
```

**Benchmark can't connect to Kafka**

```bash
# Baseline — direct connection
kubectl exec deploy/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-kafka-bootstrap:9092 --list

# Proxy scenario
kubectl exec deploy/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-cluster-ip-bootstrap:9292 --list
```

**Cluster ID conflict after reinstall**

If you see `Invalid cluster.id` errors, stale PVCs from a previous run are the cause:

```bash
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka
```

**Verify which bootstrap target is active**

OMB does not log the bootstrap address it connects to. Check the driver ConfigMap:

```bash
kubectl get configmap omb-driver-baseline -n kafka \
  -o jsonpath='{.data.driver-kafka\.yaml}' | grep bootstrap
# baseline:       bootstrap.servers=kafka-kafka-bootstrap:9092
# proxy scenario: bootstrap.servers=kafka-cluster-ip-bootstrap:9292
```
