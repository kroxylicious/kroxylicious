# Running OpenMessaging Benchmarks - Quick Start

Get baseline Kafka performance metrics in ~15 minutes.

## Prerequisites

- Kubernetes cluster running (minikube, kind, or cloud)
- `kubectl` and `helm` installed
- 8 CPU cores, 16GB RAM recommended
- [JBang](https://www.jbang.dev/download/) (for result scripts)
- **Kroxylicious operator** installed (proxy scenarios only — see step 3)

## Setup (One-time)

### 1. Start Kubernetes

```bash
# Minikube
minikube start --cpus 8 --memory 16384

# Or Kind
kind create cluster
```

### 2. Install Strimzi Operator

```bash
kubectl create namespace kafka
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Wait for operator (~30 seconds)
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

### 3. Install Kroxylicious Operator (proxy scenarios only)

> Skip this step if you are running the baseline scenario.

```bash
# Download operator release
gh release download v0.18.0 --repo kroxylicious/kroxylicious --pattern 'kroxylicious-operator-*.tar.gz'

# Extract
tar xzf kroxylicious-operator-*.tar.gz

# Install CRDs + operator
kubectl apply -f install/

# Wait for operator pod
kubectl wait --for=condition=ready pod -l app=kroxylicious -n kroxylicious-operator --timeout=300s
```

### 4. Deploy Benchmark Infrastructure

```bash
# Choose your scenario:
SCENARIO=baseline-values              # Direct Kafka, no proxy
# SCENARIO=proxy-no-filters-values    # Kroxylicious proxy, no filters

helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml
```

### 5. Wait for Kafka to Start

This takes **5-10 minutes** for Kafka cluster to bootstrap:

```bash
# Watch until STATUS shows "Ready"
kubectl get kafka kafka -n kafka -w
# Press Ctrl+C when ready

# Or use wait command (may take up to 10 minutes)
kubectl wait kafka/kafka --for=condition=Ready --timeout=600s -n kafka
```

Verify everything is running:
```bash
kubectl get pods -n kafka

# Should see:
# strimzi-cluster-operator-xxx    Running
# kafka-kafka-pool-0               Running  (Kafka broker 0)
# kafka-kafka-pool-1               Running  (Kafka broker 1)
# kafka-kafka-pool-2               Running  (Kafka broker 2)
# omb-worker-xxx (3 pods)          Running
# omb-benchmark                    Running
#
# Proxy scenarios will also show:
# benchmark-proxy-xxx              Running  (Kroxylicious proxy)
```

## Run Benchmarks

The benchmark pod has a `$WORKERS` environment variable pre-configured with all worker URLs based on your `omb.workerReplicas` setting.

The default workload is `1topic-1kb` (1 topic, 1KB messages).

### Verify Bootstrap Target

OMB does not log the bootstrap servers it connects to. To confirm which scenario is active, check the driver ConfigMap:

```bash
kubectl get configmap omb-driver-baseline -n kafka -o jsonpath='{.data.driver-kafka\.yaml}' | grep bootstrap
# baseline:  bootstrap.servers=kafka-kafka-bootstrap:9092
# proxy:     bootstrap.servers=kafka-cluster-ip-bootstrap:9292
```

### Verify Bootstrap Target

OMB does not log the bootstrap servers it connects to. To confirm which scenario is active, check the driver ConfigMap:

```bash
kubectl get configmap omb-driver-baseline -n kafka -o jsonpath='{.data.driver-kafka\.yaml}' | grep bootstrap
# baseline:  bootstrap.servers=kafka-kafka-bootstrap:9092
# proxy:     bootstrap.servers=kafka-cluster-ip-bootstrap:9292
```

### Run Default Benchmark

```bash
kubectl exec -it deploy/omb-benchmark -n kafka -- sh -c '/opt/benchmark/bin/benchmark --drivers /etc/omb/driver/driver-kafka.yaml --workers "$WORKERS" /etc/omb/workloads/workload.yaml'
```

**What you'll see:**
```
Starting benchmark...
Running warmup phase (1 minute)...
Running test phase (5 minutes)...

Results:
Pub rate: 45,234 msg/s | 44.1 MB/s
Consume rate: 45,234 msg/s | 44.1 MB/s
Pub Latency avg: 12.3ms | 95th: 28.5ms | 99th: 45.2ms
```

### Save Results

OMB writes JSON result files to the benchmark pod's working directory. Copy them off the pod before upgrading or uninstalling, as `helm upgrade` restarts pods and results are lost:

```bash
# Find the benchmark pod name
BENCHMARK_COORDINATOR=$(kubectl get pod -l app=omb-benchmark -n kafka -o jsonpath='{.items[0].metadata.name}')

# Copy result files to local machine
kubectl cp ${BENCHMARK_COORDINATOR}:. ./results/ -n kafka
```

### Compare Results

After saving results from baseline and proxy runs, compare them side-by-side:

```bash
# Build filtered sources (one-time, after checkout or version changes)
mvn process-sources -pl kroxylicious-openmessaging-benchmarks

# Compare baseline vs proxy results
./kroxylicious-openmessaging-benchmarks/scripts/compare-results.sh \
  results/baseline.json results/proxy.json
```

This outputs latency and throughput tables with deltas and percentage changes.

### Switch to Different Workload

To run a different workload, upgrade the Helm release with a different `omb.workload` value (use whichever scenario values file you deployed with):

```bash
# Set SCENARIO to match what you deployed with:
SCENARIO=baseline-values              # Direct Kafka, no proxy
# SCENARIO=proxy-no-filters-values    # Kroxylicious proxy, no filters

# 10 topics workload
helm upgrade benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  --set omb.workload=10topics-1kb \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml

# Wait for pods to restart
kubectl rollout status statefulset/omb-worker -n kafka
kubectl rollout restart deploy/omb-benchmark -n kafka
kubectl wait --for=condition=ready pod -l app=omb-benchmark -n kafka --timeout=60s

# Run benchmark
kubectl exec -it deploy/omb-benchmark -n kafka -- sh -c '/opt/benchmark/bin/benchmark --drivers /etc/omb/driver/driver-kafka.yaml --workers "$WORKERS" /etc/omb/workloads/workload.yaml'
```

**Available workloads:**
- `1topic-1kb` - 1 topic, 1 partition (default)
- `10topics-1kb` - 10 topics, 1KB messages
- `100topics-1kb` - 100 topics, 1KB messages

### Switch Scenario

To switch between baseline and proxy scenarios (e.g. after running baseline, run proxy-no-filters):

```bash
# Save results first! helm upgrade restarts pods and results are lost.

# Set the new scenario
SCENARIO=proxy-no-filters-values    # or baseline-values

helm upgrade benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml

# Wait for pods to restart
kubectl rollout status statefulset/omb-worker -n kafka
kubectl rollout restart deploy/omb-benchmark -n kafka
kubectl wait --for=condition=ready pod -l app=omb-benchmark -n kafka --timeout=60s

# Verify the bootstrap target changed
kubectl get configmap omb-driver-baseline -n kafka -o jsonpath='{.data.driver-kafka\.yaml}' | grep bootstrap
```

> **Note:** When switching *to* a proxy scenario, the Kroxylicious operator must reconcile
> the new CRs before benchmarks can run. The operator must be installed first (see step 3).

## Re-running Benchmarks

Infrastructure stays running - just re-run the `kubectl exec` command to benchmark again.

## Cleanup

```bash
# Remove all benchmark infrastructure
helm uninstall benchmark -n kafka

# IMPORTANT: Delete Kafka persistent volumes to avoid cluster ID conflicts
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka

# (Optional) Remove Kroxylicious operator (proxy scenarios only)
kubectl delete -f install/

# (Optional) Delete Strimzi operator
kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
kubectl delete namespace kafka
```

## Troubleshooting Kafka Cluster ID Mismatch

If you see `Invalid cluster.id` errors after reinstalling:

```bash
# Delete old Kafka data
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka

# Wait for PVCs to be deleted
kubectl get pvc -n kafka

# Reinstall benchmark (use the SCENARIO you were using)
helm install benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml
```

## Troubleshooting

**Kafka pods not starting?**
```bash
kubectl describe kafka kafka -n kafka
# Check Events section at bottom
```

**OMB workers failing?**
```bash
kubectl logs -l app=omb-worker -n kafka
```

**Benchmark can't connect?**
```bash
# Verify Kafka is accessible (baseline)
kubectl exec deploy/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-kafka-bootstrap:9092 --list

# Verify Kafka is accessible via proxy (proxy scenarios)
kubectl exec deploy/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-cluster-ip-bootstrap:9292 --list
```

## Configuration

Edit `kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/values.yaml` to change:
- Kafka brokers: `kafka.replicas` (default: 3)
- Benchmark duration: `benchmark.testDurationMinutes` (default: 15)
- Worker count: `omb.workerReplicas` (default: 3)

Then upgrade (use the `SCENARIO` you deployed with):
```bash
helm upgrade benchmark ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark \
  -n kafka \
  -f ./kroxylicious-openmessaging-benchmarks/helm/kroxylicious-benchmark/scenarios/${SCENARIO}.yaml
```
