# Running OpenMessaging Benchmarks - Quick Start

Get baseline Kafka performance metrics in ~15 minutes.

## Prerequisites

- Kubernetes cluster running (minikube, kind, or cloud)
- `kubectl` and `helm` installed
- 8 CPU cores, 16GB RAM recommended

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

### 3. Deploy Benchmark Infrastructure

```bash
cd kroxylicious-openmessaging-benchmarks

# Deploy Kafka + OMB workers
helm install benchmark ./helm/kroxylicious-benchmark \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  -n kafka
```

### 4. Wait for Kafka to Start

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
```

## Run Benchmarks

The benchmark pod has a `$WORKERS` environment variable pre-configured with all worker URLs based on your `omb.workerReplicas` setting.

The default workload is `1topic-1kb` (1 topic, 1KB messages, 5 minute test).

### Run Default Benchmark

```bash
kubectl exec -it omb-benchmark -n kafka -- sh -c 'bin/benchmark --drivers /config/driver-kafka.yaml --workers "$WORKERS" /workloads/workload.yaml'
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

### Switch to Different Workload

To run a different workload, upgrade the Helm release with a different `omb.workload` value:

```bash
# 10 topics workload
helm upgrade benchmark ./helm/kroxylicious-benchmark \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  --set omb.workload=10topics-1kb \
  -n kafka

# Wait for pods to restart
kubectl rollout status statefulset/omb-worker -n kafka
kubectl delete pod omb-benchmark -n kafka
kubectl wait --for=condition=ready pod -l app=omb-benchmark -n kafka --timeout=60s

# Run benchmark
kubectl exec -it omb-benchmark -n kafka -- sh -c 'bin/benchmark --drivers /config/driver-kafka.yaml --workers "$WORKERS" /workloads/workload.yaml'
```

**Available workloads:**
- `1topic-1kb` - 1 topic, 1 partition (default)
- `10topics-1kb` - 10 topics, 1KB messages
- `100topics-1kb` - 100 topics, 1KB messages

## Re-running Benchmarks

Infrastructure stays running - just re-run the `kubectl exec` command to benchmark again.

## Cleanup

```bash
# Remove all benchmark infrastructure
helm uninstall benchmark -n kafka

# IMPORTANT: Delete Kafka persistent volumes to avoid cluster ID conflicts
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka

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

# Reinstall benchmark
helm install benchmark ./helm/kroxylicious-benchmark \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  -n kafka
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
# Verify Kafka is accessible
kubectl exec omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-kafka-bootstrap:9092 --list
```

## Configuration

Edit `helm/kroxylicious-benchmark/values.yaml` to change:
- Kafka brokers: `kafka.replicas` (default: 3)
- Benchmark duration: `benchmark.testDurationMinutes` (default: 5)
- Worker count: `omb.workerReplicas` (default: 3)

Then upgrade:
```bash
helm upgrade benchmark ./helm/kroxylicious-benchmark \
  -f ./helm/kroxylicious-benchmark/scenarios/baseline-values.yaml \
  -n kafka
```
