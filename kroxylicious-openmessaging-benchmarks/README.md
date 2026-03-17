# Kroxylicious OpenMessaging Benchmarks

Performance benchmarking for Kroxylicious using the [OpenMessaging Benchmark Framework](https://openmessaging.cloud/).

**Key insight:** Kroxylicious is a transparent Kafka proxy. We measure overhead by running the same
workload twice — once connecting directly to Kafka (baseline) and once through the proxy — and
comparing the results.

For information on modifying or extending the benchmark infrastructure, see [DEV_GUIDE.md](DEV_GUIDE.md).

## Prerequisites

- Kubernetes cluster with `kubectl` configured
  - Smoke / local validation: 4 CPU, 8 GB RAM (single-node minikube/kind is fine)
  - Full benchmark run: 8 CPU, 16 GB RAM across the cluster (3 brokers + 3 OMB workers)
- `helm` 3.0+
- `gh` (GitHub CLI) — for downloading the Kroxylicious operator release
- `jbang` — for result comparison scripts ([install](https://www.jbang.dev/download/))
- `mvn` — for generating JBang source filters

## Quickstart

See [QUICKSTART.md](QUICKSTART.md) for the full walkthrough. The short version:

### 1. Set up cluster operators (once per cluster)

```bash
cd kroxylicious-openmessaging-benchmarks
./scripts/setup-cluster.sh
```

### 2. Run the proxy overhead comparison

```bash
./scripts/run-all-scenarios.sh ./results/run-$(date +%Y%m%d-%H%M%S)/
```

This runs `baseline` and `proxy-no-filters` across all three workloads and prints a side-by-side
comparison. Each run deploys fresh infrastructure and tears it down afterwards.

### 3. Compare results

```bash
./scripts/compare-results.sh ./results/baseline/*.json ./results/proxy/*.json
```

## Configuration

### Scenarios

Two scenarios are available:

- **baseline** — OMB connects directly to Kafka; no proxy in the path
- **proxy-no-filters** — OMB connects through Kroxylicious with an empty filter chain

### Workloads

| Workload | Topics | Total partitions | Message size |
|----------|--------|-----------------|--------------|
| `1topic-1kb` | 1 | 1 | 1 KB |
| `10topics-1kb` | 10 | 10 | 1 KB |
| `100topics-1kb` | 100 | 100 | 1 KB |

The producer rate is configurable. Workloads share the rate evenly across topics.

### Profiles

The chart defaults to **production-quality durations** (15 min test, 5 min warmup) with 3 Kafka
brokers and 3 OMB workers. For quick iteration, use the **smoke profile**:

```bash
./scripts/run-benchmark.sh \
  --profile ./helm/kroxylicious-benchmark/scenarios/smoke-values.yaml \
  baseline 1topic-1kb ./results/smoke/
```

| Setting | Production (default) | Smoke |
|---------|---------------------|-------|
| Test duration | 15 min | 1 min |
| Warmup duration | 5 min | 0 (disabled) |
| Kafka brokers | 3 | 1 |
| Replication factor | 3 | 1 |
| OMB workers | 3 | 2 |
| Kafka memory limit | 4Gi | 2Gi |
| OMB worker memory limit | 6Gi | 2Gi |

The smoke profile is **not suitable for performance measurement** — it exists only to verify
deployment, connectivity, and workload execution.

### Measuring overhead across a rate range

`measure-overhead.sh` deploys each scenario once and sweeps through a sequence of producer rates,
collecting one OMB result per step:

```bash
./scripts/measure-overhead.sh \
  --workload 10topics-1kb \
  --min-rate 30000 \
  --max-rate 80000 \
  --step-percent 25 \
  --output-dir ./results/sizing-$(date +%Y%m%d-%H%M%S)/
```

Use `--dry-run` to preview the rate sequence without deploying anything.

### Using a custom proxy image

By default the Kroxylicious operator deploys the image that shipped with the installed operator
version. To benchmark an unreleased build:

```bash
QUAY_ORG=your-quay-org
IMAGE_TAG=your-image-tag
kubectl set env deployment/kroxylicious-operator -n kroxylicious-operator \
  KROXYLICIOUS_IMAGE=quay.io/${QUAY_ORG}/kroxylicious-proxy:${IMAGE_TAG}
```

Clear it afterwards:

```bash
kubectl set env deployment/kroxylicious-operator -n kroxylicious-operator KROXYLICIOUS_IMAGE-
```

## Running on OpenShift

The benchmark scripts work on OpenShift clusters (tested with cluster-bot on AWS) with the
following additional considerations.

### Namespace security policy and SCCs

`setup-cluster.sh` automatically:

1. Labels the `kafka` namespace with `pod-security.kubernetes.io/enforce=privileged` — required
   because the JFR and async-profiler patches set `seccompProfile: Unconfined` on the proxy
   container, which the default `restricted` policy blocks.

2. Detects OpenShift (by checking for the `security.openshift.io` API group) and grants the
   `privileged` SCC to the `default` service account in the `kafka` namespace. OpenShift enforces
   SCCs independently of PodSecurity admission, and `seccompProfile: Unconfined` requires the
   `privileged` SCC.

If the SCC grant fails, `setup-cluster.sh` will print a warning with the manual command to run:

```bash
oc adm policy add-scc-to-user privileged -z default -n kafka
```

### Operator version requirement

JFR recording and flamegraph collection require the Kroxylicious operator to inject environment
variables and a PVC volume mount into the proxy deployment. This support was added after the
0.19.0 release and will be available in 0.20.0. Until then, follow the
[Custom Proxy Image](#using-a-custom-proxy-image) instructions to build and deploy a custom
operator image from `main` alongside the custom proxy image.

## Profiling

When a proxy pod is present in the benchmark namespace (default: `kafka`), `run-benchmark.sh`
automatically captures profiling data alongside the benchmark results.

**JFR (Java Flight Recorder)** is a low-overhead JVM profiler built into the JDK. It captures
JVM-level events — GC pauses, lock contention, thread states, and allocation pressure — giving
insight into whether bottlenecks are inside the JVM itself.

**async-profiler** complements JFR by sampling CPU activity at the native level, including stack
frames from native code such as Netty's epoll event loops. These frames are invisible to JFR,
which only sees the JVM boundary. The output is an interactive flamegraph showing where CPU time
is actually spent.

Together they answer different questions: JFR tells you _what the JVM is doing_, async-profiler
tells you _where the CPU is spending time_, including code the JVM doesn't own.

Results are written to the output directory alongside the benchmark JSON:

- `<scenario>-<workload>-benchmark.jfr` — JVM Flight Recording
- `<scenario>-<workload>-flamegraph.html` — CPU flamegraph

### Known limitation: warmup data is included

Both tools record from JVM startup, meaning the warmup phase (JIT compilation, class loading,
connection establishment) is included in the output. This can obscure steady-state hotspots.
See [#3445](https://github.com/kroxylicious/kroxylicious/issues/3445) for planned improvements.

For implementation details on how profiling is started and stopped, see [DEV_GUIDE.md](DEV_GUIDE.md).

## Interpreting Results

`compare-results.sh` outputs a table with three sections:

- **Publish Latency** — producer-side latency percentiles (avg, 50th, 75th, 95th, 99th, 99.9th)
- **End-to-End Latency** — latency from produce to consume
- **Throughput** — publish and consume rates in msg/s and MB/s

Each row shows the baseline value, proxy value, absolute delta, and percentage change.
Negative latency delta = proxy is faster; positive = proxy adds latency.

For proxy scenarios, `run-benchmark.sh` also polls the proxy management endpoint (`/metrics` on
port 9190) throughout the run, writing timestamped Prometheus snapshots to `proxy-metrics.txt`
in the output directory. The polling interval defaults to 30 seconds and can be overridden:

```bash
METRICS_INTERVAL=10 ./scripts/run-benchmark.sh proxy-no-filters 1topic-1kb ./results/
```

## Troubleshooting

### Kafka pods not starting

```bash
kubectl describe kafka kafka -n kafka
kubectl logs -l strimzi.io/cluster=kafka -n kafka --tail=50
```

### OMB workers failing

```bash
kubectl logs -l app=omb-worker -n kafka
```

### Benchmark can't connect to Kafka

```bash
# Baseline — direct connection
kubectl exec job/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-kafka-bootstrap:9092 --list

# Proxy scenario
kubectl exec job/omb-benchmark -n kafka -- \
  kafka-topics --bootstrap-server kafka-cluster-ip-bootstrap:9292 --list
```

### Cluster ID conflict after reinstall

If you see `Invalid cluster.id` errors, stale PVCs from a previous run are the cause:

```bash
kubectl delete pvc -l strimzi.io/cluster=kafka -n kafka
```

### Verify which bootstrap target is active

OMB does not log the bootstrap address it connects to. Check the driver ConfigMap:

```bash
kubectl get configmap omb-driver-baseline -n kafka \
  -o jsonpath='{.data.driver-kafka\.yaml}' | grep bootstrap
# baseline:       bootstrap.servers=kafka-kafka-bootstrap:9092
# proxy scenario: bootstrap.servers=kafka-cluster-ip-bootstrap:9292
```

## License

Apache License 2.0
