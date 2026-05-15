# Kroxylicious Sizing Guide

## Overview

This guide helps operators estimate the performance impact of deploying Kroxylicious as a proxy layer in front of Apache Kafka. It covers two configurations:

1. **Passthrough proxy** (no filters) — the baseline cost of the proxy layer itself
2. **Record encryption** — the additional cost of encrypting/decrypting record payloads

The goal is to give you the data you need to right-size your deployment: how much additional latency to expect, and where the throughput ceiling sits.

## Executive Summary

- **Passthrough proxy (no filters):** negligible overhead. Average publish latency increased by ~0.2 ms (+6%), with no measurable throughput impact. The proxy layer itself is not a bottleneck.

- **Record encryption (AES-256-GCM):** measurable but predictable overhead.
  - **Throughput:** ~26% reduction per partition (~37k msg/sec vs ~50k msg/sec baseline with 1 KB messages)
  - **Latency at sub-saturation rates:** average publish latency increased by 0.2–3 ms depending on load; p99 increased by 15–40 ms
  - **CPU cost:** encryption added ~33% CPU overhead (13% direct crypto, 20% buffer management/GC/re-encoding), consistent with the throughput reduction
  - **Scaling:** throughput scales linearly with CPU allocation — ~40,000 msg/s per core at 1 KB. Validated at 1, 2, and 4 cores. Set `requests == limits` for a predictable, stable thread count.

## Methodology

All benchmarks used [OpenMessaging Benchmark (OMB)](https://github.com/openmessaging/benchmark), an industry-standard tool for measuring messaging system performance. We ran three scenarios against the same Kafka cluster on the same hardware:

- **Baseline**: OMB producers/consumers talked directly to Kafka
- **Proxy (no filters)**: Traffic passed through Kroxylicious with no filter chain configured
- **Encryption**: Traffic passed through Kroxylicious with the record encryption filter enabled (AES-256-GCM, Vault KMS)

For throughput ceiling testing, we used **rate sweeps** — running the same workload at progressively higher producer rates and measuring where the system saturated (achieved rate dropped below 95% of target).

The primary workload used a single topic with a single partition. This deliberately concentrates all traffic on a single broker, reaching the broker's per-partition throughput limit at lower absolute rates and making proxy overhead easier to isolate and measure. Multi-topic workloads were used to verify that the overhead characteristics hold when load is spread across brokers.

### Workloads

| Workload | Topics | Partitions per topic | Message size | Producer rate |
|----------|--------|---------------------|--------------|---------------|
| 1topic-1kb | 1 | 1 | 1 KB | 50,000 msg/sec |
| 10topics-1kb | 10 | 1 | 1 KB | 5,000 msg/sec (per topic) |
| 100topics-1kb | 100 | 1 | 1 KB | 500 msg/sec (per topic) |

## Proxy Overhead (No Filters)

The passthrough proxy added minimal overhead. At sub-saturation rates, the additional latency was sub-millisecond on average.

### 10 topics, 1KB messages (5,000 msg/sec per topic)

| Metric | Baseline | Proxy | Delta |
|--------|----------|-------|-------|
| Publish latency avg | 2.62 ms | 2.79 ms | +0.17 ms (+6.6%) |
| Publish latency p99 | 14.09 ms | 15.17 ms | +1.08 ms (+7.7%) |
| E2E latency avg | 94.87 ms | 95.34 ms | +0.47 ms (+0.5%) |
| E2E latency p99 | 185.00 ms | 186.00 ms | +1.00 ms (+0.5%) |
| Publish rate | 5,002 msg/s | 5,002 msg/s | 0 |

### 100 topics, 1KB messages (500 msg/sec per topic)

| Metric | Baseline | Proxy | Delta |
|--------|----------|-------|-------|
| Publish latency avg | 2.66 ms | 2.82 ms | +0.16 ms (+6.2%) |
| Publish latency p99 | 5.54 ms | 6.07 ms | +0.53 ms (+9.5%) |
| E2E latency avg | 253.16 ms | 253.76 ms | +0.60 ms (+0.2%) |
| E2E latency p99 | 499.00 ms | 499.00 ms | 0 |
| Publish rate | 500 msg/s | 500 msg/s | 0 |

**Key takeaway**: The proxy layer itself added ~0.2 ms average publish latency and had no measurable impact on throughput. End-to-end latency was dominated by consumer fetch intervals, so the proxy's contribution was negligible in that metric.

## Record Encryption Overhead

Encryption added measurable latency and reduced the maximum sustainable throughput.

### Latency impact at sub-saturation rates

The encryption overhead scaled with producer rate. At lower rates the proxy had more headroom; as rate approached the saturation point, latency increased sharply.

1 topic, 1KB messages, RF=3 — baseline vs encryption at three sub-saturation rates:

| Rate | Metric | Baseline | Encryption | Delta |
|------|--------|----------|------------|-------|
| 10,400 | Publish latency p99 | 41 ms | 48 ms | +7 ms (+17%) |
| 13,400 | Publish latency p99 | 44 ms | 54 ms | +10 ms (+23%) |
| 14,600 | Publish latency p99 | 62 ms | 81 ms | +19 ms (+31%) |

### Multi-topic latency (sub-saturation)

At lower per-topic rates, encryption overhead is smaller in absolute terms.

**10 topics, 5,000 msg/sec total (~500 msg/sec per topic), RF=3:**

| Metric | Baseline | Encryption | Delta |
|--------|----------|------------|-------|
| Publish latency avg | 106 ms | 106 ms | 0 (noise) |
| Publish latency p99 | 207 ms | 198 ms | -9 ms (noise) |
| Publish latency p99.9 | 230 ms | 288 ms | +58 ms (+25%) |
| Publish rate | 50,084 msg/s | 50,069 msg/s | statistically identical |

At 500 msg/sec per partition, encryption overhead is invisible at p99 — latency is dominated
by broker-side batching and consumer fetch intervals. Only the p99.9 tail shows meaningful
increase.

**100 topics, 500 msg/sec total (~5 msg/sec per topic), RF=3:**

| Metric | Baseline | Encryption | Delta |
|--------|----------|------------|-------|
| Publish latency avg | 5.07 ms | 6.68 ms | +1.61 ms (+32%) |
| Publish latency p99 | 23.41 ms | 25.94 ms | +2.53 ms (+11%) |
| Publish latency p99.9 | 32.90 ms | 47.57 ms | +14.67 ms (+45%) |
| E2E latency avg | 257 ms | 259 ms | +2 ms (+0.9%) |
| E2E latency p99 | 503 ms | 506 ms | +3 ms (+0.6%) |
| Publish rate | 50,036 msg/s | 50,072 msg/s | statistically identical |

### Throughput ceiling

Using rate sweeps on a single partition (1 topic, 1KB messages, RF=3, 5% steps from 8,000–20,000 msg/sec):

- **Baseline**: sustained up to **19,400 msg/sec**. Saturated at 20,000 (19,334 achieved, p99=4,199 ms).
- **Proxy (no filters)**: clean up to **18,200 msg/sec**. Noisy above that — some probes at 18,800 and 20,000 failed; 19,400 recovered. The ceiling is approximately 18,200 msg/sec.
- **Encryption**: sustained up to **14,600 msg/sec** (81 ms p99). First saturation at 15,200 (14,932 achieved, p99=4,809 ms). Intermittent behaviour above that — some probes sustained, others saturated.

**Estimated throughput cost of encryption: ~4,800 msg/sec (~25%) per partition.**

The absolute ceiling is lower than single-host benchmarks because RF=3 Kafka ISR replication
adds ~4 ms round-trip per ack, capping single-partition throughput to ~17–19k msg/sec regardless
of proxy CPU. This is the realistic production ceiling; the proxy CPU ceiling is much higher
(see [CPU sizing coefficient](#cpu-sizing-coefficient)).

| Rate range | Behaviour |
|------------|-----------|
| 8,000–14,600 | Stable. All probes sustained, p99 38–81 ms |
| 15,200 | First saturation (encryption); baseline still stable |
| 15,800–18,200 | Encryption intermittent; baseline and no-filters stable |
| 18,800–20,000 | All scenarios saturate |

### Where the CPU goes

Comparing CPU flamegraphs between the no-filter proxy and encryption showed that the no-filter proxy was almost entirely I/O-bound (59% of CPU in send/recv syscalls). Encryption added ~13% direct crypto cost (AES-256-GCM) plus ~20% indirect costs (buffer management, GC pressure, record re-encoding), totalling ~33% additional CPU — consistent with the ~26% throughput reduction.

For a detailed breakdown, see [CPU-PROFILE-ANALYSIS.md](CPU-PROFILE-ANALYSIS.md).

> **Caveat**: Each rate point was measured once. Environmental noise (pod scheduling, GC pauses) contributed to the intermittent behaviour above the knee. Multi-pass sweeps would give tighter bounds on the exact saturation point. See [Limitations](#limitations).

### CPU sizing coefficient

The per-partition ceiling understates aggregate proxy capacity because it conflates the Kafka broker partition limit with the proxy CPU limit. Using connection sweeps (fixed per-producer rate, increasing producer count, RF=1 workload to eliminate per-partition ceilings) we isolated the proxy's saturation point independent of any single partition — and validated that it scales linearly with CPU allocation.

**Measured coefficient: ~10 millicores per MB/s of proxy throughput** on AMD EPYC-Rome at 2 GHz with AES-NI.

This is a conservative planning figure derived from 10-topic workloads, which are more representative than single-topic benchmarks (where a single partition's throughput ceiling can artificially depress the measurement). More realistic deployments with many topics show lower overhead — see the validation table below.

Encrypt and decrypt have the same per-byte cost, so the coefficient applies uniformly regardless of direction:

**Sizing formula:**

```
CPU (millicores) = 10 × total_proxy_MB_per_s
```

where `total_proxy_MB_per_s` is the sum of all traffic flowing through the proxy — count produce throughput once (encrypted), and each consumer group's consume throughput once (decrypted independently). Fan-out is handled naturally: a topic with 1 producer at 100 MB/s and 5 consumer groups each reading 100 MB/s totals 600 MB/s × 10 mc = 6,000 mc.

Equivalently: **~1 core per 100 MB/s of proxy traffic**, or **~40,000 msg/s per core at 1 KB per stream** (equivalent to 80 MB/s bidirectional per core for 1:1 produce/consume).

Example sizing table (1 KB messages):

| Scenario | Total proxy traffic | CPU required |
|----------|---------------------|--------------|
| 40k msg/s, 1 consumer group | 80 MB/s | 800 mc (~1 core) |
| 100k msg/s, 1 consumer group | 200 MB/s | 2,000 mc (2 cores) |
| 100k msg/s, 3 consumer groups | 400 MB/s | 4,000 mc (4 cores) |
| 200k msg/s, 1 consumer group | 400 MB/s | 4,000 mc (4 cores) |

Add ×1.3 headroom for GC pauses and burst traffic.

**Empirical basis — validated across 9 configurations:**

Connection sweeps were run at 1, 2, and 4 CPU cores across 1, 10, and 100 topic workloads (RF=1, 1 KB messages, 10k msg/s per producer, steps 1–32 producers). The coefficient was extracted from JFR CPU data using `analyze-cpu-coefficient.py`.

| Config | Coefficient | Stdev | n |
|--------|-------------|-------|---|
| 1 core, 1 topic | 15.7 mc/MB/s | ±8.2 | 4 |
| 2 core, 1 topic | 24.3 mc/MB/s | ±17.7 | 5 |
| 4 core, 1 topic | 30.5 mc/MB/s | ±18.6 | 5 |
| **1 core, 10 topics** | **10.0 mc/MB/s** | **±7.8** | **6** |
| 2 core, 10 topics | 19.9 mc/MB/s | ±16.1 | 6 |
| 4 core, 10 topics | 25.1 mc/MB/s | ±16.1 | 6 |
| 1 core, 100 topics | 4.3 mc/MB/s | ±2.5 | 5 |
| 2 core, 100 topics | 6.8 mc/MB/s | ±3.5 | 5 |
| 4 core, 100 topics | 8.0 mc/MB/s | ±4.4 | 5 |

The **1-core, 10-topic** measurement (10.0 mc/MB/s) is the basis for the sizing formula. The large stdev across all series reflects fixed per-connection overhead that is high at low producer counts and amortises as throughput scales — the coefficient is not constant but the mean is a reasonable planning figure.

The 1-topic series has elevated coefficients because a single partition concentrates all traffic on one broker; the proxy runs hot while Kafka itself becomes the bottleneck. The 100-topic series (4–8 mc/MB/s) represents a more realistic production deployment where load is spread across many partitions and brokers — in practice, the proxy overhead is significantly lower than the formula suggests. The 10 mc/MB/s figure is deliberately conservative.

The Kroxylicious event loop thread count defaults to `Runtime.getRuntime().availableProcessors()` (configurable via `NettySettings.workerThreadCount`). Each event loop thread handles its assigned connections serially, so proxy throughput scales with the number of threads and therefore with CPU allocation.

On JDK 21 with `-XX:+UseContainerSupport` (on by default), `availableProcessors()` is computed from the container's CFS CPU quota: `ceil(cpu.cfs_quota_us / cpu.cfs_period_us)`. For `cpu: 1000m` this gives `ceil(100ms / 100ms) = 1`; for `cpu: 2000m`, `2`; and so on. Crucially, **only CPU limits drive this — CPU requests do not**. Since JDK 21, CPU shares/requests were removed from the processor count calculation entirely. A pod with a CPU request but no limit will cause the JVM to see all host CPUs and spawn far more event loop threads than the CPU budget can sustain. Setting `requests == limits` is therefore essential for predictable thread counts, not merely a scheduling nicety. For absolute certainty, set `NettySettings.workerThreadCount` explicitly.

For 1:1 produce/consume this simplifies to `20 × produce_MB_per_s`; for fan-out, sum each consumer group separately.

> **Hardware note**: AES-NI is present on the test CPUs. On CPUs without hardware AES acceleration the coefficient will be significantly higher. Always validate with a load test before committing to a production sizing.

## Sizing Recommendations

### Passthrough proxy

The proxy layer adds negligible overhead. Size your Kafka cluster as you normally would — the proxy will not be the bottleneck.

### Record encryption

When planning for record encryption:

1. **CPU sizing**: Use the coefficient formula from [CPU sizing coefficient](#cpu-sizing-coefficient):
   ```
   CPU (millicores) = 10 × total_proxy_MB_per_s
   ```
   where `total_proxy_MB_per_s` = produce MB/s + each consumer group's consume MB/s.
   For balanced 1:1 workloads this is `20 × produce_MB_per_s`. For fan-out topics, sum
   the full consume throughput across all consumer groups. Add ×1.3 headroom for GC
   pauses and burst. Set CPU *requests* equal to *limits* (guaranteed QoS class).
   Without a CPU limit, the JVM sees all host CPUs and spawns more event loop
   threads than the CPU budget can sustain. For absolute certainty, set
   `NettySettings.workerThreadCount` explicitly.

2. **Throughput budget**: Expect ~25% throughput reduction per partition compared to direct Kafka access when the proxy has 1 core available. With more cores, the proxy ceases to be the bottleneck before the partition limit is reached.

3. **Latency budget**: At sub-saturation rates, expect:
   - Average publish latency to increase 2-34% depending on how close to the saturation point (~0.2-3 ms additional)
   - p99 publish latency to increase 30-50% (~15-40 ms additional)
   - Overhead scales with rate — well below saturation, encryption adds very little latency

4. **Scaling**: The proxy scales vertically (more cores per pod) and horizontally (more pods). Both approaches are effective. Horizontal scaling reduces blast radius per pod.

5. **KMS overhead**: DEK generation and caching means the KMS is not in the hot path for every record. In our tests, KMS operations were low (5-19 `generate_dek_pair` calls per benchmark run). KMS latency is unlikely to be a factor unless your KMS is very slow or unavailable.

## Limitations

- **Single-pass measurements**: Each rate point was measured once. Environmental noise (pod scheduling, GC pauses, noisy neighbours) can affect individual probes. Multi-pass aggregation would improve confidence.
- **Single proxy pod**: All tests used a single proxy instance. Horizontal scaling characteristics are not yet measured.
- **Single partition saturation**: The 1-topic rate sweep workload hits Kafka's per-partition throughput limit (~50k msg/sec), which is close to the encryption ceiling. The connection sweep and coefficient derivation used a 10-topic RF=1 workload to eliminate this ceiling.
- **Hardware-specific**: Encryption is CPU-bound. Results will differ on other CPU architectures and clock speeds.
- **Coefficient validated at 1, 2, and 4 cores across 1, 10, and 100 topic workloads**: The 10 mc/MB/s planning figure is anchored to the 10-topic, 1-core measurement. Behaviour beyond 4 cores is untested but expected to follow the same model.

## Appendix: Test Environment

| Component | Details |
|-----------|---------|
| Cluster | 8 nodes, 8 vCPUs / 15 GB RAM each |
| CPU | AMD EPYC-Rome, 2000 MHz |
| OS | Red Hat Enterprise Linux CoreOS 9.6 |
| Kernel | 5.14.0-570.103.1.el9_6.x86_64 |
| Kubernetes | v1.34.6 |
| Kafka | 3 brokers (Strimzi) |
| Kroxylicious | 0.20.0, single proxy pod |
| KMS | HashiCorp Vault (in-cluster) |
| Benchmark tool | OpenMessaging Benchmark |
| Orchestrator | Linux amd64, 4 vCPUs, 7 GB RAM, AMD EPYC-Rome |
