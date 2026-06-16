/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.binder.netty4.NettyAllocatorMetrics;
import io.micrometer.core.instrument.binder.netty4.NettyEventExecutorMetrics;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.channel.EventLoopGroup;

import io.kroxylicious.proxy.VersionInfo;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.globalRegistry;

public class Metrics {

    // Common Labels
    public static final String VIRTUAL_CLUSTER_LABEL = "virtual_cluster";
    public static final String NODE_ID_LABEL = "node_id";
    public static final String API_KEY_LABEL = "api_key";
    public static final String API_VERSION_LABEL = "api_version";
    public static final String DECODED_LABEL = "decoded";

    // Hot-reload / lifecycle labels
    public static final String OUTCOME_LABEL = "outcome";
    public static final String OPERATION_LABEL = "operation";
    public static final String STATE_LABEL = "state";
    public static final String FROM_STATE_LABEL = "from";
    public static final String TO_STATE_LABEL = "to";

    // Base Metric Names

    private static final String CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME = "kroxylicious_client_to_proxy_request";
    private static final String PROXY_TO_SERVER_REQUEST_BASE_METER_NAME = "kroxylicious_proxy_to_server_request";
    private static final String SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME = "kroxylicious_server_to_proxy_response";
    private static final String PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME = "kroxylicious_proxy_to_client_response";
    private static final String CLIENT_TO_PROXY_ERROR_BASE_METER_NAME = "kroxylicious_client_to_proxy_errors";
    private static final String PROXY_TO_SERVER_ERROR_BASE_METER_NAME = "kroxylicious_proxy_to_server_errors";
    private static final String CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_connections";
    private static final String CLIENT_TO_PROXY_DISCONNECTS_BASE_METER_NAME = "kroxylicious_client_to_proxy_disconnects";
    private static final String PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_connections";
    private static final String KROXYLICIOUS_SERVER_TO_PROXY_READS_PAUSED_NAME = "kroxylicious_server_to_proxy_reads_paused";
    private static final String KROXYLICIOUS_CLIENT_TO_PROXY_READS_PAUSED_NAME = "kroxylicious_client_to_proxy_reads_paused";
    private static final String CLIENT_TO_PROXY_ACTIVE_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_active_connections";
    private static final String PROXY_TO_SERVER_ACTIVE_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_active_connections";
    private static final String SIZE_SUFFIX = "_size";

    // Hot-reload metric names
    private static final String RECONFIGURE_COUNTER_NAME = "kroxylicious_reconfigure_total";
    private static final String RECONFIGURE_DURATION_NAME = "kroxylicious_reconfigure_duration_seconds";
    private static final String RECONFIGURE_CLUSTERS_AFFECTED_COUNTER_NAME = "kroxylicious_reconfigure_clusters_affected_total";
    private static final String DRAIN_DURATION_NAME = "kroxylicious_drain_duration_seconds";
    private static final String DRAIN_FORCE_CLOSED_COUNTER_NAME = "kroxylicious_drain_connections_force_closed_total";

    // Virtual cluster lifecycle metric names
    private static final String VIRTUAL_CLUSTER_STATE_NAME = "kroxylicious_virtual_cluster_state";
    private static final String VIRTUAL_CLUSTER_STATE_DURATION_NAME = "kroxylicious_virtual_cluster_state_duration_seconds";
    private static final String VIRTUAL_CLUSTER_TRANSITIONS_COUNTER_NAME = "kroxylicious_virtual_cluster_transitions_total";

    /**
     * Canonical lifecycle state label values (lower-cased {@code VirtualClusterLifecycleState}
     * record names). The whole set is registered together so every
     * {@code kroxylicious_virtual_cluster_state} series for a cluster shares an identical tag-key
     * set — required by {@code MeterRegistries}' tag-name-consistency guard.
     */
    private static final List<String> VIRTUAL_CLUSTER_STATES = List.of("initializing", "serving", "draining", "failed", "stopped");

    /**
     * Name of the build_info metric.  Note that the {@code .info} suffix is significant
     * to Micrometer and is used to indicate an 'info' metric to it.  The metric
     * name emitted by Prometheus will be called {@code kroxylicious_build_info}.
     */
    private static final String INFO_METRIC_NAME = "kroxylicious_build.info";

    /**
     * Cache for tracking the number of active connections from clients to the proxy.
     * This is used to provide metrics on the number of active connections per virtual cluster node.
     */
    private static final ConcurrentHashMap<VirtualClusterNode, AtomicInteger> CLIENT_TO_PROXY_CONNECTION_CACHE = new ConcurrentHashMap<>();

    /**
     * Cache for tracking the number of active connections from the proxy to servers.
     * This is used to provide metrics on the number of active connections per virtual cluster node.
     */
    private static final ConcurrentHashMap<VirtualClusterNode, AtomicInteger> PROXY_TO_SERVER_CONNECTION_CACHE = new ConcurrentHashMap<>();

    /**
     * Backs the {@code kroxylicious_virtual_cluster_state} state-set gauge: cluster name to
     * (state name to its 0/1 gauge value). Exactly one state per cluster reads 1 at any time.
     */
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicInteger>> VIRTUAL_CLUSTER_STATE_CACHE = new ConcurrentHashMap<>();

    private Metrics() {
        // unused
    }

    public static MeterProvider<Counter> clientToProxyMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME,
                "Count of the number of requests received by the proxy from the client.",
                clusterName, nodeId);
    }

    public static MeterProvider<DistributionSummary> proxyToClientMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME,
                "Distribution of the size, in bytes, of responses sent from the proxy to the client.",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<Counter> proxyToClientMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME,
                "Count of the number of responses sent from the proxy to the client.", clusterName,
                nodeId);
    }

    public static MeterProvider<DistributionSummary> clientToProxyMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME,
                "Distribution of the size, in bytes, of the requests received from the client.",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<DistributionSummary> serverToProxyMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME,
                "Distribution of the size, in bytes, of responses received from the server.",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<DistributionSummary> proxyToServerMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(PROXY_TO_SERVER_REQUEST_BASE_METER_NAME,
                "Distribution of size, in bytes, of requests sent to the server",
                clusterName,
                BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<Counter> serverToProxyMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME,
                "Count of the number of responses received by the proxy from the server.",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> proxyToServerMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_REQUEST_BASE_METER_NAME,
                "Count of the number of requests sent to the server.",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> clientToProxyConnectionCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME,
                "Count of the number of times a connection is accepted from the clients.", clusterName,
                nodeId);
    }

    /**
     * Creates a counter for tracking client to proxy disconnections by cause.
     *
     * @param clusterName the virtual cluster name
     * @param nodeId the node ID (can be null for bootstrap connections)
     * @param cause the disconnect cause label (e.g., "idle_timeout", "client_closed", "server_closed")
     * @return a meter provider for the disconnect counter
     */
    public static MeterProvider<Counter> clientToProxyDisconnectsCounter(String clusterName, @Nullable Integer nodeId, String cause) {
        return Counter
                .builder(CLIENT_TO_PROXY_DISCONNECTS_BASE_METER_NAME)
                .description("Count of client to proxy disconnections by cause.")
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(NODE_ID_LABEL, nodeIdToLabelValue(nodeId))
                .tag("cause", cause)
                .withRegistry(globalRegistry);
    }

    public static MeterProvider<Counter> clientToProxyErrorCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_ERROR_BASE_METER_NAME,
                "Count of the number of times a connection is closed due to any downstream error.",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> proxyToServerConnectionCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME,
                "Count of the number of times a connection is made to the server from the proxy.",
                clusterName,
                nodeId);
    }

    public static MeterProvider<Counter> proxyToServerErrorCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_ERROR_BASE_METER_NAME,
                "Count of the number of times a connection is closed due to any upstream error.",
                clusterName, nodeId);
    }

    public static MeterProvider<Timer> serverToProxyBackpressureTimer(String clusterName, @Nullable Integer nodeId) {
        return buildTimerMeterProvider(KROXYLICIOUS_SERVER_TO_PROXY_READS_PAUSED_NAME,
                "Timer showing how long the proxy has paused reading from a upstream connection because the downstream connection is unwriteable.",
                clusterName, nodeId);
    }

    public static MeterProvider<Timer> clientToProxyBackpressureTimer(String clusterName, @Nullable Integer nodeId) {
        return buildTimerMeterProvider(KROXYLICIOUS_CLIENT_TO_PROXY_READS_PAUSED_NAME,
                "Timer showing how long the proxy has paused reading from a downstream connection because the upstream connection is unwriteable.",
                clusterName, nodeId);
    }

    public static ActivationToken clientToProxyConnectionToken(VirtualClusterNode node) {
        AtomicInteger currentCounter = clientToProxyConnectionCounter(node);
        return new ActivationToken(currentCounter);
    }

    public static AtomicInteger clientToProxyConnectionCounter(VirtualClusterNode node) {
        return CLIENT_TO_PROXY_CONNECTION_CACHE.computeIfAbsent(node, n -> {
            AtomicInteger activeCount = new AtomicInteger();
            Gauge.builder(CLIENT_TO_PROXY_ACTIVE_CONNECTION_BASE_METER_NAME, activeCount, AtomicInteger::get)
                    .strongReference(true)
                    .description("Number of currently active connections from the client to the proxy.")
                    .tag(VIRTUAL_CLUSTER_LABEL, node.clusterName())
                    .tag(NODE_ID_LABEL, nodeIdToLabelValue(node.nodeId()))
                    .register(globalRegistry);
            return activeCount;
        });
    }

    public static ActivationToken proxyToServerConnectionToken(VirtualClusterNode node) {
        AtomicInteger currentCounter = proxyToServerConnectionCounter(node);
        return new ActivationToken(currentCounter);
    }

    public static AtomicInteger proxyToServerConnectionCounter(VirtualClusterNode node) {
        return PROXY_TO_SERVER_CONNECTION_CACHE.computeIfAbsent(node, n -> {
            AtomicInteger activeCount = new AtomicInteger();
            Gauge.builder(PROXY_TO_SERVER_ACTIVE_CONNECTION_BASE_METER_NAME, activeCount, AtomicInteger::get)
                    .strongReference(true)
                    .description("Number of currently active connections from the proxy to the server.")
                    .tag(VIRTUAL_CLUSTER_LABEL, node.clusterName())
                    .tag(NODE_ID_LABEL, nodeIdToLabelValue(node.nodeId()))
                    .register(globalRegistry);
            return activeCount;
        });
    }

    public static Counter taggedCounter(String counterName, List<Tag> tags) {
        return counter(counterName, tags);
    }

    public static List<Tag> tags(String name, String value) {
        return List.of(Tag.of(name, required(value)));
    }

    public static List<Tag> tags(String name1, String value1, String name2, String value2) {
        return List.of(Tag.of(name1, required(value1)), Tag.of(name2, required(value2)));
    }

    public static List<Tag> tags(String... tagsAndValues) {
        if (tagsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("tag name supplied without a value");
        }
        List<Tag> tagsList = new ArrayList<>();
        for (int i = 0; i < tagsAndValues.length; i += 2) {
            tagsList.add(Tag.of(tagsAndValues[i], required(tagsAndValues[i + 1])));
        }
        return List.copyOf(tagsList);
    }

    private static String required(String value) {
        if (Objects.isNull(value) || value.trim().isEmpty()) {
            throw new IllegalArgumentException("tag value supplied without a value");
        }
        return value;
    }

    private static String nodeIdToLabelValue(@Nullable Integer nodeId) {
        return Optional.ofNullable(nodeId).map(Object::toString).orElse("bootstrap");
    }

    private static MeterProvider<Counter> buildCounterMeterProvider(String meterName,
                                                                    String description,
                                                                    String clusterName,
                                                                    @Nullable Integer nodeId) {
        return Counter
                .builder(meterName)
                .description(description)
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(NODE_ID_LABEL, nodeIdToLabelValue(nodeId))
                .withRegistry(globalRegistry);
    }

    private static MeterProvider<Timer> buildTimerMeterProvider(String meterName,
                                                                String description,
                                                                String clusterName,
                                                                @Nullable Integer nodeId) {
        return Timer
                .builder(meterName)
                .description(description)
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(NODE_ID_LABEL, nodeIdToLabelValue(nodeId))
                .withRegistry(globalRegistry);
    }

    private static MeterProvider<DistributionSummary> buildDistributionSummaryMeterProvider(String meterName,
                                                                                            String description,
                                                                                            String clusterName,
                                                                                            String baseUnit,
                                                                                            @Nullable Integer nodeId) {
        var name = meterName + SIZE_SUFFIX;
        return DistributionSummary.builder(name)
                .baseUnit(baseUnit)
                .description(description)
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(NODE_ID_LABEL, nodeIdToLabelValue(nodeId))
                .withRegistry(globalRegistry);
    }

    /**
     * Exposes a <a href="https://www.robustperception.io/exposing-the-software-version-to-prometheus/">build info metric</a> describing Kroxylicious version etc.
     *
     * @param versionInfo version info
     */
    public static void versionInfoMetric(VersionInfo versionInfo) {
        Gauge.builder(INFO_METRIC_NAME, () -> 1.0)
                .description("Reports Kroxylicious version information")
                .tag("version", versionInfo.version())
                .tag("commit_id", versionInfo.commitId())
                .strongReference(true)
                .register(globalRegistry);
    }

    // --- Hot-reload ---

    /**
     * Counter of {@code reconfigure()} invocations that got past pre-flight validation, tagged
     * by {@code outcome} ({@code success}, {@code partial_failure}, {@code catastrophic}).
     * Pre-attempt rejections (concurrent / static-section change) are deliberately not counted
     * here — they surface as distinct exceptions and would otherwise pollute failure alerting.
     */
    public static Counter reconfigureCounter(String outcome) {
        return Counter.builder(RECONFIGURE_COUNTER_NAME)
                .description("Count of reconfigure() invocations by outcome.")
                .tag(OUTCOME_LABEL, outcome)
                .register(globalRegistry);
    }

    /**
     * Timer for end-to-end reconfigure duration. Percentile histogram is published because
     * reconfigures are rare events, so the extra bucket series are cheap and make the
     * distribution queryable in Prometheus.
     */
    public static Timer reconfigureDurationTimer() {
        return Timer.builder(RECONFIGURE_DURATION_NAME)
                .description("End-to-end duration of a reconfigure() invocation.")
                .publishPercentileHistogram()
                .register(globalRegistry);
    }

    /**
     * Counter of per-virtual-cluster operations applied during reconfigures, tagged by
     * {@code operation} ({@code add}, {@code remove}, {@code modify}) and {@code outcome}
     * ({@code success}, {@code failure}).
     */
    public static Counter reconfigureClustersAffectedCounter(String operation, String outcome) {
        return Counter.builder(RECONFIGURE_CLUSTERS_AFFECTED_COUNTER_NAME)
                .description("Count of per-virtual-cluster operations during reconfigures.")
                .tag(OPERATION_LABEL, operation)
                .tag(OUTCOME_LABEL, outcome)
                .register(globalRegistry);
    }

    /**
     * Timer for per-virtual-cluster connection drain duration. The {@code virtual_cluster}
     * label (absent from the Proposal 083 table) is included so a slow drain can be attributed
     * to a specific cluster, which the metric's stated purpose requires.
     */
    public static Timer drainDurationTimer(String clusterName) {
        return Timer.builder(DRAIN_DURATION_NAME)
                .description("Duration of a virtual cluster's connection drain.")
                .publishPercentileHistogram()
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .register(globalRegistry);
    }

    /**
     * Counter of connections force-closed after the drain timeout expired. Parallels the
     * existing {@code kroxylicious_client_to_proxy_disconnects{cause="drain_timeout"}} signal;
     * both are incremented for the same event by design — keep them in sync.
     */
    public static Counter drainConnectionsForceClosedCounter(String clusterName) {
        return Counter.builder(DRAIN_FORCE_CLOSED_COUNTER_NAME)
                .description("Count of connections force-closed after the drain timeout expired.")
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .register(globalRegistry);
    }

    // --- Virtual cluster lifecycle ---

    /**
     * Updates the {@code kroxylicious_virtual_cluster_state} state-set gauge so the series for
     * {@code state} reads 1 and every other state for {@code clusterName} reads 0.
     *
     * <p>The first call for a cluster registers the whole {@link #VIRTUAL_CLUSTER_STATES} set at
     * once. This must only be invoked once the proxy is past startup (i.e. on a lifecycle
     * transition, not at lifecycle construction)
     */
    public static void updateVirtualClusterState(String clusterName, String state) {
        var perCluster = VIRTUAL_CLUSTER_STATE_CACHE.computeIfAbsent(clusterName, Metrics::registerVirtualClusterStateGauges);
        perCluster.forEach((s, value) -> value.set(s.equals(state) ? 1 : 0));
    }

    private static ConcurrentHashMap<String, AtomicInteger> registerVirtualClusterStateGauges(String clusterName) {
        var perState = new ConcurrentHashMap<String, AtomicInteger>();
        VIRTUAL_CLUSTER_STATES.forEach(state -> perState.put(state, registerVirtualClusterStateGauge(clusterName, state)));
        return perState;
    }

    private static AtomicInteger registerVirtualClusterStateGauge(String clusterName, String state) {
        AtomicInteger value = new AtomicInteger();
        Gauge.builder(VIRTUAL_CLUSTER_STATE_NAME, value, AtomicInteger::get)
                .strongReference(true)
                .description("Current lifecycle state of the virtual cluster (1 for the active state, 0 otherwise).")
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(STATE_LABEL, state)
                .register(globalRegistry);
        return value;
    }

    /**
     * Timer for the time a virtual cluster spent in a given lifecycle state (recorded when it
     * leaves that state). Percentile histogram is published — lifecycle transitions are rare.
     */
    public static Timer virtualClusterStateDurationTimer(String clusterName, String state) {
        return Timer.builder(VIRTUAL_CLUSTER_STATE_DURATION_NAME)
                .description("Time a virtual cluster spent in a lifecycle state.")
                .publishPercentileHistogram()
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(STATE_LABEL, state)
                .register(globalRegistry);
    }

    /**
     * Counter of virtual cluster lifecycle state transitions, tagged by {@code from} and
     * {@code to} state.
     */
    public static Counter virtualClusterTransitionsCounter(String clusterName, String from, String to) {
        return Counter.builder(VIRTUAL_CLUSTER_TRANSITIONS_COUNTER_NAME)
                .description("Count of virtual cluster lifecycle state transitions.")
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(FROM_STATE_LABEL, from)
                .tag(TO_STATE_LABEL, to)
                .register(globalRegistry);
    }

    public static void clear() {
        CLIENT_TO_PROXY_CONNECTION_CACHE.clear();
        PROXY_TO_SERVER_CONNECTION_CACHE.clear();
        VIRTUAL_CLUSTER_STATE_CACHE.clear();
    }

    public static void bindNettyEventExecutorMetrics(final EventLoopGroup... eventLoopGroups) {
        for (final var eventLoopGroup : eventLoopGroups) {
            new NettyEventExecutorMetrics(eventLoopGroup).bindTo(globalRegistry);
        }
    }

    public static void bindNettyAllocatorMetrics(final ByteBufAllocator alloc) {
        if (alloc instanceof ByteBufAllocatorMetricProvider byteBufAllocatorMetricProvider) {
            new NettyAllocatorMetrics(byteBufAllocatorMetricProvider).bindTo(globalRegistry);
        }
    }

}
