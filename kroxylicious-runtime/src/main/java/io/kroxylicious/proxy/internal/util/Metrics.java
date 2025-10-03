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

    // Base Metric Names

    private static final String CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME = "kroxylicious_client_to_proxy_request";
    private static final String PROXY_TO_SERVER_REQUEST_BASE_METER_NAME = "kroxylicious_proxy_to_server_request";
    private static final String SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME = "kroxylicious_server_to_proxy_response";
    private static final String PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME = "kroxylicious_proxy_to_client_response";
    private static final String CLIENT_TO_PROXY_ERROR_BASE_METER_NAME = "kroxylicious_client_to_proxy_errors";
    private static final String PROXY_TO_SERVER_ERROR_BASE_METER_NAME = "kroxylicious_proxy_to_server_errors";
    private static final String CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_connections";
    private static final String PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_connections";
    private static final String KROXYLICIOUS_SERVER_TO_PROXY_READS_PAUSED_NAME = "kroxylicious_server_to_proxy_reads_paused";
    private static final String KROXYLICIOUS_CLIENT_TO_PROXY_READS_PAUSED_NAME = "kroxylicious_client_to_proxy_reads_paused";
    private static final String CLIENT_TO_PROXY_ACTIVE_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_active_connections";
    private static final String PROXY_TO_SERVER_ACTIVE_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_active_connections";
    private static final String SIZE_SUFFIX = "_size";

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

    private Metrics() {
        // unused
    }

    public static MeterProvider<Counter> clientToProxyMessageCounterProvider(String clusterName, Integer nodeId) {
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
        if ((Objects.isNull(value) || value.trim().isEmpty())) {
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

    public static void clear() {
        CLIENT_TO_PROXY_CONNECTION_CACHE.clear();
        PROXY_TO_SERVER_CONNECTION_CACHE.clear();
    }
}
