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

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.BaseUnits;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.globalRegistry;
import static io.micrometer.core.instrument.Metrics.summary;

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
    private static final String CLIENT_TO_PROXY_ERROR_BASE_METER_NAME = "kroxylicious_client_to_proxy_errors_total";
    private static final String PROXY_TO_SERVER_ERROR_BASE_METER_NAME = "kroxylicious_proxy_to_server_errors_total";
    private static final String CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_connections_total";
    private static final String PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_connections_total";
    private static final String SIZE_SUFFIX = "_size";

    /**
     * @deprecated use kroxylicious_client_to_proxy_request_count instead.
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES = "kroxylicious_inbound_downstream_messages";

    /**
     * @deprecated use kroxylicious_client_to_proxy_request_count instead.
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES = "kroxylicious_inbound_downstream_decoded_messages";

    private static final String KROXYLICIOUS_DOWNSTREAM = "kroxylicious_downstream_";

    private static final String KROXYLICIOUS_UPSTREAM = "kroxylicious_upstream_";

    /**
     * @deprecated use {@link #clientToProxyMessageCounterProvider(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_DOWNSTREAM_CONNECTIONS = KROXYLICIOUS_DOWNSTREAM + "connections";

    /**
     * @deprecated use {@link #clientToProxyErrorCounter(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_DOWNSTREAM_ERRORS = KROXYLICIOUS_DOWNSTREAM + "errors";

    /**
     * @deprecated use {@link #proxyToServerConnectionCounter(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTIONS = KROXYLICIOUS_UPSTREAM + "connections";

    /**
     * @deprecated use {@link #proxyToServerConnectionCounter(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_ATTEMPTS = KROXYLICIOUS_UPSTREAM + "connection_attempts";

    /**
     * @deprecated use {@link #proxyToServerErrorCounter(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_FAILURES = KROXYLICIOUS_UPSTREAM + "connection_failures";

    /**
     * @deprecated use {@link #proxyToServerErrorCounter(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_ERRORS = KROXYLICIOUS_UPSTREAM + "errors";

    /**
     * @deprecated use {@link #clientToProxyMessageSizeDistributionProvider(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES = "kroxylicious_payload_size_bytes";

    public static final String FLOWING_TAG = "flowing";

    public static final String VIRTUAL_CLUSTER_TAG = "virtualCluster";

    public static final String DOWNSTREAM = "downstream";

    public static final String UPSTREAM = "upstream";

    private Metrics() {
        // unused
    }

    public static MeterProvider<Counter> clientToProxyMessageCounterProvider(String clusterName, Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME,
                "Incremented by one every time a request arrives at the proxy from the downstream (client).",
                clusterName, nodeId);
    }

    public static MeterProvider<DistributionSummary> proxyToClientMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME,
                "Records the message size of every response leaving for the downstream (client).",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<Counter> proxyToClientMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME,
                "Incremented by one every time a response goes from the proxy to the downstream (client).", clusterName,
                nodeId);
    }

    public static MeterProvider<DistributionSummary> clientToProxyMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME,
                "Records the message size of every request arriving from the downstream (client)",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<DistributionSummary> serverToProxyMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME,
                "Records the message size of every response arriving from the upstream (server).",
                clusterName, BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<DistributionSummary> proxyToServerMessageSizeDistributionProvider(String clusterName, @Nullable Integer nodeId) {
        return buildDistributionSummaryMeterProvider(PROXY_TO_SERVER_REQUEST_BASE_METER_NAME,
                "Records the message size of every request leaving for the upstream (server)",
                clusterName,
                BaseUnits.BYTES, nodeId);
    }

    public static MeterProvider<Counter> serverToProxyMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME,
                "Incremented by one every time a response arrives at the proxy from the upstream (server).",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> proxyToServerMessageCounterProvider(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_REQUEST_BASE_METER_NAME,
                "Incremented by one every time a request goes from the proxy to the upstream (server).",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> proxyToServerErrorCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_ERROR_BASE_METER_NAME, "Incremented by one every time a connection is closed due to any upstream error.",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> proxyToServerConnectionCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME, "Incremented by one every time a connection is made to the upstream from the proxy.",
                clusterName,
                nodeId);
    }

    public static MeterProvider<Counter> clientToProxyErrorCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_ERROR_BASE_METER_NAME, "Incremented by one every time a connection is closed due to any downstream error.",
                clusterName, nodeId);
    }

    public static MeterProvider<Counter> clientToProxyConnectionCounter(String clusterName, @Nullable Integer nodeId) {
        return buildCounterMeterProvider(CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME,
                "Incremented by one every time a connection is accepted from the downstream the proxy.", clusterName,
                nodeId);
    }

    @NonNull
    public static Counter taggedCounter(String counterName, List<Tag> tags) {
        return counter(counterName, tags);
    }

    /**
     * @deprecated use {@link #proxyToClientMessageSizeDistributionProvider(String, Integer)} instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    public static DistributionSummary payloadSizeBytesUpstreamSummary(ApiKeys apiKey, short apiVersion, String virtualCluster) {
        return payloadSizeBytesSummary(apiKey, apiVersion, UPSTREAM, virtualCluster);
    }

    /**
     * @deprecated use {@link #clientToProxyMessageSizeDistributionProvider(String, Integer)}  instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    public static DistributionSummary payloadSizeBytesDownstreamSummary(ApiKeys apiKey, short apiVersion, String virtualCluster) {
        return payloadSizeBytesSummary(apiKey, apiVersion, DOWNSTREAM, virtualCluster);
    }

    private static DistributionSummary payloadSizeBytesSummary(ApiKeys apiKey, short apiVersion, String flowing, String virtualCluster) {
        List<Tag> tags = tags(
                "ApiKey", apiKey.name(),
                "ApiVersion", String.valueOf(apiVersion),
                FLOWING_TAG, flowing,
                VIRTUAL_CLUSTER_TAG, virtualCluster);
        return summary(KROXYLICIOUS_PAYLOAD_SIZE_BYTES, tags);
    }

    public static List<Tag> tags(@NonNull String name, String value) {
        return List.of(Tag.of(name, required(value)));
    }

    public static List<Tag> tags(@NonNull String name1, String value1, @NonNull String name2, String value2) {
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

    @NonNull
    private static String required(String value) {
        if ((Objects.isNull(value) || value.trim().isEmpty())) {
            throw new IllegalArgumentException("tag value supplied without a value");
        }
        return value;
    }

    @NonNull
    private static String nodeIdToLabelValue(@Nullable Integer nodeId) {
        return Optional.ofNullable(nodeId).map(Object::toString).orElse("bootstrap");
    }

    @NonNull
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

    @NonNull
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
}
