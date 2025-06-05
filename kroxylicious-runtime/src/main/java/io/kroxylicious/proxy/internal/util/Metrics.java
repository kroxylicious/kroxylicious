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
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;

import edu.umd.cs.findbugs.annotations.NonNull;

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

    private static final String KROXYLICIOUS_CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME = "kroxylicious_client_to_proxy_request";
    private static final String KROXYLICIOUS_PROXY_TO_SERVER_REQUEST_BASE_METER_NAME = "kroxylicious_proxy_to_server_request";
    private static final String KROXYLICIOUS_SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME = "kroxylicious_server_to_proxy_response";
    private static final String KROXYLICIOUS_PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME = "kroxylicious_proxy_to_client_response";
    private static final String KROXYLICIOUS_CLIENT_TO_PROXY_ERROR_BASE_METER_NAME = "kroxylicious_client_to_proxy_errors_total";
    private static final String KROXYLICIOUS_PROXY_TO_SERVER_ERROR_BASE_METER_NAME = "kroxylicious_proxy_to_server_errors_total";
    private static final String KROXYLICIOUS_CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME = "kroxylicious_client_to_proxy_connections_total";
    private static final String KROXYLICIOUS_PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME = "kroxylicious_proxy_to_server_connections_total";

    // Meter Providers
    // (Callers use the providers to create the meters they need, augmenting with any tags).
    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_CLIENT_TO_PROXY_REQUEST_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                       nodeId) -> Counter.builder(
                                                                                                                                               KROXYLICIOUS_CLIENT_TO_PROXY_REQUEST_BASE_METER_NAME)
                                                                                                                                               .description(
                                                                                                                                                       "Incremented by one every time a request arrives at the proxy from the downstream (client).")
                                                                                                                                               .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                       virtualClusterName)
                                                                                                                                               .tag(NODE_ID_LABEL,
                                                                                                                                                       nodeIdToLabelValue(
                                                                                                                                                               nodeId))
                                                                                                                                               .withRegistry(
                                                                                                                                                       globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_PROXY_TO_SERVER_REQUEST_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                       nodeId) -> Counter.builder(
                                                                                                                                               KROXYLICIOUS_PROXY_TO_SERVER_REQUEST_BASE_METER_NAME)
                                                                                                                                               .description(
                                                                                                                                                       "Incremented by one every time a request (#1) goes from the proxy to the upstream (server).")
                                                                                                                                               .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                       virtualClusterName)
                                                                                                                                               .tag(NODE_ID_LABEL,
                                                                                                                                                       nodeIdToLabelValue(
                                                                                                                                                               nodeId))
                                                                                                                                               .withRegistry(
                                                                                                                                                       globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_SERVER_TO_PROXY_RESPONSE_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                        nodeId) -> Counter.builder(
                                                                                                                                                KROXYLICIOUS_SERVER_TO_PROXY_RESPONSE_BASE_METER_NAME)
                                                                                                                                                .description(
                                                                                                                                                        "Incremented by one every time a response (#1) arrives at the proxy from the upstream (server).")
                                                                                                                                                .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                        virtualClusterName)
                                                                                                                                                .tag(NODE_ID_LABEL,
                                                                                                                                                        nodeIdToLabelValue(
                                                                                                                                                                nodeId))
                                                                                                                                                .withRegistry(
                                                                                                                                                        globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_PROXY_TO_CLIENT_RESPONSE_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                        nodeId) -> Counter.builder(
                                                                                                                                                KROXYLICIOUS_PROXY_TO_CLIENT_RESPONSE_BASE_METER_NAME)
                                                                                                                                                .description(
                                                                                                                                                        "Incremented by one every time a response goes from the proxy to the downstream (client).")
                                                                                                                                                .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                        virtualClusterName)
                                                                                                                                                .tag(NODE_ID_LABEL,
                                                                                                                                                        nodeIdToLabelValue(
                                                                                                                                                                nodeId))
                                                                                                                                                .withRegistry(
                                                                                                                                                        globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_CLIENT_TO_PROXY_ERROR_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                     nodeId) -> Counter.builder(
                                                                                                                                             KROXYLICIOUS_CLIENT_TO_PROXY_ERROR_BASE_METER_NAME)
                                                                                                                                             .description(
                                                                                                                                                     "Incremented by one every time a connection is closed due to any downstream error.")
                                                                                                                                             .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                     virtualClusterName)
                                                                                                                                             .tag(NODE_ID_LABEL,
                                                                                                                                                     nodeIdToLabelValue(
                                                                                                                                                             nodeId))
                                                                                                                                             .withRegistry(
                                                                                                                                                     globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_PROXY_TO_SERVER_ERROR_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                     nodeId) -> Counter.builder(
                                                                                                                                             KROXYLICIOUS_PROXY_TO_SERVER_ERROR_BASE_METER_NAME)
                                                                                                                                             .description(
                                                                                                                                                     "Incremented by one every time a connection is closed due to any upstream error.")
                                                                                                                                             .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                     virtualClusterName)
                                                                                                                                             .tag(NODE_ID_LABEL,
                                                                                                                                                     nodeIdToLabelValue(
                                                                                                                                                             nodeId))
                                                                                                                                             .withRegistry(
                                                                                                                                                     globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_CLIENT_TO_PROXY_CONNECTION_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                          nodeId) -> Counter.builder(
                                                                                                                                                  KROXYLICIOUS_CLIENT_TO_PROXY_CONNECTION_BASE_METER_NAME)
                                                                                                                                                  .description(
                                                                                                                                                          "Incremented by one every time a connection is accepted from the downstream the proxy.")
                                                                                                                                                  .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                          virtualClusterName)
                                                                                                                                                  .tag(NODE_ID_LABEL,
                                                                                                                                                          nodeIdToLabelValue(
                                                                                                                                                                  nodeId))
                                                                                                                                                  .withRegistry(
                                                                                                                                                          globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_PROXY_TO_SERVER_CONNECTION_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                          nodeId) -> Counter.builder(
                                                                                                                                                  KROXYLICIOUS_PROXY_TO_SERVER_CONNECTION_BASE_METER_NAME)
                                                                                                                                                  .description(
                                                                                                                                                          "Incremented by one every time a connection is made to the upstream from the proxy.")
                                                                                                                                                  .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                          virtualClusterName)
                                                                                                                                                  .tag(NODE_ID_LABEL,
                                                                                                                                                          nodeIdToLabelValue(
                                                                                                                                                                  nodeId))
                                                                                                                                                  .withRegistry(
                                                                                                                                                          globalRegistry);

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
     * @deprecated use `clientToProxyConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_DOWNSTREAM_CONNECTIONS = KROXYLICIOUS_DOWNSTREAM + "connections";

    /**
     * @deprecated use `clientToProxyErrorCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_DOWNSTREAM_ERRORS = KROXYLICIOUS_DOWNSTREAM + "errors";

    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTIONS = KROXYLICIOUS_UPSTREAM + "connections";

    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_ATTEMPTS = KROXYLICIOUS_UPSTREAM + "connection_attempts";

    /**
     * @deprecated use `proxyToServerConnectionCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_FAILURES = KROXYLICIOUS_UPSTREAM + "connection_failures";

    /**
     * @deprecated use `proxyToServerErrorCounter` instead
     */
    @Deprecated(since = "0.13.0", forRemoval = true)
    @SuppressWarnings("java:S1133")
    public static final String KROXYLICIOUS_UPSTREAM_ERRORS = KROXYLICIOUS_UPSTREAM + "errors";

    public static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES = "kroxylicious_payload_size_bytes";

    public static final String FLOWING_TAG = "flowing";

    public static final String VIRTUAL_CLUSTER_TAG = "virtualCluster";

    public static final String DOWNSTREAM = "downstream";

    public static final String UPSTREAM = "upstream";

    @NonNull
    public static Counter taggedCounter(String counterName, List<Tag> tags) {
        return counter(counterName, tags);
    }

    public static DistributionSummary payloadSizeBytesUpstreamSummary(ApiKeys apiKey, short apiVersion, String virtualCluster) {
        return payloadSizeBytesSummary(apiKey, apiVersion, UPSTREAM, virtualCluster);
    }

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
    private static String nodeIdToLabelValue(Integer nodeId) {
        return Optional.ofNullable(nodeId).map(Object::toString).orElse("bootstrap");
    }

    @FunctionalInterface
    public interface ClusterNodeSpecificMetricProviderCreator<T extends Meter> {

        MeterProvider<T> create(String virtualCluster, Integer nodeId);
    }
}
