/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.summary;

public class Metrics {

    // creating a constant for all Metrics in the one place so we can easily see what metrics there are

    private static final String KROXYLICIOUS_DOWNSTREAM = "kroxylicious_downstream_";
    private static final String KROXYLICIOUS_UPSTREAM = "kroxylicious_upstream_";

    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES = "kroxylicious_inbound_downstream_messages";
    public static final String KROXYLICIOUS_DOWNSTREAM_CONNECTIONS = KROXYLICIOUS_DOWNSTREAM + "connections";
    public static final String KROXYLICIOUS_DOWNSTREAM_ERRORS = KROXYLICIOUS_DOWNSTREAM + "errors";

    public static final String KROXYLICIOUS_UPSTREAM_CONNECTIONS = KROXYLICIOUS_UPSTREAM + "connections";
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_ATTEMPTS = KROXYLICIOUS_UPSTREAM + "connection_attempts";
    public static final String KROXYLICIOUS_UPSTREAM_CONNECTION_FAILURES = KROXYLICIOUS_UPSTREAM + "connection_failures";
    public static final String KROXYLICIOUS_UPSTREAM_ERRORS = KROXYLICIOUS_UPSTREAM + "errors";

    public static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES = "kroxylicious_inbound_downstream_decoded_messages";

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
}