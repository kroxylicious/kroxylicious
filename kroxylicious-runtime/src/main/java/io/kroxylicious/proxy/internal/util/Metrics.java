/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Tag;

import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.summary;

public class Metrics {

    // creating a constant for all Metrics in the one place so we can easily see what metrics there are

    private static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES = "kroxylicious_inbound_downstream_messages";

    private static final String KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES = "kroxylicious_inbound_downstream_decoded_messages";

    private static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES = "kroxylicious_payload_size_bytes";

    private static final String FLOWING_TAG = "flowing";

    private static final Tag FLOWING_UPSTREAM = Tag.of(FLOWING_TAG, "upstream");

    private static final Tag FLOWING_DOWNSTREAM = Tag.of(FLOWING_TAG, "downstream");

    public static Counter inboundDownstreamMessagesCounter() {
        return counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_MESSAGES, List.of(FLOWING_DOWNSTREAM));
    }

    public static Counter inboundDownstreamDecodedMessagesCounter() {
        return counter(KROXYLICIOUS_INBOUND_DOWNSTREAM_DECODED_MESSAGES, List.of(FLOWING_DOWNSTREAM));
    }

    public static DistributionSummary payloadSizeBytesUpstreamSummary(ApiKeys apiKey, short apiVersion) {
        return payloadSizeBytesSummary(apiKey, apiVersion, FLOWING_UPSTREAM);
    }

    public static DistributionSummary payloadSizeBytesDownstreamSummary(ApiKeys apiKey, short apiVersion) {
        return payloadSizeBytesSummary(apiKey, apiVersion, FLOWING_DOWNSTREAM);
    }

    private static DistributionSummary payloadSizeBytesSummary(ApiKeys apiKey, short apiVersion, Tag flowing) {
        List<Tag> tags = List.of(
                Tag.of("ApiKey", apiKey.name()),
                Tag.of("ApiVersion", String.valueOf(apiVersion)),
                flowing
        );
        return summary(KROXYLICIOUS_PAYLOAD_SIZE_BYTES, tags);
    }

}
