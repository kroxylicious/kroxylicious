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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

public class Metrics {

    private static final Metrics forGlobalRegistry = new Metrics(globalRegistry);

    private final MeterRegistry registry;

    /* exposed for testing */ Metrics(MeterRegistry registry) {
        this.registry = registry;
    }

    // creating a constant for all Metrics in the one place so we can easily see what metrics there are

    private static final String KROXYLICIOUS_RECEIVED_MESSAGES_TOTAL = "kroxylicious_received_messages_total";

    private static final String KROXYLICIOUS_DECODED_MESSAGES_TOTAL = "kroxylicious_decoded_messages_total";

    private static final String KROXYLICIOUS_PAYLOAD_SIZE_BYTES = "kroxylicious_payload_size_bytes";
    private static final String MESSAGE_TYPE_TAG = "message_type";
    private static final String REQUEST_TYPE = "request";
    private static final String RESPONSE_TYPE = "response";

    private static final Tag REQUEST_TAG = Tag.of(MESSAGE_TYPE_TAG, REQUEST_TYPE);

    private static final Tag RESPONSE_TAG = Tag.of(MESSAGE_TYPE_TAG, RESPONSE_TYPE);

    public Counter receivedRequestMessagesCounter() {
        return registry.counter(KROXYLICIOUS_RECEIVED_MESSAGES_TOTAL, List.of(REQUEST_TAG));
    }

    public Counter decodedRequestMessagesCounter() {
        return registry.counter(KROXYLICIOUS_DECODED_MESSAGES_TOTAL, List.of(REQUEST_TAG));
    }

    public DistributionSummary payloadSizeBytesRequestSummary(ApiKeys apiKey, short apiVersion) {
        return payloadSizeBytesSummary(apiKey, apiVersion, REQUEST_TAG);
    }

    public DistributionSummary payloadSizeBytesResponseSummary(ApiKeys apiKey, short apiVersion) {
        return payloadSizeBytesSummary(apiKey, apiVersion, RESPONSE_TAG);
    }

    private DistributionSummary payloadSizeBytesSummary(ApiKeys apiKey, short apiVersion, Tag messageType) {
        List<Tag> tags = List.of(
                Tag.of("api_key", apiKey.name()),
                Tag.of("api_version", String.valueOf(apiVersion)),
                messageType);
        return registry.summary(KROXYLICIOUS_PAYLOAD_SIZE_BYTES, tags);
    }

    public static Metrics forGlobalRegistry() {
        return forGlobalRegistry;
    }

}
