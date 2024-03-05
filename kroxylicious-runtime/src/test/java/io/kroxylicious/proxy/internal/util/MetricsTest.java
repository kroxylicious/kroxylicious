/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsTest {

    @Test
    void receivedRequestMessagesCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);
        metrics.receivedRequestMessagesCounter().increment();
        assertThat(registry.find("kroxylicious_received_messages_total").tag("message_type", "request").counter().count()).isEqualTo(1d);
    }

    @Test
    void decodedRequestMessagesCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);
        metrics.decodedRequestMessagesCounter().increment();
        assertThat(registry.find("kroxylicious_decoded_messages_total").tag("message_type", "request").counter().count()).isEqualTo(1d);
    }

    @Test
    void payloadSizeBytesRequestSummary() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);
        metrics.payloadSizeBytesRequestSummary(ApiKeys.METADATA, (short) 12).record(3d);
        DistributionSummary summary = registry.find("kroxylicious_payload_size_bytes")
                .tag("message_type", "request")
                .tag("api_key", "METADATA")
                .tag("api_version", "12")
                .summary();
        assertThat(summary).isNotNull();
        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.max()).isEqualTo(3d);
        assertThat(summary.mean()).isEqualTo(3d);
    }

    @Test
    void payloadSizeBytesResponseSummary() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);
        metrics.payloadSizeBytesResponseSummary(ApiKeys.METADATA, (short) 12).record(4d);
        DistributionSummary summary = registry.find("kroxylicious_payload_size_bytes")
                .tag("message_type", "response")
                .tag("api_key", "METADATA")
                .tag("api_version", "12")
                .summary();
        assertThat(summary).isNotNull();
        assertThat(summary.count()).isEqualTo(1);
        assertThat(summary.max()).isEqualTo(4d);
        assertThat(summary.mean()).isEqualTo(4d);
    }

}