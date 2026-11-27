/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.BufferAllocatingDropFilter;
import io.kroxylicious.it.testplugins.BufferAllocatingDropFilterFactory;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.apache.kafka.common.protocol.ApiKeys.CREATE_TOPICS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Reproduces <a href="https://github.com/kroxylicious/kroxylicious/issues/5312">#5312</a>: a filter
 * that allocates a buffer via {@link io.kroxylicious.proxy.filter.FilterContext#createByteBufferOutputStream(int)}
 * then drops the frame instead of forwarding it. {@link NettyLeakDetectorExtension} is expected to
 * catch the resulting leak.
 */
@ExtendWith(NettyLeakDetectorExtension.class)
class BufferLeakOnDropFilterIT {

    @Test
    void droppingRequestAfterAllocatingBufferDoesNotLeak() {
        var filterDefinition = new NamedFilterDefinitionBuilder(
                BufferAllocatingDropFilterFactory.class.getName(), BufferAllocatingDropFilterFactory.class.getName())
                .build();

        try (var tester = mockKafkaKroxyliciousTester(mockBootstrap -> proxy(mockBootstrap)
                .addToFilterDefinitions(filterDefinition)
                .addToDefaultFilters(filterDefinition.name()));
                var client = tester.simpleTestClient()) {

            for (int i = 0; i < 20; i++) {
                var createTopics = new CreateTopicsRequestData();
                createTopics.topics().add(new CreateTopicsRequestData.CreatableTopic().setName(UUID.randomUUID().toString()));
                var unused = client.get(new Request(CREATE_TOPICS, CREATE_TOPICS.latestVersion(), "client", createTopics));

                var expectedDropped = i + 1L;
                await().atMost(Duration.ofSeconds(10))
                        .untilAsserted(() -> assertThat(BufferAllocatingDropFilter.DROPPED_COUNT).hasValue(expectedDropped));

                // buffers must be garbage collected before it causes leak detection during
                // a subsequent buffer allocation
                System.gc();
            }
        }
    }
}
