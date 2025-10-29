/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test that tests Kroxylicious ability to utilise Linux io_uring.
 */
@ExtendWith(KafkaClusterExtension.class)
@EnabledIf(value = "io.netty.channel.uring.IoUring#isAvailable", disabledReason = "IOUring is not available")
class IOUringIT extends BaseIT {

    private static final String HELLO_WORLD = "helloworld";

    @Test
    void proxyUsingIOUring(KafkaCluster cluster, Topic topic) throws Exception {

        var proxy = proxy(cluster).withUseIoUring().withNewNetwork().withNewProxy().withWorkerThreadCount(2).endProxy().endNetwork();

        try (var tester = kroxyliciousTester(proxy);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                    .toIterable()
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(HELLO_WORLD);

        }
    }
}