/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

@ExtendWith(KafkaClusterExtension.class)
public class CompressionIT extends BaseIT {

    @SuppressWarnings("unused")
    public static final Set<String> compressionTypes = Set.of("none", "gzip", "snappy", "lz4", "zstd");

    @ParameterizedTest
    @FieldSource("compressionTypes")
    void shouldSupportSnappy(String compressionType, KafkaCluster kafkaCluster, Topic topic) throws IOException {
        // Given

        try (var tester = kroxyliciousTester(KroxyliciousConfigUtils.proxy(kafkaCluster));
                var proxyProducer = tester.producer(Map.of("compression.type", compressionType));
                var proxyConsumer = tester.consumer();
                InputStream recordValueStream = this.getClass().getClassLoader().getResourceAsStream("compressible.json")) {

            assumeThat(recordValueStream).isNotNull();

            proxyConsumer.subscribe(Set.of(topic.name()));

            String recordValue = new String(Objects.requireNonNull(recordValueStream).readAllBytes(), StandardCharsets.UTF_8);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic.name(),
                    recordValue);

            // When
            assertThat(proxyProducer.send(producerRecord))
                    .succeedsWithin(5, TimeUnit.SECONDS)
                    .satisfies(metadata -> {
                        assertThat(metadata).isNotNull();
                        assertThat(metadata.hasOffset()).isTrue();
                    });
            // Then

            Map<MetricName, ? extends Metric> producerMetrics = proxyProducer.metrics();
            producerMetrics.forEach((key, metric) -> {
                if (key.name().equals("compression-rate-avg")) {
                    assertThat(metric.metricValue()).asInstanceOf(InstanceOfAssertFactories.DOUBLE).isGreaterThan(0);
                }
            });
        }
    }
}
