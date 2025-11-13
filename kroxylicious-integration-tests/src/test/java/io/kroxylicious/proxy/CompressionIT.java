/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.record.CompressionType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

@ExtendWith(KafkaClusterExtension.class)
class CompressionIT extends BaseIT {

    @ParameterizedTest
    @EnumSource(CompressionType.class)
    void shouldSupportSnappy(CompressionType compressionType, KafkaCluster kafkaCluster, Topic topic) throws IOException {
        // Given

        try (var tester = kroxyliciousTester(KroxyliciousConfigUtils.proxy(kafkaCluster));
                var proxyProducer = tester.producer(
                        Map.of("compression.type", compressionType.name().toLowerCase(Locale.ROOT), "client.id", compressionType.name() + "-producer"));
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
            assertThat(producerMetrics).hasEntrySatisfying(buildMetricName(compressionType), metric -> {
                var doubleAssert = assertThat(metric.metricValue()).asInstanceOf(InstanceOfAssertFactories.DOUBLE);
                if (compressionType == CompressionType.NONE) {
                    doubleAssert.isEqualTo(1.0);
                }
                else {
                    doubleAssert.isStrictlyBetween(0.0, 1.0);
                }
            });
        }
    }

    @NonNull
    private static MetricName buildMetricName(CompressionType compressionType) {
        return new MetricName("compression-rate-avg", "producer-metrics",
                "The average compression rate of record batches, defined as the average ratio of the compressed batch size over the uncompressed size.",
                Map.of("client-id", compressionType.name() + "-producer"));
    }
}
