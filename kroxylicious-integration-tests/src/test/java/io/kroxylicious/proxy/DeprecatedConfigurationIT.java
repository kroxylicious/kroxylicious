/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.simpletransform.FetchResponseTransformation;
import io.kroxylicious.proxy.filter.simpletransform.UpperCasing;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of deprecated configurations and features
 */
public class DeprecatedConfigurationIT extends BaseIT {

    @Test
    void shouldSupportTopLevelFiltersProperty(KafkaCluster cluster, Topic topic1) throws Exception {

        FilterDefinition filterDefinition = new NamedFilterDefinitionBuilder(
                "filter-1", FetchResponseTransformation.class.getName())
                .withConfig("transformation", UpperCasing.class.getName())
                .withConfig("transformationConfig", Map.of("charset", "UTF-8")).build().asFilterDefinition();
        var config = proxy(cluster)
                .addToFilters(filterDefinition);

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Serdes.String(), Serdes.String(),
                        Map.of(CLIENT_ID_CONFIG, "shouldSupportTopLevelFiltersProperty",
                                DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
                var consumer = tester.consumer(Serdes.String(), Serdes.String(),
                        Map.of(
                                ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {

            producer.send(new ProducerRecord<>(topic1.name(), "my-key", "hello")).get();
            producer.flush();

            consumer.subscribe(Set.of(topic1.name()));
            var records = consumer.poll(Duration.ofSeconds(100));
            assertThat(records).hasSize(1);
            assertThat(records.records(topic1.name()))
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo("HELLO");
        }
    }
}
