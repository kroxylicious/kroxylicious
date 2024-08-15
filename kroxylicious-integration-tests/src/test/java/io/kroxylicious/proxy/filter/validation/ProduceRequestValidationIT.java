/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.validation;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class ProduceRequestValidationIT extends RecordValidationBaseIT {

    @SuppressWarnings({ "removal" })
    private static final String SIMPLE_NAME = ProduceValidationFilterFactory.class.getSimpleName();

    @Test
    void validJsonProduceAcceptedUsingDeprecatedFactoryName(KafkaCluster cluster, Topic topic) {
        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(SIMPLE_NAME).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                                Map.of("syntacticallyCorrectJson", Map.of()))))
                        .build());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), "my-key", JsonSyntaxRecordValidationIT.SYNTACTICALLY_CORRECT_JSON));
            assertThat(result)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isNotNull();
        }
    }

    @Test
    void invalidJsonProduceRejected(KafkaCluster cluster, Topic topic) {

        var config = proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(SIMPLE_NAME).withConfig("rules",
                        List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                                Map.of("syntacticallyCorrectJson", Map.of()))))
                        .build());
        try (var tester = kroxyliciousTester(config);
                var producer = getProducer(tester, 0, 16384)) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", JsonSyntaxRecordValidationIT.SYNTACTICALLY_INCORRECT_JSON));
            assertThat(invalid)
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableThat()
                    .withCauseInstanceOf(InvalidRecordException.class)
                    .withMessageContaining("value was not syntactically correct JSON");
        }
    }

}
