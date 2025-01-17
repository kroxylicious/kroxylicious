/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.validation;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.proxy.filter.validation.JsonSyntaxRecordValidationIT.SYNTACTICALLY_CORRECT_JSON;
import static io.kroxylicious.proxy.filter.validation.JsonSyntaxRecordValidationIT.SYNTACTICALLY_INCORRECT_JSON;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class ProduceRequestValidationIT extends RecordValidationBaseIT {

    @SuppressWarnings({ "removal" })
    private static final String SIMPLE_NAME = ProduceValidationFilterFactory.class.getSimpleName();

    @Test
    void validJsonProduceAcceptedUsingDeprecatedFactoryName(KafkaCluster cluster, Topic topic) {
        NamedFilterDefinitionBuilder filterDefinitionBuilder = new NamedFilterDefinitionBuilder(SIMPLE_NAME, SIMPLE_NAME);
        var config = proxy(cluster)
                .addToFilterDefinitions(filterDefinitionBuilder.withConfig("rules",
                        List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                                Map.of("syntacticallyCorrectJson", Map.of()))))
                        .build())
                .addToDefaultFilters(filterDefinitionBuilder.name());

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), "my-key", SYNTACTICALLY_CORRECT_JSON));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .isEqualTo(SYNTACTICALLY_CORRECT_JSON);

        }
    }

    @Test
    void invalidJsonProduceRejected(KafkaCluster cluster, Topic topic) {
        NamedFilterDefinitionBuilder filterDefinitionBuilder = new NamedFilterDefinitionBuilder(SIMPLE_NAME, SIMPLE_NAME);
        var config = proxy(cluster)
                .addToFilterDefinitions(filterDefinitionBuilder.withConfig("rules",
                        List.of(Map.of("topicNames", List.of(topic.name()), "valueRule",
                                Map.of("syntacticallyCorrectJson", Map.of()))))
                        .build())
                .addToDefaultFilters(filterDefinitionBuilder.name());
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");
        }
    }

}
