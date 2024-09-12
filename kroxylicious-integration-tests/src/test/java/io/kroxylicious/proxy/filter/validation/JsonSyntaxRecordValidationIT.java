/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.validation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicPartitions;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
class JsonSyntaxRecordValidationIT extends RecordValidationBaseIT {

    public static final String SYNTACTICALLY_CORRECT_JSON = "{\"value\":\"json\"}";
    public static final String SYNTACTICALLY_INCORRECT_JSON = "Not Json";

    @Test
    void invalidJsonProduceRejected(KafkaCluster cluster, Topic topic) {
        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(false, topic));
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");
        }
    }

    @Test
    void invalidJsonProduceRejectedUsingTopicNames(KafkaCluster cluster, Topic topic1, Topic topic2) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(false, topic1));
        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var rejected = producer.send(new ProducerRecord<>(topic1.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertThatFutureFails(rejected, InvalidRecordException.class, "value was not syntactically correct JSON");

            var accepted = producer.send(new ProducerRecord<>(topic2.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            assertThatFutureSucceeds(accepted);

            var records = consumeAll(tester, topic2);
            assertThat(records)
                               .singleElement()
                               .extracting(ConsumerRecord::value)
                               .isEqualTo(SYNTACTICALLY_INCORRECT_JSON);
        }
    }

    @Test
    void partiallyInvalidJsonTransactionalAllRejected(KafkaCluster cluster, Topic topic1, Topic topic2) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(true, topic1));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000, TRANSACTIONAL_ID_CONFIG, randomUUID().toString()))) {
            producer.initTransactions();
            producer.beginTransaction();
            var invalid = producer.send(new ProducerRecord<>(topic1.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            var valid = producer.send(new ProducerRecord<>(topic2.name(), "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");
            assertThatFutureFails(valid, InvalidRecordException.class, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
            producer.abortTransaction();
        }
    }

    @Test
    void partiallyInvalidJsonNotConfiguredToForwardAllRejected(KafkaCluster cluster, Topic topic1, Topic topic2) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(false, topic1));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000))) {
            var invalid = producer.send(new ProducerRecord<>(topic1.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            var valid = producer.send(new ProducerRecord<>(topic2.name(), "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");
            assertThatFutureFails(valid, InvalidRecordException.class, "Invalid record in another topic-partition caused whole ProduceRequest to be invalidated");
        }
    }

    @Test
    void partiallyInvalidJsonProduceRejected(KafkaCluster cluster, Topic topic1, Topic topic2) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(true, topic1));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000))) {
            var invalid = producer.send(new ProducerRecord<>(topic1.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            var valid = producer.send(new ProducerRecord<>(topic2.name(), "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");

            assertThatFutureSucceeds(valid);

            var records = consumeAll(tester, topic2);
            assertThat(records)
                               .singleElement()
                               .extracting(ConsumerRecord::value)
                               .isEqualTo(SYNTACTICALLY_CORRECT_JSON);
        }
    }

    @Test
    void partiallyInvalidAcrossPartitionsOfSameTopic(KafkaCluster cluster, @TopicPartitions(2)
    Topic topic) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(true, topic));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000))) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), 0, "my-key", SYNTACTICALLY_INCORRECT_JSON));
            var valid = producer.send(new ProducerRecord<>(topic.name(), 1, "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();
            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");

            assertThatFutureSucceeds(valid);

            var records = consumeAll(tester, topic);
            assertThat(records)
                               .singleElement()
                               .extracting(ConsumerRecord::value)
                               .isEqualTo(SYNTACTICALLY_CORRECT_JSON);
        }
    }

    @Test
    void partiallyInvalidWithinOnePartitionOfTopic(KafkaCluster cluster, Topic topic) {
        assertThat(cluster.getNumOfBrokers()).isOne();

        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(true, topic));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(LINGER_MS_CONFIG, 5000))) {
            var invalid = producer.send(new ProducerRecord<>(topic.name(), "my-key", SYNTACTICALLY_INCORRECT_JSON));
            var valid = producer.send(new ProducerRecord<>(topic.name(), "my-key", SYNTACTICALLY_CORRECT_JSON));
            producer.flush();

            assertThatFutureFails(invalid, InvalidRecordException.class, "value was not syntactically correct JSON");
            assertThatFutureFails(valid, KafkaException.class, "Failed to append record because it was part of a batch which had one more more invalid records");
        }
    }

    @Test
    void validJsonProduceAccepted(KafkaCluster cluster, Topic topic) {
        var config = proxy(cluster)
                                   .addToFilters(createFilterDef(false, topic));

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
    void allowNulls(KafkaCluster cluster, Topic topic) {
        var config = proxy(cluster)
                                   .addToFilters(
                                           new FilterDefinitionBuilder(RecordValidation.class.getName()).withConfig(
                                                   "rules",
                                                   List.of(
                                                           Map.of(
                                                                   "topicNames",
                                                                   List.of(topic.name()),
                                                                   "valueRule",
                                                                   Map.of("allowNulls", true, "syntacticallyCorrectJson", Map.of())
                                                           )
                                                   )
                                           )
                                                                                                        .build()
                                   );

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), "my-key", null));
            assertThatFutureSucceeds(result);

            var records = consumeAll(tester, topic);
            assertThat(records)
                               .singleElement()
                               .extracting(ConsumerRecord::value)
                               .isNull();
        }
    }

    @Test
    void rejectNulls(KafkaCluster cluster, Topic topic) {

        var config = proxy(cluster)
                                   .addToFilters(
                                           new FilterDefinitionBuilder(RecordValidation.class.getName()).withConfig(
                                                   "rules",
                                                   List.of(
                                                           Map.of(
                                                                   "topicNames",
                                                                   List.of(topic.name()),
                                                                   "valueRule",
                                                                   Map.of("allowNulls", false, "syntacticallyCorrectJson", Map.of())
                                                           )
                                                   )
                                           )
                                                                                                        .build()
                                   );

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer()) {
            var result = producer.send(new ProducerRecord<>(topic.name(), "my-key", null));
            assertThatFutureFails(result, InvalidRecordException.class, "Null buffer invalid");
        }
    }

    private FilterDefinition createFilterDef(boolean forwardPartialRequests, Topic... topics) {
        return new FilterDefinitionBuilder(RecordValidation.class.getName()).withConfig(
                "forwardPartialRequests",
                forwardPartialRequests,
                "rules",
                List.of(
                        Map.of(
                                "topicNames",
                                Arrays.stream(topics).map(Topic::name).toList(),
                                "valueRule",
                                Map.of("syntacticallyCorrectJson", Map.of())
                        )
                )
        )
                                                                            .build();
    }

}
