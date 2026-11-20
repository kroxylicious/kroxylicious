/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaClientTest {

    private final RecordingClient client = new RecordingClient();

    @Test
    void produceCountExpandsToIdenticalRecords() {
        client.produce("topic", "bootstrap:9092", "hello", 3);

        assertThat(client.produced).containsExactly(ProduceRecord.of("hello"), ProduceRecord.of("hello"), ProduceRecord.of("hello"));
        assertThat(client.produceConfig).isEmpty();
    }

    @Test
    void consumeUsesDefaults() {
        client.consume("topic", "bootstrap:9092", 2);

        assertThat(client.consumeTimeout).isEqualTo(KafkaClient.DEFAULT_CONSUME_TIMEOUT);
        assertThat(client.consumeGroup).isEqualTo(KafkaClient.DEFAULT_CONSUMER_GROUP);
        assertThat(client.consumeConfig).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(ClientCapability.class)
    void optionalCapabilitiesAreUnsupportedByDefault(ClientCapability capability) {
        assertThat(client.supports(capability)).isFalse();
    }

    @Test
    void produceInTransactionUnsupportedByDefault() {
        List<ProduceRecord> records = List.of(ProduceRecord.of("v"));
        assertThatThrownBy(() -> client.produceInTransaction("topic", "bootstrap:9092", records))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("TRANSACTIONS")
                .hasMessageContaining("kcat")
                .hasMessageContaining("on-cluster");
    }

    @Test
    void commitOffsetsUnsupportedByDefault() {
        Map<TopicPartition, Long> offsets = Map.of(new TopicPartition("topic", 0), 1L);
        assertThatThrownBy(() -> client.commitOffsets("group", "bootstrap:9092", offsets))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("OFFSET_COMMIT");
    }

    @Test
    void recordsDefensivelyCopyHeaders() {
        Map<String, String> headers = new HashMap<>(Map.of("h", "v"));
        var record = new ProduceRecord("k", "v", headers);
        headers.put("other", "x");

        assertThat(record.headers()).containsOnlyKeys("h");
    }

    @Test
    void topicPartitionRejectsNegativePartition() {
        assertThatThrownBy(() -> new TopicPartition("topic", -1)).isInstanceOf(IllegalArgumentException.class);
    }

    private static final class RecordingClient implements KafkaClient {
        private final List<ProduceRecord> produced = new ArrayList<>();
        private Map<String, String> produceConfig;
        private Duration consumeTimeout;
        private String consumeGroup;
        private Map<String, String> consumeConfig;

        @Override
        public ClientDriver driver() {
            return ClientDriver.KCAT;
        }

        @Override
        public ClientExecutionEnvironment executionEnvironment() {
            return ClientExecutionEnvironment.ON_CLUSTER;
        }

        @Override
        public void createTopic(String topic, String bootstrap, int partitions, int replicationFactor) {
            // not exercised
        }

        @Override
        public void produce(String topic, String bootstrap, List<ProduceRecord> records, Map<String, String> config) {
            produced.addAll(records);
            produceConfig = config;
        }

        @Override
        public List<ConsumedRecord> consume(String topic, String bootstrap, int count, Duration timeout, String groupId, Map<String, String> config) {
            consumeTimeout = timeout;
            consumeGroup = groupId;
            consumeConfig = config;
            return List.of();
        }
    }
}
