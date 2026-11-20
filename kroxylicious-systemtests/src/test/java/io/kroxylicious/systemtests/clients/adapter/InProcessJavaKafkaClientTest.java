/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.adapter;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.systemtests.api.client.ClientCapability;
import io.kroxylicious.systemtests.api.client.ConsumedRecord;
import io.kroxylicious.systemtests.api.client.KafkaClientException;
import io.kroxylicious.systemtests.api.client.ProduceRecord;
import io.kroxylicious.systemtests.api.client.TopicPartition;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(KafkaClusterExtension.class)
class InProcessJavaKafkaClientTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final InProcessJavaKafkaClient client = new InProcessJavaKafkaClient();

    @Test
    void supportsAllCapabilities() {
        assertThat(ClientCapability.values()).allMatch(client::supports);
    }

    @Test
    void produceAndConsumeRoundTrip(KafkaCluster cluster) {
        String bootstrap = cluster.getBootstrapServers();
        client.createTopic("round-trip", bootstrap, 1, 1);

        client.produce("round-trip", bootstrap, List.of(
                ProduceRecord.of("k1", "v1"),
                new ProduceRecord(null, "v2", Map.of("h", "x"))));

        List<ConsumedRecord> consumed = client.consume("round-trip", bootstrap, 2, TIMEOUT);
        assertThat(consumed).containsExactly(
                new ConsumedRecord("round-trip", 0, 0, "k1", "v1", Map.of()),
                new ConsumedRecord("round-trip", 0, 1, null, "v2", Map.of("h", "x")));
    }

    @Test
    void consumeResumesFromCommittedOffset(KafkaCluster cluster) {
        String bootstrap = cluster.getBootstrapServers();
        client.createTopic("resume", bootstrap, 1, 1);
        client.produce("resume", bootstrap, "v", 3);

        assertThat(client.consume("resume", bootstrap, 2, TIMEOUT, "g", Map.of())).extracting(ConsumedRecord::offset).containsExactly(0L, 1L);
        assertThat(client.consume("resume", bootstrap, 2, Duration.ofSeconds(5), "g", Map.of())).extracting(ConsumedRecord::offset).containsExactly(2L);
    }

    @Test
    void produceInTransactionIsVisibleToConsumers(KafkaCluster cluster) {
        String bootstrap = cluster.getBootstrapServers();
        client.createTopic("txn", bootstrap, 1, 1);

        client.produceInTransaction("txn", bootstrap, List.of(ProduceRecord.of("a"), ProduceRecord.of("b")));

        assertThat(client.consume("txn", bootstrap, 2, TIMEOUT, "txn-group", Map.of("isolation.level", "read_committed")))
                .extracting(ConsumedRecord::value)
                .containsExactly("a", "b");
    }

    @Test
    void commitOffsetsMovesConsumerGroupPosition(KafkaCluster cluster) {
        String bootstrap = cluster.getBootstrapServers();
        client.createTopic("commit", bootstrap, 1, 1);
        client.produce("commit", bootstrap, "v", 3);

        client.commitOffsets("skipper", bootstrap, Map.of(new TopicPartition("commit", 0), 2L));

        assertThat(client.consume("commit", bootstrap, 1, TIMEOUT, "skipper", Map.of())).extracting(ConsumedRecord::offset).containsExactly(2L);
    }

    @Test
    void createExistingTopicFails(KafkaCluster cluster) {
        String bootstrap = cluster.getBootstrapServers();
        client.createTopic("dup", bootstrap, 1, 1);

        assertThatThrownBy(() -> client.createTopic("dup", bootstrap, 1, 1)).isInstanceOf(KafkaClientException.class);
    }
}
