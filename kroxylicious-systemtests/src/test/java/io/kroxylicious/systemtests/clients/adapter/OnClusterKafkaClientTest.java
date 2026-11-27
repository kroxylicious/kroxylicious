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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.systemtests.api.client.ClientCapability;
import io.kroxylicious.systemtests.api.client.ClientDriver;
import io.kroxylicious.systemtests.api.client.ClientExecutionEnvironment;
import io.kroxylicious.systemtests.api.client.ConsumedRecord;
import io.kroxylicious.systemtests.api.client.KafkaClientException;
import io.kroxylicious.systemtests.api.client.ProduceRecord;
import io.kroxylicious.systemtests.clients.KafkaClient;
import io.kroxylicious.systemtests.clients.records.KcatConsumerRecord;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OnClusterKafkaClientTest {

    private static final String TOPIC = "my-topic";
    private static final String BOOTSTRAP = "proxy:9292";
    private static final ExecResult SUCCESS = new ExecResult(0, "", "");

    @Mock
    private KafkaClient legacy;

    @Mock
    private TopicCreator topicCreator;

    private OnClusterKafkaClient client() {
        return new OnClusterKafkaClient(ClientDriver.KCAT, legacy, topicCreator);
    }

    @Test
    void reportsDriverAndEnvironment() {
        var client = client();
        assertThat(client.driver()).isEqualTo(ClientDriver.KCAT);
        assertThat(client.executionEnvironment()).isEqualTo(ClientExecutionEnvironment.ON_CLUSTER);
        assertThat(ClientCapability.values()).noneMatch(client::supports);
    }

    @Test
    void createTopicDelegatesToTopicCreator() {
        client().createTopic(TOPIC, BOOTSTRAP, 3, 1);
        verify(topicCreator).createTopic(TOPIC, BOOTSTRAP, 3, 1);
    }

    @Test
    void createTopicFailureIsReportedAsKafkaClientException() {
        doThrow(new KubeClusterException("boom")).when(topicCreator).createTopic(TOPIC, BOOTSTRAP, 1, 1);
        var client = client();
        assertThatThrownBy(() -> client.createTopic(TOPIC, BOOTSTRAP, 1, 1))
                .isInstanceOf(KafkaClientException.class)
                .hasCauseInstanceOf(KubeClusterException.class);
    }

    @Test
    void produceCollapsesConsecutiveIdenticalRecordsIntoOneInvocation() {
        when(legacy.produceMessages(anyString(), anyString(), anyString(), any(), anyInt(), anyMap())).thenReturn(SUCCESS);
        Map<String, String> config = Map.of("compression.type", "zstd");

        client().produce(TOPIC, BOOTSTRAP, List.of(
                ProduceRecord.of("a"), ProduceRecord.of("a"),
                ProduceRecord.of("k", "b"),
                ProduceRecord.of("a")), config);

        InOrder inOrder = inOrder(legacy);
        inOrder.verify(legacy).produceMessages(TOPIC, BOOTSTRAP, "a", null, 2, config);
        inOrder.verify(legacy).produceMessages(TOPIC, BOOTSTRAP, "b", "k", 1, config);
        inOrder.verify(legacy).produceMessages(TOPIC, BOOTSTRAP, "a", null, 1, config);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void produceCountIsASingleInvocation() {
        when(legacy.produceMessages(anyString(), anyString(), anyString(), any(), anyInt(), anyMap())).thenReturn(SUCCESS);

        client().produce(TOPIC, BOOTSTRAP, "hello", 5);

        verify(legacy).produceMessages(TOPIC, BOOTSTRAP, "hello", null, 5, Map.of());
    }

    @Test
    void unsuccessfulProduceIsReportedAsKafkaClientException() {
        when(legacy.produceMessages(anyString(), anyString(), anyString(), any(), anyInt(), anyMap()))
                .thenReturn(new ExecResult(1, "", "TOPIC_AUTHORIZATION_FAILED"));
        var client = client();

        assertThatThrownBy(() -> client.produce(TOPIC, BOOTSTRAP, "hello", 1))
                .isInstanceOf(KafkaClientException.class)
                .hasMessageContaining("exit code 1")
                .hasMessageContaining("TOPIC_AUTHORIZATION_FAILED");
    }

    @Test
    void produceRejectsHeaders() {
        var client = client();
        List<ProduceRecord> records = List.of(new ProduceRecord(null, "v", Map.of("h", "1")));

        assertThatThrownBy(() -> client.produce(TOPIC, BOOTSTRAP, records))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("headers");
        verifyNoInteractions(legacy);
    }

    @Test
    void consumeMapsLegacyRecords() {
        var legacyRecord = new KcatConsumerRecord(List.of("h", "1"), TOPIC, "key", "value", 2, 42L);
        when(legacy.consumeMessages(TOPIC, BOOTSTRAP, 1, Duration.ofSeconds(5), Map.of(), "group")).thenReturn(List.of(legacyRecord));

        List<ConsumedRecord> consumed = client().consume(TOPIC, BOOTSTRAP, 1, Duration.ofSeconds(5), "group", Map.of());

        assertThat(consumed).containsExactly(new ConsumedRecord(TOPIC, 2, 42L, "key", "value", Map.of("h", "1")));
    }

    @Test
    void consumeRejectsGroupIdInConfig() {
        var client = client();
        Map<String, String> config = Map.of("group.id", "g");
        Duration timeout = Duration.ofSeconds(1);

        assertThatThrownBy(() -> client.consume(TOPIC, BOOTSTRAP, 1, timeout, "group", config))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(legacy);
    }
}
