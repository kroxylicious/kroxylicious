/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.adapter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import io.kroxylicious.systemtests.api.client.ClientDriver;
import io.kroxylicious.systemtests.api.client.ClientExecutionEnvironment;
import io.kroxylicious.systemtests.api.client.ConsumedRecord;
import io.kroxylicious.systemtests.api.client.KafkaClient;
import io.kroxylicious.systemtests.api.client.KafkaClientException;
import io.kroxylicious.systemtests.api.client.ProduceRecord;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

/**
 * Adapts one of the existing CLI/Job based clients (Strimzi test client, kcat, kaf, python) to the
 * intent-based {@link KafkaClient}. Every operation runs as a workload on the Kubernetes cluster.
 * <p>
 * The underlying clients can only produce {@code n} copies of one key/value per invocation, so each
 * run of consecutive identical records becomes one invocation. Record headers are not supported.
 * Some drivers decorate produced values (kcat and kaf append a sequence number), so tests should
 * assert on containment rather than equality when they need to be portable across drivers.
 */
final class OnClusterKafkaClient implements KafkaClient {

    private final ClientDriver driver;
    private final io.kroxylicious.systemtests.clients.KafkaClient delegate;
    private final TopicCreator topicCreator;

    OnClusterKafkaClient(ClientDriver driver, io.kroxylicious.systemtests.clients.KafkaClient delegate, TopicCreator topicCreator) {
        this.driver = Objects.requireNonNull(driver);
        this.delegate = Objects.requireNonNull(delegate);
        this.topicCreator = Objects.requireNonNull(topicCreator);
    }

    @Override
    public ClientDriver driver() {
        return driver;
    }

    @Override
    public ClientExecutionEnvironment executionEnvironment() {
        return ClientExecutionEnvironment.ON_CLUSTER;
    }

    @Override
    public void createTopic(String topic, String bootstrap, int partitions, int replicationFactor) {
        try {
            topicCreator.createTopic(topic, bootstrap, partitions, replicationFactor);
        }
        catch (KubeClusterException e) {
            throw new KafkaClientException("Failed to create topic '" + topic + "'", e);
        }
    }

    @Override
    public void produce(String topic, String bootstrap, List<ProduceRecord> records, Map<String, String> config) {
        if (records.stream().anyMatch(r -> !r.headers().isEmpty())) {
            throw new UnsupportedOperationException("Record headers are not supported by the " + driver.envValue() + " driver running "
                    + executionEnvironment().envValue());
        }
        for (Run run : runsOf(records)) {
            ExecResult result;
            try {
                result = delegate.produceMessages(topic, bootstrap, run.record().value(), run.record().key(), run.count(), config);
            }
            catch (KubeClusterException e) {
                throw new KafkaClientException("Failed to produce to topic '" + topic + "'", e);
            }
            if (result == null || !result.isSuccess()) {
                throw new KafkaClientException("Failed to produce to topic '" + topic + "'"
                        + (result == null ? "" : " (exit code " + result.returnCode() + "): " + result.err()));
            }
        }
    }

    @Override
    public List<ConsumedRecord> consume(String topic, String bootstrap, int count, Duration timeout, String groupId, Map<String, String> config) {
        if (config.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException("Specify the consumer group via groupId, not via the '" + ConsumerConfig.GROUP_ID_CONFIG + "' config property");
        }
        List<ConsumerRecord> consumed;
        try {
            consumed = delegate.consumeMessages(topic, bootstrap, count, timeout, config, groupId);
        }
        catch (KubeClusterException e) {
            throw new KafkaClientException("Failed to consume from topic '" + topic + "'", e);
        }
        return consumed.stream().map(OnClusterKafkaClient::toConsumedRecord).toList();
    }

    static ConsumedRecord toConsumedRecord(ConsumerRecord legacy) {
        Map<String, String> headers = legacy.getRecordHeaders() == null ? Map.of() : legacy.getRecordHeaders();
        return new ConsumedRecord(legacy.getTopic(), legacy.getPartition(), legacy.getOffset(), legacy.getKey(), legacy.getPayload(), headers);
    }

    /**
     * Collapses consecutive identical records into runs, preserving order.
     */
    static List<Run> runsOf(List<ProduceRecord> records) {
        List<Run> runs = new ArrayList<>();
        for (ProduceRecord record : records) {
            int last = runs.size() - 1;
            if (last >= 0 && runs.get(last).record().equals(record)) {
                runs.set(last, new Run(record, runs.get(last).count() + 1));
            }
            else {
                runs.add(new Run(record, 1));
            }
        }
        return runs;
    }

    record Run(ProduceRecord record, int count) {}
}
