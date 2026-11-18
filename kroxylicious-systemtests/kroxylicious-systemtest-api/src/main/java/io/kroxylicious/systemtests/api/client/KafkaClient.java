/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A test-facing Kafka client that expresses <em>intent</em> (produce, consume, create a topic, ...)
 * without exposing which protocol implementation is used ({@link ClientDriver}) or where the
 * client runs ({@link ClientExecutionEnvironment}).
 * <p>
 * The framework builds the client from a {@link ClientSelection}, so the same test can be run
 * against the Java client in-process, librdkafka in a Kubernetes Job, and so on, without change.
 * <p>
 * Produce, consume and topic creation are mandatory for every implementation. The remaining
 * operations are optional: check {@link #supports(ClientCapability)} before relying on them.
 * Unsupported operations throw {@link UnsupportedOperationException}.
 * <p>
 * Failures (e.g. the proxy denying a request) are reported by throwing {@link KafkaClientException}.
 */
public interface KafkaClient {

    /** The timeout used by {@link #consume(String, String, int)}. */
    Duration DEFAULT_CONSUME_TIMEOUT = Duration.ofMinutes(1);

    /** The consumer group used when the test does not specify one. */
    String DEFAULT_CONSUMER_GROUP = "kroxylicious-systemtest-group";

    /**
     * The protocol implementation this client uses.
     *
     * @return driver
     */
    @NonNull
    ClientDriver driver();

    /**
     * Where this client runs.
     *
     * @return execution environment
     */
    @NonNull
    ClientExecutionEnvironment executionEnvironment();

    /**
     * Whether this client supports the given optional operation.
     *
     * @param capability optional operation
     * @return {@code true} if supported
     */
    default boolean supports(@NonNull ClientCapability capability) {
        return false;
    }

    /**
     * Creates a topic.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param partitions number of partitions
     * @param replicationFactor replication factor
     * @throws KafkaClientException if the topic could not be created
     */
    void createTopic(@NonNull String topic, @NonNull String bootstrap, int partitions, int replicationFactor);

    /**
     * Produces {@code count} records with the given value, no key and no headers.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param value record value
     * @param count number of records
     * @throws KafkaClientException if the records could not be produced
     */
    default void produce(@NonNull String topic, @NonNull String bootstrap, @NonNull String value, int count) {
        produce(topic, bootstrap, Collections.nCopies(count, ProduceRecord.of(value)));
    }

    /**
     * Produces the given records, in order.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param records records to produce
     * @throws KafkaClientException if the records could not be produced
     */
    default void produce(@NonNull String topic, @NonNull String bootstrap, @NonNull List<ProduceRecord> records) {
        produce(topic, bootstrap, records, Map.of());
    }

    /**
     * Produces the given records, in order, applying additional client configuration.
     * <p>
     * {@code config} uses Kafka client property names (e.g. {@code compression.type},
     * {@code sasl.mechanism}). Drivers translate them as far as they are able; properties that only
     * exist in the Java client (e.g. {@code sasl.jaas.config}) are not portable across drivers.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param records records to produce
     * @param config additional client configuration
     * @throws KafkaClientException if the records could not be produced
     */
    void produce(@NonNull String topic, @NonNull String bootstrap, @NonNull List<ProduceRecord> records, @NonNull Map<String, String> config);

    /**
     * Consumes up to {@code count} records using {@link #DEFAULT_CONSUMER_GROUP},
     * waiting at most {@link #DEFAULT_CONSUME_TIMEOUT}.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param count number of records wanted
     * @return the consumed records; fewer than {@code count} if the timeout elapsed first
     * @throws KafkaClientException if consumption failed
     */
    @NonNull
    default List<ConsumedRecord> consume(@NonNull String topic, @NonNull String bootstrap, int count) {
        return consume(topic, bootstrap, count, DEFAULT_CONSUME_TIMEOUT);
    }

    /**
     * Consumes up to {@code count} records using {@link #DEFAULT_CONSUMER_GROUP}.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param count number of records wanted
     * @param timeout maximum time to wait for {@code count} records
     * @return the consumed records; fewer than {@code count} if the timeout elapsed first
     * @throws KafkaClientException if consumption failed
     */
    @NonNull
    default List<ConsumedRecord> consume(@NonNull String topic, @NonNull String bootstrap, int count, @NonNull Duration timeout) {
        return consume(topic, bootstrap, count, timeout, DEFAULT_CONSUMER_GROUP, Map.of());
    }

    /**
     * Consumes up to {@code count} records.
     * <p>
     * {@code config} follows the same conventions as {@link #produce(String, String, List, Map)}.
     * The consumer group must be given via {@code groupId}, not via {@code config}.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param count number of records wanted
     * @param timeout maximum time to wait for {@code count} records
     * @param groupId consumer group
     * @param config additional client configuration
     * @return the consumed records; fewer than {@code count} if the timeout elapsed first
     * @throws KafkaClientException if consumption failed
     */
    @NonNull
    List<ConsumedRecord> consume(@NonNull String topic, @NonNull String bootstrap, int count, @NonNull Duration timeout, @NonNull String groupId,
                                 @NonNull Map<String, String> config);

    /**
     * Produces the given records within a single transaction and commits it.
     * Requires {@link ClientCapability#TRANSACTIONS}.
     *
     * @param topic topic name
     * @param bootstrap bootstrap address
     * @param records records to produce
     * @throws KafkaClientException if the transaction could not be committed
     * @throws UnsupportedOperationException if this client does not support transactions
     */
    default void produceInTransaction(@NonNull String topic, @NonNull String bootstrap, @NonNull List<ProduceRecord> records) {
        throw unsupported(ClientCapability.TRANSACTIONS);
    }

    /**
     * Commits the given offsets on behalf of a consumer group.
     * Requires {@link ClientCapability#OFFSET_COMMIT}.
     *
     * @param groupId consumer group
     * @param bootstrap bootstrap address
     * @param offsets offsets to commit, keyed by partition
     * @throws KafkaClientException if the offsets could not be committed
     * @throws UnsupportedOperationException if this client does not support committing offsets
     */
    default void commitOffsets(@NonNull String groupId, @NonNull String bootstrap, @NonNull Map<TopicPartition, Long> offsets) {
        throw unsupported(ClientCapability.OFFSET_COMMIT);
    }

    private UnsupportedOperationException unsupported(ClientCapability capability) {
        return new UnsupportedOperationException(
                capability + " is not supported by the " + driver().envValue() + " driver running " + executionEnvironment().envValue());
    }
}
