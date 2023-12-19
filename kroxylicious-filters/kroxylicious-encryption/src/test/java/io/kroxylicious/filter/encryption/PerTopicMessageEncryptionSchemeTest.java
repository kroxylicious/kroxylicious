/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class PerTopicMessageEncryptionSchemeTest {

    private final InvocationCountingMessageEncryptionScheme plainTextEncryptionScheme = new InvocationCountingMessageEncryptionScheme();
    private final InvocationCountingMessageEncryptionScheme cipherTextEncryptionScheme = new InvocationCountingMessageEncryptionScheme();

    @Test
    void shouldDelegateToDiscreteRecordEncryptionSchemes() {
        // Given
        final PerTopicMessageEncryptionScheme<String> encryptionScheme = new PerTopicMessageEncryptionScheme<>(
                Map.of("plaintext", plainTextEncryptionScheme, "encrypted", cipherTextEncryptionScheme), new PlaintextEncryptionScheme<>());
        final ProduceRequestData produceRequestData = produceRequestForTopic("plaintext", "encrypted");

        // When
        final CompletionStage<ProduceRequestData> result = encryptionScheme.encrypt(null, produceRequestData, ByteBufferOutputStream::new);

        // Then
        assertThat(result).isCompleted();
        assertThat(plainTextEncryptionScheme.getEncryptionCalls()).describedAs("the plaintext scheme to be invoked").isOne();
        assertThat(cipherTextEncryptionScheme.getEncryptionCalls()).describedAs("the enciphering scheme to be invoked").isOne();
    }

    @Test
    void shouldFallbackToDefaultSchemeForUnknownTopic() {
        // Given
        final PerTopicMessageEncryptionScheme<String> encryptionScheme = new PerTopicMessageEncryptionScheme<>(
                Map.of("encrypted", cipherTextEncryptionScheme), plainTextEncryptionScheme);
        final ProduceRequestData produceRequestData = produceRequestForTopic("plaintext", "encrypted");

        // When
        final CompletionStage<ProduceRequestData> result = encryptionScheme.encrypt(null, produceRequestData, ByteBufferOutputStream::new);

        // Then
        assertThat(result).isCompleted();
        assertThat(plainTextEncryptionScheme.getEncryptionCalls()).describedAs("the plaintext scheme to be invoked").isOne();
    }

    @Test
    void shouldLookupEncryptionSchemeForTopic() {
        // Given
        final PerTopicMessageEncryptionScheme<String> encryptionScheme = new PerTopicMessageEncryptionScheme<>(
                Map.of("encrypted", cipherTextEncryptionScheme), plainTextEncryptionScheme);
        final ProduceRequestData produceRequestData = produceRequestForTopic("encrypted");

        // When
        final CompletionStage<ProduceRequestData> result = encryptionScheme.encrypt(null, produceRequestData, ByteBufferOutputStream::new);

        // Then
        assertThat(result).isCompleted();
        assertThat(cipherTextEncryptionScheme.getEncryptionCalls()).describedAs("the plaintext scheme to be invoked").isOne();
        assertThat(plainTextEncryptionScheme.getEncryptionCalls()).describedAs("the plaintext scheme to be invoked").isZero();
    }

    @NonNull
    private static ProduceRequestData produceRequestForTopic(String... topics) {
        final ProduceRequestData produceRequestData = new ProduceRequestData();
        final ProduceRequestData.TopicProduceDataCollection topicProduceData = new ProduceRequestData.TopicProduceDataCollection();
        for (String topicName : topics) {
            final ProduceRequestData.TopicProduceData produceData = new ProduceRequestData.TopicProduceData();
            final ProduceRequestData.PartitionProduceData partitionProduceData = new ProduceRequestData.PartitionProduceData();
            final MemoryRecordsBuilder memoryRecordsBuilder = memoryRecordsBuilderForStream();
            memoryRecordsBuilder.append(new SimpleRecord(new byte[10]));
            partitionProduceData.setRecords(memoryRecordsBuilder.build());
            produceData.setName(topicName);
            produceData.partitionData().add(partitionProduceData);
            topicProduceData.add(produceData);
        }
        produceRequestData.setTopicData(topicProduceData);
        return produceRequestData;
    }

    @NonNull
    private static MemoryRecordsBuilder memoryRecordsBuilderForStream() {
        return new MemoryRecordsBuilder(new ByteBufferOutputStream(1024), RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, 1024);
    }

    private static class InvocationCountingMessageEncryptionScheme implements RecordEncryptionScheme<String> {
        private int encryptionCalls = 0;
        private int decryptionCalls = 0;

        public int getEncryptionCalls() {
            return encryptionCalls;
        }

        public int getDecryptionCalls() {
            return decryptionCalls;
        }

        @Override
        public CompletionStage<Record> encrypt(Record plainTextRecord, RequestHeaderData recordHeaders, ProduceRequestData produceRequestData) {
            encryptionCalls++;
            return CompletableFuture.completedStage(plainTextRecord);

        }

        @Override
        public CompletionStage<Record> decrypt(Record encipheredRecord, ResponseHeaderData responseHeaderData, FetchResponseData fetchResponseData) {
            decryptionCalls++;
            return CompletableFuture.completedStage(encipheredRecord);
        }
    }
}