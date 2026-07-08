/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RoutingEventTest {

    @Test
    void shouldReturnEmptyWhenBodyIsNotProduceRequest() {
        // Given
        var event = makeRequest(new MetadataRequestData());

        // When / Then
        assertThat(event.firstProducerId()).isEqualTo(OptionalLong.empty());
        assertThat(event.firstBaseSequence()).isEqualTo(OptionalInt.empty());
        assertThat(event.firstProducerEpoch()).isEqualTo(OptionalInt.empty());
    }

    @Test
    void shouldReturnEmptyWhenNoIdempotentBatchPresent() {
        // Given: PRODUCE body with a non-idempotent batch (NO_PRODUCER_ID)
        var event = makeRequest(produceWith(nonIdempotentRecords()));

        // When / Then
        assertThat(event.firstProducerId()).isEqualTo(OptionalLong.empty());
        assertThat(event.firstBaseSequence()).isEqualTo(OptionalInt.empty());
        assertThat(event.firstProducerEpoch()).isEqualTo(OptionalInt.empty());
    }

    @Test
    void firstProducerIdShouldReturnProducerIdForIdempotentBatch() {
        // Given
        var event = makeRequest(produceWith(idempotentRecords(42L, (short) 1, 0)));

        // When / Then
        assertThat(event.firstProducerId()).isEqualTo(OptionalLong.of(42L));
    }

    @Test
    void firstBaseSequenceShouldReturnBaseSequenceForIdempotentBatch() {
        // Given
        var event = makeRequest(produceWith(idempotentRecords(42L, (short) 1, 7)));

        // When / Then
        assertThat(event.firstBaseSequence()).isEqualTo(OptionalInt.of(7));
    }

    @Test
    void firstProducerEpochShouldReturnEpochForIdempotentBatch() {
        // Given
        var event = makeRequest(produceWith(idempotentRecords(42L, (short) 3, 0)));

        // When / Then
        assertThat(event.firstProducerEpoch()).isEqualTo(OptionalInt.of(3));
    }

    @Test
    void shouldSkipPartitionsWithNullRecords() {
        // Given: partition with null records followed by no idempotent batches
        var produce = new ProduceRequestData();
        var topic = new ProduceRequestData.TopicProduceData().setName("t");
        topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0));
        produce.topicData().add(topic);
        var event = makeRequest(produce);

        // When / Then
        assertThat(event.firstProducerId()).isEqualTo(OptionalLong.empty());
        assertThat(event.firstBaseSequence()).isEqualTo(OptionalInt.empty());
        assertThat(event.firstProducerEpoch()).isEqualTo(OptionalInt.empty());
    }

    @Test
    void requestAccessorsShouldReturnConstructorValues() {
        // Given
        var header = new RequestHeaderData().setCorrelationId(5);
        var body = new MetadataRequestData();
        var event = new RoutingEvent.Request("sess", "r1", 10, -100, ApiKeys.FETCH, (short) 12, header, body);

        // When / Then
        assertThat(event.sessionId()).isEqualTo("sess");
        assertThat(event.route()).isEqualTo("r1");
        assertThat(event.clientCorrelationId()).isEqualTo(10);
        assertThat(event.routingCorrelationId()).isEqualTo(-100);
        assertThat(event.apiKey()).isEqualTo(ApiKeys.FETCH);
        assertThat(event.apiVersion()).isEqualTo((short) 12);
        assertThat(event.header()).isSameAs(header);
        assertThat(event.body()).isSameAs(body);
    }

    @Test
    void responseAccessorsShouldReturnConstructorValues() {
        // Given
        var header = new ResponseHeaderData().setCorrelationId(99);
        var body = new MetadataRequestData();
        var event = new RoutingEvent.Response("sess", "r2", -200, ApiKeys.METADATA, header, body);

        // When / Then
        assertThat(event.sessionId()).isEqualTo("sess");
        assertThat(event.route()).isEqualTo("r2");
        assertThat(event.routingCorrelationId()).isEqualTo(-200);
        assertThat(event.apiKey()).isEqualTo(ApiKeys.METADATA);
        assertThat(event.header()).isSameAs(header);
        assertThat(event.body()).isSameAs(body);
    }

    private static RoutingEvent.Request makeRequest(org.apache.kafka.common.protocol.ApiMessage body) {
        return new RoutingEvent.Request("s", "r", 1, -1, ApiKeys.PRODUCE, (short) 9, new RequestHeaderData(), body);
    }

    private static MemoryRecords idempotentRecords(long producerId, short epoch, int baseSequence) {
        try (var builder = new MemoryRecordsBuilder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                producerId,
                epoch,
                baseSequence,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                0)) {
            builder.append(0L, null, "v".getBytes());
            return builder.build();
        }
    }

    private static MemoryRecords nonIdempotentRecords() {
        try (var builder = new MemoryRecordsBuilder(
                ByteBuffer.allocate(1024),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                (short) -1,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                0)) {
            builder.append(0L, null, "v".getBytes());
            return builder.build();
        }
    }

    private static ProduceRequestData produceWith(MemoryRecords records) {
        var produce = new ProduceRequestData();
        var topic = new ProduceRequestData.TopicProduceData().setName("t");
        topic.partitionData().add(new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(records));
        produce.topicData().add(topic);
        return produce;
    }
}
