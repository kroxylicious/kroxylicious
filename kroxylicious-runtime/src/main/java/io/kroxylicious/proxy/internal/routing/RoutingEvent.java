/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Events captured at the router layer as requests are sent to routes
 * and responses return. Used by integration tests for white-box
 * verification of router behaviour.
 */
public sealed interface RoutingEvent {

    AtomicReference<Consumer<RoutingEvent>> EVENT_LISTENER = new AtomicReference<>();

    /**
     * Installs a listener that receives router events. Intended for
     * integration tests — production code should never call this.
     */
    static void setEventListener(@Nullable Consumer<RoutingEvent> listener) {
        EVENT_LISTENER.set(listener);
    }

    String sessionId();

    String route();

    int routingCorrelationId();

    ApiKeys apiKey();

    /**
     * A request sent to a named route via {@link RouterContextImpl#sendRequest}.
     */
    record Request(
                   String sessionId,
                   String route,
                   int clientCorrelationId,
                   int routingCorrelationId,
                   ApiKeys apiKey,
                   short apiVersion,
                   RequestHeaderData header,
                   ApiMessage body)
            implements RoutingEvent {

        /**
         * Extracts the producer ID from the first idempotent {@link RecordBatch}
         * in a PRODUCE request body. Returns empty for non-PRODUCE requests or
         * when no idempotent batch is present.
         */
        public OptionalLong firstProducerId() {
            return firstBatchField(RecordBatch::producerId, RecordBatch.NO_PRODUCER_ID);
        }

        /**
         * Extracts the base sequence from the first idempotent {@link RecordBatch}.
         */
        public OptionalInt firstBaseSequence() {
            if (!(body instanceof ProduceRequestData produce)) {
                return OptionalInt.empty();
            }
            for (var td : produce.topicData()) {
                for (var pd : td.partitionData()) {
                    if (pd.records() == null) {
                        continue;
                    }
                    MemoryRecords records = (MemoryRecords) pd.records();
                    for (RecordBatch batch : records.batches()) {
                        if (batch.producerId() != RecordBatch.NO_PRODUCER_ID) {
                            return OptionalInt.of(batch.baseSequence());
                        }
                    }
                }
            }
            return OptionalInt.empty();
        }

        /**
         * Extracts the producer epoch from the first idempotent {@link RecordBatch}.
         */
        public OptionalInt firstProducerEpoch() {
            if (!(body instanceof ProduceRequestData produce)) {
                return OptionalInt.empty();
            }
            for (var td : produce.topicData()) {
                for (var pd : td.partitionData()) {
                    if (pd.records() == null) {
                        continue;
                    }
                    MemoryRecords records = (MemoryRecords) pd.records();
                    for (RecordBatch batch : records.batches()) {
                        if (batch.producerId() != RecordBatch.NO_PRODUCER_ID) {
                            return OptionalInt.of(batch.producerEpoch());
                        }
                    }
                }
            }
            return OptionalInt.empty();
        }

        private OptionalLong firstBatchField(java.util.function.ToLongFunction<RecordBatch> extractor,
                                             long sentinel) {
            if (!(body instanceof ProduceRequestData produce)) {
                return OptionalLong.empty();
            }
            for (var td : produce.topicData()) {
                for (var pd : td.partitionData()) {
                    if (pd.records() == null) {
                        continue;
                    }
                    MemoryRecords records = (MemoryRecords) pd.records();
                    for (RecordBatch batch : records.batches()) {
                        long value = extractor.applyAsLong(batch);
                        if (value != sentinel) {
                            return OptionalLong.of(value);
                        }
                    }
                }
            }
            return OptionalLong.empty();
        }
    }

    /**
     * A response received from a route.
     */
    record Response(
                    String sessionId,
                    String route,
                    int routingCorrelationId,
                    ApiKeys apiKey,
                    ResponseHeaderData header,
                    ApiMessage body)
            implements RoutingEvent {}
}
