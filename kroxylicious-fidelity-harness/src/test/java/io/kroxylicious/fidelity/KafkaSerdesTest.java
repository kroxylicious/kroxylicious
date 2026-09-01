/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.List;

import io.kroxylicious.kafka.common.message.RequestHeaderData;

class KafkaSerdesTest extends AbstractSerdesTest<RequestHeaderData> {

    private static RequestHeaderData newMessage() {
        return new RequestHeaderData();
    }

    @Override
    RequestHeaderData populate(short requestApiKey, short requestApiVersion, int correlationId, String clientId) {
        return newMessage().setRequestApiKey(requestApiKey)
                .setRequestApiVersion(requestApiVersion)
                .setCorrelationId(correlationId)
                .setClientId(clientId);
    }

    @Override
    Snapshot snapshot(RequestHeaderData message) {
        List<TagSnapshot> tags = message.unknownTaggedFields().stream()
                .map(field -> new TagSnapshot(field.tag(), toBoxedList(field.data())))
                .toList();
        return new Snapshot(message.requestApiKey(), message.requestApiVersion(), message.correlationId(), message.clientId(), tags);
    }

    @Override
    byte[] write(RequestHeaderData message, short version) {
        return KafkaSerdes.write(message, version);
    }

    @Override
    ReadResult<RequestHeaderData> read(byte[] bytes, short version) {
        return KafkaSerdes.read(newMessage(), bytes, version);
    }
}