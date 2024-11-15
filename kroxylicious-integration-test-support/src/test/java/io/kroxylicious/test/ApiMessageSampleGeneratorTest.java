/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.DefaultRecord;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.AbstractIterator;
import org.junit.jupiter.api.Test;

import static org.apache.kafka.common.protocol.ApiKeys.PRODUCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApiMessageSampleGeneratorTest {

    @Test
    void testResponseSampleGenerationDeterministic() {
        Map<ApiMessageSampleGenerator.ApiAndVersion, ApiMessage> a = ApiMessageSampleGenerator.createResponseSamples();
        Map<ApiMessageSampleGenerator.ApiAndVersion, ApiMessage> b = ApiMessageSampleGenerator.createResponseSamples();

        allSupportedApiVersions().forEach(apiAndVersion -> {
            assertEquals(a.get(apiAndVersion), b.get(apiAndVersion));
        });
    }

    @Test
    void testRequestSampleGenerationDeterministic() {
        Map<ApiMessageSampleGenerator.ApiAndVersion, ApiMessage> a = ApiMessageSampleGenerator.createRequestSamples();
        Map<ApiMessageSampleGenerator.ApiAndVersion, ApiMessage> b = ApiMessageSampleGenerator.createRequestSamples();

        allSupportedApiVersions().forEach(apiAndVersion -> {
            assertEquals(a.get(apiAndVersion), b.get(apiAndVersion));
        });
    }

    public static Stream<ApiMessageSampleGenerator.ApiAndVersion> allSupportedApiVersions() {
        return DataClasses.getRequestClasses().keySet().stream().flatMap(apiKeys -> {
            ApiMessageType messageType = apiKeys.messageType;
            IntStream supported = IntStream.range(messageType.lowestSupportedVersion(), apiKeys.messageType.highestSupportedVersion(true) + 1);
            return supported.mapToObj(version -> new ApiMessageSampleGenerator.ApiAndVersion(apiKeys, (short) version));
        });
    }

    /**
     * Go deep on one type that involves nested collections and MemoryRecords which have
     * been the most fraught area to generate
     */
    @Test
    void testProduceCollectionData() {
        Map<ApiMessageSampleGenerator.ApiAndVersion, ApiMessage> messages = ApiMessageSampleGenerator.createRequestSamples();
        ProduceRequestData message = (ProduceRequestData) messages.get(new ApiMessageSampleGenerator.ApiAndVersion(PRODUCE, PRODUCE.latestVersion()));
        assertEquals(1, message.topicData().size());
        ProduceRequestData.TopicProduceDataCollection topicProduceData = message.topicData();
        Optional<ProduceRequestData.TopicProduceData> first = topicProduceData.stream().findFirst();
        assertTrue(first.isPresent());
        ProduceRequestData.TopicProduceData data = first.get();
        List<ProduceRequestData.PartitionProduceData> partitionProduceData = data.partitionData();
        assertNotNull(partitionProduceData);
        assertEquals(1, partitionProduceData.size());
        ProduceRequestData.PartitionProduceData element = partitionProduceData.get(0);
        BaseRecords records = element.records();
        assertNotNull(records);
        AbstractIterator<MutableRecordBatch> iterator = ((MemoryRecords) records).batchIterator();
        MutableRecordBatch batch = iterator.next();
        assertFalse(iterator.hasNext());
        Iterator<Record> batchIterator = batch.iterator();
        DefaultRecord record = (DefaultRecord) batchIterator.next();
        assertNotNull(record);
        assertFalse(batchIterator.hasNext());
        byte[] keyBytes = new byte[record.keySize()];
        record.key().get(keyBytes);
        byte[] valueBytes = new byte[record.valueSize()];
        record.value().get(valueBytes);
        assertTrue(new String(keyBytes, StandardCharsets.UTF_8).startsWith("key-"));
        assertTrue(new String(valueBytes, StandardCharsets.UTF_8).startsWith("value-"));
    }

    @Test
    void allProduceRequestAllowReply() {
        var prdRequests = ApiMessageSampleGenerator.createRequestSamples().entrySet().stream()
                .map(Map.Entry::getValue)
                .filter(ProduceRequestData.class::isInstance)
                .map(ProduceRequestData.class::cast)
                .toList();

        assertThat(prdRequests)
                .hasSizeGreaterThan(0)
                .allMatch(p -> p.acks() != 0);
    }
}
