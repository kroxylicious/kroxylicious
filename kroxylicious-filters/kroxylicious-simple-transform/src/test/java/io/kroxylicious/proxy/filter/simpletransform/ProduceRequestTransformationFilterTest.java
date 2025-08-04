/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.simpletransform;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProduceRequestTransformationFilterTest {

    private static final String TOPIC_NAME = "mytopic";
    private static final String ORIGINAL_RECORD_VALUE = "lowercasevalue";
    private static final String EXPECTED_TRANSFORMED_RECORD_VALUE = ORIGINAL_RECORD_VALUE.toUpperCase(Locale.ROOT);
    private static final String RECORD_KEY = "key";
    private ProduceRequestTransformationFilter filter;

    @Mock(strictness = Mock.Strictness.LENIENT)
    FilterFactoryContext factoryContext;

    @Mock(strictness = Mock.Strictness.LENIENT)
    FilterContext context;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private RequestFilterResult responseFilterResult;

    @Captor
    private ArgumentCaptor<Integer> bufferInitialCapacity;

    @Captor
    private ArgumentCaptor<RequestHeaderData> requestHeaderDataCaptor;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @BeforeEach
    void setUp() {
        filter = new ProduceRequestTransformationFilter(new UpperCasing.Transformation(
                new UpperCasing.Config("UTF-8")));

        when(context.forwardRequest(requestHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(responseFilterResult));

        when(responseFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(responseFilterResult.header()).thenAnswer(invocation -> requestHeaderDataCaptor.getValue());

        when(context.createByteBufferOutputStream(bufferInitialCapacity.capture())).thenAnswer(
                (Answer<ByteBufferOutputStream>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Integer size = (Integer) args[0];
                    return new ByteBufferOutputStream(size);
                });
    }

    @Test
    void testFactoryRejectsNullConfig() {
        var factory = new ProduceRequestTransformation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage(ProduceRequestTransformation.class.getSimpleName() + " requires configuration, but config object is null");
    }

    @Test
    void testFactoryAcceptedValidConfig() {
        var factory = new ProduceRequestTransformation();
        var tfactory = mock(ByteBufferTransformationFactory.class);
        when(factoryContext.pluginInstance(any(), any())).thenReturn(tfactory);
        var cfg = new ProduceRequestTransformation.Config(null, null);
        factory.initialize(factoryContext, cfg);
    }

    @Test
    void testFactory() {
        var factory = new ProduceRequestTransformation();
        FilterFactoryContext constructContext = mock(FilterFactoryContext.class);
        doReturn(new UpperCasing()).when(constructContext).pluginInstance(any(), any());
        var config = new ProduceRequestTransformation.Config(UpperCasing.class.getName(),
                new UpperCasing.Config("UTF-8"));
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(ProduceRequestTransformationFilter.class);
    }

    @Test
    void filterProduceRequest() throws Exception {

        var produceRequest = new ProduceRequestData();
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setName(TOPIC_NAME));

        var stage = filter.onProduceRequest(produceRequest.apiKey(), new RequestHeaderData(), produceRequest, context);
        assertThat(stage).isCompleted();

        var filteredRequest = (ProduceRequestData) stage.toCompletableFuture().get().message();

        // verify that the response now has the topic name
        assertThat(filteredRequest.topicData())
                .withFailMessage("expected same number of topics in the request")
                .hasSameSizeAs(produceRequest.topicData())
                .withFailMessage("expected topic request to have been augmented with topic name")
                .anyMatch(ftr -> Objects.equals(ftr.name(), TOPIC_NAME));

        var filteredRecords = requestToRecordStream(filteredRequest).toList();
        assertThat(filteredRecords)
                .withFailMessage("unexpected number of records in the filtered request")
                .hasSize(1);

        var filteredRecord = filteredRecords.get(0);
        assertThat(decodeUtf8Value(filteredRecord))
                .withFailMessage("expected record value to have been transformed")
                .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
    }

    private Stream<Record> requestToRecordStream(ProduceRequestData filteredResponse) {
        return Stream.of(filteredResponse.topicData())
                .flatMap(Collection::stream)
                .map(TopicProduceData::partitionData)
                .flatMap(Collection::stream)
                .map(PartitionProduceData::records)
                .map(Records.class::cast)
                .map(Records::records)
                .map(Iterable::spliterator)
                .flatMap(si -> StreamSupport.stream(si, false));
    }

    private static TopicProduceData createTopicProduceDataWithOneRecord(String key, String value) {
        var topicProduceData = new TopicProduceData();
        var partitionData = new PartitionProduceData();
        partitionData.setRecords(buildOneRecord(key, value));
        topicProduceData.partitionData().add(partitionData);
        return topicProduceData;
    }

    private static MemoryRecords buildOneRecord(String key, String value) {
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(1024), RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis())) {
            builder.append(0L, key.getBytes(), value.getBytes());
            return builder.build();
        }
    }

    private String decodeUtf8Value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

}
