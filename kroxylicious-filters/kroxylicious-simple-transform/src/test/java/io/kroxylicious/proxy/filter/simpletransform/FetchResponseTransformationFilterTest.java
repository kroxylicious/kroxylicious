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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
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
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FetchResponseTransformationFilterTest {

    private static final String TOPIC_NAME = "mytopic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String ORIGINAL_RECORD_VALUE = "lowercasevalue";
    private static final String EXPECTED_TRANSFORMED_RECORD_VALUE = ORIGINAL_RECORD_VALUE.toUpperCase(Locale.ROOT);
    private static final String RECORD_KEY = "key";
    private FetchResponseTransformationFilter filter;
    @Mock(strictness = Mock.Strictness.LENIENT)
    FilterContext context;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ResponseFilterResult responseFilterResult;

    @Mock(strictness = Mock.Strictness.LENIENT, extraInterfaces = CloseOrTerminalStage.class)
    private ResponseFilterResultBuilder responseFilterResultBuilder;

    @Captor
    private ArgumentCaptor<Integer> bufferInitialCapacity;

    @Captor
    private ArgumentCaptor<ResponseHeaderData> responseHeaderDataCaptor;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        filter = new FetchResponseTransformationFilter(new UpperCasing.Transformation(
                new UpperCasing.Config("UTF-8")));

        when(context.forwardResponse(responseHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(responseFilterResult));
        when(context.responseFilterResultBuilder()).thenReturn(responseFilterResultBuilder);

        when(responseFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(responseFilterResult.header()).thenAnswer(invocation -> responseHeaderDataCaptor.getValue());

        when(responseFilterResultBuilder.forward(responseHeaderDataCaptor.capture(), apiMessageCaptor.capture()))
                .thenReturn((CloseOrTerminalStage<ResponseFilterResult>) responseFilterResultBuilder);
        when(((CloseOrTerminalStage<ResponseFilterResult>) responseFilterResultBuilder).completed()).thenReturn(CompletableFuture.completedStage(responseFilterResult));

        when(context.createByteBufferOutputStream(bufferInitialCapacity.capture())).thenAnswer(
                (Answer<ByteBufferOutputStream>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Integer size = (Integer) args[0];
                    return new ByteBufferOutputStream(size);
                });
    }

    @Test
    void testFactory() {
        FetchResponseTransformation factory = new FetchResponseTransformation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage(FetchResponseTransformation.class.getSimpleName() + " requires configuration, but config object is null");
        FilterFactoryContext constructContext = mock(FilterFactoryContext.class);
        doReturn(new UpperCasing()).when(constructContext).pluginInstance(any(), any());
        FetchResponseTransformation.Config config = new FetchResponseTransformation.Config(UpperCasing.class.getName(),
                new UpperCasing.Config("UTF-8"));
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(FetchResponseTransformationFilter.class);
    }

    @Test
    void filterHandlesPreV13ResponseBasedOnTopicNames() throws Exception {

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopic(TOPIC_NAME)); // Version 12

        var stage = filter.onFetchResponse(fetchResponse.apiKey(), new ResponseHeaderData(), fetchResponse, context);
        assertThat(stage).isCompleted();

        var filteredResponse = (FetchResponseData) stage.toCompletableFuture().get().message();
        var filteredRecords = responseToRecordStream(filteredResponse).toList();
        assertThat(filteredRecords)
                .withFailMessage("unexpected number of records in the filter response")
                .hasSize(1);

        var filteredRecord = filteredRecords.get(0);
        assertThat(decodeUtf8Value(filteredRecord))
                .withFailMessage("expected record value to have been transformed")
                .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
    }

    @Test
    void filterHandlesV13OrHigherResponseBasedOnTopicIds() throws Exception {

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopicId(TOPIC_ID));

        var metadataResponse = new MetadataResponseData();
        metadataResponse.topics().add(new MetadataResponseData.MetadataResponseTopic().setTopicId(TOPIC_ID).setName(TOPIC_NAME));

        when(context.sendRequest(isA(RequestHeaderData.class), isA(MetadataRequestData.class)))
                .thenReturn(CompletableFuture.completedStage(metadataResponse));

        var stage = filter.onFetchResponse(fetchResponse.apiKey(), new ResponseHeaderData(), fetchResponse, context);
        assertThat(stage).isCompleted();

        var filteredResponse = (FetchResponseData) stage.toCompletableFuture().get().message();

        // verify that the response now has the topic name
        assertThat(filteredResponse.responses())
                .withFailMessage("expected same number of topics in the response")
                .hasSameSizeAs(fetchResponse.responses())
                .withFailMessage("expected topic response to have been augmented with topic name")
                .anyMatch(ftr -> Objects.equals(ftr.topic(), TOPIC_NAME))
                .withFailMessage("expected topic response to still have the topic id")
                .anyMatch(ftr -> Objects.equals(ftr.topicId(), TOPIC_ID));

        var filteredRecords = responseToRecordStream(filteredResponse).toList();
        assertThat(filteredRecords)
                .withFailMessage("unexpected number of records in the filter response")
                .hasSize(1);

        var filteredRecord = filteredRecords.get(0);
        assertThat(decodeUtf8Value(filteredRecord))
                .withFailMessage("expected record value to have been transformed")
                .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
    }

    @Test
    void filterHandlesMetadataRequestError() {

        var fetchResponse = new FetchResponseData();
        // Version 13 switched to topic id rather than topic names.
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopicId(TOPIC_ID));

        var metadataResponse = new MetadataResponseData();
        metadataResponse.topics().add(new MetadataResponseData.MetadataResponseTopic().setTopicId(TOPIC_ID).setName(TOPIC_NAME));

        when(context.sendRequest(isA(RequestHeaderData.class), isA(MetadataRequestData.class)))
                .thenReturn(CompletableFuture.failedStage(new IllegalStateException("out-of-band request exception")));

        var stage = filter.onFetchResponse(fetchResponse.apiKey(), new ResponseHeaderData(), fetchResponse, context);
        assertThat(stage)
                .withFailMessage("out-of-band request exception")
                .isCompletedExceptionally();
    }

    private Stream<Record> responseToRecordStream(FetchResponseData filteredResponse) {
        return Stream.of(filteredResponse.responses())
                .flatMap(Collection::stream)
                .map(FetchableTopicResponse::partitions)
                .flatMap(Collection::stream)
                .map(PartitionData::records)
                .map(Records.class::cast)
                .map(Records::records)
                .map(Iterable::spliterator)
                .flatMap(si -> StreamSupport.stream(si, false));
    }

    @NonNull
    private static FetchableTopicResponse createFetchableTopicResponseWithOneRecord(String key, String value) {
        var fetchableTopicResponse = new FetchableTopicResponse();
        var partitionData1 = new PartitionData();
        partitionData1.setRecords(buildOneRecord(key, value));
        fetchableTopicResponse.partitions().add(partitionData1);
        return fetchableTopicResponse;
    }

    private static MemoryRecords buildOneRecord(String key, String value) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis())) {
            builder.append(0L, key.getBytes(), value.getBytes());
            return builder.build();
        }
    }

    @NonNull
    private String decodeUtf8Value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

}
