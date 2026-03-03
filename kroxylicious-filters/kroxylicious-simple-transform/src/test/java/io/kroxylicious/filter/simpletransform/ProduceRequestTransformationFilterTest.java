/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
    public static final Uuid TOPIC_ID = new Uuid(5L, 5L);
    private ProduceRequestTransformationFilter filter;

    @Mock(strictness = Mock.Strictness.LENIENT)
    FilterFactoryContext factoryContext;

    @BeforeEach
    void setUp() {
        filter = new ProduceRequestTransformationFilter(new UpperCasing.Transformation(
                new UpperCasing.Config("UTF-8")));
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
        assertThatCode(() -> {
            factory.initialize(factoryContext, cfg);
        }).doesNotThrowAnyException();
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
    void uppercasingProduceRequest() {
        var produceRequest = new ProduceRequestData();
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setName(TOPIC_NAME));
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, produceRequest).build();
        var stage = filter.onProduceRequest(produceRequest.apiKey(), header, produceRequest, mockFilterContext);

        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result)
                    .isForwardRequest().hasMessageInstanceOfSatisfying(ProduceRequestData.class, filteredRequest -> {
                        assertThat(filteredRequest.topicData())
                                .withFailMessage("expected same number of topics in the request")
                                .hasSameSizeAs(produceRequest.topicData());

                        var filteredRecords = requestToRecordStream(filteredRequest).toList();
                        assertThat(filteredRecords)
                                .withFailMessage("unexpected number of records in the filtered request")
                                .hasSize(1);

                        var filteredRecord = filteredRecords.get(0);
                        assertThat(decodeUtf8Value(filteredRecord))
                                .withFailMessage("expected record value to have been transformed")
                                .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
        });

    }

    @Test
    void topicNamePassedToTransformerWhenRequestUsesTopicId() {
        // replaces the value with the topic name
        var topicNameReplacingFilter = getTopicNameReplacingFilter();
        var produceRequest = new ProduceRequestData();
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopicId(TOPIC_ID));

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, produceRequest).withTopicName(TOPIC_ID, TOPIC_NAME).build();
        var stage = topicNameReplacingFilter.onProduceRequest(produceRequest.apiKey(), header, produceRequest, mockFilterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result)
                    .isForwardRequest().hasMessageInstanceOfSatisfying(ProduceRequestData.class, filteredRequest -> {
                        assertThat(filteredRequest.topicData())
                                .withFailMessage("expected same number of topics in the request")
                                .hasSameSizeAs(produceRequest.topicData());

                        var filteredRecords = requestToRecordStream(filteredRequest).toList();
                        assertThat(filteredRecords)
                                .withFailMessage("unexpected number of records in the filtered request")
                                .hasSize(1);

                        var filteredRecord = filteredRecords.get(0);

                        // verify that the filtered request value is now the topic name
                        assertThat(decodeUtf8Value(filteredRecord))
                                .withFailMessage("expected record value to have been transformed")
                                .isEqualTo(TOPIC_NAME);
                    });
        });
    }

    @Test
    void topicNamePassedToTransformerWhenRequestUsesTopicName() {
        // replaces the value with the topic name
        var topicNameReplacingFilter = getTopicNameReplacingFilter();
        var produceRequest = new ProduceRequestData();
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setName(TOPIC_NAME));

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, produceRequest).build();
        var stage = topicNameReplacingFilter.onProduceRequest(produceRequest.apiKey(), header, produceRequest, mockFilterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(result -> {
            MockFilterContextAssert.assertThat(result)
                    .isForwardRequest().hasMessageInstanceOfSatisfying(ProduceRequestData.class, filteredRequest -> {
                        assertThat(filteredRequest.topicData())
                                .withFailMessage("expected same number of topics in the request")
                                .hasSameSizeAs(produceRequest.topicData());

                        var filteredRecords = requestToRecordStream(filteredRequest).toList();
                        assertThat(filteredRecords)
                                .withFailMessage("unexpected number of records in the filtered request")
                                .hasSize(1);

                        var filteredRecord = filteredRecords.get(0);

                        // verify that the response now has the topic name
                        assertThat(decodeUtf8Value(filteredRecord))
                                .withFailMessage("expected record value to have been transformed")
                                .isEqualTo(TOPIC_NAME);
                    });
        });
    }

    @NonNull
    private static ProduceRequestTransformationFilter getTopicNameReplacingFilter() {
        return new ProduceRequestTransformationFilter((topicName, original) -> ByteBuffer.wrap(topicName.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void filterProduceRequestWithFailedTopicIdLookup() {
        var produceRequest = new ProduceRequestData();
        produceRequest.setAcks((short) 1);
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopicId(TOPIC_ID));

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, produceRequest).build();

        var stage = filter.onProduceRequest(produceRequest.apiKey(), header, produceRequest, mockFilterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isShortCircuitResponse().isErrorResponse()
                    .errorResponse().isInstanceOf(UnknownServerException.class);
        });
    }

    @Test
    void filterProduceRequestWithFailedTopicIdLookupZeroAck() {
        var produceRequest = new ProduceRequestData();
        produceRequest.setAcks((short) 0);
        produceRequest.topicData().add(createTopicProduceDataWithOneRecord(RECORD_KEY, ORIGINAL_RECORD_VALUE).setTopicId(TOPIC_ID));

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext mockFilterContext = MockFilterContext.builder(header, produceRequest).build();

        var stage = filter.onProduceRequest(produceRequest.apiKey(), header, produceRequest, mockFilterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isDropRequest();
        });
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
