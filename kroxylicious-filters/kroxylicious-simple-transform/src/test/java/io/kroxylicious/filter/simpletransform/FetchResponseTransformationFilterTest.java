/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchResponse;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class FetchResponseTransformationFilterTest {

    private static final String TOPIC_NAME = "mytopic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String ORIGINAL_RECORD_VALUE = "lowercasevalue";
    private static final String EXPECTED_TRANSFORMED_RECORD_VALUE = ORIGINAL_RECORD_VALUE.toUpperCase(Locale.ROOT);
    private static final String RECORD_KEY = "key";
    public static final Uuid TOPIC_ID_2 = Uuid.randomUuid();
    private final FetchResponseTransformationFilter filter = new FetchResponseTransformationFilter(new UpperCasing.Transformation(
            new UpperCasing.Config("UTF-8")));

    private static final ObjectMapper MAPPER = ConfigParser.createObjectMapper();

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
    void shouldRequireConfigForReplacingFilter() {
        // Given
        FetchResponseTransformation factory = new FetchResponseTransformation();

        // When
        // Then
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage(FetchResponseTransformation.class.getSimpleName() + " requires configuration, but config object is null");
    }

    @Test
    void shouldConstructReplacingFilter() {
        FetchResponseTransformation factory = new FetchResponseTransformation();
        FilterFactoryContext constructContext = mock(FilterFactoryContext.class);
        doReturn(new Replacing()).when(constructContext).pluginInstance(any(), any());
        FetchResponseTransformation.Config config = new FetchResponseTransformation.Config(Replacing.class.getName(),
                new Replacing.Config(null, "foo", "bar", null));
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(FetchResponseTransformationFilter.class);
    }

    @Test
    void shouldConstructReplacingFilterWithPath(@TempDir Path tempDir) throws IOException {
        Path replamventValuePath = Files.createFile(Path.of(tempDir.toAbsolutePath().toString(), "replacement-value.txt"));
        Files.writeString(replamventValuePath, "bar", StandardCharsets.UTF_8);
        FetchResponseTransformation factory = new FetchResponseTransformation();
        FilterFactoryContext constructContext = mock(FilterFactoryContext.class);
        doReturn(new Replacing()).when(constructContext).pluginInstance(any(), any());
        FetchResponseTransformation.Config config = new FetchResponseTransformation.Config(Replacing.class.getName(),
                new Replacing.Config(null, "foo", null, replamventValuePath));
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(FetchResponseTransformationFilter.class);
    }

    @Test
    void shouldConstructReplacingFilterWithPathConfig(@TempDir Path tempDir) throws IOException {
        // Given
        Path replamventValuePath = Files.createFile(Path.of(tempDir.toAbsolutePath().toString(), "replacement-value.txt"));
        Path valuePath = Files.writeString(replamventValuePath, "bar", StandardCharsets.UTF_8);

        FetchResponseTransformation.Config config = new FetchResponseTransformation.Config(Replacing.class.getName(),
                new Replacing.Config(null, "foo", null, Paths.get(valuePath.toUri())));
        // toUri called so the expected and actual match see
        // https://github.com/FasterXML/jackson-databind/blob/099481bf725afd11dfd4f3c23eed9465fa3391da/src/main/java/com/fasterxml/jackson/databind/ext/NioPathDeserializer.java#L65

        String snippet = MAPPER.writeValueAsString(config).stripTrailing();

        // When
        FetchResponseTransformation.Config actual = MAPPER.readValue(snippet, FetchResponseTransformation.Config.class);

        // Then
        assertThat(actual)
                .isInstanceOf(FetchResponseTransformation.Config.class)
                .isEqualTo(config);
    }

    @Test
    void filterHandlesPreV13ResponseBasedOnTopicNames() {

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord().setTopic(TOPIC_NAME)); // Version 12

        ResponseHeaderData header = new ResponseHeaderData();
        MockFilterContext filterContext = MockFilterContext.builder(header, fetchResponse).build();
        var stage = filter.onFetchResponse(fetchResponse.apiKey(), header, fetchResponse, filterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult1 -> MockFilterContextAssert.assertThat(responseFilterResult1).isForwardResponse()
                .hasMessageInstanceOfSatisfying(FetchResponseData.class, filteredResponse -> {
                    var filteredRecords = responseToRecordStream(filteredResponse.responses()).toList();
                    assertThat(filteredRecords)
                            .withFailMessage("unexpected number of records in the filter response")
                            .hasSize(1);

                    var filteredRecord = filteredRecords.get(0);
                    assertThat(decodeUtf8Value(filteredRecord))
                            .withFailMessage("expected record value to have been transformed")
                            .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
                }));
    }

    @Test
    void filterHandlesV13OrHigherResponseBasedOnTopicIds() {

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord().setTopicId(TOPIC_ID));

        ResponseHeaderData header = new ResponseHeaderData();
        MockFilterContext filterContext = MockFilterContext.builder(header, fetchResponse).withTopicName(TOPIC_ID, TOPIC_NAME).build();
        var stage = filter.onFetchResponse(fetchResponse.apiKey(), header, fetchResponse, filterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult1 -> MockFilterContextAssert.assertThat(responseFilterResult1).isForwardResponse()
                .hasMessageInstanceOfSatisfying(FetchResponseData.class, filteredResponse -> {
                    // verify that the response now has the topic name
                    assertThat(filteredResponse.responses())
                            .withFailMessage("expected same number of topics in the response")
                            .hasSameSizeAs(fetchResponse.responses())
                            .withFailMessage("expected topic response to still have the topic id")
                            .anyMatch(ftr -> Objects.equals(ftr.topicId(), TOPIC_ID));

                    var filteredRecords = responseToRecordStream(filteredResponse.responses()).toList();
                    assertThat(filteredRecords)
                            .withFailMessage("unexpected number of records in the filter response")
                            .hasSize(1);

                    var filteredRecord = filteredRecords.get(0);
                    assertThat(decodeUtf8Value(filteredRecord))
                            .withFailMessage("expected record value to have been transformed")
                            .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
                }));
    }

    @Test
    void filterHandlesPartialTopicIdMappingFailures() {

        var fetchResponse = new FetchResponseData();
        FetchableTopicResponse responseForMappableId = createFetchableTopicResponseWithOneRecord().setTopicId(TOPIC_ID);
        fetchResponse.responses().add(responseForMappableId);
        FetchableTopicResponse responseForFailedId = createFetchableTopicResponseWithOneRecord().setTopicId(TOPIC_ID_2);
        fetchResponse.responses().add(responseForFailedId);

        ResponseHeaderData header = new ResponseHeaderData();
        MockFilterContext filterContext = MockFilterContext.builder(header, fetchResponse).withTopicName(TOPIC_ID, TOPIC_NAME).build();
        var stage = filter.onFetchResponse(fetchResponse.apiKey(), header, fetchResponse, filterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult1 -> MockFilterContextAssert.assertThat(responseFilterResult1).isForwardResponse()
                .hasMessageInstanceOfSatisfying(FetchResponseData.class, filteredResponse -> {
                    // verify that the response now has the topic name
                    ListAssert<FetchableTopicResponse> responseAssert = assertThat(filteredResponse.responses())
                            .withFailMessage("expected same number of topics in the response")
                            .hasSameSizeAs(fetchResponse.responses());
                    responseAssert.element(0).isSameAs(responseForMappableId).satisfies(fetchableTopicResponse -> {
                        var filteredRecords = responseToRecordStream(List.of(fetchableTopicResponse)).toList();
                        assertThat(filteredRecords)
                                .withFailMessage("unexpected number of records in the filter response")
                                .hasSize(1);

                        var filteredRecord = filteredRecords.get(0);
                        assertThat(decodeUtf8Value(filteredRecord))
                                .withFailMessage("expected record value to have been transformed")
                                .isEqualTo(EXPECTED_TRANSFORMED_RECORD_VALUE);
                    });
                    responseAssert.element(1).isSameAs(responseForFailedId).satisfies(fetchableTopicResponse -> {
                        assertThat(fetchableTopicResponse.partitions()).isNotEmpty().allSatisfy(partitionData -> {
                            PartitionData expectedErrorResponse = new PartitionData()
                                    .setPartitionIndex(partitionData.partitionIndex())
                                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                                    .setHighWatermark(FetchResponse.INVALID_HIGH_WATERMARK)
                                    .setRecords(MemoryRecords.EMPTY);
                            assertThat(partitionData).isEqualTo(expectedErrorResponse);
                        });
                    });
                }));
    }

    @Test
    void filterHandlesTopicIdNameError() {

        var fetchResponse = new FetchResponseData();
        fetchResponse.responses().add(createFetchableTopicResponseWithOneRecord().setTopicId(TOPIC_ID));

        ResponseHeaderData header = new ResponseHeaderData();
        TopicNameMappingException unknownTopicIdException = new TopicNameMappingException(Errors.UNKNOWN_TOPIC_ID, "unknown topic id");
        MockFilterContext filterContext = MockFilterContext.builder(header, fetchResponse).withTopicNameError(TOPIC_ID, unknownTopicIdException).build();
        var stage = filter.onFetchResponse(fetchResponse.apiKey(), header, fetchResponse, filterContext);
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                .hasMessageInstanceOfSatisfying(FetchResponseData.class, filteredResponse -> {
                    // verify that the response now has the topic name
                    assertThat(filteredResponse.responses())
                            .withFailMessage("expected same number of topics in the response")
                            .hasSameSizeAs(fetchResponse.responses())
                            .withFailMessage("expected topic response to still have the topic id")
                            .anyMatch(ftr -> Objects.equals(ftr.topicId(), TOPIC_ID));

                    assertThat(filteredResponse.responses()).isNotEmpty().allSatisfy(fetchableTopicResponse -> {
                        assertThat(fetchableTopicResponse.partitions()).isNotEmpty().allSatisfy(partitionData -> {
                            assertThat(partitionData.records()).isEqualTo(MemoryRecords.EMPTY);
                            assertThat(partitionData.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_ID.code());
                        });
                    });
                }));
    }

    private Stream<Record> responseToRecordStream(List<FetchableTopicResponse> responses) {
        return Stream.of(responses)
                .flatMap(Collection::stream)
                .map(FetchableTopicResponse::partitions)
                .flatMap(Collection::stream)
                .map(PartitionData::records)
                .map(Records.class::cast)
                .map(Records::records)
                .map(Iterable::spliterator)
                .flatMap(si -> StreamSupport.stream(si, false));
    }

    private static FetchableTopicResponse createFetchableTopicResponseWithOneRecord() {
        var fetchableTopicResponse = new FetchableTopicResponse();
        var partitionData1 = new PartitionData();
        partitionData1.setPartitionIndex(1);
        partitionData1.setRecords(buildOneRecord());
        fetchableTopicResponse.partitions().add(partitionData1);
        return fetchableTopicResponse;
    }

    private static MemoryRecords buildOneRecord() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        try (MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, 0L, System.currentTimeMillis())) {
            builder.append(0L, FetchResponseTransformationFilterTest.RECORD_KEY.getBytes(), FetchResponseTransformationFilterTest.ORIGINAL_RECORD_VALUE.getBytes());
            return builder.build();
        }
    }

    private String decodeUtf8Value(Record record) {
        return StandardCharsets.UTF_8.decode(record.value()).toString();
    }

}
