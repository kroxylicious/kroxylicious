/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sample;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
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
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.sample.config.SampleFilterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SampleProduceRequestFilterTest {

    private static final short API_VERSION = ApiMessageType.PRODUCE.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String POST_TRANSFORM_VALUE = "this is what the value will be transformed to";
    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";

    @Mock
    private FilterContext context;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private RequestFilterResult requestFilterResult;
    @Captor
    private ArgumentCaptor<Integer> bufferInitialCapacity;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;
    @Captor
    private ArgumentCaptor<RequestHeaderData> requestHeaderDataCaptor;

    private SampleProduceRequestFilter filter;
    private RequestHeaderData headerData;

    @BeforeEach
    public void beforeEach() {
        setupContextMock();
        SampleFilterConfig config = new SampleFilterConfig(CONFIG_FIND_VALUE, CONFIG_REPLACE_VALUE);
        this.filter = new SampleProduceRequestFilter(config);
        this.headerData = new RequestHeaderData();
    }

    /**
     * Unit Test: Checks that transformation is applied when request data contains configured value.
     */
    @Test
    void willTransformProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE);
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var forwardedRequest = stage.toCompletableFuture().get().message();
        var unpackedRequest = unpackProduceRequestData((ProduceRequestData) forwardedRequest);
        // We should see that the unpacked request value has changed from the input value, and
        // We should see that the unpacked request value has been transformed to the correct value
        assertThat(unpackedRequest)
                                   .doesNotContain(PRE_TRANSFORM_VALUE)
                                   .containsExactly(POST_TRANSFORM_VALUE);
    }

    /**
     * Unit Test: Checks that transformation is not applied when request data does not contain configured
     * value.
     */
    @Test
    void wontTransformProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(NO_TRANSFORM_VALUE);
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var forwardedRequest = stage.toCompletableFuture().get().message();
        var unpackedRequest = unpackProduceRequestData((ProduceRequestData) forwardedRequest);
        // We should see that the unpacked request value has not changed from the input value
        assertThat(unpackedRequest)
                                   .containsExactly(NO_TRANSFORM_VALUE);
    }

    /**
     * Unit Test: Checks that when a transformation is applied that the request record(s) metadata matches what was sent.
     */
    @Test
    void onTransformMetadataRetainedProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE);
        var sent = requestData.duplicate(); // due to pass-by-reference issues we need a safe copy to compare with
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var received = (ProduceRequestData) stage.toCompletableFuture().get().message();
        compareRecords(received, sent);
    }

    /**
     * Unit Test: Checks that when a transformation is not applied that the request record(s) metadata matches what was sent.
     */
    @Test
    void onNoTransformMetadataRetainedProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(NO_TRANSFORM_VALUE);
        var sent = requestData.duplicate(); // due to pass-by-reference issues we need a safe copy to compare with
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var received = (ProduceRequestData) stage.toCompletableFuture().get().message();
        compareRecords(received, sent);
    }

    private void setupContextMock() {
        when(context.forwardRequest(requestHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(requestFilterResult)
        );
        when(requestFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(requestFilterResult.header()).thenAnswer(invocation -> requestHeaderDataCaptor.getValue());

        when(context.createByteBufferOutputStream(bufferInitialCapacity.capture())).thenAnswer(
                (Answer<ByteBufferOutputStream>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Integer size = (Integer) args[0];
                    return new ByteBufferOutputStream(size);
                }
        );
    }

    private static ProduceRequestData buildProduceRequestData(String transformValue) {
        var requestData = new ProduceRequestData();
        // Build stream
        var stream = new ByteBufferOutputStream(ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)));
        // Build records from stream
        var recordsBuilder = new MemoryRecordsBuilder(
                stream,
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0,
                RecordBatch.NO_TIMESTAMP,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                stream.remaining()
        );
        // Create record Headers
        Header header = new RecordHeader("myKey", "myValue".getBytes());
        // Add transformValue as buffer to records
        recordsBuilder.append(RecordBatch.NO_TIMESTAMP, null, ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)).position(0), new Header[]{ header });
        var records = recordsBuilder.build();
        // Build partitions from built records
        var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
        var partitionData = new ProduceRequestData.PartitionProduceData();
        partitionData.setRecords(records);
        partitions.add(partitionData);
        // Build topics from built partitions
        var topics = new ProduceRequestData.TopicProduceDataCollection();
        var topicData = new ProduceRequestData.TopicProduceData();
        topicData.setPartitionData(partitions);
        topics.add(topicData);
        // Add built topics to ProduceRequestData object so that we can return it
        requestData.setTopicData(topics);
        return requestData;
    }

    private static List<String> unpackProduceRequestData(ProduceRequestData requestData) {
        var recordList = unpackRecords(requestData);
        var values = new ArrayList<String>();
        for (Record record : recordList) {
            values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
        }
        return values;
    }

    private static List<Record> unpackRecords(ProduceRequestData requestData) {
        List<Record> records = new ArrayList<>();
        for (ProduceRequestData.TopicProduceData topicProduceData : requestData.topicData()) {
            for (ProduceRequestData.PartitionProduceData partitionProduceData : topicProduceData.partitionData()) {
                for (Record record : ((Records) partitionProduceData.records()).records()) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private static void compareRecords(ProduceRequestData a, ProduceRequestData b) {
        var aRecords = unpackRecords(a).listIterator();
        var bRecords = unpackRecords(b).listIterator();
        assertThat(aRecords).isNotNull();
        assertThat(bRecords).isNotNull();
        while (aRecords.hasNext()) {
            assertThat(bRecords).hasNext();
            checkRecordMetadataMatches(aRecords.next(), bRecords.next());
        }
    }

    private static void checkRecordMetadataMatches(Record a, Record b) {
        // We don't compare record size, value, or value size as we expect the filter to change these
        assertThat(a).usingRecursiveComparison()
                     .ignoringFields("sizeInBytes", "valueSize", "value")
                     .isEqualTo(b);
        assertThat(a.headers()).usingRecursiveComparison().isEqualTo(b.headers());
    }
}
