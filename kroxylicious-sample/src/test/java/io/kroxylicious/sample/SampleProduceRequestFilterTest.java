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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.stubbing.Answer;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.sample.config.SampleFilterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SampleProduceRequestFilterTest {

    private static final short API_VERSION = ApiMessageType.PRODUCE.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String POST_TRANSFORM_VALUE = "this is what the value will be transformed to";
    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";

    private KrpcFilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor = ArgumentCaptor.forClass(ApiMessage.class);

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
    public void willTransformProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(PRE_TRANSFORM_VALUE);
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var forwardedRequest = ((ResponseFilterResult) stage.toCompletableFuture().get()).response();
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
    public void wontTransformProduceRequestTest() throws Exception {
        var requestData = buildProduceRequestData(NO_TRANSFORM_VALUE);
        var stage = filter.onProduceRequest(API_VERSION, headerData, requestData, context);
        assertThat(stage).isCompleted();
        var forwardedRequest = ((ResponseFilterResult) stage.toCompletableFuture().get()).response();
        var unpackedRequest = unpackProduceRequestData((ProduceRequestData) forwardedRequest);
        // We should see that the unpacked request value has not changed from the input value
        assertThat(unpackedRequest)
                .containsExactly(NO_TRANSFORM_VALUE);
    }

    private void setupContextMock() {
        context = mock(KrpcFilterContext.class);
        // create stub for createByteBufferOutputStream method
        ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(Integer.class);
        when(context.createByteBufferOutputStream(argument.capture())).thenAnswer(
                (Answer<ByteBufferOutputStream>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Integer size = (Integer) args[0];
                    return new ByteBufferOutputStream(size);
                });
    }

    private static ProduceRequestData buildProduceRequestData(String transformValue) {
        var requestData = new ProduceRequestData();
        // Build stream
        var stream = new ByteBufferOutputStream(ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)));
        // Build records from stream
        var recordsBuilder = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
        // Add transformValue as buffer to records
        recordsBuilder.append(RecordBatch.NO_TIMESTAMP, null, ByteBuffer.wrap(transformValue.getBytes(StandardCharsets.UTF_8)).position(0));
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
        var topics = requestData.topicData();
        var values = new ArrayList<String>();
        for (ProduceRequestData.TopicProduceData topicData : topics) {
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                for (Record record : ((MemoryRecords) partitionData.records()).records()) {
                    values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
                }
            }
        }
        return values;
    }
}
