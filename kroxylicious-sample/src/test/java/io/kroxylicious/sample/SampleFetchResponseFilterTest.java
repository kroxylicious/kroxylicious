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

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.sample.config.SampleFilterConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SampleFetchResponseFilterTest {

    private static final short API_VERSION = ApiMessageType.FETCH.highestSupportedVersion(true); // this is arbitrary for our filter
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String POST_TRANSFORM_VALUE = "this is what the value will be transformed to";
    private static final String CONFIG_FIND_VALUE = "from";
    private static final String CONFIG_REPLACE_VALUE = "to";
    @Mock
    private KrpcFilterContext context;

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ResponseFilterResult responseFilterResult;
    @Captor
    private ArgumentCaptor<Integer> bufferInitialCapacity;

    @Captor
    private ArgumentCaptor<ResponseHeaderData> responseHeaderDataCaptor;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;
    private SampleFetchResponseFilter filter;
    private ResponseHeaderData headerData;

    @BeforeEach
    public void beforeEach() {
        setupContextMock();
        SampleFilterConfig config = new SampleFilterConfig(CONFIG_FIND_VALUE, CONFIG_REPLACE_VALUE);
        this.filter = new SampleFetchResponseFilter(config);
        this.headerData = new ResponseHeaderData();
    }

    /**
     * Unit Test: Checks that transformation is applied when response data contains configured value.
     */
    @Test
    public void willTransformFetchResponseTest() throws Exception {
        var responseData = buildFetchResponseData(PRE_TRANSFORM_VALUE);
        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        var response = stage.toCompletableFuture().get().message();
        var unpackedResponse = unpackFetchResponseData(((FetchResponseData) response));
        // We only put 1 record in, we should only get 1 record back, and
        // We should see that the unpacked response value has changed from the input value, and
        // We should see that the unpacked response value has been transformed to the correct value
        assertThat(unpackedResponse)
                .doesNotContain(PRE_TRANSFORM_VALUE)
                .containsExactly(POST_TRANSFORM_VALUE);
    }

    /**
     * Unit Test: Checks that transformation is not applied when response data does not contain configured
     * value.
     */
    @Test
    public void wontTransformFetchResponseTest() throws Exception {
        var responseData = buildFetchResponseData(NO_TRANSFORM_VALUE);
        var stage = filter.onFetchResponse(API_VERSION, headerData, responseData, context);

        var response = stage.toCompletableFuture().get().message();
        var unpackedResponse = unpackFetchResponseData((FetchResponseData) response);
        // We only put 1 record in, we should only get 1 record back, and
        // We should see that the unpacked response value has not changed from the input value
        assertThat(unpackedResponse).containsExactly(NO_TRANSFORM_VALUE);
    }

    private void setupContextMock() {
        when(context.forwardResponse(responseHeaderDataCaptor.capture(), apiMessageCaptor.capture())).thenAnswer(
                invocation -> CompletableFuture.completedStage(responseFilterResult));
        when(responseFilterResult.message()).thenAnswer(invocation -> apiMessageCaptor.getValue());
        when(responseFilterResult.header()).thenAnswer(invocation -> responseHeaderDataCaptor.getValue());

        when(context.createByteBufferOutputStream(bufferInitialCapacity.capture())).thenAnswer(
                (Answer<ByteBufferOutputStream>) invocation -> {
                    Object[] args = invocation.getArguments();
                    Integer size = (Integer) args[0];
                    return new ByteBufferOutputStream(size);
                });
    }

    /**
     * Builds a FetchResponseData object from a given string. The string provided will then be unwrapped by
     * the SampleFetchResponseFilter during the test, after which it will attempt to perform a
     * transformation on that string based on the provided configuration.
     * @param transformValue  the content to be embedded in the response and transformed (or not) in the test
     * @return the FetchResponseData object to be passed to the SampleFetchResponseFilter during the test
     */
    private static FetchResponseData buildFetchResponseData(String transformValue) {
        var responseData = new FetchResponseData();
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
        var partitions = new ArrayList<FetchResponseData.PartitionData>();
        var partitionData = new FetchResponseData.PartitionData();
        partitionData.setRecords(records);
        partitions.add(partitionData);
        // Build responses from built partitions
        var responses = new ArrayList<FetchResponseData.FetchableTopicResponse>();
        var response = new FetchResponseData.FetchableTopicResponse();
        response.setPartitions(partitions);
        responses.add(response);
        // Add built responses to FetchResponseData object so that we can return it
        responseData.setResponses(responses);
        return responseData;
    }

    private static List<String> unpackFetchResponseData(FetchResponseData responseData) {
        var responses = responseData.responses();
        var values = new ArrayList<String>();
        for (FetchResponseData.FetchableTopicResponse response : responses) {
            for (FetchResponseData.PartitionData partitionData : response.partitions()) {
                for (Record record : ((MemoryRecords) partitionData.records()).records()) {
                    values.add(new String(StandardCharsets.UTF_8.decode(record.value()).array()));
                }
            }
        }
        return values;
    }
}
