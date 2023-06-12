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
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;
import io.kroxylicious.proxy.filter.KrpcFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SampleFetchResponseFilterTest {

    // this is arbitrary for our filter, so set to 0
    private static final short API_VERSION = 0;
    private static final String PRE_TRANSFORM_VALUE = "this is what the value will be transformed from";
    private static final String NO_TRANSFORM_VALUE = "this value will not be transformed";
    private static final String POST_TRANSFORM_VALUE = "this is what the value will be transformed to";
    private static final String CONFIG_FROM = "from";
    private static final String CONFIG_TO = "to";
    private static final ApiMessageType API_MESSAGE_TYPE = ApiMessageType.FETCH;

    @Mock
    private KrpcFilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor = ArgumentCaptor.forClass(ApiMessage.class);

    private SampleFetchResponseFilter.SampleFetchResponseConfig config;
    private SampleFetchResponseFilter filter;
    private FilterInvoker invoker;

    @BeforeEach
    public void beforeEach() {
        buildContextMock();
        config = new SampleFetchResponseFilter.SampleFetchResponseConfig(CONFIG_FROM, CONFIG_TO);
        filter = new SampleFetchResponseFilter(config);
        invoker = FilterInvokers.from(filter);
    }

    @Test
    public void validateSampleFetchResponseConfigTest() {
        assertThat(config.getFrom()).isEqualTo(CONFIG_FROM);
        assertThat(config.getTo()).isEqualTo(CONFIG_TO);
    }

    @Test
    public void willTransformFetchResponseTest() {
        var headerData = new ResponseHeaderData();
        var responseData = buildFetchResponseData(PRE_TRANSFORM_VALUE);
        invoker.onResponse(ApiKeys.forId(API_MESSAGE_TYPE.apiKey()), API_VERSION, headerData, responseData, context);
        verify(context).forwardResponse(any(), apiMessageCaptor.capture());
        var unpackedResponse = unpackFetchResponseData((FetchResponseData) apiMessageCaptor.getValue());
        // We only put 1 record in, we should only get 1 record back, and
        // We should see that the unpacked response value has changed from the input value, and
        // We should see that the unpacked response value has been transformed to the correct value
        assertThat(unpackedResponse)
                .hasSize(1)
                .first().asString()
                .doesNotContain(PRE_TRANSFORM_VALUE)
                .contains(POST_TRANSFORM_VALUE);
    }

    @Test
    public void wontTransformFetchResponseTest() {
        var headerData = new ResponseHeaderData();
        var responseData = buildFetchResponseData(NO_TRANSFORM_VALUE);
        invoker.onResponse(ApiKeys.forId(API_MESSAGE_TYPE.apiKey()), API_VERSION, headerData, responseData, context);
        verify(context).forwardResponse(any(), apiMessageCaptor.capture());
        var unpackedResponse = unpackFetchResponseData((FetchResponseData) apiMessageCaptor.getValue());
        // Check we only have 1 unpacked record, and that its value is the same as the input value
        assertThat(unpackedResponse)
                .hasSize(1)
                .first().asString()
                .contains(NO_TRANSFORM_VALUE);
    }

    private void buildContextMock() {
        context = mock(KrpcFilterContext.class);
        // create stub for createByteBufferOutputStream method
        ArgumentCaptor<Integer> argument = ArgumentCaptor.forClass(Integer.class);
        when(context.createByteBufferOutputStream(argument.capture())).thenAnswer(
                new Answer() {
                    public Object answer(InvocationOnMock invocation) {
                        Object[] args = invocation.getArguments();
                        Integer size = (Integer) args[0];
                        return new ByteBufferOutputStream(size);
                    }
                }
        );
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
        var recordsBuilder = new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0, RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false, RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
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
                var records = (MemoryRecords) partitionData.records();
                values.add(new String(StandardCharsets.UTF_8.decode(records.buffer()).array()));
            }
        }
        return values;
    }
}
