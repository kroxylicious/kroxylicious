/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DecodedRequestFrameTest {

    private static final int CORRELATION_ID = 1;
    private static final short API_VERSION = ApiKeys.METADATA.latestVersion();

    @Test
    void testDecodedRequestFrame() {
        DecodedRequestFrame<MetadataRequestData> frame = createRequestFrame();
        ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        MetadataResponseData responseBody = new MetadataResponseData();
        DecodedResponseFrame<? extends ApiMessage> responseFrame = frame.responseFrame(responseHeaderData, responseBody);
        assertThat(responseFrame.correlationId()).isEqualTo(CORRELATION_ID);
        assertThat(responseFrame.apiVersion()).isEqualTo(API_VERSION);
        assertThat(responseFrame.body()).isSameAs(responseBody);
        assertThat(responseFrame.header()).isSameAs(responseHeaderData);
    }

    @Test
    void testResponseFrameApiKeyMustMatch() {
        DecodedRequestFrame<MetadataRequestData> frame = createRequestFrame();
        ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData responseBody = new ApiVersionsResponseData();
        assertThatThrownBy(() -> {
            frame.responseFrame(responseHeaderData, responseBody);
        }).isInstanceOf(AssertionError.class).hasMessage("Attempt to create responseFrame with ApiMessage of type API_VERSIONS but request is of type METADATA");
    }

    private static DecodedRequestFrame<MetadataRequestData> createRequestFrame() {
        RequestHeaderData header = new RequestHeaderData();
        header.setCorrelationId(CORRELATION_ID);
        MetadataRequestData request = new MetadataRequestData();
        return new DecodedRequestFrame<>(API_VERSION, CORRELATION_ID,
                true, header, request);
    }

}