/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.PathElement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InternalRequestFrameTest {
    private static final int CORRELATION_ID = 1;
    private static final short API_VERSION = ApiKeys.METADATA.latestVersion();
    public static final CompletableFuture<Object> PROMISE = new CompletableFuture<>();

    @Test
    void testDecodedRequestFrame() {
        InternalRequestFrame<MetadataRequestData> frame = createRequestFrame();
        ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        MetadataResponseData responseBody = new MetadataResponseData();
        DecodedResponseFrame<? extends ApiMessage> responseFrame = frame.responseFrame(responseHeaderData, responseBody);
        assertThat(responseFrame).isInstanceOfSatisfying(InternalResponseFrame.class, internalResponseFrame -> {
            assertThat(internalResponseFrame.correlationId()).isEqualTo(CORRELATION_ID);
            assertThat(internalResponseFrame.apiVersion()).isEqualTo(API_VERSION);
            assertThat(internalResponseFrame.body()).isSameAs(responseBody);
            assertThat(internalResponseFrame.header()).isSameAs(responseHeaderData);
            assertThat(internalResponseFrame.promise()).isSameAs(PROMISE);
            assertThat(internalResponseFrame.path()).isSameAs(frame.path());
        });
    }

    @Test
    void testResponseFrameApiKeyMustMatch() {
        InternalRequestFrame<MetadataRequestData> frame = createRequestFrame();
        ResponseHeaderData responseHeaderData = new ResponseHeaderData();
        ApiVersionsResponseData responseBody = new ApiVersionsResponseData();
        assertThatThrownBy(() -> {
            frame.responseFrame(responseHeaderData, responseBody);
        }).isInstanceOf(AssertionError.class).hasMessage("Attempt to create responseFrame with ApiMessage of type API_VERSIONS but request is of type METADATA");
    }

    private InternalRequestFrame<MetadataRequestData> createRequestFrame() {
        RequestHeaderData header = new RequestHeaderData();
        header.setCorrelationId(CORRELATION_ID);
        MetadataRequestData request = new MetadataRequestData();
        var frame = new InternalRequestFrame<>(API_VERSION, CORRELATION_ID, true, header, request);
        frame.setPath(new PathElement.Filter("test-filter", 0, PROMISE, null));
        return frame;
    }
}
