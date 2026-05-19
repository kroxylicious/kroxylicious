/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.integration.client;

import java.io.IOException;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.testing.integration.codec.DecodedRequestFrame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaClientHandler} lifecycle behavior.
 */
class KafkaClientHandlerTest {

    private final KafkaClientHandler handler = new KafkaClientHandler();
    private final EmbeddedChannel channel = new EmbeddedChannel(handler);

    @AfterEach
    void tearDown() {
        channel.close();
    }

    @Test
    void channelInactive_completesQueuedRequestsExceptionally() {
        // given
        var requestFrame = createRequestFrame();
        var responseFuture = handler.sendRequest(requestFrame);

        // when
        channel.pipeline().fireChannelInactive();

        // then
        assertThat(responseFuture)
                .isCompletedExceptionally();
        assertThatThrownBy(responseFuture::join)
                .hasCauseInstanceOf(ChannelClosedException.class);
    }

    @Test
    void exceptionCaught_failsQueuedRequestsWithCause() {
        // given
        var requestFrame = createRequestFrame();
        var responseFuture = handler.sendRequest(requestFrame);
        var testException = new IOException("test error");

        // when
        channel.pipeline().fireExceptionCaught(testException);

        // then
        assertThat(responseFuture)
                .isCompletedExceptionally();
        assertThatThrownBy(responseFuture::join)
                .hasCauseExactlyInstanceOf(IOException.class)
                .hasMessageContaining("test error");
    }

    @Test
    void channelActive_sendsQueuedRequests() {
        // given
        var requestFrame = createRequestFrame();
        var responseFuture = handler.sendRequest(requestFrame);

        // when

        // fireChannelActive activates the channel, with processes pending writes, async, on the Netty thread.
        // the runPendingTasks is present to ensure that the async work has actually happened.
        channel.pipeline().fireChannelActive();
        channel.runPendingTasks();

        // then
        assertThat(channel.outboundMessages())
                .hasSize(1);
        assertThat(responseFuture)
                .isNotCompleted();
    }

    private DecodedRequestFrame<ApiVersionsRequestData> createRequestFrame() {
        var header = new org.apache.kafka.common.message.RequestHeaderData()
                .setRequestApiKey(ApiKeys.API_VERSIONS.id)
                .setRequestApiVersion(ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION)
                .setClientId("test-client")
                .setCorrelationId(0);
        return new DecodedRequestFrame<>(
                header.requestApiVersion(),
                header.correlationId(),
                header,
                new ApiVersionsRequestData(),
                ApiVersionsRequestData.HIGHEST_SUPPORTED_VERSION);
    }
}
