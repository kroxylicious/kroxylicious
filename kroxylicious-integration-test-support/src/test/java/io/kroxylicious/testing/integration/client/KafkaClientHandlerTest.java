/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.testing.integration.client;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.testing.integration.codec.DecodedRequestFrame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link KafkaClientHandler} lifecycle behavior.
 */
class KafkaClientHandlerTest {

    @Test
    void channelInactive_completesQueuedRequestsExceptionally() {
        // given
        var handler = new KafkaClientHandler();
        var channel = new EmbeddedChannel(handler);
        var requestFrame = createRequestFrame();
        CompletableFuture<SequencedResponse> future = handler.sendRequest(requestFrame);

        // when
        channel.pipeline().fireChannelInactive();

        // then
        assertThat(future)
                .isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .hasCauseInstanceOf(ChannelClosedException.class);
    }

    @Test
    void exceptionCaught_failsQueuedRequestsWithCause() {
        // given
        var handler = new KafkaClientHandler();
        var channel = new EmbeddedChannel(handler);
        var requestFrame = createRequestFrame();
        CompletableFuture<SequencedResponse> future = handler.sendRequest(requestFrame);

        // when
        var testException = new IOException("test error");
        channel.pipeline().fireExceptionCaught(testException);

        // then
        assertThat(future)
                .isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .hasCauseExactlyInstanceOf(IOException.class)
                .hasMessageContaining("test error");
    }

    @Test
    void channelActive_sendsQueuedRequests() {
        // given
        var handler = new KafkaClientHandler();
        var channel = new EmbeddedChannel(handler);
        var requestFrame = createRequestFrame();
        CompletableFuture<SequencedResponse> future = handler.sendRequest(requestFrame);

        // when
        channel.pipeline().fireChannelActive();
        channel.runPendingTasks();

        // then
        assertThat(channel.outboundMessages())
                .hasSize(1);
        assertThat(future)
                .isNotCompleted();

        channel.close();
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
