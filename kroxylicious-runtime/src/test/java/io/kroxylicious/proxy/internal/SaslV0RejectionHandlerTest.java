/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import io.netty.channel.embedded.EmbeddedChannel;

import io.kroxylicious.proxy.frame.DecodedResponseFrame;
import io.kroxylicious.proxy.frame.Frame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SaslV0RejectionHandlerTest {

    private EmbeddedChannel channel;
    private SaslV0RejectionHandler saslV0RejectionHandler;

    @BeforeEach
    void setUp() {
        saslV0RejectionHandler = new SaslV0RejectionHandler();
        channel = new EmbeddedChannel(saslV0RejectionHandler);
    }

    @AfterEach
    void tearDown() {
        if (channel.isActive()) {
            channel.finish();
        }
    }

    @Test
    void v0SaslHandshakeRequestRejected() {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn((short) 0);
        when(frame.apiKeyId()).thenReturn(ApiKeys.SASL_HANDSHAKE.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        Object o = channel.readOutbound();
        assertThat(o).isInstanceOfSatisfying(DecodedResponseFrame.class, decodedResponseFrame -> {
            assertThat(decodedResponseFrame.apiVersion()).isEqualTo((short) 0);
            assertThat(decodedResponseFrame.correlationId()).isEqualTo(3);
            assertThat(decodedResponseFrame.header()).isEqualTo(new ResponseHeaderData().setCorrelationId(3));
            assertThat(decodedResponseFrame.body()).isInstanceOfSatisfying(SaslHandshakeResponseData.class, saslHandshakeResponseData -> {
                assertThat(saslHandshakeResponseData.errorCode()).isEqualTo(Errors.UNSUPPORTED_VERSION.code());
            });
        });
        assertThat(channel.isOpen()).isFalse();
    }

    public static Stream<Arguments> otherSaslHandshakeRequestVersionsForwarded() {
        return IntStream.rangeClosed(1, ApiKeys.SASL_HANDSHAKE.latestVersion(true))
                .mapToObj(i -> Arguments.argumentSet("SASL_HANDSHAKE version " + i, (short) i));
    }

    @MethodSource
    @ParameterizedTest
    void otherSaslHandshakeRequestVersionsForwarded(short apiVersion) {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn(apiVersion);
        when(frame.apiKeyId()).thenReturn(ApiKeys.SASL_HANDSHAKE.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        Object o = channel.readInbound();
        assertThat(o).isSameAs(frame);
        assertThat(channel.isOpen()).isTrue();
        assertThat(channel.outboundMessages()).isEmpty();
    }

    @EnumSource(value = ApiKeys.class, names = { "SASL_HANDSHAKE" }, mode = EnumSource.Mode.EXCLUDE)
    @ParameterizedTest
    void otherRpcsForwarded(ApiKeys apiKeys) {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn((short) 0);
        when(frame.apiKeyId()).thenReturn(apiKeys.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        Object o = channel.readInbound();
        assertThat(o).isSameAs(frame);
        assertThat(channel.isOpen()).isTrue();
        assertThat(channel.outboundMessages()).isEmpty();
    }

    @EnumSource(value = ApiKeys.class, names = { "SASL_HANDSHAKE", "API_VERSIONS" }, mode = EnumSource.Mode.EXCLUDE)
    @ParameterizedTest
    void handlerDeregisteredOnFirstNonHandshakeRpc(ApiKeys apiKeys) {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn((short) 0);
        when(frame.apiKeyId()).thenReturn(apiKeys.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        assertThat(channel.pipeline().first()).isNull();
    }

    @CsvSource({ "SASL_HANDSHAKE,1", "SASL_HANDSHAKE,2" })
    @ParameterizedTest
    void handlerDeregisteredOnNonV0HandshakeRpc() {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn((short) 1);
        when(frame.apiKeyId()).thenReturn(ApiKeys.SASL_HANDSHAKE.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        assertThat(channel.pipeline().first()).isNull();
    }

    @Test
    void handlerNotDeregisteredOnApiVersionsRpc() {
        Frame frame = mock(Frame.class);
        when(frame.apiVersion()).thenReturn((short) 0);
        when(frame.apiKeyId()).thenReturn(ApiKeys.API_VERSIONS.id);
        when(frame.correlationId()).thenReturn(3);
        channel.writeOneInbound(frame);
        assertThat(channel.pipeline().first()).isSameAs(saslV0RejectionHandler);
    }

}