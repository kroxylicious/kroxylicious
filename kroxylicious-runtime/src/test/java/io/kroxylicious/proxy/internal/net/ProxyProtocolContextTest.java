/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.handler.codec.haproxy.HAProxyTLV;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyProtocolContextTest {

    @Test
    void shouldExtractFieldsFromHAProxyMessage() {
        // Given
        var msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "192.168.1.100",
                "10.0.0.1",
                54321,
                9092);

        // When
        var context = ProxyProtocolContext.from(msg);

        // Then
        assertThat(context.command()).isEqualTo(HAProxyCommand.PROXY);
        assertThat(context.proxiedProtocol()).isEqualTo(HAProxyProxiedProtocol.TCP4);
        assertThat(context.sourceAddress()).isEqualTo("192.168.1.100");
        assertThat(context.destinationAddress()).isEqualTo("10.0.0.1");
        assertThat(context.sourcePort()).isEqualTo(54321);
        assertThat(context.destinationPort()).isEqualTo(9092);
        assertThat(context.tlvs()).isEmpty();

        msg.release();
    }

    @Test
    void shouldRemainValidAfterMessageRelease() {
        // Given
        var msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "10.0.0.50",
                "172.16.0.1",
                12345,
                8080);

        // When
        var context = ProxyProtocolContext.from(msg);
        msg.release();

        // Then — context remains usable after message release
        assertThat(context.sourceAddress()).isEqualTo("10.0.0.50");
        assertThat(context.destinationAddress()).isEqualTo("172.16.0.1");
        assertThat(context.sourcePort()).isEqualTo(12345);
        assertThat(context.destinationPort()).isEqualTo(8080);
    }

    @Test
    void shouldDeepCopyTlvContent() {
        // Given
        var authorityBytes = "broker.example.com".getBytes(StandardCharsets.UTF_8);
        var tlv = new HAProxyTLV(HAProxyTLV.Type.PP2_TYPE_AUTHORITY,
                Unpooled.copiedBuffer(authorityBytes));

        var msg = new HAProxyMessage(
                HAProxyProtocolVersion.V2,
                HAProxyCommand.PROXY,
                HAProxyProxiedProtocol.TCP4,
                "10.0.0.1",
                "10.0.0.2",
                12345,
                9092,
                List.of(tlv));

        // When
        var context = ProxyProtocolContext.from(msg);
        msg.release();

        // Then — TLV content is a deep copy, safe after release
        assertThat(context.tlvs()).containsEntry("PP2_TYPE_AUTHORITY", authorityBytes);
    }

    @Test
    void shouldExposeTcp4HeaderAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                "192.168.1.100", "10.0.0.1", 54321, 9092);
        var expectedSource = new InetSocketAddress("192.168.1.100", 54321);
        var expectedDestination = new InetSocketAddress("10.0.0.1", 9092);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).hasValueSatisfying(c -> {
            assertThat(c.sourceAddress()).isEqualTo(expectedSource);
            assertThat(c.destinationAddress()).isEqualTo(expectedDestination);
        });
        msg.release();
    }

    @Test
    void shouldExposeTcp6HeaderAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP6,
                "2001:db8::1", "2001:db8::2", 54321, 9092);
        var expectedSource = new InetSocketAddress("2001:db8::1", 54321);
        var expectedDestination = new InetSocketAddress("2001:db8::2", 9092);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).hasValueSatisfying(c -> {
            assertThat(c.sourceAddress()).isEqualTo(expectedSource);
            assertThat(c.destinationAddress()).isEqualTo(expectedDestination);
        });
        msg.release();
    }

    @Test
    void shouldNotExposeLocalCommandAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.LOCAL, HAProxyProxiedProtocol.TCP4,
                "192.168.1.100", "10.0.0.1", 54321, 9092);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
        msg.release();
    }

    @Test
    void shouldNotExposeUnknownHeaderAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.UNKNOWN,
                null, null, 0, 0);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
        msg.release();
    }

    @Test
    void shouldNotExposeUdp4HeaderAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.UDP4,
                "192.168.1.100", "10.0.0.1", 54321, 9092);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
        msg.release();
    }

    @Test
    void shouldNotExposeUnixStreamHeaderAsPublicContext() {
        // Given
        var msg = new HAProxyMessage(HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.UNIX_STREAM,
                "/run/haproxy/src.sock", "/run/haproxy/dst.sock", 0, 0);

        // When
        var publicContext = ProxyProtocolContext.from(msg).toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
        msg.release();
    }

    @Test
    void shouldNotExposeTcp4HeaderWithNullAddressAsPublicContext() {
        // Given
        var context = new ProxyProtocolContext(HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                null, "10.0.0.1", 54321, 9092, Map.of());

        // When
        var publicContext = context.toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
    }

    @Test
    void shouldNotExposeTcp4HeaderWithNonIpLiteralAddressAsPublicContext() {
        // Given
        var context = new ProxyProtocolContext(HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
                "not-an-ip.example", "10.0.0.1", 54321, 9092, Map.of());

        // When
        var publicContext = context.toClientProxyProtocolContext();

        // Then
        assertThat(publicContext).isEmpty();
    }
}
