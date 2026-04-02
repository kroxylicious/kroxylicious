/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;

import io.kroxylicious.proxy.config.ProxyProtocolMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class HaProxyProtocolDetectionHandlerTest {

    // PROXY protocol v2 binary signature (12 bytes)
    private static final byte[] PROXY_V2_SIGNATURE = {
            0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
    };

    // Minimal valid PROXY v2 header: 12-byte sig + version/command + family + length + addresses
    private static final byte[] PROXY_V2_TCP4_HEADER = buildProxyV2Tcp4Header("192.168.1.100", 54321, "10.0.0.1", 9092);

    // ---- REQUIRED mode ----

    @Test
    void requiredModeShouldAddDecoderWhenProxyV2Detected() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.REQUIRED, pcsm);
        var channel = new EmbeddedChannel(handler);

        channel.writeInbound(Unpooled.wrappedBuffer(PROXY_V2_TCP4_HEADER));

        // Detection handler should have removed itself
        assertThat(channel.pipeline().get(HaProxyProtocolDetectionHandler.class)).isNull();

        // HAProxyMessageHandler consumes the message and forwards to state machine
        verify(pcsm).onHAProxyMessageReceived(any(HAProxyMessage.class));
    }

    @Test
    void requiredModeShouldCloseChannelWhenNonProxyBytesReceived() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.REQUIRED, pcsm);
        var channel = new EmbeddedChannel(handler);

        channel.writeInbound(Unpooled.wrappedBuffer("not a proxy header at all!".getBytes()));

        // Channel should be closed
        assertThat(channel.isOpen()).isFalse();

        // No inbound messages should have been passed through
        assertThat((Object) channel.readInbound()).isNull();

        // State machine should not have been called
        verify(pcsm, never()).onHAProxyMessageReceived(any());
    }

    // ---- AUTO mode ----

    @Test
    void autoModeShouldAddDecoderWhenProxyV2Detected() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.AUTO, pcsm);
        var channel = new EmbeddedChannel(handler);

        channel.writeInbound(Unpooled.wrappedBuffer(PROXY_V2_TCP4_HEADER));

        // Detection handler should have removed itself
        assertThat(channel.pipeline().get(HaProxyProtocolDetectionHandler.class)).isNull();

        // HAProxyMessageHandler consumes the message and forwards to state machine
        verify(pcsm).onHAProxyMessageReceived(any(HAProxyMessage.class));
    }

    @Test
    void autoModeShouldPassThroughWhenNonProxyBytesReceived() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.AUTO, pcsm);
        var channel = new EmbeddedChannel(handler);

        byte[] kafkaBytes = "some kafka data!".getBytes();
        channel.writeInbound(Unpooled.wrappedBuffer(kafkaBytes));

        // Channel should remain open
        assertThat(channel.isOpen()).isTrue();

        // Detection handler should have removed itself
        assertThat(channel.pipeline().get(HaProxyProtocolDetectionHandler.class)).isNull();

        // No decoder should have been added
        assertThat(channel.pipeline().get(HAProxyMessageDecoder.class)).isNull();

        // Bytes should have been passed through
        ByteBuf passedThrough = channel.readInbound();
        assertThat(passedThrough).isNotNull();
        byte[] actual = new byte[passedThrough.readableBytes()];
        passedThrough.readBytes(actual);
        assertThat(actual).isEqualTo(kafkaBytes);
        passedThrough.release();
    }

    @Test
    void autoModeShouldNotCallStateMachineWhenNonProxyBytesReceived() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.AUTO, pcsm);
        var channel = new EmbeddedChannel(handler);

        channel.writeInbound(Unpooled.wrappedBuffer("kafka bytes here".getBytes()));

        verify(pcsm, never()).onHAProxyMessageReceived(any());
    }

    @Test
    void shouldPassThroughNonByteBufMessages() throws Exception {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.REQUIRED, pcsm);
        var channel = new EmbeddedChannel(handler);

        String stringMsg = "hello";
        channel.writeInbound(stringMsg);

        // Should pass through without removal
        assertThat(channel.pipeline().get(HaProxyProtocolDetectionHandler.class)).isNotNull();
        assertThat((Object) channel.readInbound()).isEqualTo(stringMsg);
    }

    // ---- PROXY v1 detection ----

    @Test
    void autoModeShouldDetectProxyV1Header() {
        var pcsm = mock(ProxyChannelStateMachine.class);
        var handler = new HaProxyProtocolDetectionHandler(ProxyProtocolMode.AUTO, pcsm);
        var channel = new EmbeddedChannel(handler);

        // PROXY v1 text header
        String v1Header = "PROXY TCP4 192.168.1.100 10.0.0.1 54321 9092\r\n";
        channel.writeInbound(Unpooled.wrappedBuffer(v1Header.getBytes()));

        // Should have detected PROXY header (decoder auto-removes after decoding)
        assertThat(channel.pipeline().get(HaProxyProtocolDetectionHandler.class)).isNull();

        // HAProxyMessageHandler consumes the message and forwards to state machine
        verify(pcsm).onHAProxyMessageReceived(any(HAProxyMessage.class));
    }

    // ---- Helper to build a minimal valid PROXY v2 TCP4 header ----

    private static byte[] buildProxyV2Tcp4Header(String srcAddr, int srcPort, String dstAddr, int dstPort) {
        // PROXY v2 header format:
        // 12 bytes signature
        // 1 byte: version (0x2) | command (0x1 = PROXY)
        // 1 byte: address family (0x11 = AF_INET + STREAM)
        // 2 bytes: address length (12 for TCP4: 4+4+2+2)
        // 4 bytes: src addr
        // 4 bytes: dst addr
        // 2 bytes: src port
        // 2 bytes: dst port
        byte[] header = new byte[28]; // 12 + 4 + 12
        System.arraycopy(PROXY_V2_SIGNATURE, 0, header, 0, 12);
        header[12] = 0x21; // version 2, PROXY command
        header[13] = 0x11; // AF_INET, STREAM
        header[14] = 0x00; // address length high byte
        header[15] = 0x0C; // address length low byte (12)

        // Source address
        String[] srcParts = srcAddr.split("\\.");
        for (int i = 0; i < 4; i++) {
            header[16 + i] = (byte) Integer.parseInt(srcParts[i]);
        }
        // Destination address
        String[] dstParts = dstAddr.split("\\.");
        for (int i = 0; i < 4; i++) {
            header[20 + i] = (byte) Integer.parseInt(dstParts[i]);
        }
        // Source port
        header[24] = (byte) (srcPort >> 8);
        header[25] = (byte) srcPort;
        // Destination port
        header[26] = (byte) (dstPort >> 8);
        header[27] = (byte) dstPort;

        return header;
    }
}
