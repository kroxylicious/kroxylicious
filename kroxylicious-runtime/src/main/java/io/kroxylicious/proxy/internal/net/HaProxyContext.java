/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import io.netty.util.NetUtil;

import io.kroxylicious.proxy.proxyprotocol.ClientProxyProtocolContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Immutable snapshot of a decoded PROXY protocol header.
 * <p>
 * Captures the command, proxied transport protocol, source/destination addresses, ports and
 * any TLV extensions from the Netty {@link HAProxyMessage} so that the reference-counted
 * message can be released immediately after extraction.
 * </p>
 *
 * @param command            the PROXY protocol command (PROXY or LOCAL)
 * @param proxiedProtocol    the proxied transport protocol (e.g. TCP4, TCP6, UDP4, UNIX_STREAM, UNKNOWN)
 * @param sourceAddress      the original client address reported by the load-balancer, or null when the header carries no address (e.g. a LOCAL command or UNKNOWN protocol)
 * @param destinationAddress the destination address the client connected to, or null when the header carries no address (e.g. a LOCAL command or UNKNOWN protocol)
 * @param sourcePort         the original client port
 * @param destinationPort    the destination port
 * @param tlvs               HaProxy v2 TLV extensions keyed by type name, values are {@code byte[]}
 */
public record HaProxyContext(
                             HAProxyCommand command,
                             HAProxyProxiedProtocol proxiedProtocol,
                             @Nullable String sourceAddress,
                             @Nullable String destinationAddress,
                             int sourcePort,
                             int destinationPort,
                             Map<String, byte[]> tlvs) {

    /**
     * Compact constructor that deep-copies and wraps the TLV map to ensure immutability.
     */
    public HaProxyContext {
        tlvs = tlvs.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().clone()));
    }

    /**
     * Returns a defensive copy of the TLV map to prevent mutation of internal state.
     *
     * @return a new map with cloned byte array values
     */
    @Override
    public Map<String, byte[]> tlvs() {
        return tlvs.entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().clone()));
    }

    /**
     * Creates an {@link HaProxyContext} from a Netty {@link HAProxyMessage}.
     * <p>
     * TLV content is deep-copied so the returned context is safe to use
     * after the message is released.
     * </p>
     *
     * @param msg the decoded proxy message
     * @return an immutable context snapshot
     */
    public static HaProxyContext from(HAProxyMessage msg) {
        Map<String, byte[]> tlvs = msg.tlvs().stream()
                .collect(
                        Collectors.toMap(
                                t -> t.type().name(),
                                t -> ByteBufUtil.getBytes(t.content())));
        return new HaProxyContext(
                msg.command(),
                msg.proxiedProtocol(),
                msg.sourceAddress(),
                msg.destinationAddress(),
                msg.sourcePort(),
                msg.destinationPort(),
                tlvs);
    }

    /**
     * Adapts this internal snapshot to the public {@link ClientProxyProtocolContext} API.
     * <p>
     * Only a {@link HAProxyCommand#PROXY} header carrying a {@link HAProxyProxiedProtocol#TCP4} or
     * {@link HAProxyProxiedProtocol#TCP6} transport is exposed, using {@link InetSocketAddress}. Other
     * commands and transports (including {@link HAProxyProxiedProtocol#UNIX_STREAM}) return empty so that
     * {@code clientAddress()} falls back to the immediate transport peer.
     * </p>
     *
     * @return the public context, or empty if the header does not carry a supported TCP4/TCP6 endpoint
     */
    public Optional<ClientProxyProtocolContext> toClientProxyProtocolContext() {
        if (command != HAProxyCommand.PROXY) {
            return Optional.empty();
        }
        return switch (proxiedProtocol) {
            case TCP4, TCP6 -> inetContext();
            default -> Optional.empty();
        };
    }

    private Optional<ClientProxyProtocolContext> inetContext() {
        InetAddress source = parseInetAddress(sourceAddress);
        InetAddress destination = parseInetAddress(destinationAddress);
        if (source == null || destination == null) {
            return Optional.empty();
        }
        return Optional.of(new ClientProxyProtocolContextImpl(new InetSocketAddress(source, sourcePort), new InetSocketAddress(destination, destinationPort)));
    }

    @Nullable
    private static InetAddress parseInetAddress(@Nullable String address) {
        if (address == null) {
            return null;
        }
        // PROXY protocol INET addresses are IP literals; NetUtil parses them without a DNS lookup and returns null otherwise.
        return NetUtil.createInetAddressFromIpAddressString(address);
    }

    private record ClientProxyProtocolContextImpl(SocketAddress sourceAddress, SocketAddress destinationAddress)
            implements ClientProxyProtocolContext {}
}
