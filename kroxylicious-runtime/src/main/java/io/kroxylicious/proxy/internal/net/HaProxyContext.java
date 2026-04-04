/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.haproxy.HAProxyMessage;

/**
 * Immutable snapshot of a decoded PROXY protocol header.
 * <p>
 * Captures the source/destination addresses, ports and any TLV extensions
 * from the Netty {@link HAProxyMessage} so that the reference-counted
 * message can be released immediately after extraction.
 * </p>
 *
 * @param sourceAddress      the original client address reported by the load-balancer
 * @param destinationAddress the destination address the client connected to
 * @param sourcePort         the original client port
 * @param destinationPort    the destination port
 * @param tlvs               HaProxy v2 TLV extensions keyed by type name, values are {@code byte[]}
 */
public record HaProxyContext(
                             String sourceAddress,
                             String destinationAddress,
                             int sourcePort,
                             int destinationPort,
                             Map<String, byte[]> tlvs) {

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
                msg.sourceAddress(),
                msg.destinationAddress(),
                msg.sourcePort(),
                msg.destinationPort(),
                tlvs);
    }
}
