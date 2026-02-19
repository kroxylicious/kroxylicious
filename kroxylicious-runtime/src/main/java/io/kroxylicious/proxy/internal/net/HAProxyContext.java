/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;
import java.util.stream.Collectors;

import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyTLV;

/**
 * Immutable snapshot of a decoded HAProxy PROXY protocol header.
 * <p>
 * Captures the source/destination addresses, ports and any TLV extensions
 * from the Netty {@link HAProxyMessage} so that the reference-counted
 * message can be released immediately after {@code channelRead}.
 * </p>
 *
 * @param sourceAddress      the original client address reported by the load-balancer
 * @param destinationAddress the destination address the client connected to
 * @param sourcePort         the original client port
 * @param destinationPort    the destination port
 * @param tlvs               HAProxy v2 TLV extensions keyed by type name
 */
public record HAProxyContext(
                             String sourceAddress,
                             String destinationAddress,
                             int sourcePort,
                             int destinationPort,
                             Map<String, Object> tlvs) {

    /**
     * Creates an {@link HAProxyContext} from a Netty {@link HAProxyMessage}.
     *
     * @param msg the decoded HAProxy message (caller is responsible for releasing it)
     * @return an immutable context snapshot
     */
    public static HAProxyContext from(HAProxyMessage msg) {
        Map<String, Object> tlvs = msg.tlvs().stream()
                .collect(
                        Collectors.toMap(
                                t -> t.type().name(),
                                HAProxyTLV::content));
        return new HAProxyContext(
                msg.sourceAddress(),
                msg.destinationAddress(),
                msg.sourcePort(),
                msg.destinationPort(),
                tlvs);
    }
}
