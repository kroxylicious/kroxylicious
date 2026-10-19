/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import java.net.SocketAddress;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.proxyprotocol.ClientProxyProtocolContext;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A filter that records the client address information observed on the first request it sees, keyed by a
 * caller-supplied ID so that the test and the filter instance running inside the proxy can share data without
 * a network boundary.
 */
public class ClientAddressRecordingFilter implements RequestFilter {

    /**
     * The client address information observed by the filter.
     *
     * @param peerAddress the immediate transport peer address
     * @param clientAddress the effective client address
     * @param proxyProtocolSourceAddress the PROXY-protocol source address, or {@code null} if no PROXY header was received
     */
    public record Observation(SocketAddress peerAddress, SocketAddress clientAddress, @Nullable SocketAddress proxyProtocolSourceAddress) {}

    private static final ConcurrentHashMap<String, Observation> OBSERVATIONS = new ConcurrentHashMap<>();

    @Nullable
    public static Observation observationFor(String id) {
        return OBSERVATIONS.get(id);
    }

    public static void reset(String id) {
        OBSERVATIONS.remove(id);
    }

    private final String recorderId;

    ClientAddressRecordingFilter(String recorderId) {
        this.recorderId = recorderId;
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, FilterContext context) {
        SocketAddress proxyProtocolSource = context.clientProxyProtocolContext().map(ClientProxyProtocolContext::sourceAddress).orElse(null);
        OBSERVATIONS.putIfAbsent(recorderId, new Observation(context.peerAddress(), context.clientAddress(), proxyProtocolSource));
        return context.forwardRequest(header, body);
    }
}
