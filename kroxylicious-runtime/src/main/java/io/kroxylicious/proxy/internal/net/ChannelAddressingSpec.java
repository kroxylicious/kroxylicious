/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.List;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Resolves the identity of an incoming connection across the gateways sharing an acceptor
 * channel.
 * <p>
 * When multiple gateways multiplex over one channel (e.g. SNI-based routing), only one of
 * them will recognise a given connection. This tries each candidate's {@link AddressingSpec}
 * in turn and returns the first that does.
 */
final class ChannelAddressingSpec {

    /**
     * The gateway that recognised the connection, and what it resolved the connection's
     * target to.
     *
     * @param gateway the gateway that recognised the connection
     * @param target  the resolved target
     */
    record Match(EndpointGateway gateway, AddressingSpec.Target target) {}

    private final List<EndpointGateway> candidates;

    ChannelAddressingSpec(List<EndpointGateway> candidates) {
        this.candidates = candidates;
    }

    /**
     * Identifies the target of an incoming connection by asking each candidate gateway's
     * {@link AddressingSpec} in turn.
     *
     * @param port        the actual local port the connection arrived on
     * @param sniHostname the TLS SNI hostname presented by the client, or {@code null}
     * @return the first candidate's non-{@link AddressingSpec.Target.NotRecognised} match,
     *         or {@link Optional#empty()} if none recognise the connection
     */
    Optional<Match> identify(int port, @Nullable String sniHostname) {
        for (EndpointGateway candidate : candidates) {
            var target = candidate.addressingSpec().identify(port, sniHostname);
            if (!(target instanceof AddressingSpec.Target.NotRecognised)) {
                return Optional.of(new Match(candidate, target));
            }
        }
        return Optional.empty();
    }
}
