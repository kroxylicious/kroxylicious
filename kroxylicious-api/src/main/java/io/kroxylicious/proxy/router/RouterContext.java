/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.topology.Bootstrap;
import io.kroxylicious.proxy.topology.EndpointType;
import io.kroxylicious.proxy.topology.VirtualNode;

/**
 * Context passed to {@link Router#onRequest} for issuing requests
 * to named routes and constructing the routing outcome.
 *
 * <p>A fresh instance is created for each {@link Router#onRequest}
 * invocation. All methods on this interface, and all
 * {@link java.util.concurrent.CompletionStage CompletionStage}
 * callbacks chained on futures returned by {@link #sendRequest},
 * execute on the same Netty event loop thread. Router implementations
 * do not need to synchronise access to their own state.</p>
 *
 * <h2>Response delivery</h2>
 *
 * <p>The runtime automatically rewrites correlation IDs in responses
 * to match the client's original request. Router implementations do
 * not need to manage correlation IDs.</p>
 *
 * <h2>Error handling</h2>
 *
 * <p>If the {@code CompletionStage<RouterResponse>} returned by
 * {@link Router#onRequest} completes exceptionally, or an unchecked
 * exception escapes from {@code onRequest}, the runtime closes the
 * client connection. For protocol-level errors (e.g. topic not found,
 * authorization failure) routers should use
 * {@link #respondWithError} rather than throwing.</p>
 */
public interface RouterContext {

    /**
     * Returns the endpoint type for this client connection.
     *
     * <p>When the client connected to a broker-specific endpoint (i.e. an
     * address that corresponds to a particular broker in the cluster topology),
     * this returns a {@link VirtualNode} identifying that broker.
     * When the client connected to a bootstrap address, this returns a
     * {@link Bootstrap}.</p>
     *
     * @return the endpoint type for this connection
     */
    EndpointType endpoint();

    /**
     * Returns the virtual node for this connection, if the client connected to a
     * broker-specific endpoint.
     *
     * @return the virtual node, or empty if the client connected to a bootstrap address
     * @deprecated since 0.23, use {@link #endpoint()} and pattern-match on
     *     {@link io.kroxylicious.proxy.topology.VirtualNode VirtualNode} instead.
     *     This method will be removed in a future release.
     */
    @Deprecated(since = "0.23", forRemoval = true)
    default Optional<VirtualNode> virtualNode() {
        EndpointType ep = endpoint();
        return ep instanceof VirtualNode vn ? Optional.of(vn) : Optional.empty();
    }

    /**
     * Returns a virtual node for dispatch to any broker on the named route.
     *
     * @param route the route name
     * @return a virtual node (always throws — this method is removed)
     * @deprecated since 0.23, use {@link #sendToRoute(String, RequestHeaderData, ApiMessage)}
     *     instead. This method will be removed in a future release.
     * @throws UnsupportedOperationException always
     */
    @Deprecated(since = "0.23", forRemoval = true)
    default VirtualNode anyNode(String route) {
        throw new UnsupportedOperationException(
                "anyNode() is removed; use sendToRoute(route, header, request) instead");
    }

    /**
     * Converts an integer node ID from a protocol response body into a
     * {@link VirtualNode}.
     *
     * <p>This is the bridge between the Kafka wire protocol (which uses
     * integer node IDs) and the {@code VirtualNode} API. Routers need this
     * when interpreting node IDs in protocol messages — for example, broker
     * node IDs in {@code METADATA} responses, coordinator node IDs in
     * {@code FIND_COORDINATOR} responses, or leader IDs in partition
     * metadata.</p>
     *
     * @param virtualNodeId the integer node ID from a protocol response
     * @return the corresponding virtual node
     */
    VirtualNode nodeForId(int virtualNodeId);

    /**
     * Sends a request to a specific broker identified by virtual node.
     *
     * <p>The runtime resolves the virtual node to a specific upstream broker
     * address, opening a new connection if necessary. The returned stage
     * completes when the broker produces a response.</p>
     *
     * <p>The {@code node} can be:</p>
     * <ul>
     *   <li>A {@link VirtualNode} obtained from
     *       {@link #endpoint()} — sends to the broker the client
     *       connected to</li>
     *   <li>A value obtained from {@link #nodeForId(int)} — sends to a
     *       broker whose ID was learned from a protocol response</li>
     *   <li>A value obtained from
     *       {@link io.kroxylicious.proxy.topology.TopologyService
     *       TopologyService} discovery methods (e.g.
     *       {@link io.kroxylicious.proxy.topology.PartitionLeaders#leaderOf
     *       PartitionLeaders.leaderOf})</li>
     * </ul>
     *
     * @param node the virtual node of the target broker
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response body from the broker
     * @throws IllegalStateException if the upstream address for the node is
     *         not yet known (metadata not yet reconciled)
     */
    CompletionStage<ApiMessage> sendRequest(
                                            VirtualNode node,
                                            RequestHeaderData header,
                                            ApiMessage request);

    /**
     * Sends a request to any broker on the named route's cluster.
     *
     * <p>This is used for initial discovery requests (e.g. {@code METADATA},
     * {@code FIND_COORDINATOR}) before the router has learned the cluster
     * topology, and for requests that are not broker-specific. The runtime
     * selects which broker to use.</p>
     *
     * @param route the name of the route
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response body from the broker
     * @throws IllegalArgumentException if the route name is not known
     */
    CompletionStage<ApiMessage> sendToRoute(
                                            String route,
                                            RequestHeaderData header,
                                            ApiMessage request);

    /**
     * Returns the unique identifier for the current proxy session.
     *
     * <p>The same session ID is observed for all routers (including
     * nested routers) and filters that handle requests from a given
     * client connection.</p>
     *
     * @return the unique identifier for the current proxy session
     */
    String sessionId();

    /**
     * <p>Returns the client subject.</p>
     *
     * <p>Depending on configuration, the subject can be based on network-level
     * or Kafka protocol-level information (or both):</p>
     * <ul>
     *   <li>This will return an anonymous {@code Subject} (one with an empty
     *   {@code principals} set) when no authentication is configured, or the
     *   transport layer cannot provide authentication (e.g. TCP or non-mutual
     *   TLS transports).</li>
     *   <li>When client mutual TLS authentication is configured this will
     *   initially return a non-anonymous {@code Subject} based on the TLS
     *   certificate presented by the client.</li>
     *   <li>Because of the possibility of <em>reauthentication</em> it is
     *   also possible for the subject to change.</li>
     * </ul>
     *
     * <p>Because the subject can change, callers are advised to avoid caching
     * subjects, or decisions derived from them.</p>
     *
     * <p>Which principals are present in the returned subject, and what their
     * {@code name}s look like, depends on the configuration of network and/or
     * authentication filters. In general, routers should be configurable with
     * respect to the principal type when interrogating the returned subject.</p>
     *
     * <p><strong>SASL filter placement:</strong>
     * This value reflects authentication performed on the virtual cluster
     * filter chain only. "SASL initiator" filters placed on per-route
     * filter chains do <strong>not</strong> affect the subject returned here
     * — those authenticate the proxy's outbound connection to the upstream
     * cluster, not the client's identity.</p>
     *
     * @return the client subject
     */
    Subject authenticatedSubject();

    /**
     * Begins building a router response that delivers the given response
     * body to the client. The runtime provides a response header.
     *
     * @param body the response body
     * @return a stage that can optionally close the connection
     */
    CloseOrTerminalStage respondWith(ApiMessage body);

    /**
     * Begins building a router response that delivers a synthesised
     * response to the client.
     *
     * @param header the response header
     * @param body the response body
     * @return a stage that can optionally close the connection
     */
    CloseOrTerminalStage respondWith(
                                     ResponseHeaderData header,
                                     ApiMessage body);

    /**
     * Begins building a router result that generates an error response
     * for the client. The generated error response is API-specific.
     *
     * @param header the request header
     * @param request the request body
     * @param exception the exception that triggered the error
     * @return a stage that can optionally close the connection
     */
    CloseOrTerminalStage respondWithError(
                                          RequestHeaderData header,
                                          ApiMessage request,
                                          ApiException exception);

    /**
     * Begins building a router result for a fire-and-forget request
     * where no response is delivered to the client (e.g. acks=0
     * {@code PRODUCE}).
     *
     * @return a stage that can optionally close the connection
     */
    CloseOrTerminalStage respondWithoutReply();
}
