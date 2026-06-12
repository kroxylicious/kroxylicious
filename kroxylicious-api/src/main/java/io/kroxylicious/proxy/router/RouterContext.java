/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.OptionalInt;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Context passed to {@link Router#onRequest} for issuing requests
 * to named routes.
 */
public interface RouterContext {

    /**
     * Returns the virtual node ID of the broker that the client connected to.
     *
     * <p>When the client connected to a broker-specific endpoint (i.e. an
     * address that corresponds to a particular broker in the cluster topology),
     * this returns that broker's virtual node ID. The router can use this to
     * send requests — such as {@code API_VERSIONS} — to the specific broker
     * the client believes it is talking to, rather than an arbitrary broker.</p>
     *
     * <p>When the client connected to a bootstrap address, this returns empty,
     * because the proxy does not know which broker the client intended.
     * In that case the router should use {@link #anyNodeId(String)} to obtain
     * a node ID for sending requests.</p>
     *
     * @return the virtual node ID if the client connected to a broker-specific
     *         endpoint, or empty if the client connected to a bootstrap address
     */
    OptionalInt virtualNodeId();

    /**
     * Returns a virtual node ID that, when passed to {@link #sendRequestToNode},
     * causes the runtime to send the request to an arbitrary broker on the
     * named route's cluster.
     *
     * <p>This is used for initial discovery requests (e.g. {@code METADATA},
     * {@code FIND_COORDINATOR}) before the router has learned the cluster
     * topology, and for requests that are not broker-specific. The runtime
     * selects which broker to use.</p>
     *
     * @param route the name of the route
     * @return a virtual node ID representing any broker on the route's cluster
     * @throws IllegalArgumentException if the route name is not known
     */
    int anyNodeId(String route);

    /**
     * Sends a request to a specific broker identified by virtual node ID.
     *
     * <p>The runtime derives the route from the virtual node ID and resolves
     * it to a specific upstream broker address, opening a new connection if
     * necessary. The returned stage completes when the broker produces a
     * response.</p>
     *
     * <p>The {@code virtualNodeId} can be:</p>
     * <ul>
     *   <li>A value obtained from {@link #virtualNodeId()} — sends to the
     *       broker the client connected to</li>
     *   <li>A value obtained from {@link #anyNodeId(String)} — sends to an
     *       arbitrary broker on a route</li>
     *   <li>A virtual node ID learned from a previous response (e.g. a
     *       partition leader from a {@code METADATA} response)</li>
     * </ul>
     *
     * @param virtualNodeId the virtual node ID of the target broker
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response body from the broker
     * @throws IllegalStateException if the upstream address for the node is
     *         not yet known (metadata not yet reconciled)
     */
    CompletionStage<ApiMessage> sendRequestToNode(
                                                  int virtualNodeId,
                                                  RequestHeaderData header,
                                                  ApiMessage request);

    /**
     * Returns the unique identifier for the current proxy session.
     * The same session id will be observed for all routers and filters
     * that observe the requests from a given client connection.
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
     * @return the client subject
     */
    Subject authenticatedSubject();

    /**
     * Resolves a topic ID to its topic name.
     *
     * <p>The runtime guarantees that all topic IDs present in the current
     * request have been resolved before {@link Router#onRequest} is called,
     * so this method returns synchronously. It reads from a per-connection
     * cache populated by an internal filter that sends METADATA requests
     * on cache miss.</p>
     *
     * @param topicId the topic ID to resolve
     * @return the topic name, or {@code null} if the topic ID could not
     *         be resolved (e.g. the topic was deleted)
     */
    @Nullable
    String topicName(Uuid topicId);

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
