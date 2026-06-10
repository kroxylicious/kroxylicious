/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Context passed to {@link Router#onRequest} for issuing requests
 * to named routes.
 */
public interface RouterContext {

    /**
     * Returns the virtual node ID of a broker on the named route's cluster.
     *
     * <p>This is used to send initial requests (e.g. {@code METADATA}) before
     * the router has discovered the cluster's full broker topology. Once
     * {@code METADATA} responses arrive, the router uses the virtual node IDs
     * from those responses to address specific brokers.</p>
     *
     * <p>The runtime selects which broker to return.</p>
     *
     * @param route the name of the route
     * @return the virtual node ID of a bootstrap broker on the route's cluster
     * @throws IllegalArgumentException if the route name is not known
     */
    int bootstrapNodeId(String route);

    /**
     * Sends a request to a specific broker identified by route and virtual node ID.
     *
     * <p>The runtime uses the route to determine which target cluster, and
     * resolves the virtual node ID to a specific upstream broker address,
     * opening a new connection if necessary. The returned stage completes
     * when the broker produces a response.</p>
     *
     * @param route the name of the route
     * @param virtualNodeId the virtual node ID of the target broker
     * @param header the request header
     * @param request the request body
     * @return a stage that completes with the response from the broker
     * @throws IllegalArgumentException if the route name is not known
     * @throws IllegalStateException if the upstream address for the node is
     *         not yet known (metadata not yet reconciled)
     */
    CompletionStage<Response> sendRequestToNode(
                                                String route,
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
    default String topicName(Uuid topicId) {
        return null;
    }
}
