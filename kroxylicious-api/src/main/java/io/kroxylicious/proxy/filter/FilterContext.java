/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import javax.annotation.Nullable;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.metadata.TopicNameMapping;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.proxy.tls.ClientTlsContext;

/**
 * A context to allow filters to interact with other filters and the pipeline.
 */
public interface FilterContext {
    /**
     * A description of the downstream/client channel.
     * @return A description of this channel (typically used for logging).
     */
    String channelDescriptor();

    /**
     * An id which uniquely identifies the connection with the client in both time and space.
     * In other words this will have a different value even if a client re-establishes a
     * TCP connection from the same IP address and source port.
     * @return the ID allocated to this client session.
     */
    String sessionId();

    /**
     * Create a ByteBufferOutputStream of the given capacity.
     * The backing buffer will be deallocated when the request processing is completed
     * @param initialCapacity The initial capacity of the buffer.
     * @return The allocated ByteBufferOutputStream
     */
    ByteBufferOutputStream createByteBufferOutputStream(int initialCapacity);

    /**
     * The SNI hostname provided by the client, if any.
     * @return the SNI hostname provided by the client.  Will be null if the client is
     * using a non-TLS connection or the TLS client hello didn't provide one.
     */
    @Nullable
    String sniHostname();

    /**
     * Creates a builder for a request filter result objects.  This object encapsulates
     * the request to forward and optionally orders for actions such as closing
     * the connection or dropping the request.
     * <br/>
     * The builder returns either {@link CompletionStage<RequestFilterResult>} object
     * ready to be returned by the request filter, or a {@link ResponseFilterResult} object.
     * The latter facilitates asynchronous programming patterns where requests must be
     * forwarded after other work has completed.
     *
     * @return builder
     */
    RequestFilterResultBuilder requestFilterResultBuilder();

    /**
     * Generates a completed filter results containing the given header and request.  When
     * request filters implementations return this result, the request will be sent towards
     * the broker, invoking upstream filters.
     * <br/>
     * Invoking this method is identical to invoking:
     * {@code requestFilterResultBuilder.forward(header, request).complete()}
     *
     * @param header The header to forward to the broker.
     * @param request The request to forward to the broker.
     * @return completed filter results.
     */
    CompletionStage<RequestFilterResult> forwardRequest(RequestHeaderData header, ApiMessage request);

    /**
     * Send a request from a filter towards the broker.   The response to the request will be made available to the
     * filter asynchronously, by way of the {@link CompletionStage}.  The CompletionStage will contain the response
     * object, or null of the request does not have a response.
     * <h4>Header</h4>
     * <p>The caller is required to provide a {@link RequestHeaderData}. It is recommended that the
     * caller specify the {@link RequestHeaderData#requestApiVersion()}. This can be done conveniently
     * with forms such as:</p>
     * <pre>{@code new RequestHeaderData().setRequestApiVersion(4)}</pre>
     * <p>The caller may also provide a {@link RequestHeaderData#clientId()} an
     * {@link RequestHeaderData#unknownTaggedFields()}.
     * <p>Kroxylicious will automatically set the {@link RequestHeaderData#requestApiKey()} to be consistent
     * with the {@code request}. {@link RequestHeaderData#correlationId()} is ignored.</p>
     * <h4>Filtering</h4>
     * <p>The request will pass through all filters upstream of the filter that invoked the operation,
     * invoking them.  Similarly, the response will pass through all filters upstream
     * of the filter that invoked the operation, invoking them, but not itself. The response does not
     * pass through filters downstream.
     * </p>
     * <h4>Chained Computation stages</h4>
     * <p>Default and asynchronous default computation stages chained to the returned
     * {@link java.util.concurrent.CompletionStage} are guaranteed to be executed by the thread associated with the
     * connection. See {@link io.kroxylicious.proxy.filter} for more details.
     * </p>
     *
     * @param <M>     The type of the response
     * @param header  The request header.
     * @param request The request data.
     * @return CompletionStage that will yield the response.
     * @see io.kroxylicious.proxy.filter Thread Safety
     */
    <M extends ApiMessage> CompletionStage<M> sendRequest(RequestHeaderData header,
                                                          ApiMessage request);

    /**
     * Attempts to map all the given {@code topicIds} to the current corresponding topic names.
     * @param topicIds topic ids to map to names
     * @return a CompletionStage that will be completed with a complete mapping, with every requested topic id mapped to either an
     * {@link TopicNameMappingException} or a name. All failure modes should complete the stage with a TopicNameMapping, with the
     * TopicNameMapping used to convey the reason for failure, rather than failing the Stage.
     * <h4>Chained Computation stages</h4>
     * <p>Default and asynchronous default computation stages chained to the returned
     * {@link java.util.concurrent.CompletionStage} are guaranteed to be executed by the thread
     * associated with the connection. See {@link io.kroxylicious.proxy.filter} for more details.
     * </p>
     */
    CompletionStage<TopicNameMapping> topicNames(Collection<Uuid> topicIds);

    /**
     * Generates a completed filter results containing the given header and response.  When
     * response filters implementations return this result, the response will be sent towards
     * the client, invoking downstream filters.
     * <br/>
     * Invoking this method is identical to invoking:
     * {@code responseFilterResultBuilder.forward(header, response).complete()}
     *
     * @param header The header to forward to the broker.
     * @param response The request to forward to the broker.
     * @return completed filter results.
     */
    CompletionStage<ResponseFilterResult> forwardResponse(ResponseHeaderData header, ApiMessage response);

    /**
     * Creates a builder for a request filter result objects.  This object encapsulates
     * the response to forward and optionally orders for actions such as closing
     * the connection or dropping the response.
     * <br/>
     * The builder returns either {@link CompletionStage<ResponseFilterResult>} object
     * ready to be returned by the response filter, or a {@link ResponseFilterResult} object.
     * The latter facilitates asynchronous programming patterns where responses must be
     * forwarded after other work has completed.
     *
     * @return builder
     */
    ResponseFilterResultBuilder responseFilterResultBuilder();

    /**
     * Allows the filter to identify which cluster it is processing a request for
     * @return virtual cluster name
     */
    String getVirtualClusterName();

    /**
     * @return The TLS context for the client connection, or empty if the client connection is not TLS.
     */
    Optional<ClientTlsContext> clientTlsContext();

    /**
     *
     * Allows a filter (typically one which implements {@link SaslAuthenticateRequestFilter})
     * to announce a successful authentication outcome with the Kafka client to other plugins.
     * After calling this method the results of {@link #clientSaslContext()}
     * and {@link #authenticatedSubject()} will both be non-empty for this and other filters.
     *
     * In order to support reauthentication, calls to this method and
     * {@link #clientSaslAuthenticationFailure(String, String, Exception)}
     * may be arbitrarily interleaved during the lifetime of a given filter instance.
     *
     * @param mechanism The SASL mechanism used
     * @param authorizedId The authorizedId
     *
     * @deprecated Callers should use {@link #clientSaslAuthenticationSuccess(String, Subject)}
     * to announce authentication outcomes instead of this method.
     * When this method is used the result of {@link #authenticatedSubject()} will be a non-empty Optional
     * with a {@link Subject} having a single {@link User} principal with the given {@code authorizedId}
     */
    @Deprecated(since = "0.17")
    void clientSaslAuthenticationSuccess(String mechanism,
                                         String authorizedId);

    /**
     * Allows a filter (typically one which implements {@link SaslAuthenticateRequestFilter})
     * to announce a successful authentication outcome with the Kafka client to other plugins.
     * After calling this method the results of {@link #clientSaslContext()}
     * and {@link #authenticatedSubject()} will both be non-empty for this and other filters.
     *
     * In order to support reauthentication, calls to this method and
     * {@link #clientSaslAuthenticationFailure(String, String, Exception)}
     * may be arbitrarily interleaved during the lifetime of a given filter instance.
     * @param mechanism The SASL mechanism used
     * @param subject The subject
     */
    void clientSaslAuthenticationSuccess(String mechanism,
                                         Subject subject);

    /**
     * Allows a filter (typically one which implements {@link SaslAuthenticateRequestFilter})
     * to announce a failed authentication outcome with the Kafka client.
     * After calling this method the result of {@link #clientSaslContext()} will
     * be empty for this and other filters.
     * It is the filter's responsibility to return the right error response to a client, and/or disconnect.
     *
     * In order to support reauthentication, calls to this method and
     * {@link #clientSaslAuthenticationSuccess(String, String)}
     * may be arbitrarily interleaved during the lifetime of a given filter instance.
     * @param mechanism The SASL mechanism used, or null if this is not known.
     * @param authorizedId The authorizedId, or null if this is not known.
     * @param exception An exception describing the authentication failure.
     */
    void clientSaslAuthenticationFailure(@Nullable String mechanism,
                                         @Nullable String authorizedId,
                                         Exception exception);

    /**
     * @return The SASL context for the client connection, or empty if the client
     * has not successfully authenticated using SASL.
     */
    Optional<ClientSaslContext> clientSaslContext();

    /**
     * <p>Returns the client subject.</p>
     *
     * <p>Depending on configuration, the subject can be based on network-level or Kafka protocol-level information (or both):</p>
     * <ul>
     *   <li>This will return an
     *   anonymous {@code Subject} (one with an empty {@code principals} set) when
     *   no authentication is configured, or the transport layer cannot provide authentication (e.g. TCP or non-mutual TLS transports).</li>
     *   <li>When client mutual TLS authentication is configured this will
     *   initially return a non-anonymous {@code Subject} based on the TLS certificate presented by the client.</li>
     *   <li>At any point, if a filter invokes {@link #clientSaslAuthenticationSuccess(String, Subject)} then that subject
     *   will override the existing subject.</li>
     *   <li>Because of the possibility of <em>reauthentication</em> it is also possible for the
     *   subject to change even after then initial SASL reauthentication.</li>
     * </ul>
     *
     * <p>Because the subject can change, callers are advised to be careful to avoid
     * caching subjects, or decisions derived from them.</p>
     *
     * <p>Which principals are present in the returned subject, and what their {@code name}s look like,
     * depends on the configuration of network
     * and/or {@link #clientSaslAuthenticationSuccess(String, Subject)}-calling filters.
     * In general, filters should be configurable with respect to the principal type when interrogating the returned
     * subject.</p>
     *
     * @return The client subject
     * @see #clientSaslAuthenticationSuccess(String, Subject)
     */
    Subject authenticatedSubject();

}
