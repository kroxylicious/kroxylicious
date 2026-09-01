/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter.impl;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kafka.common.message.MetadataRequestData;
import io.kroxylicious.kafka.common.message.MetadataResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

/**
 * An internal filter that causes the system to eagerly learn the cluster's topology by spontaneously emitting
 * an out-of-band Metadata request at the earliest legal point in the Kafka conversation.  The response allows
 * endpoint reconciliation to take place so that restricted upstream bindings are replaced by true bindings to
 * the actual upstream brokers.  Once the bindings are made, the filter causes the client's connection to close
 * in order to force the client to reconnect, thus ensuring the client has a connection to the intended broker.
 *
 * <p><strong>Direct routing only.</strong> This filter must only be installed for virtual clusters using
 * {@link io.kroxylicious.proxy.internal.routing.DirectRouting}. Direct routing binds listening ports to
 * specific broker node IDs but cannot know the real upstream broker addresses at startup; this filter exists
 * to learn those addresses before real client traffic flows.
 *
 * <p>Dynamic routing virtual clusters do not have this problem: their
 * {@link io.kroxylicious.proxy.internal.routing.RoutingHandler} resolves upstream addresses per-request
 * through the router's own node-ID mapping, so there is no bootstrap placeholder to replace. Installing
 * this filter on a dynamic routing connection would cause an {@link IllegalStateException} when it
 * attempts to use the upstream target of a
 * {@link io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding}.
 *
 * @see io.kroxylicious.proxy.internal.net.EndpointRegistry
 * @see io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding
 */
public class EagerMetadataLearner implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EagerMetadataLearner.class);

    /**
     * The set of the API keys that are permitted before the client would normally send a METADATA request.
     */
    private static final Set<ApiKeys> KAFKA_PRELUDE = Set.of(ApiKeys.API_VERSIONS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE);

    /**
     * Create EagerMetadataLearner
     */
    public EagerMetadataLearner() {
        // explicit default constructor for javadoc
    }

    @Override
    public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, short apiVersion, RequestHeaderData header, ApiMessage body, FilterContext context) {
        if (KAFKA_PRELUDE.contains(apiKey)) {
            return context.requestFilterResultBuilder().forward(header, body).completed();
        }
        else {
            // Send an out-of-band Metadata request. The response will be intercepted by the in-built BrokerAddressFilter.
            // By the time control returns to the handler, the upstream addresses will have been reconciled.
            var requestHeader = determineMetadataRequestHeader(header);
            var useClientRequest = requestHeader.equals(header);
            var request = useClientRequest ? (MetadataRequestData) body : new MetadataRequestData();

            var future = new CompletableFuture<RequestFilterResult>();
            context.<MetadataResponseData> sendRequest(requestHeader, request)
                    .thenAccept(metadataResponse -> {
                        // closing the connection is important. This client connection is connected to bootstrap (it could
                        // be any broker or maybe not something else). we must close the connection to force the client to
                        // connect again.
                        var builder = context.requestFilterResultBuilder();
                        if (useClientRequest) {
                            // The client's request matched our out-of-band message, so we may as well return the
                            // response.
                            future.complete(builder.shortCircuitResponse(metadataResponse).withCloseConnection().build());
                        }
                        else {
                            future.complete(builder.withCloseConnection().build());

                        }
                        LOGGER.atInfo()
                                .addKeyValue("sessionId", context.sessionId())
                                .log("Closing upstream bootstrap connection now that endpoint reconciliation is complete");
                    });
            return future;
        }
    }

    private RequestHeaderData determineMetadataRequestHeader(RequestHeaderData header) {
        if (header.requestApiKey() == ApiKeys.METADATA.id) {
            return header;
        }
        else {
            // TODO: use a version appearing the intersection calculated by ApiVersionFilter.
            return new RequestHeaderData().setRequestApiVersion(MetadataRequestData.LOWEST_SUPPORTED_VERSION);
        }
    }

}
