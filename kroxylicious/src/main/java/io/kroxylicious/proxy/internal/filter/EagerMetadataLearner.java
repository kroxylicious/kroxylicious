/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.HashMap;
import java.util.Set;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.internal.net.EndpointReconciler;
import io.kroxylicious.proxy.model.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

/**
 * An internal filter that causes the system to eagerly learn the cluster's topology by spontaneously emitting
 * an out-of-band Metadata request at the earliest legal point in the Kafka conversation.  The response to allows
 * the Endpoint reconciliation to take place so that ephemeral upstream bindings are replaced by true bindings to
 * the actual upstream brokers.
 * <br/>
 * Once the bindings are made, the filter causes the client's connection to close.   This is done
 * in order to force the client to reconnect, thus ensuring the client has a connection to the intended broker.
 *
 * @see io.kroxylicious.proxy.internal.net.EndpointRegistry
 */
public class EagerMetadataLearner implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EagerMetadataLearner.class);

    /**
     * The set of the API keys that are permitted before the client would normally send a METADATA request.
     */
    private final static Set<ApiKeys> KAFKA_PRELUDE = Set.of(ApiKeys.API_VERSIONS, ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_AUTHENTICATE);

    private final VirtualCluster virtualCluster;
    private final EndpointReconciler reconciler;

    public EagerMetadataLearner(VirtualCluster virtualCluster, EndpointReconciler reconciler) {
        this.virtualCluster = virtualCluster;
        this.reconciler = reconciler;
    }

    @Override
    public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        if (KAFKA_PRELUDE.contains(apiKey)) {
            filterContext.forwardRequest(header, body);
        }
        else {
            final short apiVersion = determineMetadataApiVersion(header);
            var unused = filterContext.sendRequest(apiVersion, new MetadataRequestData()).thenAccept(apiMessage -> {
                // Once https://github.com/kroxylicious/kroxylicious/issues/445 lands, we will be able to
                // rely on BrokerAddressFilter to do the reconciliation, removing the repetitious
                // code we have here. All this filter needs to do is return forward the response to the
                // downstream (if the request was indeed for metadata), then close the connection.

                if (!(apiMessage instanceof MetadataResponseData data)) {
                    throw new IllegalStateException("Unexpected response " + apiMessage.getClass() + " to out-of-band request.");
                }
                var nodeMap = new HashMap<Integer, HostPort>();
                for (MetadataResponseBroker broker : data.brokers()) {
                    nodeMap.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
                }
                reconciler.reconcile(virtualCluster, nodeMap).toCompletableFuture().join();
                LOGGER.info("Closing upstream bootstrap connection {} now that endpoint reconciliation is complete.", filterContext.channelDescriptor());
                filterContext.closeConnection();
            });
        }
    }

    private short determineMetadataApiVersion(RequestHeaderData header) {
        final short apiVersion;
        if (header.requestApiKey() == ApiKeys.METADATA.id) {
            apiVersion = header.requestApiVersion();
        }
        else {
            // TODO: use a version appearing the intersection calculated by ApiVersionFilter.
            apiVersion = MetadataRequestData.LOWEST_SUPPORTED_VERSION;
        }
        return apiVersion;
    }

}
