/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.HashMap;

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
 * An internal filter that rewrites broker addresses in all relevant responses to the corresponding proxy address. It also
 * is responsible for updating the virtual cluster's cache of upstream broker endpoints.
 */
public class MetadataAndCloseFilter implements RequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataAndCloseFilter.class);

    private final VirtualCluster virtualCluster;
    private final EndpointReconciler reconciler;

    public MetadataAndCloseFilter(VirtualCluster virtualCluster, EndpointReconciler reconciler) {
        this.virtualCluster = virtualCluster;
        this.reconciler = reconciler;
    }

    @Override
    public void onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage body, KrpcFilterContext filterContext) {
        // sorry client, it's all over for you, asking for metadata on bootstrap endpoint
        filterContext.sendRequest((short) 1, new MetadataRequestData()).thenAccept(apiMessage -> {
            LOGGER.debug("got a metadata response!");
            if (!(apiMessage instanceof MetadataResponseData data)) {
                throw new RuntimeException("fail");
            }
            var nodeMap = new HashMap<Integer, HostPort>();
            for (MetadataResponseBroker broker : data.brokers()) {
                nodeMap.put(broker.nodeId(), new HostPort(broker.host(), broker.port()));
            }
            LOGGER.debug("reconcile that nodemap: " + nodeMap);
            reconciler.reconcile(virtualCluster, nodeMap, false).toCompletableFuture().join();
            filterContext.closeChannel();
        });
    }
}
