/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

/** Wire-test filter that delays a request and generates traffic downstream of the MDS gate. */
@Plugin(configType = Void.class)
public class DelayedTraffic implements FilterFactory<Void, Void> {
    @Override
    @Nullable
    public Void initialize(FilterFactoryContext context, @Nullable Void config) {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext factoryContext, @Nullable Void initializationData) {
        int[] groups = { 0 };
        // The runtime's deferred-result timeout bounds the scheduled work and request.
        @SuppressWarnings("FutureReturnValueIgnored")
        RequestFilter filter = (apiKey, apiVersion, header, request, context) -> {
            if (apiKey == ApiKeys.LIST_GROUPS && ++groups[0] == 2) {
                var delay = new CompletableFuture<Void>();
                factoryContext.filterDispatchExecutor().schedule(() -> delay.complete(null), 3100, TimeUnit.MILLISECONDS);
                return delay.thenCompose(ignored -> context.sendRequest(new RequestHeaderData().setRequestApiVersion((short) 3),
                        new ApiVersionsRequestData().setClientSoftwareName("filter").setClientSoftwareVersion("1")))
                        .thenCompose(ignored -> context.forwardRequest(header, request));
            }
            return context.forwardRequest(header, request);
        };
        return filter;
    }
}
