/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.ApiVersionsResponseFilter;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * This Filter contains code that should fail explosively if it is exposed to an ApiVersionsResponse that contains
 * APIs or versions that the proxy cannot decode. It should work without issue, unless the framework behaviour changes.
 */
@Plugin(configType = Void.class)
public class UnsafeApiVersionsCanary implements FilterFactory<Void, Void> {
    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        return new UnsafeApiVersionsCanaryFilter();
    }

    static class UnsafeApiVersionsCanaryFilter implements ApiVersionsResponseFilter {
        @Override
        public CompletionStage<ResponseFilterResult> onApiVersionsResponse(short apiVersion, ResponseHeaderData header, ApiVersionsResponseData response,
                                                                           FilterContext context) {
            for (ApiVersionsResponseData.ApiVersion apiKey : response.apiKeys()) {
                // Unsafe if this Filter is exposed to api keys that are not known to the proxy.
                // The Proxy should prevent this happening
                ApiKeys apiKeys = ApiKeys.forId(apiKey.apiKey());
                short minVersion = apiKey.minVersion();
                short maxVersion = apiKey.maxVersion();
                // Produce is allowed to offer v0-v2 despite the broker not really supporting it
                if (apiKeys != ApiKeys.PRODUCE && minVersion < apiKeys.oldestVersion()) {
                    throw new IllegalStateException(
                            "received an ApiVersionsResponse with a min version " + minVersion + " for " + apiKeys + " below what the proxy supports "
                                    + apiKeys.oldestVersion());
                }
                if (maxVersion > apiKeys.latestVersion(true)) {
                    throw new IllegalStateException(
                            "received an ApiVersionsResponse with a max version " + maxVersion + " for " + apiKeys + " above what the proxy supports "
                                    + apiKeys.latestVersion(true));
                }
            }
            return context.forwardResponse(header, response);
        }

    }
}
