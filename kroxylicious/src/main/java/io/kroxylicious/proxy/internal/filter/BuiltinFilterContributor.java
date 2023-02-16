/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.FilterConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.internal.filter.ApiVersionsFilter.ApiVersionsFilterConfig;
import io.kroxylicious.proxy.internal.filter.BrokerAddressFilter.BrokerAddressFilterConfig;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationFilterConfig;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationFilterConfig;

public class BuiltinFilterContributor implements FilterContributor {

    @Override
    public Class<? extends FilterConfig> getConfigType(String shortName) {
        switch (shortName) {
            case "ApiVersions":
                return ApiVersionsFilterConfig.class;
            case "BrokerAddress":
                return BrokerAddressFilterConfig.class;
            case "ProduceRequestTransformation":
                return ProduceRequestTransformationFilterConfig.class;
            case "FetchResponseTransformation":
                return FetchResponseTransformationFilterConfig.class;
            default:
                return null;
        }
    }

    @Override
    public KrpcFilter getFilter(String shortName, ProxyConfig proxyConfig, FilterConfig filterConfig) {
        switch (shortName) {
            case "ApiVersions":
                return new ApiVersionsFilter();
            case "BrokerAddress":
                return new BrokerAddressFilter(proxyConfig);
            case "ProduceRequestTransformation":
                return new ProduceRequestTransformationFilter((ProduceRequestTransformationFilterConfig) filterConfig);
            case "FetchResponseTransformation":
                return new FetchResponseTransformationFilter((FetchResponseTransformationFilterConfig) filterConfig);
            default:
                return null;
        }
    }
}
