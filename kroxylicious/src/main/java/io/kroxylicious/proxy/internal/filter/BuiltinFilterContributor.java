/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.List;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.ProduceRequestTransformationConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class BuiltinFilterContributor extends BaseContributor<List<KrpcFilter>> implements FilterContributor {

    public static final BaseContributorBuilder<List<KrpcFilter>> FILTERS = BaseContributor.<List<KrpcFilter>> builder()
            .add("ApiVersions", () -> List.of(new ApiVersionsFilter()))
            .add("ProduceRequestTransformation", ProduceRequestTransformationConfig.class, config -> List.of(new ProduceRequestTransformationFilter(config)))
            .add("FetchResponseTransformation", FetchResponseTransformationConfig.class, config -> List.of(new FetchResponseTransformationFilter(config)));

    public BuiltinFilterContributor() {
        super(FILTERS);
    }
}
