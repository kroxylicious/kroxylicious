/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import java.util.List;
import java.util.function.Function;

import io.kroxylicious.proxy.filter.ClientIdPrefixingFilter.ClientIdPrefixingFilterConfig;
import io.kroxylicious.proxy.filter.FixedClientIdFilter.FixedClientIdFilterConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class TestFilterContributor extends BaseContributor<List<KrpcFilter>> implements FilterContributor {

    public static final BaseContributorBuilder<List<KrpcFilter>> FILTERS = BaseContributor.<List<KrpcFilter>> builder()
            .add("FixedClientId", FixedClientIdFilterConfig.class, config -> List.of(new FixedClientIdFilter(config)))
            .add("FixedPrefixingClientId", CombinedConfiguration.class, fixedClientIdWithPrefix())
            .add("CreateTopicRejectFilter", () -> List.of(new CreateTopicRejectFilter()));

    private static Function<CombinedConfiguration, List<KrpcFilter>> fixedClientIdWithPrefix() {
        return config -> {
            FixedClientIdFilter fixedClientIdFilter = new FixedClientIdFilter(new FixedClientIdFilterConfig(config.getFixedClientId()));
            ClientIdPrefixingFilter clientIdPrefixingFilter = new ClientIdPrefixingFilter(new ClientIdPrefixingFilterConfig(config.getPrefix()));
            return List.of(fixedClientIdFilter, clientIdPrefixingFilter);
        };
    }

    public TestFilterContributor() {
        super(FILTERS);
    }
}
