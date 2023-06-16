/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import java.util.List;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.service.BaseContributor;

public class MultiTenantFilterContributor extends BaseContributor<List<KrpcFilter>> implements FilterContributor {

    public static final BaseContributorBuilder<List<KrpcFilter>> FILTERS = BaseContributor.<List<KrpcFilter>> builder()
            .add("MultiTenant", () -> List.of(new MultiTenantTransformationFilter()));

    public MultiTenantFilterContributor() {
        super(FILTERS);
    }
}
