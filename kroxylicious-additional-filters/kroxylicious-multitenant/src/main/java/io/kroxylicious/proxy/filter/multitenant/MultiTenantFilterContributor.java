/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.ContributorContext;

public class MultiTenantFilterContributor extends BaseContributor<KrpcFilter, ContributorContext> implements FilterContributor {

    public static final BaseContributorBuilder<KrpcFilter, ContributorContext> FILTERS = BaseContributor.<KrpcFilter, ContributorContext> builder()
            .add("MultiTenant", MultiTenantTransformationFilter::new);

    public MultiTenantFilterContributor() {
        super(FILTERS);
    }
}
