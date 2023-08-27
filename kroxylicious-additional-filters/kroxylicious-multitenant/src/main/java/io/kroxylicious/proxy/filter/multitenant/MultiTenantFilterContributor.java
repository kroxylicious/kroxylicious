/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.multitenant;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.service.BaseContributor;
import io.kroxylicious.proxy.service.Context;

public class MultiTenantFilterContributor extends BaseContributor<Filter, Context> implements FilterContributor {

    public static final BaseContributorBuilder<Filter> FILTERS = BaseContributor.<Filter> builder()
            .add("MultiTenant", MultiTenantTransformationFilter::new);

    public MultiTenantFilterContributor() {
        super(FILTERS);
    }
}
