/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.proxy.plugin.Plugin;

/**
 * A {@link FilterFactory} for {@link MultiTenantFilter}.
 *
 * @deprecated use {@link MultiTenant} instead.
 */
@Deprecated(since = "0.10.0", forRemoval = true)
@Plugin(configType = MultiTenantConfig.class)
public class MultiTenantTransformationFilterFactory extends MultiTenant {
}
