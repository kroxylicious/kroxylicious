/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper.fixedcluster;

import io.kroxylicious.proxy.addressmapper.AddressManager;
import io.kroxylicious.proxy.addressmapper.AddressMapper;
import io.kroxylicious.proxy.filter.NetFilterContext;

public class FixedClusterAddressManager implements AddressManager {
    private final FixedClusterAddressManagerConfig filterConfig;

    public FixedClusterAddressManager(FixedClusterAddressManagerConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    @Override
    public AddressMapper createMapper(NetFilterContext netFilterContext) {
        return new FixedClusterAddressMapper(filterConfig.bootstrapServers());
    }
}
