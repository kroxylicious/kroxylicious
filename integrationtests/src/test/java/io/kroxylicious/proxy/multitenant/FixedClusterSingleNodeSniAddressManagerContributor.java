/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.multitenant;

import io.kroxylicious.proxy.addressmapper.AddressManager;
import io.kroxylicious.proxy.addressmapper.AddressManagerContributor;
import io.kroxylicious.proxy.service.BaseContributor;

public class FixedClusterSingleNodeSniAddressManagerContributor extends BaseContributor<AddressManager> implements AddressManagerContributor {

    public static final BaseContributor.BaseContributorBuilder<AddressManager> ADDRESS_MANAGERS = BaseContributor.<AddressManager> builder()
            .add("FixedClusterSni", FixedClusterSingleNodeSniAddressManagerConfig.class, FixedClusterSingleNodeSniAddressManager::new);

    public FixedClusterSingleNodeSniAddressManagerContributor() {
        super(ADDRESS_MANAGERS);
    }
}
