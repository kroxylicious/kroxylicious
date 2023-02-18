/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.addressmapper;

import io.kroxylicious.proxy.addressmapper.fixedcluster.FixedClusterAddressManager;
import io.kroxylicious.proxy.addressmapper.fixedcluster.FixedClusterAddressManagerConfig;
import io.kroxylicious.proxy.service.BaseContributor;

public class BuiltinAddressManagerContributor extends BaseContributor<AddressManager> implements AddressManagerContributor {

    public static final BaseContributor.BaseContributorBuilder<AddressManager> ADDRESS_MANAGERS = BaseContributor.<AddressManager> builder()
            .add("FixedCluster", FixedClusterAddressManagerConfig.class, FixedClusterAddressManager::new);

    public BuiltinAddressManagerContributor() {
        super(ADDRESS_MANAGERS);
    }

}
