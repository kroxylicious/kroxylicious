/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.addressmapper;

import io.kroxylicious.proxy.config.AbstractContributorManager;

public class AddressManagerContributorManager extends AbstractContributorManager<AddressManagerContributor, AddressManager> {

    private static final AddressManagerContributorManager INSTANCE = new AddressManagerContributorManager();

    private AddressManagerContributorManager() {
        super(AddressManagerContributor.class);
    }

    public static AddressManagerContributorManager getInstance() {
        return INSTANCE;
    }
}
