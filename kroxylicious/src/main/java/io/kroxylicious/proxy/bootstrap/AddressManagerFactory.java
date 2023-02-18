/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.bootstrap;

import io.kroxylicious.proxy.addressmapper.AddressManager;
import io.kroxylicious.proxy.addressmapper.AddressManagerContributorManager;
import io.kroxylicious.proxy.config.Configuration;

/**
 * Abstracts the creation of an address manager, hiding the configuration
 * required for instantiation at the point at which instances are created.
 *
 * Exactly one instance of the address manager is created by the {@link io.kroxylicious.proxy.KafkaProxy} as
 * it starts-up.
 */
public class AddressManagerFactory {

    private final Configuration config;

    public AddressManagerFactory(Configuration config) {
        this.config = config;
    }

    /**
     * Create the address manager
     *
     * @return address manager
     */
    public AddressManager createAddressManager() {
        var manager = AddressManagerContributorManager.getInstance();

        var addressManager = config.getAddressManager();
        return manager.getInstance(addressManager.type(), config.proxy(), addressManager.config());
    }
}
