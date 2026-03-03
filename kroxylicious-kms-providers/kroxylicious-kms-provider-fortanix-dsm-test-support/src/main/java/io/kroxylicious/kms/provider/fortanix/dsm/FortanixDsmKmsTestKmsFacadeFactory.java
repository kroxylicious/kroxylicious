/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for the FortanixDsm test facade.
 */
public class FortanixDsmKmsTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, FortanixDsmKmsEdek> {
    /**
     * {@inheritDoc}
     */
    @Override
    public FortanixDsmKmsTestKmsFacade build() {
        return new FortanixDsmKmsTestKmsFacade();
    }
}