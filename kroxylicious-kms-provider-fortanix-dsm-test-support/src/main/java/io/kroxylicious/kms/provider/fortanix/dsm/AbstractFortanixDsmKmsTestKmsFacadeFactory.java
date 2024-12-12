/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

public abstract class AbstractFortanixDsmKmsTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, FortanixDsmKmsEd> {
    @Override
    public abstract AbstractFortanixDsmKmsTestKmsFacade build();
}