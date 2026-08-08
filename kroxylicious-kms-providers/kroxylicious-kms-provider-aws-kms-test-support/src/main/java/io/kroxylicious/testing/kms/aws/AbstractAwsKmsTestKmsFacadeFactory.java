/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.aws;

import io.kroxylicious.kms.provider.aws.kms.AwsKmsEdek;
import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Abstract factory for {@link AbstractAwsKmsTestKmsFacade}s.
 */
public abstract class AbstractAwsKmsTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, AwsKmsEdek> {
    /**
     * Creates the factory.
     */
    protected AbstractAwsKmsTestKmsFacadeFactory() {
        // Intentionally empty
    }

    @Override
    public abstract AbstractAwsKmsTestKmsFacade build();
}