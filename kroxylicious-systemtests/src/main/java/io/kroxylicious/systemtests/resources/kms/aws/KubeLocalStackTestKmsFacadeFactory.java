/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacadeFactory;

/**
 * Factory for {@link KubeLocalStackTestKmsFacade}s.
 */
public class KubeLocalStackTestKmsFacadeFactory extends AbstractAwsKmsTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public KubeLocalStackTestKmsFacade build() {
        return new KubeLocalStackTestKmsFacade();
    }
}
