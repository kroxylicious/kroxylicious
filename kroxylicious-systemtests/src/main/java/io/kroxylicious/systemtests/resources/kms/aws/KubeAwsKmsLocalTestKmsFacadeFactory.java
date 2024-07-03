/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacadeFactory;

/**
 * Factory for {@link KubeAwsKmsLocalTestKmsFacade}s.
 */
public class KubeAwsKmsLocalTestKmsFacadeFactory extends AbstractAwsKmsTestKmsFacadeFactory {

    /**
     * {@inheritDoc}
     */
    @Override
    public KubeAwsKmsLocalTestKmsFacade build() {
        return new KubeAwsKmsLocalTestKmsFacade();
    }
}