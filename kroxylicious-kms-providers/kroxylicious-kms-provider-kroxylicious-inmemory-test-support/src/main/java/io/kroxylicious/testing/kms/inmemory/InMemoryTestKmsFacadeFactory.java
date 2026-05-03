/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.inmemory;

import java.util.UUID;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Factory for {@link InMemoryTestKmsFacade}s.
 */
public class InMemoryTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, UUID, InMemoryEdek> {
    /**
     * {@inheritDoc}
     */
    @Override
    public InMemoryTestKmsFacade build() {
        return new InMemoryTestKmsFacade();
    }
}
