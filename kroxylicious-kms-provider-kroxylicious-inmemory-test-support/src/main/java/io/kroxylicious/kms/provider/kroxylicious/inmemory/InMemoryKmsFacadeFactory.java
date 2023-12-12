/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

/**
 * Factory for {@link InMemoryKmsFacade}s.
 */
public class InMemoryKmsFacadeFactory implements TestKmsFacadeFactory<Config, UUID, InMemoryEdek> {
    /**
     * {@inheritDoc}
     */
    @Override
    public InMemoryKmsFacade build() {
        return new InMemoryKmsFacade();
    }
}
