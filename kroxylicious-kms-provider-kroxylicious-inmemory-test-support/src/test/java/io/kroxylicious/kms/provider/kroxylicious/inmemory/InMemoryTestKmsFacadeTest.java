/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.AbstractTestKmsFacadeTest;

class InMemoryTestKmsFacadeTest extends AbstractTestKmsFacadeTest<Config, UUID, InMemoryEdek> {

    InMemoryTestKmsFacadeTest() {
        super(new InMemoryTestKmsFacadeFactory());
    }

}
