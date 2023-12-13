/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.DockerClientFactory;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.service.AbstractTestKmsFacadeTest;

import static org.assertj.core.api.Assumptions.assumeThat;

class VaultTestKmsFacadeTest extends AbstractTestKmsFacadeTest<Config, String, VaultEdek> {

    VaultTestKmsFacadeTest() {
        super(new VaultTestKmsFacadeFactory());
    }

    @BeforeEach
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();
    }
}
