/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import org.testcontainers.DockerClientFactory;

public class VaultKmsFacadeFactory implements TestKmsFacadeFactory {
    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    public TestKmsFacade build() {
        return new VaultKmsFacade();
    }
}
