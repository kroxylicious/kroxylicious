/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;

public class VaultKmsFacadeFactory implements TestKmsFacadeFactory<Config, String> {

    @Override
    public TestKmsFacade<Config, String> build() {
        return new VaultKmsFacade();
    }
}
