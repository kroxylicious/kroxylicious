/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustEdek;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

/**
 * Factory for creating CipherTrust test facade instances using mock server.
 */
public class CipherTrustTestKmsFacadeFactory implements TestKmsFacadeFactory<Config, String, CipherTrustEdek> {

    /**
     * Creates a factory.
     */
    public CipherTrustTestKmsFacadeFactory() {
    }

    @Override
    public TestKmsFacade<Config, String, CipherTrustEdek> build() {
        return new CipherTrustTestKmsFacade();
    }
}
