/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.sasl.credentialstore.keystore;

import java.security.SecureRandom;

/**
 * Backward-compatibility wrapper for {@link KeystoreCredentialManager}.
 * <p>
 * This class exists to maintain compatibility with existing test code.
 * New code should use {@link KeystoreCredentialManager} directly.
 * </p>
 *
 * @deprecated Use {@link KeystoreCredentialManager} instead
 */
@Deprecated(since = "0.20.0", forRemoval = true)
public class TestCredentialGenerator extends KeystoreCredentialManager {

    /**
     * Create a credential generator with a new {@link SecureRandom} instance.
     */
    public TestCredentialGenerator() {
        super();
    }

    /**
     * Create a credential generator with the specified {@link SecureRandom}.
     *
     * @param secureRandom the random source to use for salt generation
     */
    public TestCredentialGenerator(SecureRandom secureRandom) {
        super(secureRandom);
    }
}
