/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

/**
 * Exposes the ability to manage the KEKs on a KMS implementation.
 */
public interface TestKekManager {
    /**
     * Creates a KEK in the KMS with given alias.
     *
     * @param alias kek alias
     * @throws AlreadyExistsException alias already exists
     */
    void generateKek(String alias);

    /**
     * Rotates the kek with the given alias
     *
     * @param alias kek alias
     * @throws UnknownAliasException a KEK with the given alias is not found
     */
    void rotateKek(String alias);

    /**
     * Tests whether kek with given alias exists.
     *
     * @param alias kek alias
     * @return true if the alias exist, false otherwise.
     */
    boolean exists(String alias);

    class AlreadyExistsException extends RuntimeException {
        public AlreadyExistsException(String message) {
            super(message);
        }
    }

}
