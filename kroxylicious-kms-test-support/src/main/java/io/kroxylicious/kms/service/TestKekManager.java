/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.concurrent.CompletionStage;

/**
 * Exposes the ability to manage the KEKs on a KMS implementation.
 */
public interface TestKekManager {
    /**
     * Creates a KEK in the KMS with given alias.
     *
     * @param alias kek alias
     * @return completion stage that completes when the KEK exists
     * and its alias is assigned.
     * @throws AlreadyExistsException alias already exists
     */
    CompletionStage<Void> generateKek(String alias);

    /**
     * Rotates the kek with the given alias
     * @param alias kek alias
     * @return completion stage that completes when the KEK is rotated.
     * @throws UnknownAliasException a KEK with the given alias is not found
     */
    CompletionStage<Void> rotateKek(String alias);

    class AlreadyExistsException extends RuntimeException {
        public AlreadyExistsException(String message) {
            super(message);
        }
    }

}
