/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.concurrent.CompletionException;

import edu.umd.cs.findbugs.annotations.NonNull;

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
     * Removes a KEK from the KMS with given alias.
     *
     * @param alias kek alias
     * @throws UnknownAliasException alias already exists
     */
    void deleteKek(String alias);

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
    default boolean exists(String alias) {
        try {
            read(alias);
            return true;
        }
        catch (UnknownAliasException uae) {
            return false;
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof UnknownAliasException) {
                return false;
            }
            throw toRuntimeException(e);
        }
    }

    /**
     * Read kek with given alias.
     *
     * @param alias kek alias
     * @return the object
     */
    Object read(String alias);

    class AlreadyExistsException extends KmsException {
        public AlreadyExistsException(String alias) {
            super(alias);
        }
    }

    default RuntimeException toRuntimeException(@NonNull CompletionException e) {
        var cause = e.getCause();
        return cause instanceof RuntimeException re ? re : new RuntimeException(cause);
    }
}
