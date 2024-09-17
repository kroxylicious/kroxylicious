/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * The type Abstract test kek manager.
 */
public abstract class AbstractTestKekManager implements TestKekManager {

    @Override
    public void generateKek(String alias) {
        Objects.requireNonNull(alias);

        if (exists(alias)) {
            throw new AlreadyExistsException(alias);
        }
        else {
            create(alias);
        }
    }

    @Override
    public void deleteKek(String alias) {
        if (exists(alias)) {
            delete(alias);
        }
        else {
            throw new UnknownAliasException(alias);
        }
    }

    @Override
    public void rotateKek(String alias) {
        Objects.requireNonNull(alias);

        if (exists(alias)) {
            rotate(alias);
        }
        else {
            throw new UnknownAliasException(alias);
        }
    }

    @Override
    public boolean exists(String alias) {
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
            else {
                throw unwrapRuntimeException(e);
            }
        }
    }

    /**
     * Read tha KEK from KMB with given alias
     *
     * @param alias the alias
     * @return the KEK data
     */
    protected abstract Object read(String alias);

    /**
     * Creates a KEK in the KMS with given alias.
     *
     * @param alias the alias
     */
    protected abstract void create(String alias);

    /**
     * Removes a KEK from the KMS with given alias.
     *
     * @param alias the alias
     */
    protected abstract void delete(String alias);

    /**
     * Rotates the KEK with the given alias
     *
     * @param alias the alias
     */
    protected abstract void rotate(String alias);

    private RuntimeException unwrapRuntimeException(Exception e) {
        return e.getCause() instanceof RuntimeException re ? re : new RuntimeException(e.getCause());
    }
}
