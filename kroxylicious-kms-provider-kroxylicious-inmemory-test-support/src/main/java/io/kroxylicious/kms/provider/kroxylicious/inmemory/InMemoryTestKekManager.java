/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;

public class InMemoryTestKekManager implements TestKekManager {
    private final InMemoryKms kms;

    public InMemoryTestKekManager(InMemoryKms kms) {
        this.kms = kms;
    }

    @Override
    public UUID read(String alias) {
        return kms.resolveAlias(alias).toCompletableFuture().join();
    }

    @Override
    public void generateKek(String alias) {
       Objects.requireNonNull(alias);

        try {
            read(alias);
            throw new AlreadyExistsException(alias);
        }
        catch (CompletionException e) {
            if (e.getCause() instanceof UnknownAliasException) {
                var kekId = kms.generateKey();
                kms.createAlias(kekId, alias);
            }
            else {
                throw unwrapRuntimeException(e);
            }
        }
    }

    @Override
    public void deleteKek(String alias) {
        Objects.requireNonNull(alias);
        try {
            var kekRef = read(alias);
            kms.deleteAlias(alias);
            kms.deleteKey(kekRef);
        }
        catch (CompletionException e) {
            throw unwrapRuntimeException(e);
        }
    }

    @Override
    public void rotateKek(String alias) {
        Objects.requireNonNull(alias);

        try {
            read(alias);
            var kekId = kms.generateKey();
            kms.createAlias(kekId, alias);
        }
        catch (CompletionException e) {
            throw unwrapRuntimeException(e);
        }
    }
}
