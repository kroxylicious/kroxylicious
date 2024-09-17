/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import io.kroxylicious.kms.service.AbstractTestKekManager;

public class InMemoryTestKekManager extends AbstractTestKekManager {
    private final InMemoryKms kms;

    public InMemoryTestKekManager(InMemoryKms kms) {
        this.kms = kms;
    }

    @Override
    protected UUID read(String alias) {
        return kms.resolveAlias(alias).toCompletableFuture().join();
    }

    @Override
    protected void create(String alias) {
        var kekId = kms.generateKey();
        kms.createAlias(kekId, alias);
    }

    @Override
    protected void delete(String alias) {
        var kekRef = read(alias);
        kms.deleteAlias(alias);
        kms.deleteKey(kekRef);
    }

    @Override
    protected void rotate(String alias) {
        create(alias);
    }
}
