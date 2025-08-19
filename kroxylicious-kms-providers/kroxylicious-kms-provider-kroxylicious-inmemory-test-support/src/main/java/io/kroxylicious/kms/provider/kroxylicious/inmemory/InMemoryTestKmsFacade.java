/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.Nullable;

public class InMemoryTestKmsFacade implements TestKmsFacade<Config, UUID, InMemoryEdek> {

    private final UUID kmsId = UUID.randomUUID();
    private @Nullable InMemoryKms kms;
    private @Nullable IntegrationTestingKmsService service;

    @Override
    public void start() {
        var config = new Config(kmsId.toString());

        service = IntegrationTestingKmsService.newInstance();
        service.initialize(config);
        kms = service.buildKms();
    }

    @Override
    public void stop() {
        Optional.ofNullable(service).ifPresent(IntegrationTestingKmsService::close);
        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new InMemoryTestKekManager();
    }

    @Override
    public Class<IntegrationTestingKmsService> getKmsServiceClass() {
        return IntegrationTestingKmsService.class;
    }

    @Override
    public InMemoryKms getKms() {
        return kms;
    }

    @Override
    public Config getKmsServiceConfig() {
        return new Config(kmsId.toString());
    }

    private class InMemoryTestKekManager implements TestKekManager {

        @Override
        public UUID read(String alias) {
            return kms.resolveAlias(alias).toCompletableFuture().join();
        }

        @Override
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            try {
                var existing = read(alias);
                if (existing != null) {
                    throw new AlreadyExistsException(alias);
                }
            }
            catch (CompletionException e) {
                if (e.getCause() instanceof UnknownAliasException) {
                    var kekId = kms.generateKey();
                    kms.createAlias(kekId, alias);
                }
                else {
                    throw toRuntimeException(e);
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
                throw toRuntimeException(e);
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
                throw toRuntimeException(e);
            }
        }
    }
}
