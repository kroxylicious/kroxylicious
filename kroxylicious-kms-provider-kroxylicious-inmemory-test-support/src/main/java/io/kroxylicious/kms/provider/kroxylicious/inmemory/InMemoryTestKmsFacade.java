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

public class InMemoryTestKmsFacade implements TestKmsFacade<Config, UUID, InMemoryEdek> {

    private final UUID kmsId = UUID.randomUUID();
    private InMemoryKms kms;
    private IntegrationTestingKmsService service;

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
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            try {
                kms.resolveAlias(alias).toCompletableFuture().join();
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
                var kekRef = kms.resolveAlias(alias).toCompletableFuture().join();
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
                kms.resolveAlias(alias).toCompletableFuture().join();
                var kekId = kms.generateKey();
                kms.createAlias(kekId, alias);
            }
            catch (CompletionException e) {
                throw unwrapRuntimeException(e);
            }
        }

        @Override
        public boolean exists(String alias) {
            try {
                kms.resolveAlias(alias).toCompletableFuture().join();
                return true;
            }
            catch (CompletionException e) {
                if (e.getCause() instanceof UnknownAliasException) {
                    return false;
                }
                throw unwrapRuntimeException(e);
            }
        }

        private RuntimeException unwrapRuntimeException(Exception e) {
            return e.getCause() instanceof RuntimeException re ? re : new RuntimeException(e.getCause());
        }

    }
}
