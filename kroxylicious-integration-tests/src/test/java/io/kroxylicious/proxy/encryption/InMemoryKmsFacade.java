/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.UnknownAliasException;

public class InMemoryKmsFacade implements TestKmsFacade<Config, UUID> {

    private final UUID kmsId = UUID.randomUUID();
    private InMemoryKms kms;

    @Override
    public void start() {
        kms = IntegrationTestingKmsService.newInstance().buildKms(new Config(kmsId.toString()));
    }

    @Override
    public void stop() {
        IntegrationTestingKmsService.delete(kmsId.toString());
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new TestKekManager() {
            @Override
            public CompletionStage<Void> generateKek(String alias) {
                Objects.requireNonNull(alias);

                return kms.resolveAlias(alias)
                        .handle((u, t) -> {
                            if (t instanceof UnknownAliasException) {
                                var kekId = kms.generateKey();
                                kms.createAlias(kekId, alias);
                                return null;
                            }
                            else {
                                throw new AlreadyExistsException("key with alias " + alias + " already exists");
                            }
                        });
            }

            @Override
            public CompletionStage<Void> rotateKek(String alias) {
                Objects.requireNonNull(alias);
                return kms.resolveAlias(alias)
                        .thenApply(u -> {
                            var kekId = kms.generateKey();
                            kms.createAlias(kekId, alias);
                            return null;
                        });

            }
        };
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
}
