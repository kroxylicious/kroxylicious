/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.util.UUID;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService.Config;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;

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
        return new InMemoryTestKekManager(getKms());
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
