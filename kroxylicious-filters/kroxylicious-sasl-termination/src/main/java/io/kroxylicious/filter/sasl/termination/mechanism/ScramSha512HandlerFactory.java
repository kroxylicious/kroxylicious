/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.time.Clock;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;

import io.kroxylicious.filter.sasl.termination.MechanismConfig;
import io.kroxylicious.filter.sasl.termination.ScramMechanismConfig;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Factory for creating SCRAM-SHA-512 mechanism handlers.
 * <p>
 * Manages the credential store lifecycle and creates per-connection handlers.
 * </p>
 */
public class ScramSha512HandlerFactory implements MechanismHandlerFactory {

    private static final String MECHANISM_NAME = ScramMechanism.SCRAM_SHA_512.mechanismName();

    @Nullable
    private ScramCredentialStore credentialStore;
    @Nullable
    private ScramCredentialStoreService<?> credentialStoreService;
    private Clock clock = Clock.systemUTC();

    @Override
    public String mechanismName() {
        return MECHANISM_NAME;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void initialize(MechanismConfig config, FilterFactoryContext context, Clock clock) throws PluginConfigurationException {
        this.clock = clock;
        if (!(config instanceof ScramMechanismConfig scramConfig)) {
            throw new PluginConfigurationException(
                    "SCRAM-SHA-512 requires SCRAM mechanism configuration with credentialStore");
        }

        ScramCredentialStoreService<Object> service = context.pluginInstance(
                ScramCredentialStoreService.class,
                scramConfig.credentialStore());
        service.initialize(scramConfig.credentialStoreConfig());
        this.credentialStore = service.buildCredentialStore();
        this.credentialStoreService = service;
    }

    @Override
    public MechanismHandler createHandler() {
        return new ScramHandler(ScramMechanism.SCRAM_SHA_512, credentialStore, clock);
    }

    @Override
    public void close() {
        if (credentialStoreService != null) {
            credentialStoreService.close();
            credentialStoreService = null;
        }
    }
}
