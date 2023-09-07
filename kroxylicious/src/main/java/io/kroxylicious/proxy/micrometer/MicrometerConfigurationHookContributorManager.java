/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.Optional;
import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.InstanceFactory;

public class MicrometerConfigurationHookContributorManager {

    private static final MicrometerConfigurationHookContributorManager INSTANCE = new MicrometerConfigurationHookContributorManager();

    private final ServiceLoader<MicrometerConfigurationHookContributor> contributors;

    public static MicrometerConfigurationHookContributorManager getInstance() {
        return INSTANCE;
    }

    private MicrometerConfigurationHookContributorManager() {
        this.contributors = ServiceLoader.load(MicrometerConfigurationHookContributor.class);
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        for (MicrometerConfigurationHookContributor contributor : contributors) {
            Optional<InstanceFactory<MicrometerConfigurationHook>> factory = contributor.getInstanceFactory(shortName);
            if (factory.isPresent()) {
                return factory.get().getConfigClass();
            }
        }

        throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
    }

    public MicrometerConfigurationHook getHook(String shortName, BaseConfig filterConfig) {
        for (MicrometerConfigurationHookContributor contributor : contributors) {
            Optional<InstanceFactory<MicrometerConfigurationHook>> factory = contributor.getInstanceFactory(shortName);
            if (factory.isPresent()) {
                return factory.get().getInstance(filterConfig);
            }
        }

        throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
    }

}
