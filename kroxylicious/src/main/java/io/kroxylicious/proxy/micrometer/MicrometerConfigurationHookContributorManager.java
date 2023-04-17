/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.ServiceLoader;

import io.kroxylicious.proxy.config.BaseConfig;

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
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
    }

    public MicrometerConfigurationHook getHook(String shortName, BaseConfig filterConfig) {
        for (MicrometerConfigurationHookContributor contributor : contributors) {
            MicrometerConfigurationHook hook = contributor.getInstance(shortName, null, filterConfig);
            if (hook != null) {
                return hook;
            }
        }

        throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
    }

}
