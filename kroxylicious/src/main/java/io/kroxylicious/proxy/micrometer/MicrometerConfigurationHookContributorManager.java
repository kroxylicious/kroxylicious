/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ContributionManager;

import static io.kroxylicious.proxy.service.Context.wrap;

@SuppressWarnings("java:S6548")
public class MicrometerConfigurationHookContributorManager {

    public static final MicrometerConfigurationHookContributorManager INSTANCE = new MicrometerConfigurationHookContributorManager();

    public static MicrometerConfigurationHookContributorManager getInstance() {
        return INSTANCE;
    }

    private MicrometerConfigurationHookContributorManager() {
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        try {
            return ContributionManager.INSTANCE.getDefinition(MicrometerConfigurationHookContributor.class, shortName).configurationType();
        }
        catch (IllegalArgumentException e) {
            // Catch and re-throw with a more user-friendly error message
            throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
        }
    }

    public MicrometerConfigurationHook getHook(String shortName, BaseConfig filterConfig) {
        try {
            return ContributionManager.INSTANCE.getInstance(MicrometerConfigurationHookContributor.class, shortName, wrap(filterConfig));
        }
        catch (IllegalArgumentException e) {
            // Catch and re-throw with a more user-friendly error message
            throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
        }
    }
}