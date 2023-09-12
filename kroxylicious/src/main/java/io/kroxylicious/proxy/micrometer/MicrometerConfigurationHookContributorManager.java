/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.Context;
import io.kroxylicious.proxy.service.ContributionManager;

import static io.kroxylicious.proxy.service.Context.wrap;

public class MicrometerConfigurationHookContributorManager {

    private static final MicrometerConfigurationHookContributorManager INSTANCE = new MicrometerConfigurationHookContributorManager();
    private final ContributionManager<MicrometerConfigurationHook, Context, MicrometerConfigurationHookContributor> contributionManager;

    public static MicrometerConfigurationHookContributorManager getInstance() {
        return INSTANCE;
    }

    private MicrometerConfigurationHookContributorManager() {
        contributionManager = new ContributionManager<>();
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        try {
            return this.contributionManager.getDefinition(MicrometerConfigurationHookContributor.class, shortName).configurationType();
        }
        catch (IllegalArgumentException e) {
            // Catch and re-throw with a more user-friendly error message
            throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
        }
    }

    public MicrometerConfigurationHook getHook(String shortName, BaseConfig filterConfig) {
        try {
            return this.contributionManager.getInstance(MicrometerConfigurationHookContributor.class, shortName, wrap(filterConfig));
        }
        catch (IllegalArgumentException e) {
            // Catch and re-throw with a more user-friendly error message
            throw new IllegalArgumentException("No micrometer configuration hook found for name '" + shortName + "'");
        }
    }
}