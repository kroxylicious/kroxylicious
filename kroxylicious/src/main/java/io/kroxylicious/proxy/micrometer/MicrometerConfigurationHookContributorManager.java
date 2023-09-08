/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.internal.ContributorManager;

public class MicrometerConfigurationHookContributorManager
        extends ContributorManager<MicrometerConfigurationHook, MicrometerConfigurationHookContributor> {

    private static final MicrometerConfigurationHookContributorManager INSTANCE = new MicrometerConfigurationHookContributorManager();

    public static MicrometerConfigurationHookContributorManager getInstance() {
        return INSTANCE;
    }

    private MicrometerConfigurationHookContributorManager() {
        super(MicrometerConfigurationHookContributor.class, "micrometer configuration hook");
    }

}
