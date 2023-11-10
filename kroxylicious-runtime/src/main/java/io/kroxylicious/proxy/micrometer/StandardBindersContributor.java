/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.micrometer.StandardBindersHook.StandardBindersHookConfig;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class StandardBindersContributor implements MicrometerConfigurationHookContributor<StandardBindersHookConfig> {

    @NonNull
    @Override
    public Class<? extends MicrometerConfigurationHook> getServiceType() {
        return StandardBindersHook.class;
    }

    @Override
    public Class<StandardBindersHookConfig> getConfigType() {
        return StandardBindersHookConfig.class;
    }

    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public MicrometerConfigurationHook createInstance(Context<StandardBindersHookConfig> context) {
        return new StandardBindersHook(context.getConfig());
    }

}
