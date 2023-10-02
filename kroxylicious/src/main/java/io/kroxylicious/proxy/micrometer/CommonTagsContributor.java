/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.micrometer.CommonTagsHook.CommonTagsHookConfig;
import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class CommonTagsContributor implements MicrometerConfigurationHookContributor<CommonTagsHookConfig> {

    @Override
    @NonNull
    public Class<? extends MicrometerConfigurationHook> getServiceType() {
        return CommonTagsHook.class;
    }

    @Override
    @NonNull
    public Class<CommonTagsHookConfig> getConfigType() {
        return CommonTagsHookConfig.class;
    }

    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public MicrometerConfigurationHook createInstance(Context<CommonTagsHookConfig> context) {
        return new CommonTagsHook(context.getConfig());
    }

}
