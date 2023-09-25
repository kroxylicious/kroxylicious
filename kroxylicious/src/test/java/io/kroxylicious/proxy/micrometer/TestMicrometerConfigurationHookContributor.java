/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import io.kroxylicious.proxy.service.Context;

import edu.umd.cs.findbugs.annotations.NonNull;

public class TestMicrometerConfigurationHookContributor implements MicrometerConfigurationHookContributor {

    @NonNull
    @Override
    public String getTypeName() {
        return SHORT_NAME;
    }

    @NonNull
    @Override
    public Class<?> getConfigType() {
        return Config.class;
    }

    @NonNull
    @Override
    public boolean requiresConfiguration() {
        return true;
    }

    @NonNull
    @Override
    public MicrometerConfigurationHook getInstance(Context context) {
        return new TestHook(SHORT_NAME, context.getConfig(), context);
    }

    public static class Config {

    }

    public static final String SHORT_NAME = "test";

}
