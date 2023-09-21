/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ConfigurationDefinition;
import io.kroxylicious.proxy.service.Context;

public class TestMicrometerConfigurationHookContributor implements MicrometerConfigurationHookContributor {

    @NonNull
    @Override
    public String getTypeName() {
        return SHORT_NAME;
    }

    @NonNull
    @Override
    public ConfigurationDefinition getConfigDefinition() {
        return new ConfigurationDefinition(Config.class, true);
    }

    @NonNull
    @Override
    public MicrometerConfigurationHook getInstance(Context context) {
        return new TestHook(SHORT_NAME, context.getConfig(), context);
    }

    public static class Config extends BaseConfig {

    }

    public static final String SHORT_NAME = "test";

}