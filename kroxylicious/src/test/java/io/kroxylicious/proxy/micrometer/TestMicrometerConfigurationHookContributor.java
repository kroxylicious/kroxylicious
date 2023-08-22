/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import java.util.Objects;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ContributorContext;

public class TestMicrometerConfigurationHookContributor implements MicrometerConfigurationHookContributor {

    public static class Config extends BaseConfig {

    }

    public static final String SHORT_NAME = "test";

    @Override
    public Class<? extends BaseConfig> getConfigType(String shortName) {
        if (Objects.equals(shortName, SHORT_NAME)) {
            return Config.class;
        }
        else {
            return null;
        }
    }

    @Override
    public MicrometerConfigurationHook getInstance(String shortName, BaseConfig config, ContributorContext context) {
        if (!Objects.equals(shortName, SHORT_NAME)) {
            return null;
        }
        return new TestHook(shortName, config, context);
    }
}
