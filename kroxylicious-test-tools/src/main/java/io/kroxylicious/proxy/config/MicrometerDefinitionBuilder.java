/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookContributor;
import io.kroxylicious.proxy.service.ContributionManager;

public class MicrometerDefinitionBuilder extends AbstractDefinitionBuilder<MicrometerDefinition> {
    public MicrometerDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected MicrometerDefinition buildInternal(String type, Map<String, Object> config) {
        Class<? extends BaseConfig> result;
        try {
            result = ContributionManager.INSTANCE.getDefinition(MicrometerConfigurationHookContributor.class, type, (clazz, typeName) -> "").configurationType();
        }
        catch (IllegalArgumentException e) {
            // Catch and re-throw with a more user-friendly error message
            throw new IllegalArgumentException("No micrometer configuration hook found for name '" + type + "'");
        }
        var configType = result;
        return new MicrometerDefinition(type, mapper.convertValue(config, configType));
    }
}