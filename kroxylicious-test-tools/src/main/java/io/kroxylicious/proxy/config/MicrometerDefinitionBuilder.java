/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookContributorManager;

public class MicrometerDefinitionBuilder extends AbstractDefinitionBuilder<MicrometerDefinition> {
    public MicrometerDefinitionBuilder(String type) {
        super(type);
    }

    @Override
    protected MicrometerDefinition buildInternal(String type, Map<String, Object> config) {
        var configType = MicrometerConfigurationHookContributorManager.getInstance().getConfigType(type);
        return new MicrometerDefinition(type, mapper.convertValue(config, configType));
    }
}
