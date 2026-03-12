/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Map;

import io.kroxylicious.proxy.audit.AuditEmitterFactory;

public class AuditEmitterConfigBuilder extends AbstractDefinitionBuilder<AuditEmitterConfig> {
    private final String name;

    public AuditEmitterConfigBuilder(String name, String type) {
        super(type);
        this.name = name;
    }

    @Override
    protected AuditEmitterConfig buildInternal(String type, Map<String, Object> config) {
        var configType = new ServiceBasedPluginFactoryRegistry().pluginFactory(AuditEmitterFactory.class).configType(type);
        return new AuditEmitterConfig(name, type, mapper.convertValue(config, configType));
    }

    public String name() {
        return name;
    }
}
