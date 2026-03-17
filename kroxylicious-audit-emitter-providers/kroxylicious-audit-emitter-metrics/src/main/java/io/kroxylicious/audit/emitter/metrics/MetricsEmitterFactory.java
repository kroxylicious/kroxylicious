/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.Map;
import java.util.Optional;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditEmitterFactory;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = MetricsEmitterConfig.class)
public class MetricsEmitterFactory implements AuditEmitterFactory<MetricsEmitterConfig> {
    @Override
    public AuditEmitter create(MetricsEmitterConfig configuration) {
        if (configuration == null) {
            configuration = new MetricsEmitterConfig(Map.of());
        }
        return new MetricsEmitter(Optional.ofNullable(configuration.objectScopeMapping()).orElse(Map.of()));
    }
}
