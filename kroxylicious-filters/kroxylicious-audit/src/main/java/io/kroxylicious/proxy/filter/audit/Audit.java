/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Objects;
import java.util.Set;

@Plugin(configType = Audit.Config.class)
public class Audit implements FilterFactory<Audit.Config, Audit.Config> {

    @Override
    public Audit.Config initialize(FilterFactoryContext context, Audit.Config config) {
        return config;
    }

    @NonNull
    @Override
    public AuditFilter createFilter(FilterFactoryContext context, Audit.Config configuration) {
        EventSinkService<Object> factory = context.pluginInstance(EventSinkService.class, configuration.sink());
        var sink = factory.createAuditSink(configuration.sinkConfig());
        return new AuditFilter(configuration, sink);
    }


    public record Config(@JsonProperty(required = true) Set<ApiKeys> apiKeys,
                         @JsonProperty(required = true) @PluginImplName(EventSinkService.class) String sink,
                         @PluginImplConfig(implNameProperty = "sink") Object sinkConfig) {
        @JsonCreator
        public Config {
            Objects.requireNonNull(apiKeys);
            Objects.requireNonNull(sink);
        }

    }

}
