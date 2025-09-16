/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Factory for {@link SaslInspectionFilter}.
 */
@Plugin(configType = Config.class)
public class SaslInspection implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context,
                             @Nullable Config config)
            throws PluginConfigurationException {
        return config == null ? new Config(false) : config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        Objects.requireNonNull(config);
        Set<String> enabled = new HashSet<>(context.pluginInstanceNames(SaslObserverFactory.class));
        var mechFactories = enabled.stream()
                .map(instanceName -> context.pluginInstance(SaslObserverFactory.class, instanceName))
                .filter(sof -> !sof.isInsecure() || config.enableInsecureMechanisms())
                .collect(Collectors.toMap(SaslObserverFactory::mechanismName, Function.identity(),
                        (sof1, sof2) -> {
                            if (sof1.getClass() != sof1.getClass()) {
                                throw new IllegalStateException(sof1.getClass().getSimpleName() + " and " + sof2.getClass().getSimpleName()
                                        + " both register the same SASL mechanism name " + sof1.mechanismName());
                            }
                            return sof1;
                        }));

        return new SaslInspectionFilter(mechFactories);
    }
}
