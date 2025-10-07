/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Factory for {@link SaslInspectionFilter}.
 */
@Plugin(configType = Config.class)
public class SaslInspection implements FilterFactory<Config, Void> {

    private Map<String, SaslObserverFactory> observerFactoryMap;

    @Override
    public Void initialize(FilterFactoryContext context,
                             @Nullable Config config)
            throws PluginConfigurationException {
        observerFactoryMap = buildEnabledObserverFactoryMap(context, config);
        return null;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void unused) {
        return new SaslInspectionFilter(observerFactoryMap);
    }

    private Map<String, SaslObserverFactory> buildEnabledObserverFactoryMap(FilterFactoryContext context, @Nullable Config config) {
        Set<String> all = new HashSet<>(context.pluginImplementationNames(SaslObserverFactory.class));
        var enableInsecureMechanisms = Optional.ofNullable(config).map(Config::enableInsecureMechanisms).orElse(false);

        var mechMap = all.stream()
                .map(instanceName -> context.pluginInstance(SaslObserverFactory.class, instanceName))
                .filter(sof -> !sof.transmitsCredentialInCleartext() || enableInsecureMechanisms)
                .collect(Collectors.toMap(SaslObserverFactory::mechanismName, Function.identity(),
                        (sof1, sof2) -> {
                            if (sof1.getClass() != sof2.getClass()) {
                                throw new IllegalStateException(sof1.getClass().getSimpleName() + " and " + sof2.getClass().getSimpleName()
                                        + " both register the same SASL mechanism name " + sof1.mechanismName());
                            }
                            return sof1;
                        }));
        if (mechMap.isEmpty()) {
            throw new PluginConfigurationException("The SaslObserver requires at at least one enabled SaslObserver implementation. "
            + "Discovered implementations names: [" + String.join(",", all) + "]");
        }
        return mechMap;
    }

    @VisibleForTesting
    public Map<String, SaslObserverFactory> getObserverFactoryMap() {
        return observerFactoryMap;
    }

}
