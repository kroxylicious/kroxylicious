/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
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
        var allNames = context.pluginImplementationNames(SaslObserverFactory.class);
        var allMap = allNames
                .stream()
                .map(instanceName -> context.pluginInstance(SaslObserverFactory.class, instanceName))
                .collect(Collectors.toMap(SaslObserverFactory::mechanismName, Function.identity(),
                        (sof1, sof2) -> {
                            if (sof1.getClass() != sof2.getClass()) {
                                throw new IllegalStateException(sof1.getClass().getSimpleName() + " and " + sof2.getClass().getSimpleName()
                                        + " both register the same SASL mechanism name " + sof1.mechanismName());
                            }
                            return sof1;
                        }));

        var secureOnly = allMap.entrySet()
                .stream().filter(Predicate.not(e -> e.getValue().transmitsCredentialInCleartext()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, HashMap::new));

        var enabledMechNames = Optional.ofNullable(config).map(Config::enabledMechanisms).orElse(secureOnly.keySet());

        var unknownMechNames = enabledMechNames.stream().filter(Predicate.not(allMap::containsKey)).collect(Collectors.toSet());
        if (!unknownMechNames.isEmpty()) {
            throw new PluginConfigurationException("The following enabled SASL mechanism names are unknown: [" + String.join(",", unknownMechNames) + "]");
        }

        Map<String, SaslObserverFactory> onlyEnabled = new HashMap<>(allMap);
        onlyEnabled.keySet().retainAll(enabledMechNames);

        if (onlyEnabled.isEmpty()) {
            throw new PluginConfigurationException("The SaslObserver requires at at least one enabled SaslObserver implementation. "
                    + "Discovered implementations names: [" + String.join(",", allNames) + "]");
        }
        return onlyEnabled;
    }

    @VisibleForTesting
    public Map<String, SaslObserverFactory> getObserverFactoryMap() {
        return observerFactoryMap;
    }

}
