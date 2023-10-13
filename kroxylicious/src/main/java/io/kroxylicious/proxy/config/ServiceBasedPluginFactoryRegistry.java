/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.PluginConfigType;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;
import io.kroxylicious.proxy.plugin.UnknownPluginTypeException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link PluginFactoryRegistry} that is implemented using {@link ServiceLoader} discovery.
 */
public class ServiceBasedPluginFactoryRegistry implements PluginFactoryRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBasedPluginFactoryRegistry.class);

    public record ProviderAndConfigType<P>(@NonNull ServiceLoader.Provider<P> provider,
                                           @NonNull Class<?> config) {
        public ProviderAndConfigType {
            Objects.requireNonNull(provider);
            Objects.requireNonNull(config);
        }
    }

    private final Map<Class<?>, Map<String, ProviderAndConfigType<?>>> pluginInterfaceToNameToProvider = new HashMap<>();

    Map<String, ProviderAndConfigType<?>> load(Class<?> pluginInterface) {
        return pluginInterfaceToNameToProvider.computeIfAbsent(pluginInterface,
                i -> loadProviders(pluginInterface));
    }

    private static Map<String, ProviderAndConfigType<?>> loadProviders(Class<?> pluginInterface) {
        HashMap<String, Set<ProviderAndConfigType<?>>> nameToProviders = new HashMap<>();
        ServiceLoader<?> load = ServiceLoader.load(pluginInterface);
        load.stream().forEach(provider -> {
            Class<?> providerType = provider.type();
            PluginConfigType annotation = providerType.getAnnotation(PluginConfigType.class);
            if (annotation == null) {
                LOGGER.warn("Failed to find a @PluginConfigType on provider {} of service {}", providerType, pluginInterface);
            }
            else {
                ProviderAndConfigType<?> providerAndConfigType = new ProviderAndConfigType<>(provider, annotation.value());
                Stream.of(providerType.getName(), providerType.getSimpleName()).forEach(name2 -> nameToProviders.compute(name2, (k2, v) -> {
                    if (v == null) {
                        v = new HashSet<>();
                    }
                    v.add(providerAndConfigType);
                    return v;
                }));
            }
        });
        var bySingleton = nameToProviders.entrySet().stream().collect(
                Collectors.partitioningBy(e -> e.getValue().size() == 1));
        if (LOGGER.isWarnEnabled()) {
            for (Map.Entry<String, Set<ProviderAndConfigType<?>>> ambiguousInstanceNameToProviders : bySingleton.get(false)) {
                LOGGER.warn("'{}' would be an ambiguous reference to a {} provider. "
                        + "It could refer to any of {}"
                        + " so to avoid ambiguous behaviour those fully qualified names must be used",
                        ambiguousInstanceNameToProviders.getKey(),
                        pluginInterface.getSimpleName(),
                        ambiguousInstanceNameToProviders.getValue().stream().map(p -> p.provider().type().getName()).collect(Collectors.joining(", ")));
            }
        }
        return bySingleton.get(true).stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().iterator().next()));
    }

    @Override
    public <P> @NonNull PluginFactory<P> pluginFactory(@NonNull Class<P> pluginClass) {
        var nameToProvider = load(pluginClass);
        if (nameToProvider != null && !nameToProvider.isEmpty()) {
            return new PluginFactory<>() {
                @Override
                public @NonNull P pluginInstance(String instanceName) {
                    var provider = nameToProvider.get(instanceName);
                    if (provider != null) {
                        return pluginClass.cast(provider.provider().get());
                    }
                    throw unknownPluginInstanceException(instanceName);
                }

                private UnknownPluginInstanceException unknownPluginInstanceException(String name) {
                    return new UnknownPluginInstanceException("Unknown " + pluginClass.getName() + " plugin instance for name '" + name + "'. "
                            + "Known plugin instances are " + nameToProvider.keySet());
                }

                @NonNull
                @Override
                public Class<?> configType(@NonNull String instanceName) {
                    var providerAndConfigType = nameToProvider.get(instanceName);
                    if (providerAndConfigType != null) {
                        return providerAndConfigType.config();
                    }
                    throw unknownPluginInstanceException(instanceName);
                }
            };
        }
        throw new UnknownPluginTypeException("Unknown plugin type " + pluginClass.getName());
    }
}
