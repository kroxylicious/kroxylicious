/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link PluginFactoryRegistry} that is implemented using {@link ServiceLoader} discovery.
 */
public class ServiceBasedPluginFactoryRegistry implements PluginFactoryRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBasedPluginFactoryRegistry.class);

    public record ProviderAndConfigType(ServiceLoader.Provider<?> provider,
                                        Class<?> config) {
        public ProviderAndConfigType {
            Objects.requireNonNull(provider);
            Objects.requireNonNull(config);
        }
    }

    private final Map<Class<?>, Map<String, ProviderAndConfigType>> pluginInterfaceToNameToProvider = new ConcurrentHashMap<>();

    Map<String, ProviderAndConfigType> load(Class<?> pluginInterface) {
        Objects.requireNonNull(pluginInterface);
        return pluginInterfaceToNameToProvider.computeIfAbsent(pluginInterface,
                i -> loadProviders(pluginInterface));
    }

    private static Map<String, ProviderAndConfigType> loadProviders(Class<?> pluginInterface) {
        HashMap<String, Set<ProviderAndConfigType>> nameToProviders = new HashMap<>();
        ServiceLoader<?> load = ServiceLoader.load(pluginInterface);
        load.stream().forEach(provider -> {
            Class<?> providerType = provider.type();
            Plugin annotation = providerType.getAnnotation(Plugin.class);
            if (annotation == null) {
                LOGGER.warn("Failed to find a @Plugin on provider {} of service {}", providerType, pluginInterface);
            }
            else {
                ProviderAndConfigType providerAndConfigType = new ProviderAndConfigType(provider, annotation.configType());
                Stream<String> names = Stream.of(providerType.getName(), providerType.getSimpleName());
                names = maybeAddOldNames(providerType, names);
                names.forEach(name -> nameToProviders.compute(name, (k2, v) -> {
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
            for (Map.Entry<String, Set<ProviderAndConfigType>> ambiguousInstanceNameToProviders : bySingleton.get(false)) {
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

    private static Stream<String> maybeAddOldNames(Class<?> providerType, Stream<String> names) {
        if (providerType.isAnnotationPresent(DeprecatedPluginName.class)) {
            String oldName = providerType.getAnnotation(DeprecatedPluginName.class).oldName();
            if (oldName.equals(providerType.getName())) {
                LOGGER.warn("@DeprecatedPluginName annotation on {} "
                        + "specifies an oldName == newName. "
                        + "This annotation is not being used correctly.",
                        providerType.getName());
            }
            else {
                names = Stream.concat(names, Stream.of(oldName));
                String shortName = shortName(oldName);
                if (shortName != null) {
                    names = Stream.concat(names, Stream.of(shortName));
                }
            }
        }
        return names;
    }

    private static @Nullable String shortName(String oldName) {
        String substring = null;
        var idx = oldName.lastIndexOf('.');
        if (idx != -1 && idx != oldName.length() - 1) {
            substring = oldName.substring(idx + 1);
        }
        return substring;
    }

    @Override
    public <P> PluginFactory<P> pluginFactory(Class<P> pluginClass) {
        var nameToProvider = load(pluginClass);
        return new PluginFactory<>() {
            @Override
            public P pluginInstance(String instanceName) {
                if (Objects.requireNonNull(instanceName).isEmpty()) {
                    throw new IllegalArgumentException();
                }
                var provider = nameToProvider.get(instanceName);
                if (provider != null) {
                    Class<?> type = provider.provider().type();
                    maybeWarnAboutDeprecatedPluginClass(instanceName, type, pluginClass);
                    maybeWarnAboutDeprecatedPluginName(instanceName, type, pluginClass);
                    return pluginClass.cast(provider.provider().get());
                }
                throw unknownPluginInstanceException(instanceName);
            }

            private UnknownPluginInstanceException unknownPluginInstanceException(String name) {
                return new UnknownPluginInstanceException("Unknown " + pluginClass.getName() + " plugin instance for name '" + name + "'. "
                        + "Known plugin instances are " + nameToProvider.keySet() + ". "
                        + "Plugins must be loadable by java.util.ServiceLoader and annotated with @" + Plugin.class.getSimpleName() + ".");
            }

            @Override
            public Class<?> configType(String instanceName) {
                var providerAndConfigType = nameToProvider.get(instanceName);
                if (providerAndConfigType != null) {
                    return providerAndConfigType.config();
                }
                throw unknownPluginInstanceException(instanceName);
            }

            @Override
            public Set<String> registeredInstanceNames() {
                return Collections.unmodifiableSet(nameToProvider.keySet());
            }
        };
    }

    private static <P> void maybeWarnAboutDeprecatedPluginName(String instanceName, Class<?> type, Class<P> pluginClass) {
        if (type.isAnnotationPresent(DeprecatedPluginName.class)) {
            DeprecatedPluginName deprecatedName = type.getAnnotation(DeprecatedPluginName.class);
            if (isOldInstanceName(instanceName, deprecatedName, type)) {
                LOGGER.warn("{} plugin with name '{}' should now be referred to using the name '{}'. "
                        + "The plugin has been renamed and "
                        + "in the future the old name '{}' will cease to work.",
                        pluginClass.getName(),
                        instanceName,
                        type.getName(),
                        instanceName);
            }
        }
    }

    private static <P> void maybeWarnAboutDeprecatedPluginClass(String instanceName, Class<?> type, Class<P> pluginClass) {
        if (type.isAnnotationPresent(Deprecated.class)) {
            LOGGER.warn("{} plugin with name '{}' is deprecated.",
                    pluginClass.getName(),
                    instanceName);
        }
    }

    private static boolean isOldInstanceName(String instanceName, DeprecatedPluginName deprecatedName, Class<?> type) {
        return instanceName.equals(deprecatedName.oldName())
                || (instanceName.indexOf('.') == -1 // is a short name
                        && !instanceName.equals(type.getSimpleName()) // given shortName is not the class's simpleName
                        && instanceName.equals(shortName(deprecatedName.oldName())) // but is the short form of the old name
                );
    }
}
