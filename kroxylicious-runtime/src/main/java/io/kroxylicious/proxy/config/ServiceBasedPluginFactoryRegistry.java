/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.Version;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;
import io.kroxylicious.proxy.plugin.ApiVersion;

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
        ApiVersion apiVersion = pluginInterface.getAnnotation(ApiVersion.class);
        if (apiVersion == null) {
            LOGGER.atWarn()
               .addKeyValue("api", pluginInterface.getName())
               .log("No @ApiVersion annotation found on plugin API. "
                       + "Missing @ApiVersion will be treated as an error in a future release");
        }
        else {
            var version = Version.parse(apiVersion.value());
            if (!version.isStable()) {
                Version.throwUnlessApiIsAllowed(pluginInterface.getName(), version);
                LOGGER.atWarn()
                        .addKeyValue("api", pluginInterface.getName())
                        .addKeyValue("version", apiVersion.value())
                        .log("Unstable API; this API could evolve incompatibly in a future release");
            }
        }
        Map<String, Set<ProviderAndConfigType>> nameToProviders = new HashMap<>();
        ServiceLoader.load(pluginInterface).stream()
                .forEach(provider -> registerProvider(provider, nameToProviders, pluginInterface));
        var partitioned = nameToProviders.entrySet().stream().collect(
                Collectors.partitioningBy(e -> e.getValue().size() == 1));
        var ambiguousEntries = partitioned.get(false);
        var unambiguousEntries = partitioned.get(true);
        if (LOGGER.isWarnEnabled()) {
            ambiguousEntries.forEach(ambiguousEntry -> handleAmbiguousEntry(ambiguousEntry, pluginInterface));
        }
        return unambiguousEntries.stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().iterator().next()));
    }

    private static void registerProvider(ServiceLoader.Provider<?> provider,
                                         Map<String, Set<ProviderAndConfigType>> nameToProviders,
                                         Class<?> pluginInterface) {
        Class<?> providerType = provider.type();
        Plugin annotation = providerType.getAnnotation(Plugin.class);
        if (annotation == null) {
            LOGGER.atWarn()
                    .addKeyValue("providerType", providerType)
                    .addKeyValue("service", pluginInterface)
                    .log("Failed to find @Plugin on provider of service");
            return;
        }
        ProviderAndConfigType providerAndConfigType = new ProviderAndConfigType(provider, annotation.configType());
        Stream<String> names = Stream.of(providerType.getName(), providerType.getSimpleName());
        names = maybeAddOldNames(providerType, names);
        names.forEach(name -> nameToProviders.computeIfAbsent(name, k -> new HashSet<>()).add(providerAndConfigType));
    }

    private static void handleAmbiguousEntry(Map.Entry<String, Set<ProviderAndConfigType>> ambiguousEntry,
                                             Class<?> pluginInterface) {
        String ambiguousKey = ambiguousEntry.getKey();
        List<Class<?>> implementationClasses = ambiguousEntry.getValue().stream()
                .<Class<?>> map(p -> p.provider().type())
                .sorted(Comparator.comparing(Class::getName))
                .toList();
        Optional<Map.Entry<Class<?>, Class<?>>> fqCollision = findDeprecatedNameCollision(implementationClasses);
        if (fqCollision.isPresent()) {
            var entry = fqCollision.get();
            var annotatedClass = entry.getKey();
            var classWithCollidingFqName = entry.getValue();
            LOGGER.atWarn()
                    .addKeyValue("annotatedClass", annotatedClass.getName())
                    .addKeyValue("annotation", DeprecatedPluginName.class.getSimpleName())
                    .addKeyValue("oldName", annotatedClass.getAnnotation(DeprecatedPluginName.class).oldName())
                    .addKeyValue("collidingClass", classWithCollidingFqName.getName())
                    .log("Plugin implementation class is annotated with @DeprecatedPluginName which collides with another plugin implementation class, you must remove one of these classes from the class path");
            throw new RuntimeException("Ambiguous plugin implementation name '" + ambiguousKey + "'");
        }
        else {
            LOGGER.atWarn()
                    .addKeyValue("ambiguousKey", ambiguousKey)
                    .addKeyValue("pluginInterface", pluginInterface.getSimpleName())
                    .addKeyValue("candidates", implementationClasses.stream()
                            .map(Class::getName)
                            .collect(Collectors.joining(", ")))
                    .log("Ambiguous reference to provider, it could refer to multiple implementations so to avoid ambiguous behaviour those fully qualified names must be used");
        }
    }

    private static Optional<Map.Entry<Class<?>, Class<?>>> findDeprecatedNameCollision(List<Class<?>> implementationClasses) {
        return implementationClasses.stream()
                .filter(c -> c.isAnnotationPresent(DeprecatedPluginName.class))
                .flatMap(c -> implementationClasses.stream()
                        .filter(c2 -> isDeprecatedNameCollision(c, c2))
                        .map(c2 -> Map.<Class<?>, Class<?>> entry(c, c2)))
                .findFirst();
    }

    private static boolean isDeprecatedNameCollision(Class<?> annotatedClass, Class<?> other) {
        if (annotatedClass.equals(other)) {
            return false;
        }
        String oldName = annotatedClass.getAnnotation(DeprecatedPluginName.class).oldName();
        return other.getName().equals(oldName)
                || (other.isAnnotationPresent(DeprecatedPluginName.class)
                        && other.getAnnotation(DeprecatedPluginName.class).oldName().equals(oldName));
    }

    private static Stream<String> maybeAddOldNames(Class<?> providerType, Stream<String> names) {
        if (providerType.isAnnotationPresent(DeprecatedPluginName.class)) {
            String oldName = providerType.getAnnotation(DeprecatedPluginName.class).oldName();
            if (oldName.equals(providerType.getName())) {
                LOGGER.atWarn()
                        .addKeyValue("providerType", providerType.getName())
                        .log("@DeprecatedPluginName annotation specifies an oldName == newName, this annotation is not being used correctly");
            }
            else {
                names = Stream.concat(names, Stream.of(oldName));
                String shortName = simpleName(oldName);
                if (shortName != null) {
                    names = Stream.concat(names, Stream.of(shortName));
                }
            }
        }
        return names;
    }

    private static @Nullable String simpleName(String oldName) {
        String substring = null;
        var idx = oldName.lastIndexOf('.');
        if (idx != -1 && idx != oldName.length() - 1) {
            substring = oldName.substring(idx + 1);
        }
        return substring;
    }

    @Override
    public <P> PluginFactory<P> pluginFactory(Class<P> pluginInterface) {
        var nameToProvider = load(pluginInterface);
        return new PluginFactory<>() {
            @Override
            public P pluginInstance(String pluginImplementation) {
                if (Objects.requireNonNull(pluginImplementation).isEmpty()) {
                    throw new IllegalArgumentException();
                }
                var provider = nameToProvider.get(pluginImplementation);
                if (provider != null) {
                    Class<?> pluginImplClass = provider.provider().type();
                    maybeWarnAboutDeprecatedPluginClass(pluginImplementation, pluginImplClass, pluginInterface);
                    maybeWarnAboutDeprecatedPluginName(pluginImplementation, pluginImplClass, pluginInterface);
                    return pluginInterface.cast(provider.provider().get());
                }
                throw unknownPluginInstanceException(pluginImplementation);
            }

            private UnknownPluginInstanceException unknownPluginInstanceException(String name) {
                return new UnknownPluginInstanceException("Unknown " + pluginInterface.getName() + " plugin instance for name '" + name + "'. "
                        + "Known plugin instances are " + nameToProvider.keySet() + ". "
                        + "Plugins must be loadable by java.util.ServiceLoader and annotated with @" + Plugin.class.getSimpleName() + ".");
            }

            @Override
            public Class<?> configType(String pluginImplementation) {
                var providerAndConfigType = nameToProvider.get(pluginImplementation);
                if (providerAndConfigType != null) {
                    return providerAndConfigType.config();
                }
                throw unknownPluginInstanceException(pluginImplementation);
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
                LOGGER.atWarn()
                        .addKeyValue("pluginClass", pluginClass.getName())
                        .addKeyValue("oldName", instanceName)
                        .addKeyValue("newName", type.getName())
                        .log("Plugin should now be referred to using the new name, the plugin has been renamed and in the future the old name will cease to work");
            }
        }
    }

    private static <P> void maybeWarnAboutDeprecatedPluginClass(String instanceName, Class<?> type, Class<P> pluginClass) {
        if (type.isAnnotationPresent(Deprecated.class)) {
            LOGGER.atWarn()
                    .addKeyValue("pluginClass", pluginClass.getName())
                    .addKeyValue("name", instanceName)
                    .log("Plugin is deprecated");
        }
    }

    private static boolean isOldInstanceName(String instanceName, DeprecatedPluginName deprecatedName, Class<?> type) {
        return instanceName.equals(deprecatedName.oldName())
                || (!isFqName(instanceName) // is a short name
                        && !instanceName.equals(type.getSimpleName()) // given shortName is not the class's simpleName
                        && instanceName.equals(simpleName(deprecatedName.oldName())) // but is the short form of the old name
                );
    }

    private static boolean isFqName(String instanceName) {
        return instanceName.indexOf('.') != -1;
    }
}
