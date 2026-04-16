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

    public record ImplementationAndConfigType(ServiceLoader.Provider<?> provider,
                                              Map<String, Class<?>> configVersions) {
        public ImplementationAndConfigType {
            Objects.requireNonNull(provider);
            Objects.requireNonNull(configVersions);
            if (configVersions.isEmpty()) {
                throw new IllegalArgumentException("configVersions must not be empty");
            }
        }

        /**
         * Returns the legacy (unversioned) config type, or the sole config type
         * if only one version is registered.
         */
        public Class<?> config() {
            Class<?> legacy = configVersions.get("");
            if (legacy != null) {
                return legacy;
            }
            if (configVersions.size() == 1) {
                return configVersions.values().iterator().next();
            }
            throw new IllegalStateException(
                    "Plugin has multiple config versions but no legacy (unversioned) config type: " + configVersions.keySet());
        }
    }

    private final Map<Class<?>, Map<String, ImplementationAndConfigType>> pluginInterfaceToNameToProvider = new ConcurrentHashMap<>();

    Map<String, ImplementationAndConfigType> load(Class<?> pluginInterface) {
        Objects.requireNonNull(pluginInterface);
        return pluginInterfaceToNameToProvider.computeIfAbsent(pluginInterface,
                i -> loadImplementations(pluginInterface));
    }

    private static Map<String, ImplementationAndConfigType> loadImplementations(Class<?> pluginInterface) {
        HashMap<String, Set<ImplementationAndConfigType>> nameToImplementation = new HashMap<>();
        ServiceLoader<?> load = ServiceLoader.load(pluginInterface);
        load.stream().forEach(provider -> {
            Class<?> providerType = provider.type();
            Plugin[] annotations = providerType.getAnnotationsByType(Plugin.class);
            if (annotations.length == 0) {
                LOGGER.atWarn()
                        .addKeyValue("providerType", providerType)
                        .addKeyValue("service", pluginInterface)
                        .log("Failed to find @Plugin on provider of service");
            }
            else {
                Map<String, Class<?>> versions = new HashMap<>();
                for (Plugin annotation : annotations) {
                    versions.put(annotation.configVersion(), annotation.configType());
                }
                ImplementationAndConfigType implementationAndConfigType = new ImplementationAndConfigType(
                        provider, Collections.unmodifiableMap(versions));
                Stream<String> names = Stream.of(providerType.getName(), providerType.getSimpleName());
                names = maybeAddOldNames(providerType, names);
                names.forEach(name -> nameToImplementation.compute(name, (k2, v) -> {
                    if (v == null) {
                        v = new HashSet<>();
                    }
                    v.add(implementationAndConfigType);
                    return v;
                }));
            }
        });
        var bySingleton = nameToImplementation.entrySet().stream().collect(
                Collectors.partitioningBy(e -> e.getValue().size() == 1));
        // log a warning about ambiguous names
        if (LOGGER.isWarnEnabled()) {
            for (Map.Entry<String, Set<ImplementationAndConfigType>> ambiguousInstanceNameToProviders : bySingleton.get(false)) {
                String ambiguousKey = ambiguousInstanceNameToProviders.getKey();
                var implementationClasses = ambiguousInstanceNameToProviders.getValue().stream()
                        .map(p -> p.provider().type())
                        .sorted(Comparator.comparing(Class::getName))
                        .toList();
                var fqCollision = implementationClasses.stream()
                        .filter(c -> c.isAnnotationPresent(DeprecatedPluginName.class))
                        .flatMap(c -> implementationClasses.stream()
                                .filter(c2 -> {
                                    var cOldName = c.getAnnotation(DeprecatedPluginName.class).oldName();
                                    return !c.equals(c2) && (c2.getName().equals(cOldName)
                                            || (c2.isAnnotationPresent(DeprecatedPluginName.class) &&
                                                    c2.getAnnotation(DeprecatedPluginName.class).oldName().equals(cOldName)));
                                })
                                .map(c2 -> Map.entry(c, c2)))
                        .findFirst();
                if (fqCollision.isPresent()) {
                    var entry = fqCollision.get();
                    var annotatedClass = entry.getKey();
                    var classWithCollidingFqName = entry.getValue();
                    LOGGER.atWarn()
                            .addKeyValue("annotatedClass", annotatedClass.getName())
                            .addKeyValue("annotation", DeprecatedPluginName.class.getSimpleName())
                            .addKeyValue("oldName", annotatedClass.getAnnotation(DeprecatedPluginName.class).oldName())
                            .addKeyValue("collidingClass", classWithCollidingFqName.getName())
                            .log("Plugin implementation class is annotated with @DeprecatedPluginName which collides "
                                    + "with another plugin implementation class, you must remove one of these classes from the class path");
                    throw new RuntimeException("Ambiguous plugin implementation name '" + ambiguousKey + "'");
                }
                else {
                    LOGGER.atWarn()
                            .addKeyValue("ambiguousKey", ambiguousKey)
                            .addKeyValue("pluginInterface", pluginInterface.getSimpleName())
                            .addKeyValue("candidates", implementationClasses.stream()
                                    .map(Class::getName)
                                    .collect(Collectors.joining(", ")))
                            .log("Ambiguous reference to provider, it could refer to multiple implementations "
                                    + "so to avoid ambiguous behaviour those fully qualified names must be used");
                }
            }
        }
        // return the unambiguous names
        return bySingleton.get(true).stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().iterator().next()));
    }

    private static Stream<String> maybeAddOldNames(Class<?> providerType, Stream<String> names) {
        if (providerType.isAnnotationPresent(DeprecatedPluginName.class)) {
            String oldName = providerType.getAnnotation(DeprecatedPluginName.class).oldName();
            if (oldName.equals(providerType.getName())) {
                LOGGER.atWarn()
                        .addKeyValue("providerType", providerType.getName())
                        .log("@DeprecatedPluginName annotation specifies an oldName == newName, "
                                + "this annotation is not being used correctly");
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
        var nameToImplementation = load(pluginInterface);
        return new ServiceBasedPluginFactory<>(nameToImplementation, pluginInterface);
    }

    private static class ServiceBasedPluginFactory<P> implements PluginFactory<P> {
        private static <P> void maybeWarnAboutDeprecatedPluginName(String instanceName, Class<?> type, Class<P> pluginClass) {
            if (type.isAnnotationPresent(DeprecatedPluginName.class)) {
                DeprecatedPluginName deprecatedName = type.getAnnotation(DeprecatedPluginName.class);
                if (isOldInstanceName(instanceName, deprecatedName, type)) {
                    LOGGER.atWarn()
                            .addKeyValue("pluginClass", pluginClass.getName())
                            .addKeyValue("oldName", instanceName)
                            .addKeyValue("newName", type.getName())
                            .log("Plugin should now be referred to using the new name, "
                                    + "the plugin has been renamed and in the future the old name will cease to work");
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

        private final Map<String, ImplementationAndConfigType> nameToImplementation;
        private final Class<P> pluginInterface;

        ServiceBasedPluginFactory(Map<String, ImplementationAndConfigType> nameToImplementation, Class<P> pluginInterface) {
            this.nameToImplementation = nameToImplementation;
            this.pluginInterface = pluginInterface;
        }

        @Override
        public P pluginInstance(String instanceName) {
            if (Objects.requireNonNull(instanceName).isEmpty()) {
                throw new IllegalArgumentException();
            }
            var implementationAndConfigType = nameToImplementation.get(instanceName);
            if (implementationAndConfigType != null) {
                Class<?> type = implementationAndConfigType.provider().type();
                maybeWarnAboutDeprecatedPluginClass(instanceName, type, pluginInterface);
                maybeWarnAboutDeprecatedPluginName(instanceName, type, pluginInterface);
                return pluginInterface.cast(implementationAndConfigType.provider().get());
            }
            throw unknownPluginInstanceException(instanceName);
        }

        private UnknownPluginInstanceException unknownPluginInstanceException(String name) {
            return new UnknownPluginInstanceException("Unknown " + pluginInterface.getName() + " plugin instance for name '" + name + "'. "
                    + "Known plugin instances are " + nameToImplementation.keySet() + ". "
                    + "Plugins must be loadable by java.util.ServiceLoader and annotated with @" + Plugin.class.getSimpleName() + ".");
        }

        @Override
        public Class<?> configType(String instanceName) {
            var providerAndConfigType = nameToImplementation.get(instanceName);
            if (providerAndConfigType != null) {
                return providerAndConfigType.config();
            }
            throw unknownPluginInstanceException(instanceName);
        }

        @Override
        public Map<String, Class<?>> configVersions(String instanceName) {
            var providerAndConfigType = nameToImplementation.get(instanceName);
            if (providerAndConfigType != null) {
                return providerAndConfigType.configVersions();
            }
            throw unknownPluginInstanceException(instanceName);
        }

        @Override
        public Class<?> implementationType(String instanceName) {
            var providerAndConfigType = nameToImplementation.get(instanceName);
            if (providerAndConfigType != null) {
                return providerAndConfigType.provider().type();
            }
            throw unknownPluginInstanceException(instanceName);
        }

        @Override
        public Set<String> registeredInstanceNames() {
            return Collections.unmodifiableSet(nameToImplementation.keySet());
        }
    }
}
