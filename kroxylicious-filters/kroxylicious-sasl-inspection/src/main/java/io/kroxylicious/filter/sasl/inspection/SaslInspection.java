/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.inspection;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.DeprecatedPluginName;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Factory for {@link SaslInspectionFilter}.
 */
@Plugin(configType = Config.class)
@Plugin(configVersion = "v1alpha1", configType = ConfigV1.class)
@DeprecatedPluginName(oldName = "io.kroxylicious.filters.sasl.inspection.SaslInspection", since = "0.19.0")
public class SaslInspection implements FilterFactory<Object, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaslInspection.class);

    public static final SaslSubjectBuilder DEFAULT_SUBJECT_BUILDER = context -> CompletableFuture
            .completedStage(new Subject(Set.of(new User(context.clientSaslContext().authorizationId()))));
    private @Nullable Map<String, SaslObserverFactory> observerFactoryMap;
    private @Nullable SaslSubjectBuilder subjectBuilder;
    private boolean authenticationRequired = false;

    @Override
    public Void initialize(FilterFactoryContext context,
                           @Nullable Object config)
            throws PluginConfigurationException {
        if (config instanceof ConfigV1 v1) {
            return initializeV1(context, v1);
        }
        else {
            return initializeLegacy(context, config instanceof Config c ? c : null);
        }
    }

    @Nullable
    private Void initializeLegacy(FilterFactoryContext context,
                                  @Nullable Config config) {
        observerFactoryMap = buildEnabledObserverFactoryMap(context, config);
        subjectBuilder = buildSubjectBuilderLegacy(context, config);
        authenticationRequired = config != null && config.requireAuthentication() != null && config.requireAuthentication();
        return null;
    }

    @Nullable
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Void initializeV1(FilterFactoryContext context,
                              @NonNull ConfigV1 config) {
        // V1 configs use the legacy observer map mechanism (observers are discovered, not referenced)
        observerFactoryMap = buildEnabledObserverFactoryMap(context,
                new Config(config.enabledMechanisms(), null, null, config.requireAuthentication()));

        if (config.subjectBuilder() != null) {
            ResolvedPluginRegistry registry = context.resolvedPluginRegistry()
                    .orElseThrow(() -> new PluginConfigurationException(
                            "v1alpha1 config requires a ResolvedPluginRegistry but none is available"));
            SaslSubjectBuilderService subjectBuilderFactory = registry.pluginInstance(
                    SaslSubjectBuilderService.class, config.subjectBuilder());
            Object subjectBuilderConfig = registry.pluginConfig(
                    SaslSubjectBuilderService.class.getName(), config.subjectBuilder());
            ((SaslSubjectBuilderService) subjectBuilderFactory).initialize(subjectBuilderConfig);
            subjectBuilder = subjectBuilderFactory.build();
        }
        else {
            LOGGER.debug("No `subjectBuilder` configured. The default SaslSubjectBuilder will be used.");
            subjectBuilder = DEFAULT_SUBJECT_BUILDER;
        }

        authenticationRequired = config.requireAuthentication() != null && config.requireAuthentication();
        return null;
    }

    @NonNull
    private static SaslSubjectBuilder buildSubjectBuilderLegacy(FilterFactoryContext context,
                                                                @Nullable Config config) {
        if (config == null || config.subjectBuilder() == null) {
            LOGGER.atDebug()
                    .log("No `subjectBuilder` configured. The default SaslSubjectBuilder will be used");
            return DEFAULT_SUBJECT_BUILDER;
        }
        else {
            SaslSubjectBuilderService subjectBuilderFactory = context.pluginInstance(SaslSubjectBuilderService.class, config.subjectBuilder());
            subjectBuilderFactory.initialize(config.subjectBuilderConfig());
            return subjectBuilderFactory.build();
        }
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, @Nullable Void unused) {
        Objects.requireNonNull(observerFactoryMap);
        Objects.requireNonNull(subjectBuilder);
        return new SaslInspectionFilter(observerFactoryMap, subjectBuilder, authenticationRequired);
    }

    private Map<String, SaslObserverFactory> buildEnabledObserverFactoryMap(FilterFactoryContext context,
                                                                            @Nullable Config config) {
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
    @NonNull
    Map<String, SaslObserverFactory> getObserverFactoryMap() {
        return observerFactoryMap;
    }

}
