/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * @deprecated We want to remove the generic Contributor type so this has to be rethought/reimplemented.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
@Deprecated(since = "0.3.0", forRemoval = true)
public class ContributionManager {
    public static final ContributionManager INSTANCE = new ContributionManager();

    private final Map<Class, Iterable> contributors;
    private final Function<Class, Iterable> loaderFunction;

    private ContributionManager() {
        this(ServiceLoader::load);
    }

    @VisibleForTesting
    ContributionManager(Function<Class, Iterable> loaderFunction) {
        this.contributors = new ConcurrentHashMap<>();
        this.loaderFunction = loaderFunction;
    }

    public <S extends Contributor> ConfigurationDefinition getDefinition(
            Class<S> contributorClass,
            String typeName
    ) {
        return (ConfigurationDefinition) findContributor(contributorClass, typeName, s -> new ConfigurationDefinition(s.getConfigType(), s.requiresConfiguration()));
    }

    public <T, S extends Contributor> T createInstance(
            Class<S> contributorClass,
            String typeName,
            Context constructionContext
    ) {
        return (T) findContributor(contributorClass, typeName, contributor -> contributor.createInstance(constructionContext));
    }

    private <T, S extends Contributor<T, ?, ?>, X> X findContributor(
            Class<S> contributorClass,
            String typeName,
            Function<S, X> extractor
    ) {
        final Iterable<S> contributorsForClass = this.contributors.computeIfAbsent(contributorClass, loaderFunction);
        for (S contributor : contributorsForClass) {
            if (matches(typeName, contributor)) {
                return extractor.apply(contributor);
            }
        }
        throw new IllegalArgumentException("Name '" + typeName + "' is not contributed by any " + contributorClass);
    }

    private static <T, S extends Contributor<T, ?, ?>> boolean matches(String typeName, S contributor) {
        Class<?> contributorClass = contributor.getServiceType();
        boolean matchesShortNameForTopLevelClass = !contributorClass.isMemberClass()
                                                   && !contributorClass.isLocalClass()
                                                   && !contributorClass.isAnonymousClass()
                                                   && contributorClass.getSimpleName().equals(typeName);
        return contributorClass.getName().equals(typeName) || matchesShortNameForTopLevelClass;
    }

    public record ConfigurationDefinition(
            Class<?> configurationType,
            boolean configurationRequired
    ) {
    }
}
