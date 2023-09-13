/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ContributionManager /* <T, C extends Context, S extends Contributor<T, C>> */ {
    public static final ContributionManager INSTANCE = new ContributionManager();

    private final Map<Class, Iterable> contributors;
    private final Function<Class, Iterable> loaderFunction;

    private ContributionManager() {
        this(ServiceLoader::load);
    }

    /* test */ ContributionManager(Function<Class, Iterable> loaderFunction) {
        this.contributors = new ConcurrentHashMap<>();
        this.loaderFunction = loaderFunction;
    }

    public <T, C extends Context, S extends Contributor<T, C>> ConfigurationDefinition getDefinition(Class<S> contributorClass, String typeName,
                                                                                                     BiFunction<Class<S>, String, String> errorGenerator) {
        return findContributor(contributorClass, typeName, (typName, contributor) -> contributor.getConfigDefinition(typName), errorGenerator);
    }

    public <T, C extends Context, S extends Contributor<T, C>> T getInstance(Class<S> contributorClass, String typeName, C constructionContext,
                                                                             BiFunction<Class<S>, String, String> errorGenerator) {
        return findContributor(contributorClass, typeName, (typName, contributor) -> contributor.getInstance(typeName, constructionContext), errorGenerator);
    }

    private <T, C extends Context, S extends Contributor<T, C>, X> X findContributor(Class<S> contributorClass, String typeName, BiFunction<String, S, X> extractor,
                                                                                     BiFunction<Class<S>, String, String> errorGenerator) {
        final Iterable<S> contributorsForClass = this.contributors.computeIfAbsent(contributorClass, loaderFunction);
        for (S contributor : contributorsForClass) {
            if (contributor.contributes(typeName)) {
                return extractor.apply(typeName, contributor);
            }
        }
        throw new IllegalArgumentException(errorGenerator.apply(contributorClass, typeName));
    }
}