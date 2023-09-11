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

public class ContributionManager<T, C extends Context, S extends Contributor<T, C>> {

    private final Map<Class<S>, Iterable<S>> contributors;
    private final Function<Class<S>, Iterable<S>> loaderFunction;

    public ContributionManager() {
        this(ServiceLoader::load);
    }

    /* test */ ContributionManager(Function<Class<S>, Iterable<S>> loaderFunction) {
        this.contributors = new ConcurrentHashMap<>();
        this.loaderFunction = loaderFunction;
    }

    public ConfigurationDefinition getDefinition(Class<S> contributorClass, String typeName) {
        return findContributor(contributorClass, typeName, (typName, contributor) -> contributor.getConfigDefinition(typName));
    }

    private <X> X findContributor(Class<S> contributorClass, String typeName, BiFunction<String, S, X> extractor) {
        final Iterable<S> contributorsForClass = this.contributors.computeIfAbsent(contributorClass, loaderFunction);
        for (S contributor : contributorsForClass) {
            if (contributor.contributes(typeName)) {
                return extractor.apply(typeName, contributor);
            }
        }
        throw new IllegalArgumentException("No Contributor for type " + contributorClass.getSimpleName() + " is registered with name " + typeName);
    }

    public T getInstance(Class<S> contributorClass, String typeName, C constructionContext) {
        return findContributor(contributorClass, typeName, (typName, contributor) -> contributor.getInstance(typeName, constructionContext));
    }
}