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

public class ContributionManager<S extends Contributor<?, ?>> {

    private final Map<Class<S>, Iterable<S>> contributors;
    private final Function<Class<S>, Iterable<S>> loaderFunction;

    public ContributionManager() {
        this(ServiceLoader::load);
    }

    protected ContributionManager(Function<Class<S>, Iterable<S>> loaderFunction) {
        this.contributors = new ConcurrentHashMap<>();
        this.loaderFunction = loaderFunction;
    }

    public ConfigurationDefinition getDefinition(Class<S> contributorClass, String typeName) {
        final Iterable<S> contributorsForClass = this.contributors.computeIfAbsent(contributorClass, loaderFunction);
        for (S contributor : contributorsForClass) {
            if (contributor.contributes(typeName)) {
                return contributor.getConfigDefinition(typeName);
            }
        }
        throw new IllegalArgumentException("No Contributor for type " + contributorClass.getSimpleName() + " is registered with name " + typeName);
    }
}