/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

/**
 * Test-only filter factory whose {@code initialize} always fails. Used by hot-reload ITs that
 * need to drive ReplaceCluster down its failure path (new chain fails to initialise during the
 * add step of {@code RemoveCluster + AddCluster}).
 */
@Plugin(configType = Void.class)
public class FailingInitFilterFactory implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) throws PluginConfigurationException {
        throw new PluginConfigurationException("FailingInitFilterFactory deliberately fails on initialize");
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Void initializationData) {
        throw new IllegalStateException("createFilter must not be reached when initialize fails");
    }
}
