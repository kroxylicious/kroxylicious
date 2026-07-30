/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it.testplugins;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = RequestCountingFilterFactory.Config.class)
public class RequestCountingFilterFactory implements FilterFactory<RequestCountingFilterFactory.Config, RequestCountingFilterFactory.Config> {

    public record Config(String counterId) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public RequestCountingFilter createFilter(FilterFactoryContext context, Config configuration) {
        return new RequestCountingFilter(configuration.counterId());
    }
}
