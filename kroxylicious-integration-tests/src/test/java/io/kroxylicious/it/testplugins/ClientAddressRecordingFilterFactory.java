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

@Plugin(configType = ClientAddressRecordingFilterFactory.Config.class)
public class ClientAddressRecordingFilterFactory implements FilterFactory<ClientAddressRecordingFilterFactory.Config, ClientAddressRecordingFilterFactory.Config> {

    public record Config(String recorderId) {}

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public ClientAddressRecordingFilter createFilter(FilterFactoryContext context, Config configuration) {
        return new ClientAddressRecordingFilter(configuration.recorderId());
    }
}
