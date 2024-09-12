/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.plugin.Plugin;

import static org.junit.jupiter.api.Assertions.fail;

@Plugin(configType = ExamplePluginInstance.Config.class)
public class ExamplePluginInstance implements ExamplePluginFactory<ExamplePluginInstance.Config> {

    @Override
    public ExamplePlugin createExamplePlugin(Config configuration) {
        fail("unexpected call");
        return null;
    }

    public record Config(String pluginKey) {
    }
}
