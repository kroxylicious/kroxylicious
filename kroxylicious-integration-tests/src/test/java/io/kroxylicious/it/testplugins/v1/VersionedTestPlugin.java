/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins.v1;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.Version;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Version 1 of test plugin for demonstrating version-based disambiguation.
 * Uses a simple string configuration.
 */
@Version("v1")
@Plugin(configType = VersionedTestPlugin.ConfigV1.class)
public class VersionedTestPlugin implements FilterFactory<VersionedTestPlugin.ConfigV1, VersionedTestPlugin.ConfigV1> {

    @Override
    public ConfigV1 initialize(FilterFactoryContext context, ConfigV1 config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public VersionedTestPluginFilter createFilter(FilterFactoryContext context, ConfigV1 configuration) {
        return new VersionedTestPluginFilter(configuration.message(), configuration.tag);
    }

    public record ConfigV1(@JsonProperty(required = true) String message, @JsonProperty(required = true) int tag) {}
}
