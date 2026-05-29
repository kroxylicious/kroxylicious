/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.testplugins.v2;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.Version;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Version 2 of test plugin for demonstrating version-based disambiguation.
 * Uses an incompatible integer configuration instead of string.
 */
@Version("v2")
@Plugin(configType = VersionedTestPlugin.ConfigV2.class)
public class VersionedTestPlugin implements FilterFactory<VersionedTestPlugin.ConfigV2, VersionedTestPlugin.ConfigV2> {

    @Override
    public ConfigV2 initialize(FilterFactoryContext context, ConfigV2 config) {
        return Plugins.requireConfig(this, config);
    }

    @NonNull
    @Override
    public VersionedTestPluginFilter createFilter(FilterFactoryContext context, ConfigV2 configuration) {
        return new VersionedTestPluginFilter(configuration.tagValue(), configuration.tag);
    }

    public record ConfigV2(@JsonProperty(required = true) int tagValue, @JsonProperty(required = true) int tag) {}
}
