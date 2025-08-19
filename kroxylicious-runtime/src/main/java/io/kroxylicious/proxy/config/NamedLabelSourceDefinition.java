/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.internal.util.Assertions;
import io.kroxylicious.proxy.labels.source.LabelSourceFactory;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A named label source definition
 * @param name The name of the label source
 * @param type The type of the label source
 * @param config Config for the label source
 * @see Configuration#filterDefinitions()
 */
public record NamedLabelSourceDefinition(
                                         @JsonProperty(required = true) String name,
                                         @PluginImplName(LabelSourceFactory.class) @JsonProperty(required = true) String type,
                                         @Nullable @PluginImplConfig(implNameProperty = "type") Object config) {

    @JsonCreator
    public NamedLabelSourceDefinition {
        Objects.requireNonNull(name);
        Assertions.requireStrictlyPositive(name.length(), "name length");
        Objects.requireNonNull(type);
    }

}
