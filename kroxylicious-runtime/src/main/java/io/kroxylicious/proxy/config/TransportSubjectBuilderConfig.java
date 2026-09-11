/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.TransportSubjectBuilderService;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configures how a virtual cluster builds the client's subject from transport-level
 * information (such as a TLS client certificate).
 *
 * @param type the {@link TransportSubjectBuilderService} plugin implementation name
 * @param config optional plugin-specific configuration
 */
public record TransportSubjectBuilderConfig(@PluginImplName(TransportSubjectBuilderService.class) @JsonProperty(required = true) String type,
                                            @Nullable @PluginImplConfig(implNameProperty = "type") Object config) {}
