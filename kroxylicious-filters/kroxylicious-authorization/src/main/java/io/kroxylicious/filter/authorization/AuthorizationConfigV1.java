/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

/**
 * Config2 (versioned) configuration for the {@link Authorization} filter.
 * References an {@link AuthorizerService} plugin instance by name rather
 * than embedding its configuration inline.
 *
 * @param authorizer name of the AuthorizerService plugin instance
 */
public record AuthorizationConfigV1(
                                    @JsonProperty(required = true) String authorizer)
        implements HasPluginReferences {

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return Stream.of(new PluginReference<>(AuthorizerService.class.getName(), authorizer));
    }
}
