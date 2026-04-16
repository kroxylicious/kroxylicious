/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

/**
 * Config2 (versioned) configuration for the {@link FetchResponseTransformation} filter.
 * References a {@link ByteBufferTransformationFactory} plugin instance by name
 * rather than embedding its configuration inline.
 *
 * @param transformation name of the ByteBufferTransformationFactory plugin instance
 */
public record FetchResponseTransformationConfigV1(
                                                  @JsonProperty(required = true) String transformation)
        implements HasPluginReferences {

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return Stream.of(new PluginReference<>(ByteBufferTransformationFactory.class.getName(), transformation));
    }
}
