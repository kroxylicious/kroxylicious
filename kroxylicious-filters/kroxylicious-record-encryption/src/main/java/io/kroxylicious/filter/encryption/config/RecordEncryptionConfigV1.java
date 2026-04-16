/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Config2 (versioned) configuration for the {@link io.kroxylicious.filter.encryption.RecordEncryption} filter.
 * References {@link KmsService} and {@link KekSelectorService} plugin instances by name
 * rather than embedding their configuration inline.
 *
 * @param kms name of the KmsService plugin instance
 * @param selector name of the KekSelectorService plugin instance
 * @param experimental experimental tuning parameters (cache sizes, timeouts, buffer sizes)
 * @param unresolvedKeyPolicy what to do when a key cannot be resolved for a record
 */
public record RecordEncryptionConfigV1(
                                       @JsonProperty(required = true) String kms,
                                       @JsonProperty(required = true) String selector,
                                       @Nullable @JsonProperty Map<String, Object> experimental,
                                       @Nullable @JsonProperty UnresolvedKeyPolicy unresolvedKeyPolicy)
        implements HasPluginReferences {

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return Stream.of(
                new PluginReference<>(KmsService.class.getName(), kms),
                new PluginReference<>(KekSelectorService.class.getName(), selector));
    }
}
