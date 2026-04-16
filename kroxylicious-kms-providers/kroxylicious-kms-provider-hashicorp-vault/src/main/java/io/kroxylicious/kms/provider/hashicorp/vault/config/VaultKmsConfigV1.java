/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.net.URI;
import java.util.Objects;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.PluginReference;
import io.kroxylicious.proxy.tls.KeyMaterialProvider;
import io.kroxylicious.proxy.tls.TrustMaterialProvider;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Config2 (versioned) configuration for the Vault KMS service.
 * References {@link KeyMaterialProvider} and {@link TrustMaterialProvider} plugin
 * instances by name rather than embedding TLS configuration inline.
 *
 * @param vaultTransitEngineUrl URL of the Vault Transit Engine
 * @param vaultToken the Vault authentication token
 * @param keyMaterial name of the KeyMaterialProvider plugin instance, or null if not using client TLS
 * @param trustMaterial name of the TrustMaterialProvider plugin instance, or null if using system trust
 */
public record VaultKmsConfigV1(
                               @JsonProperty(value = "vaultTransitEngineUrl", required = true) URI vaultTransitEngineUrl,
                               @JsonProperty(required = true) String vaultToken,
                               @Nullable String keyMaterial,
                               @Nullable String trustMaterial)
        implements HasPluginReferences {

    public VaultKmsConfigV1 {
        Objects.requireNonNull(vaultTransitEngineUrl);
        Objects.requireNonNull(vaultToken);
    }

    @Override
    public Stream<PluginReference<?>> pluginReferences() {
        return Stream.<PluginReference<?>> of(
                keyMaterial == null ? null
                        : new PluginReference<>(KeyMaterialProvider.class.getName(), keyMaterial),
                trustMaterial == null ? null
                        : new PluginReference<>(TrustMaterialProvider.class.getName(), trustMaterial))
                .filter(Objects::nonNull);
    }
}
