/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.Nullable;

public record RecordEncryptionConfig(@JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                                     @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                                     @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                                     @PluginImplConfig(implNameProperty = "selector") Object selectorConfig,
                                     @JsonProperty Map<String, Object> experimental,
                                     @JsonProperty UnresolvedKeyPolicy unresolvedKeyPolicy) {

    public static final int RECORD_BUFFER_DEFAULT_INITIAL_BYTES = 1024 * 1024;

    public RecordEncryptionConfig {
        experimental = experimental == null ? Map.of() : experimental;
    }

    public KmsCacheConfig kmsCache() {
        Integer decryptedDekCacheSize = getExperimentalInt("decryptedDekCacheSize");
        Long decryptedDekExpireAfterAccessSeconds = getExperimentalLong("decryptedDekExpireAfterAccessSeconds");
        Integer resolvedAliasCacheSize = getExperimentalInt("resolvedAliasCacheSize");
        Long resolvedAliasExpireAfterWriteSeconds = getExperimentalLong("resolvedAliasExpireAfterWriteSeconds");
        Long resolvedAliasRefreshAfterWriteSeconds = getExperimentalLong("resolvedAliasRefreshAfterWriteSeconds");
        Long notFoundAliasExpireAfterWriteSeconds = getExperimentalLong("notFoundAliasExpireAfterWriteSeconds");
        Long encryptionDekRefreshAfterWriteSeconds = getExperimentalLong("encryptionDekRefreshAfterWriteSeconds");
        Long encryptionDekExpireAfterWriteSeconds = getExperimentalLong("encryptionDekExpireAfterWriteSeconds");
        return new KmsCacheConfig(decryptedDekCacheSize, decryptedDekExpireAfterAccessSeconds, resolvedAliasCacheSize, resolvedAliasExpireAfterWriteSeconds,
                resolvedAliasRefreshAfterWriteSeconds, notFoundAliasExpireAfterWriteSeconds, encryptionDekRefreshAfterWriteSeconds, encryptionDekExpireAfterWriteSeconds);
    }

    public EncryptionBufferConfig encryptionBuffer() {
        Integer minimumBufSize = getExperimentalIntOrElse("encryptionBufferMinimumSizeBytes", RECORD_BUFFER_DEFAULT_INITIAL_BYTES);
        Integer maximumBufSize = getExperimentalIntOrElse("encryptionBufferMaximumSizeBytes", 1024 * 1024 * 8);
        return new EncryptionBufferConfig(minimumBufSize, maximumBufSize);
    }

    public DekManagerConfig dekManager() {
        Long maxEncryptionsPerDek = getExperimentalLong("maxEncryptionsPerDek");
        return new DekManagerConfig(maxEncryptionsPerDek);

    }

    @Nullable
    private Integer getExperimentalInt(String property) {
        return getExperimentalIntOrElse(property, null);
    }

    private Integer getExperimentalIntOrElse(String property, Integer defaultValue) {
        return Optional.ofNullable(experimental.get(property)).map(value -> {
            if (value instanceof Number number) {
                return number.intValue();
            }
            else if (value instanceof String stringValue) {
                return Integer.parseInt(stringValue);
            }
            else {
                throw new IllegalArgumentException("could not convert " + property + " with type " + value.getClass().getSimpleName() + " to Integer");
            }
        }).orElse(defaultValue);
    }

    @Nullable
    private Long getExperimentalLong(String property) {
        return Optional.ofNullable(experimental.get(property)).map(value -> {
            if (value instanceof Number number) {
                return number.longValue();
            }
            else if (value instanceof String stringValue) {
                return Long.parseLong(stringValue);
            }
            else {
                throw new IllegalArgumentException("could not convert " + property + " with type " + value.getClass().getSimpleName() + " to Integer");
            }
        }).orElse(null);
    }

    @Override
    public UnresolvedKeyPolicy unresolvedKeyPolicy() {
        return unresolvedKeyPolicy == null ? UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED : unresolvedKeyPolicy;
    }
}
