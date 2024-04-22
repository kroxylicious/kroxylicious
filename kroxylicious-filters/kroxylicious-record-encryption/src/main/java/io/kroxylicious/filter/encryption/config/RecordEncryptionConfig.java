/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.Nullable;

public record RecordEncryptionConfig(
                                     @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                                     @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                                     @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                                     @PluginImplConfig(implNameProperty = "selector") Object selectorConfig,
                                     @JsonProperty Map<String, Object> experimental) {
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
        return new KmsCacheConfig(decryptedDekCacheSize, decryptedDekExpireAfterAccessSeconds, resolvedAliasCacheSize,
                resolvedAliasExpireAfterWriteSeconds,
                resolvedAliasRefreshAfterWriteSeconds, notFoundAliasExpireAfterWriteSeconds);
    }

    @Nullable
    private Integer getExperimentalInt(String property) {
        if (experimental.containsKey(property)) {
            Object value = experimental.get(property);
            if (value instanceof Number number) {
                return number.intValue();
            }
        }
        return null;
    }

    @Nullable
    private Long getExperimentalLong(String property) {
        if (experimental.containsKey(property)) {
            Object value = experimental.get(property);
            if (value instanceof Number number) {
                return number.longValue();
            }
        }
        return null;
    }

}
