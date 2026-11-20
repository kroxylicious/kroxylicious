/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.filterspecs;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Experimental configuration options for record encryption KMS caching behavior.
 *
 * @param resolvedAliasExpireAfterWriteSeconds   cache expiry for resolved aliases, or {@code null} for default
 * @param resolvedAliasRefreshAfterWriteSeconds  cache refresh for resolved aliases, or {@code null} for default
 * @param encryptionDekRefreshAfterWriteSeconds  cache refresh for encryption DEKs, or {@code null} for default
 * @param encryptionDekExpireAfterWriteSeconds   cache expiry for encryption DEKs, or {@code null} for default
 */
public record ExperimentalKmsConfig(
                                    @Nullable Long resolvedAliasExpireAfterWriteSeconds,
                                    @Nullable Long resolvedAliasRefreshAfterWriteSeconds,
                                    @Nullable Long encryptionDekRefreshAfterWriteSeconds,
                                    @Nullable Long encryptionDekExpireAfterWriteSeconds) {}