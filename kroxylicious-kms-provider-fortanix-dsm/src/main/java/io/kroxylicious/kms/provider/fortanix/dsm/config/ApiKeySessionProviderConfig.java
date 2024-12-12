/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Api Key authenticated session provider.
 *
 * @param apiKey Fortanix API Key
 * @param sessionLifetimeFactor  the factor applied to determine how long until a session is preemptively refreshed
 *
 */
public record ApiKeySessionProviderConfig(@JsonProperty(value = "apiKey", required = true) @NonNull PasswordProvider apiKey,
                                          @JsonProperty(value = "sessionLifetimeFactor", required = false) Double sessionLifetimeFactor) {}
