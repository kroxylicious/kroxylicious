/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 *
 * Configuration providing long-term, fixed, credentials.
 *
 * @param accessKeyId AWS accessKeyId
 * @param secretAccessKey AWS secretAccessKey
 *
 * @see <a href="https://docs.aws.amazon.com/sdkref/latest/guide/access-iam-users.html">long-term credentials</a>.
 */
public record LongTermCredentialsProviderConfig(@JsonProperty(value = "accessKeyId", required = true) @NonNull PasswordProvider accessKeyId,
                                                @JsonProperty(value = "secretAccessKey", required = true) @NonNull PasswordProvider secretAccessKey) {}
