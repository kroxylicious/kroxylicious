/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import java.util.Objects;

import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.LongTermCredentialsProvider;
import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Credentials provider for long-term credentials (from an accessKey / secret key tuple).
 *
 * @param accessKey AWS accessKey
 * @param secretKey AWS secretKey
 */
public record LongTermCredentialsProviderConfig(@NonNull PasswordProvider accessKey, @NonNull PasswordProvider secretKey)
        implements CredentialsProviderConfig {
    public LongTermCredentialsProviderConfig {
        Objects.requireNonNull(accessKey);
        Objects.requireNonNull(secretKey);
    }

    @Override
    public @NonNull CredentialsProvider createCredentialsProvider() {
        return new LongTermCredentialsProvider(this);
    }
}
