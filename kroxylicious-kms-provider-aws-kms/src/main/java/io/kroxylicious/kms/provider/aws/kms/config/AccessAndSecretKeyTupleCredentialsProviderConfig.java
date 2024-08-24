/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.config;

import io.kroxylicious.kms.provider.aws.kms.credentials.AccessAndSecretKeyTupleCredentialsProvider;
import io.kroxylicious.kms.provider.aws.kms.credentials.CredentialsProvider;
import io.kroxylicious.proxy.config.secret.PasswordProvider;

/**
 * Credentials provided from an accessKey / secret key tuple.
 *
 * @param accessKey AWS accessKey
 * @param secretKey AWS secretKey
 */
public record AccessAndSecretKeyTupleCredentialsProviderConfig(PasswordProvider accessKey, PasswordProvider secretKey)
        implements CredentialsProviderConfig {
    @Override
    public CredentialsProvider createCredentialsProvider() {
        return new AccessAndSecretKeyTupleCredentialsProvider(this);
    }
}
