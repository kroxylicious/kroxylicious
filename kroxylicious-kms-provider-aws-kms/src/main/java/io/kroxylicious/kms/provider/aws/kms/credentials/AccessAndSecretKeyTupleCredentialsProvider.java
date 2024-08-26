/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kms.provider.aws.kms.config.AccessAndSecretKeyTupleCredentialsProviderConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

public class AccessAndSecretKeyTupleCredentialsProvider implements CredentialsProvider {
    private final AccessAndSecretKeyTupleCredentialsProviderConfig config;

    public AccessAndSecretKeyTupleCredentialsProvider(@NonNull AccessAndSecretKeyTupleCredentialsProviderConfig config) {
        this.config = config;
    }

    @NonNull
    @Override
    public CompletionStage<Credentials> getCredentials() {
        var value = new Credentials() {

            @NonNull
            @Override
            public String accessKeyId() {
                return config.accessKey().getProvidedPassword();
            }

            @NonNull
            @Override
            public String secretAccessKey() {
                return config.secretKey().getProvidedPassword();
            }
        };

        return CompletableFuture.completedStage(value);
    }
}
