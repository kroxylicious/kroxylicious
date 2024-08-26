/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents AWS <a href="https://docs.aws.amazon.com/sdkref/latest/guide/access-iam-users.html">long-term credentials</a>.
 */
public class LongTermCredentialsProvider implements CredentialsProvider {
    private final CompletionStage<Credentials> credentialsCompletionStage;

    public LongTermCredentialsProvider(@NonNull LongTermCredentialsProviderConfig config) {
        Objects.requireNonNull(config);
        var accessKeyId = config.accessKey().getProvidedPassword();
        var secretAccessKey = config.secretKey().getProvidedPassword();
        credentialsCompletionStage = CompletableFuture.completedStage(
                new LongTermCredentials(accessKeyId, secretAccessKey));
    }

    @NonNull
    @Override
    public CompletionStage<Credentials> getCredentials() {
        return credentialsCompletionStage;
    }

    record LongTermCredentials(String accessKeyId, String secretAccessKey) implements Credentials {
        LongTermCredentials {
            Objects.requireNonNull(accessKeyId);
            Objects.requireNonNull(secretAccessKey);
        }
    }
}
