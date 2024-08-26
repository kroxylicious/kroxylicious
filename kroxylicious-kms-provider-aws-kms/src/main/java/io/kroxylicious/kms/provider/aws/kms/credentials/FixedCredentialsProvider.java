/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.kms.provider.aws.kms.config.FixedCredentialsProviderConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provider that obtains fixed {@link Credentials} that are statically defined by configuration.
 *
 * @see <a href="https://docs.aws.amazon.com/sdkref/latest/guide/access-iam-users.html">long term credentials</a>.
 */
public class FixedCredentialsProvider implements CredentialsProvider {
    private final CompletionStage<Credentials> credentialsCompletionStage;

    /**
     * Creates the fixed credentials provider.
     *
     * @param config config.
     */
    public FixedCredentialsProvider(@NonNull FixedCredentialsProviderConfig config) {
        Objects.requireNonNull(config);
        var accessKeyId = config.accessKeyId().getProvidedPassword();
        var secretAccessKey = config.secretAccessKey().getProvidedPassword();
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
