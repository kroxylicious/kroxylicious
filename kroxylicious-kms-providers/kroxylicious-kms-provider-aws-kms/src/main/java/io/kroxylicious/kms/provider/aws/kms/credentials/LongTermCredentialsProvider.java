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

/**
 * Provider that obtains long-term {@link Credentials} that are statically defined by configuration.
 *
 * @see <a href="https://docs.aws.amazon.com/sdkref/latest/guide/access-iam-users.html">long-term credentials</a>.
 */
public class LongTermCredentialsProvider implements CredentialsProvider {
    private final CompletionStage<Credentials> credentialsCompletionStage;

    /**
     * Creates the fixed credentials provider.
     *
     * @param config config.
     */
    public LongTermCredentialsProvider(LongTermCredentialsProviderConfig config) {
        Objects.requireNonNull(config);
        var accessKeyId = config.accessKeyId().getProvidedPassword();
        var secretAccessKey = config.secretAccessKey().getProvidedPassword();
        credentialsCompletionStage = CompletableFuture.completedStage(
                new FixedCredentials(accessKeyId, secretAccessKey));
    }

    @Override
    public CompletionStage<Credentials> getCredentials() {
        return credentialsCompletionStage;
    }

    record FixedCredentials(String accessKeyId, String secretAccessKey) implements Credentials {

        FixedCredentials {
            Objects.requireNonNull(accessKeyId);
            Objects.requireNonNull(secretAccessKey);
        }

        @Override
        public String toString() {
            return "FixedCredentials{" +
                    "accessKeyId='*****'" +
                    ", secretAccessKey='*****'" +
                    '}';
        }
    }

    /**
     * Utility method for producing fixed credentials.  Used by test code.
     *
     * @param accessKeyId access key
     * @param secretAccessKey secret access key
     * @return fixed credentials.
     */
    public static Credentials fixedCredentials(String accessKeyId, String secretAccessKey) {
        Objects.requireNonNull(accessKeyId);
        Objects.requireNonNull(secretAccessKey);
        return new FixedCredentials(accessKeyId, secretAccessKey);
    }
}
