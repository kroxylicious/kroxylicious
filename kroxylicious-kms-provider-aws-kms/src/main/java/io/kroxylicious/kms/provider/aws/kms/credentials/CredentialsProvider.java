/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms.credentials;

import java.io.Closeable;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * AWS Credentials Provider
 */
public interface CredentialsProvider extends Closeable {

    /**
     * Gets the current credentials.
     *
     * @return AWS credentials.
     */
    @NonNull
    CompletionStage<? extends Credentials> getCredentials();

    default void close() {
    }
}
