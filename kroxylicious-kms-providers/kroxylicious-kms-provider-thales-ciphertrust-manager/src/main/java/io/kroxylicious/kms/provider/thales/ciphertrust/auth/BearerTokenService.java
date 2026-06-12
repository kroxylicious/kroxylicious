/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust.auth;

import java.util.concurrent.CompletionStage;

/**
 * Service for obtaining bearer tokens
 */
public interface BearerTokenService extends AutoCloseable {
    /**
     * Get a bearer token
     * @return stage which will be completed with a BearerToken or failed if a token cannot be acquired
     */
    CompletionStage<BearerToken> getBearerToken();

    @Override
    void close();
}
