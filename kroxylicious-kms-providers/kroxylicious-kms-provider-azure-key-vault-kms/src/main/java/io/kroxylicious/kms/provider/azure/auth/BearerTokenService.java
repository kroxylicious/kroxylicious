/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.util.concurrent.CompletionStage;

/**
 * Service for Obtaining Bearer Tokens. Designed to work with Microsoft Entra
 */
public interface BearerTokenService extends AutoCloseable {
    /**
     * Get a Bearer Token
     * @return stage which will be completed with a BearerToken or failed if a token cannot be acquired
     */
    CompletionStage<BearerToken> getBearerToken();

    void close();
}
