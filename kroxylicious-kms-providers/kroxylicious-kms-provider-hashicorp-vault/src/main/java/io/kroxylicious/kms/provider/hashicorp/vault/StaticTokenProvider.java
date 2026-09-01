/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A VaultTokenProvider that yields a static, unchanging token.
 */
public class StaticTokenProvider implements VaultTokenProvider {

    private final CompletableFuture<String> tokenFuture;

    /**
     * Creates a new StaticTokenProvider.
     *
     * @param token the static token
     */
    public StaticTokenProvider(String token) {
        Objects.requireNonNull(token);
        this.tokenFuture = CompletableFuture.completedFuture(token);
    }

    @Override
    public CompletionStage<String> getToken() {
        return tokenFuture;
    }
}
