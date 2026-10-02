/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.concurrent.CompletionStage;

/**
 * Provides Vault tokens for authenticating with the HashiCorp Vault transit engine.
 */
public interface VaultTokenProvider {

    /**
     * Retrieves a Vault token. Depending on the implementation, this may fetch a new token asynchronously.
     * @return a completion stage yielding the token
     */
    CompletionStage<String> getToken();

}
