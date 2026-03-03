/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.azure;

import java.net.URI;

/**
 * The interface Azure kms client.
 */
public interface AzureKmsClient {
    /**
     * Is available boolean.
     *
     * @return the boolean
     */
    boolean isAvailable();

    /**
     * Deploy.
     */
    void deploy();

    /**
     * Gets Azure KMS url.
     *
     * @return the Azure KMS url
     */
    URI getDefaultVaultBaseUrl();

    /**
     * Gets endpoint authority.
     *
     * @return  the endpoint authority
     */
    String getEndpointAuthority();

    /**
     * Delete.
     */
    void delete();
}
