/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.net.URI;

import io.kroxylicious.systemtests.Environment;

/**
 * The interface Aws kms client.
 */
public interface AwsKmsClient {
    /**
     * Gets aws cmd.
     *
     * @return the aws cmd
     */
    String getAwsCmd();

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
     * Gets aws url.
     *
     * @return the aws url
     */
    URI getAwsUrl();

    /**
     * Gets region.
     *
     * @return the region
     */
    String getRegion();

    /**
     * Gets access key.
     *
     * @return the access key
     */
    default String getAccessKey() {
        return Environment.AWS_ACCESS_KEY_ID;
    }

    /**
     * Gets secret key.
     *
     * @return the secret key
     */
    default String getSecretKey() {
        return Environment.AWS_SECRET_ACCESS_KEY;
    }

    /**
     * Delete.
     */
    void delete();
}
