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
     * Gets AWS KMS url.
     *
     * @return the AWS KMS url
     */
    URI getAwsKmsUrl();

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
     * Gets kroxylicious access key.
     *
     * @return the kroxylicious access key
     */
    default String getKroxyliciousAccessKey() {
        return Environment.AWS_KROXYLICIOUS_ACCESS_KEY_ID;
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
     * Gets kroxylicious secret key.
     *
     * @return the kroxylicious secret key
     */
    default String getKroxyliciousSecretKey() {
        return Environment.AWS_KROXYLICIOUS_SECRET_ACCESS_KEY;
    }

    /**
     * Delete.
     */
    void delete();
}
