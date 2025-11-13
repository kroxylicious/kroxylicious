/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.net.URI;

import io.kroxylicious.systemtests.Environment;

/**
 * The type Aws kms cloud.
 */
public class AwsKmsCloud implements AwsKmsClient {

    /**
     * Instantiates a new Aws.
     *
     */
    public AwsKmsCloud() {
        // nothing to do
    }

    @Override
    public boolean isAvailable() {
        return Environment.USE_CLOUD_KMS.equalsIgnoreCase("true");
    }

    @Override
    public void deploy() {
        // nothing to deploy
    }

    @Override
    public void delete() {
        // nothing to delete
    }

    @Override
    public URI getAwsKmsUrl() {
        return URI.create("https://kms." + getRegion() + ".amazonaws.com");
    }

    @Override
    public String getRegion() {
        return Environment.AWS_REGION;
    }
}
