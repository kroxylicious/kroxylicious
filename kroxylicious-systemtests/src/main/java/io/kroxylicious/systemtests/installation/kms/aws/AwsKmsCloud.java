/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;

/**
 * The type Aws kms cloud.
 */
public class AwsKmsCloud implements AwsKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsKmsCloud.class);
    private static final String AWS_CMD = "aws";
    private String region;

    /**
     * Instantiates a new Aws.
     *
     */
    public AwsKmsCloud() {
        // nothing to initialize
    }

    @Override
    public String getAwsCmd() {
        return AWS_CMD;
    }

    @Override
    public boolean isAvailable() {
        boolean awsCloudSelected = Environment.AWS_USE_CLOUD.equalsIgnoreCase("true");
        boolean keyIdDefaulted = Environment.AWS_ACCESS_KEY_ID.equals(Environment.AWS_ACCESS_KEY_ID_DEFAULT);
        boolean kroxyliciousKeyIdDefaulted = Environment.AWS_KROXYLICIOUS_ACCESS_KEY_ID.equals(Environment.AWS_KROXYLICIOUS_ACCESS_KEY_ID_DEFAULT);
        if (awsCloudSelected && !keyIdDefaulted && !kroxyliciousKeyIdDefaulted) {
            LOGGER.atInfo().log("Using AWS Kms Cloud");
            return true;
        }
        else {
            if (awsCloudSelected) {
                LOGGER.atWarn().log("AWS Cloud selected, but AWS_ACCESS_KEY_ID and/or AWS_KROXYLICIOUS_ACCESS_KEY_ID are not specified. Please specify a key id.");
            }
            else if (!keyIdDefaulted) {
                LOGGER.atWarn().log("AWS LocalStack selected, but AWS_ACCESS_KEY_ID is specified. Please do not specify a key id or select AWS_USE_CLOUD to true.");
            }
        }

        return false;
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
    public URI getAwsUrl() {
        return URI.create("https://kms." + getRegion() + ".amazonaws.com");
    }

    @Override
    public String getRegion() {
        if (region == null) {
            ExecResult execResult = Exec.exec(AWS_CMD, "configure", "get", "region");
            if (!execResult.isSuccess()) {
                throw new UnknownError(execResult.err());
            }
            region = execResult.out().trim();
        }

        return region;
    }
}
