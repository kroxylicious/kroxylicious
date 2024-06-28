/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import java.net.URI;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;

/**
 * The type Aws kms cloud.
 */
public class AwsKms implements AwsKmsClient {
    private static final String AWS_CMD = "aws";

    /**
     * Instantiates a new Aws.
     *
     */
    public AwsKms() {
        // nothing to initialize
    }

    @Override
    public String getAwsCmd() {
        return AWS_CMD;
    }

    @Override
    public boolean isAvailable() {
        ExecResult execResult = Exec.exec(AWS_CMD, "--version");
        return execResult.isSuccess();
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
        ExecResult execResult = Exec.exec(AWS_CMD, "configure", "get", "region");
        if (!execResult.isSuccess()) {
            throw new UnsupportedOperationException(execResult.err());
        }
        return execResult.out().trim();
    }
}
