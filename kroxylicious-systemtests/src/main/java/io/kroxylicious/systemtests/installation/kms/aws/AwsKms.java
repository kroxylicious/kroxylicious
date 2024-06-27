/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kms.aws;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;

/**
 * The type Aws kms local.
 */
public class AwsKms implements AwsKmsClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsKms.class);
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
        return true;
        // try (var output = new ByteArrayOutputStream();
        // var exec = kubeClient().getClient().pods()
        // .inNamespace(deploymentNamespace)
        // .withName(podName)
        // .writingOutput(output)
        // .exec("sh", "-c", AWS_LOCAL_CMD + " --version")) {
        // int exitCode = exec.exitCode().join();
        // return exitCode == 0 &&
        // output.toString().toLowerCase().contains("aws-cli/");
        // }
        // catch (IOException e) {
        // throw new UncheckedIOException(e);
        // }
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
    public String getAwsUrl() {
        return "kms." + getRegion() + ".amazonaws.com";
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
