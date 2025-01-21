/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacade;
import io.kroxylicious.systemtests.installation.kms.aws.LocalStack;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for AWS Kms running inside Kube (localstack).
 */
public class KubeLocalStackTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    private final LocalStack localStack;

    /**
     * Instantiates a new Kube LocalStack test kms facade.
     *
     */
    public KubeLocalStackTestKmsFacade() {
        this.localStack = new LocalStack();
    }

    @Override
    public boolean isAvailable() {
        return localStack.isAvailable();
    }

    @Override
    public void startKms() {
        localStack.deploy();
    }

    @Override
    public void stopKms() {
        localStack.delete();
    }

    @NonNull
    @Override
    protected URI getAwsUrl() {
        return localStack.getAwsKmsUrl();
    }

    @Override
    protected String getRegion() {
        return localStack.getRegion();
    }

    @Override
    protected String getSecretKey() {
        return localStack.getSecretKey();
    }

    @Override
    protected String getAccessKey() {
        return localStack.getAccessKey();
    }
}
