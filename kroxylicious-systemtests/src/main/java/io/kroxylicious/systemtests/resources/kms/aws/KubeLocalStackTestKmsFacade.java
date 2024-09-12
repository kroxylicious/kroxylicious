/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacade;
import io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade;
import io.kroxylicious.systemtests.installation.kms.aws.LocalStack;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.utils.VersionComparator;

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
        String installedVersion = getLocalStackVersion();
        String expectedVersion = AwsKmsTestKmsFacade.LOCALSTACK_IMAGE.getVersionPart();
        if (!isCorrectVersionInstalled(installedVersion, expectedVersion)) {
            throw new KubeClusterException(
                    "LocalStack version installed "
                                           + installedVersion
                                           + " does not match with the expected: '"
                                           + expectedVersion
                                           + "'"
            );
        }
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

    /**
     * Gets LocalStack version.
     *
     * @return the LocalStack version
     */
    public String getLocalStackVersion() {
        return localStack.getLocalStackVersionInstalled();
    }

    private boolean isCorrectVersionInstalled(String installedVersion, String expectedVersion) {
        VersionComparator comparator = new VersionComparator(installedVersion);
        return comparator.compareTo(expectedVersion) == 0;
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
