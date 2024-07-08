/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacade;
import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsClient;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for AWS Kms running inside Kube.
 * Uses command line interaction so to avoid the complication of exposing the AWS endpoint
 * to the test outside the cluster.
 */
public abstract class AbstractKubeAwsKmsTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    protected static final String KMS = "kms";
    protected static final String CREATE = "create-key";
    protected static final String ROTATE = "rotate-key-on-demand";
    protected static final String CREATE_ALIAS = "create-alias";
    protected static final String UPDATE_ALIAS = "update-alias";
    protected static final String DELETE_ALIAS = "delete-alias";
    protected static final String DESCRIBE_KEY = "describe-key";
    protected static final String SCHEDULE_KEY_DELETION = "schedule-key-deletion";
    protected static final String PARAM_ALIAS_NAME = "--alias-name";
    protected static final String PARAM_TARGET_KEY_ID = "--target-key-id";
    protected static final String PARAM_KEY_ID = "--key-id";
    protected static final String PARAM_PENDING_WINDOW_IN_DAYS = "--pending-window-in-days";
    protected AwsKmsClient awsKmsClient;

    /**
     * Gets kek key id.
     *
     * @return the kek key id
     */
    public abstract String getKekKeyId();

    @Override
    public boolean isAvailable() {
        return awsKmsClient.isAvailable();
    }

    @Override
    public void startKms() {
        awsKmsClient.deploy();
    }

    @Override
    public void stopKms() {
        awsKmsClient.delete();
    }

    @NonNull
    @Override
    protected URI getAwsUrl() {
        return awsKmsClient.getAwsUrl();
    }

    @Override
    protected String getRegion() {
        return awsKmsClient.getRegion();
    }

    @Override
    public final Config getKmsServiceConfig() {
        return new Config(getAwsUrl(), new InlinePassword(getKroxyliciousAccessKey()), new InlinePassword(getKroxyliciousSecretKey()), getRegion(), null);
    }

    @Override
    protected String getSecretKey() {
        return awsKmsClient.getSecretKey();
    }

    @Override
    protected String getAccessKey() {
        return awsKmsClient.getAccessKey();
    }

    protected String getKroxyliciousSecretKey() {
        return awsKmsClient.getKroxyliciousSecretKey();
    }

    protected String getKroxyliciousAccessKey() {
        return awsKmsClient.getKroxyliciousAccessKey();
    }

    @Override
    public TestKekManager getTestKekManager() {
        throw new UnsupportedOperationException();
    }
}
