/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.config.CredentialsConfig;
import io.kroxylicious.kms.provider.aws.kms.config.LongTermCredentialsProviderConfig;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsCloud;
import io.kroxylicious.testing.kms.aws.AbstractAwsKmsTestKmsFacade;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for AWS Kms Cloud.
 */
public class KubeAwsKmsCloudTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    private final AwsKmsCloud awsKmsCloud;

    /**
     * Instantiates a new Kube AWS Kms Cloud test kms facade.
     *
     */
    public KubeAwsKmsCloudTestKmsFacade() {
        this.awsKmsCloud = new AwsKmsCloud();
    }

    @Override
    public boolean isAvailable() {
        return awsKmsCloud.isAvailable();
    }

    @Override
    public void startKms() {
        awsKmsCloud.deploy();
    }

    @Override
    public void stopKms() {
        awsKmsCloud.delete();
    }

    @NonNull
    @Override
    public URI getAwsUrl() {
        return awsKmsCloud.getAwsKmsUrl();
    }

    @Override
    public String getRegion() {
        return awsKmsCloud.getRegion();
    }

    @Override
    public final Config getKmsServiceConfig() {
        var longTermCredentialsProviderConfig = new LongTermCredentialsProviderConfig(new InlinePassword(getKroxyliciousAccessKey()),
                new InlinePassword(getKroxyliciousSecretKey()));
        var credentials = new CredentialsConfig(longTermCredentialsProviderConfig, null, null, null);
        return new Config(getAwsUrl(), null, null, credentials, getRegion(), null);
    }

    @Override
    public String getSecretKey() {
        return awsKmsCloud.getSecretKey();
    }

    @Override
    public String getAccessKey() {
        return awsKmsCloud.getAccessKey();
    }

    private String getKroxyliciousSecretKey() {
        return awsKmsCloud.getKroxyliciousSecretKey();
    }

    private String getKroxyliciousAccessKey() {
        return awsKmsCloud.getKroxyliciousAccessKey();
    }
}
