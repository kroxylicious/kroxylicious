/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade;
import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.provider.aws.kms.model.RotateKeyRequest;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.proxy.config.secret.InlinePassword;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsCloud;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KMS Facade for AWS Kms Cloud.
 * Uses command line interaction so to avoid the complication of exposing the AWS Cloud endpoint
 * to the test outside the cluster.
 */
public class KubeAwsKmsCloudTestKmsFacade extends AwsKmsTestKmsFacade {
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
        return new Config(getAwsUrl(), new InlinePassword(getKroxyliciousAccessKey()), new InlinePassword(getKroxyliciousSecretKey()), getRegion(), null);
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

    @Override
    public TestKekManager getTestKekManager() {
        return new AwsKmsCloudTestKekManager();
    }

    class AwsKmsCloudTestKekManager extends AwsKmsTestKmsFacade.AwsKmsTestKekManager {

        @Override
        protected void rotate(String alias) {
            var key = read(alias);
            final RotateKeyRequest rotateKey = new RotateKeyRequest(key.keyMetadata().keyId());
            var rotateKeyRequest = createRequest(rotateKey, TRENT_SERVICE_ROTATE_KEY);
            sendRequestExpectingNoResponse(rotateKeyRequest);
        }
    }
}
