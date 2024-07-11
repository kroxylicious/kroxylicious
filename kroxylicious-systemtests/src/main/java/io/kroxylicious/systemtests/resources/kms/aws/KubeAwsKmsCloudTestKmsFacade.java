/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.time.Duration;
import java.util.List;

import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsCloud;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static io.kroxylicious.kms.provider.aws.kms.AwsKms.ALIAS_PREFIX;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.CREATE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.DESCRIBE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF;

/**
 * KMS Facade for AWS Kms Cloud.
 * Uses command line interaction so to avoid the complication of exposing the AWS Cloud endpoint
 * to the test outside the cluster.
 */
public class KubeAwsKmsCloudTestKmsFacade extends AbstractKubeAwsKmsTestKmsFacade {
    private final String awsCmd;

    /**
     * Instantiates a new Kube AWS Kms Cloud test kms facade.
     *
     */
    public KubeAwsKmsCloudTestKmsFacade() {
        this.awsKmsClient = new AwsKmsCloud();
        awsCmd = awsKmsClient.getAwsCmd();
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new AwsKmsCloudTestKekManager();
    }

    class AwsKmsCloudTestKekManager extends AbstractAwsKmsTestKekManager {
        @Override
        DescribeKeyResponse read(String alias) {
            return runAwsKmsCommand(DESCRIBE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, DESCRIBE_KEY, PARAM_KEY_ID, ALIAS_PREFIX + alias);
        }

        @Override
        void create(String alias) {
            var createKeyResponse = runAwsKmsCommand(CREATE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, CREATE);
            runAwsKmsCommand(awsCmd, KMS, CREATE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias, PARAM_TARGET_KEY_ID, createKeyResponse.keyMetadata().keyId());
        }

        @Override
        void rotate(String alias, String keyId) {
            runAwsKmsCommand(awsCmd, KMS, ROTATE, PARAM_KEY_ID, keyId);
        }

        @Override
        void delete(String alias, String keyId) {
            runAwsKmsCommand(SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF,
                    awsCmd, KMS, SCHEDULE_KEY_DELETION, PARAM_KEY_ID, keyId, PARAM_PENDING_WINDOW_IN_DAYS, "7" /* Minimum allowed */);

            runAwsKmsCommand(awsCmd, KMS, DELETE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias);
        }

        @Override
        ExecResult runAwsKmsCommand(String... command) {
            ExecResult execResult = Exec.exec(null, List.of(command), Duration.ofSeconds(20), true, false, null);

            if (!execResult.isSuccess()) {
                throw new KubeClusterException("Failed to run AWS Kms: %s, exit code: %d, stderr: %s".formatted(List.of(command),
                        execResult.returnCode(), execResult.err()));
            }

            return execResult;
        }
    }
}
