/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.util.List;

import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kms.aws.LocalStack;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static io.kroxylicious.kms.provider.aws.kms.AwsKms.ALIAS_PREFIX;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.CREATE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.DESCRIBE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;

/**
 * KMS Facade for AWS Kms running inside Kube (localstack).
 * Uses command line interaction so to avoid the complication of exposing the AWS Local endpoint
 * to the test outside the cluster.
 */
public class KubeLocalStackTestKmsFacade extends AbstractKubeAwsKmsTestKmsFacade {
    private final String namespace;
    private final String awsCmd;
    private String kekKeyId;

    /**
     * Instantiates a new Kube AWS Kms test kms facade.
     *
     */
    public KubeLocalStackTestKmsFacade() {
        this.namespace = LocalStack.LOCALSTACK_DEFAULT_NAMESPACE;
        this.awsKmsClient = new LocalStack();
        awsCmd = awsKmsClient.getAwsCmd();
    }

    /**
     * Gets kek key id.
     *
     * @return the kek key id
     */
    public String getKekKeyId() {
        return kekKeyId;
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new LocalStackTestKekManager();
    }

    class LocalStackTestKekManager extends AbstractAwsKmsTestKekManager {

        @Override
        void create(String alias) {
            var createKeyResponse = runAwsKmsCommand(CREATE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, CREATE);
            kekKeyId = createKeyResponse.keyMetadata().keyId();

            runAwsKmsCommand(awsCmd, KMS, CREATE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias, PARAM_TARGET_KEY_ID, kekKeyId);
        }

        @Override
        DescribeKeyResponse read(String alias) {
            return runAwsKmsCommand(DESCRIBE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, DESCRIBE_KEY, PARAM_KEY_ID, ALIAS_PREFIX + alias);
        }

        @Override
        void rotate(String alias) {
            // RotateKeyOnDemand is not implemented in localstack.
            // https://docs.localstack.cloud/references/coverage/coverage_kms/#:~:text=Show%20Tests-,RotateKeyOnDemand,-ScheduleKeyDeletion
            // https://github.com/localstack/localstack/issues/10723
            var createKeyResponse = runAwsKmsCommand(CREATE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, CREATE);
            kekKeyId = createKeyResponse.keyMetadata().keyId();

            runAwsKmsCommand(awsCmd, KMS, UPDATE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias, PARAM_TARGET_KEY_ID, kekKeyId);
        }

        @Override
        void delete(String alias) {
            var key = read(alias);
            var keyId = key.keyMetadata().keyId();
            runAwsKmsCommand(SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF,
                    awsCmd, KMS, SCHEDULE_KEY_DELETION, PARAM_KEY_ID, keyId, PARAM_PENDING_WINDOW_IN_DAYS, "7" /* Minimum allowed */);

            runAwsKmsCommand(awsCmd, KMS, DELETE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias);
        }

        @Override
        ExecResult runAwsKmsCommand(String... command) {
            ExecResult execResult = cmdKubeClient(namespace).execInPod(((LocalStack) awsKmsClient).getPodName(), true, command);

            if (!execResult.isSuccess()) {
                throw new KubeClusterException("Failed to run AWS Kms: %s, exit code: %d, stderr: %s".formatted(List.of(command),
                        execResult.returnCode(), execResult.err()));
            }

            return execResult;
        }
    }
}
