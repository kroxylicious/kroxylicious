/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.AbstractAwsKmsTestKmsFacade;
import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKms;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsClient;
import io.kroxylicious.systemtests.installation.kms.aws.AwsKmsLocal;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kms.provider.aws.kms.AwsKms.ALIAS_PREFIX;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.CREATE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.DESCRIBE_KEY_RESPONSE_TYPE_REF;
import static io.kroxylicious.kms.provider.aws.kms.AwsKmsTestKmsFacade.SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;

/**
 * KMS Facade for AWS Kms running inside Kube.
 * Uses command line interaction so to avoid the complication of exposing the AWS endpoint
 * to the test outside the cluster.
 */
public class KubeAwsKmsTestKmsFacade extends AbstractAwsKmsTestKmsFacade {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KMS = "kms";
    private static final String CREATE = "create-key";
    private static final String CREATE_ALIAS = "create-alias";
    private static final String UPDATE_ALIAS = "update-alias";
    private static final String DELETE_ALIAS = "delete-alias";
    private static final String DESCRIBE_KEY = "describe-key";
    private static final String SCHEDULE_KEY_DELETION = "schedule-key-deletion";
    private static final String PARAM_ALIAS_NAME = "--alias-name";
    private static final String PARAM_TARGET_KEY_ID = "--target-key-id";
    private static final String PARAM_KEY_ID = "--key-id";
    private static final String PARAM_PENDING_WINDOW_IN_DAYS = "--pending-window-in-days";
    private final String namespace;
    private final AwsKmsClient awsKmsClient;
    private String kekKeyId;
    private final String awsCmd;

    /**
     * Instantiates a new Kube AWS Kms test kms facade.
     *
     */
    public KubeAwsKmsTestKmsFacade() {
        this.namespace = AwsKmsLocal.LOCALSTACK_DEFAULT_NAMESPACE;
        this.awsKmsClient = getAwsKmsClient();
        awsCmd = this.awsKmsClient.getAwsCmd();
    }

    private AwsKmsClient getAwsKmsClient() {
        if (Environment.AWS_ACCESS_KEY_ID.equals(Environment.AWS_ACCESS_KEY_ID_DEFAULT)) {
            return new AwsKmsLocal();
        }
        else {
            return new AwsKms();
        }
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
    protected String getSecretKey() {
        return awsKmsClient.getSecretKey();
    }

    @Override
    protected String getAccessKey() {
        return awsKmsClient.getAccessKey();
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new AwsKmsTestKekManager();
    }

    class AwsKmsTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                throw new AlreadyExistsException(alias);
            }
            else {
                create(alias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (!exists(alias)) {
                throw new UnknownAliasException(alias);
            }
            else {
                rotate(alias);
            }
        }

        @Override
        public void deleteKek(String alias) {
            if (!exists(alias)) {
                throw new UnknownAliasException(alias);
            }
            else {
                delete(alias);
            }
        }

        @Override
        public boolean exists(String alias) {
            try {
                read(alias);
                return true;
            }
            catch (KubeClusterException nfe) {
                return false;
            }
        }

        private void create(String alias) {
            var createKeyResponse = runAwsKmsCommand(CREATE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, CREATE);
            kekKeyId = createKeyResponse.keyMetadata().keyId();

            runAwsKmsCommand(awsCmd, KMS, CREATE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias, PARAM_TARGET_KEY_ID, kekKeyId);
        }

        private DescribeKeyResponse read(String alias) {
            return runAwsKmsCommand(DESCRIBE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, DESCRIBE_KEY, PARAM_KEY_ID, ALIAS_PREFIX + alias);
        }

        private void rotate(String alias) {
            // RotateKeyOnDemand is not implemented in localstack.
            // https://docs.localstack.cloud/references/coverage/coverage_kms/#:~:text=Show%20Tests-,RotateKeyOnDemand,-ScheduleKeyDeletion
            // https://github.com/localstack/localstack/issues/10723
            var createKeyResponse = runAwsKmsCommand(CREATE_KEY_RESPONSE_TYPE_REF, awsCmd, KMS, CREATE);
            kekKeyId = createKeyResponse.keyMetadata().keyId();

            runAwsKmsCommand(awsCmd, KMS, UPDATE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias, PARAM_TARGET_KEY_ID, kekKeyId);
        }

        private void delete(String alias) {
            var key = read(alias);
            var keyId = key.keyMetadata().keyId();
            runAwsKmsCommand(SCHEDULE_KEY_DELETION_RESPONSE_TYPE_REF,
                    awsCmd, KMS, SCHEDULE_KEY_DELETION, PARAM_KEY_ID, keyId, PARAM_PENDING_WINDOW_IN_DAYS, "7" /* Minimum allowed */);

            runAwsKmsCommand(awsCmd, KMS, DELETE_ALIAS, PARAM_ALIAS_NAME, ALIAS_PREFIX + alias);
        }

        private <T> T runAwsKmsCommand(TypeReference<T> valueTypeRef, String... command) {
            try {
                var execResult = runAwsKmsCommand(command);
                return OBJECT_MAPPER.readValue(execResult.out(), valueTypeRef);
            }
            catch (IOException e) {
                throw new KubeClusterException("Failed to run AWS Kms command: %s".formatted(List.of(command)), e);
            }
        }

        private ExecResult runAwsKmsCommand(String... command) {
            ExecResult execResult;
            if (awsKmsClient.getClass().equals(AwsKms.class)) {
                execResult = Exec.exec(null, List.of(command), Duration.ofSeconds(20), true, false, null);
                ;
            }
            else {
                execResult = cmdKubeClient(namespace).execInPod(((AwsKmsLocal) awsKmsClient).getPodName(), true, command);
            }

            if (!execResult.isSuccess()) {
                throw new KubeClusterException("Failed to run AWS Kms: %s, exit code: %d, stderr: %s".formatted(List.of(command),
                        execResult.returnCode(), execResult.err()));
            }

            return execResult;
        }
    }
}
