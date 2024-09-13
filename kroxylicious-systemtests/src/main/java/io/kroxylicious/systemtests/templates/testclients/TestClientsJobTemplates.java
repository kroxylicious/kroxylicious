/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.templates.ContainerTemplates;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The type Test Clients job templates.
 */
public class TestClientsJobTemplates {
    private static final String BOOTSTRAP_VAR = "BOOTSTRAP_SERVERS";
    private static final String TOPIC_VAR = "TOPIC";
    private static final String MESSAGE_VAR = "MESSAGE";
    private static final String MESSAGE_KEY_VAR = "MESSAGE_KEY";
    private static final String MESSAGE_COUNT_VAR = "MESSAGE_COUNT";
    private static final String GROUP_ID_VAR = "GROUP_ID";
    private static final String LOG_LEVEL_VAR = "LOG_LEVEL";
    private static final String CLIENT_TYPE_VAR = "CLIENT_TYPE";
    private static final String PRODUCER_ACKS_VAR = "PRODUCER_ACKS";
    private static final String DELAY_MS_VAR = "DELAY_MS";
    private static final String OUTPUT_FORMAT_VAR = "OUTPUT_FORMAT";

    private TestClientsJobTemplates() {
    }

    private static JobBuilder baseClientJob(String jobName) {
        Map<String, String> labelSelector = Map.of("app", jobName);
        return new JobBuilder()
                               .withApiVersion("batch/v1")
                               .withKind(Constants.JOB)
                               .withNewMetadata()
                               .withName(jobName)
                               .addToLabels(labelSelector)
                               .endMetadata()
                               .withNewSpec()
                               .withBackoffLimit(0)
                               .withCompletions(1)
                               .withParallelism(1)
                               .withNewTemplate()
                               .withNewMetadata()
                               .withLabels(labelSelector)
                               .withName(jobName)
                               .endMetadata()
                               .withNewSpec()
                               .withRestartPolicy(Constants.RESTART_POLICY_NEVER)
                               .endSpec()
                               .endTemplate()
                               .endSpec();
    }

    /**
     * Default admin client job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return the deployment builder
     */
    public static JobBuilder defaultAdminClientJob(String jobName, List<String> args) {
        return baseClientJob(jobName)
                                     .editSpec()
                                     .editTemplate()
                                     .editSpec()
                                     .withContainers(
                                             ContainerTemplates.baseImageBuilder("admin", Constants.TEST_CLIENTS_IMAGE)
                                                               .withCommand("admin-client")
                                                               .withArgs(args)
                                                               .build()
                                     )
                                     .endSpec()
                                     .endTemplate()
                                     .endSpec();
    }

    /**
     * Default test client producer job builder.
     *
     * @param jobName the job name
     * @param bootstrap the bootstrap
     * @param topicName the topic name
     * @param numOfMessages the num of messages
     * @param message the message
     * @param messageKey
     * @return the job builder
     */
    public static JobBuilder defaultTestClientProducerJob(
            String jobName,
            String bootstrap,
            String topicName,
            int numOfMessages,
            String message,
            @Nullable
            String messageKey
    ) {
        return newJobForContainer(
                jobName,
                "test-client-producer",
                Constants.TEST_CLIENTS_IMAGE,
                testClientsProducerEnvVars(bootstrap, topicName, numOfMessages, message, messageKey)
        );
    }

    private static JobBuilder newJobForContainer(String jobName, String containerName, String image, List<EnvVar> envVars) {
        return baseClientJob(jobName)
                                     .editSpec()
                                     .editTemplate()
                                     .editSpec()
                                     .withContainers(
                                             ContainerTemplates.baseImageBuilder(containerName, image)
                                                               .withEnv(envVars)
                                                               .build()
                                     )
                                     .endSpec()
                                     .endTemplate()
                                     .endSpec();
    }

    /**
     * Default test client consumer job builder.
     *
     * @param jobName the job name
     * @param bootstrap the bootstrap
     * @param topicName the topic name
     * @param numOfMessages the num of messages
     * @return the job builder
     */
    public static JobBuilder defaultTestClientConsumerJob(String jobName, String bootstrap, String topicName, int numOfMessages) {
        return newJobForContainer(
                jobName,
                "test-client-consumer",
                Constants.TEST_CLIENTS_IMAGE,
                testClientsConsumerEnvVars(bootstrap, topicName, numOfMessages)
        );
    }

    /**
     * Default kcat job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return the job builder
     */
    public static JobBuilder defaultKcatJob(String jobName, List<String> args) {
        return baseClientJob(jobName)
                                     .editSpec()
                                     .editTemplate()
                                     .editSpec()
                                     .withContainers(
                                             ContainerTemplates.baseImageBuilder("kcat", Constants.KCAT_CLIENT_IMAGE)
                                                               .withArgs(args)
                                                               .build()
                                     )
                                     .endSpec()
                                     .endTemplate()
                                     .endSpec();
    }

    /**
     * Default kafka go consumer job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return the job builder
     */
    public static JobBuilder defaultKafkaGoConsumerJob(String jobName, List<String> args) {
        return baseClientJob(jobName)
                                     .editSpec()
                                     .withBackoffLimit(3)
                                     .editTemplate()
                                     .editSpec()
                                     .withRestartPolicy(Constants.RESTART_POLICY_ONFAILURE)
                                     .withContainers(
                                             ContainerTemplates.baseImageBuilder("kafka-go-consumer", Constants.KAF_CLIENT_IMAGE)
                                                               .withArgs(args)
                                                               .build()
                                     )
                                     .endSpec()
                                     .endTemplate()
                                     .endSpec();
    }

    private static EnvVar envVar(String name, String value) {
        return new EnvVarBuilder()
                                  .withName(name)
                                  .withValue(value)
                                  .build();
    }

    private static List<EnvVar> testClientsProducerEnvVars(
            String bootstrap,
            String topicName,
            int numOfMessages,
            String message,
            @Nullable
            String messageKey
    ) {
        List<EnvVar> envVars = new ArrayList<>(
                List.of(
                        envVar(BOOTSTRAP_VAR, bootstrap),
                        envVar(DELAY_MS_VAR, "200"),
                        envVar(TOPIC_VAR, topicName),
                        envVar(MESSAGE_COUNT_VAR, String.valueOf(numOfMessages)),
                        envVar(MESSAGE_VAR, message),
                        envVar(PRODUCER_ACKS_VAR, "all"),
                        envVar(LOG_LEVEL_VAR, "INFO"),
                        envVar(CLIENT_TYPE_VAR, "KafkaProducer")
                )
        );
        if (messageKey != null) {
            envVars.add(envVar(MESSAGE_KEY_VAR, messageKey));
        }
        return envVars;
    }

    private static List<EnvVar> testClientsConsumerEnvVars(String bootstrap, String topicName, int numOfMessages) {
        return List.of(
                envVar(BOOTSTRAP_VAR, bootstrap),
                envVar(TOPIC_VAR, topicName),
                envVar(MESSAGE_COUNT_VAR, String.valueOf(numOfMessages)),
                envVar(GROUP_ID_VAR, "my-group"),
                envVar(LOG_LEVEL_VAR, "INFO"),
                envVar(CLIENT_TYPE_VAR, "KafkaConsumer"),
                envVar(OUTPUT_FORMAT_VAR, "json")
        );
    }
}
