/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMapVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
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
    private static final String ADDITIONAL_CONFIG_VAR = "ADDITIONAL_CONFIG";

    private TestClientsJobTemplates() {
    }

    private static JobBuilder baseClientJob(String jobName) {
        Map<String, String> labelSelector = Map.of("app", jobName);

        PodSpecBuilder podSpecBuilder = new PodSpecBuilder();

        if (Environment.TEST_CLIENTS_PULL_SECRET != null && !Environment.TEST_CLIENTS_PULL_SECRET.isEmpty()) {
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.TEST_CLIENTS_PULL_SECRET));
            podSpecBuilder.withImagePullSecrets(imagePullSecrets);
        }

        // @formatter:off
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
                        .withNewSpecLike(podSpecBuilder.build())
                            .withRestartPolicy(Constants.RESTART_POLICY_NEVER)
                        .endSpec()
                    .endTemplate()
                .endSpec();
        // @formatter:on
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
                .withContainers(ContainerTemplates.baseImageBuilder("admin", Environment.TEST_CLIENTS_IMAGE)
                        .withCommand("admin-client")
                        .withArgs(args)
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    /**
     * Authentication admin client job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return  the job builder
     */
    public static JobBuilder authenticationAdminClientJob(String jobName, List<String> args) {
        List<VolumeMount> volumeMounts = new ArrayList<>();
        List<Volume> volumes = new ArrayList<>();
        Volume configVolume = new VolumeBuilder()
                .withName(Constants.KAFKA_ADMIN_CLIENT_CONFIG_NAME)
                .withConfigMap(new ConfigMapVolumeSourceBuilder()
                        .withName(Constants.KAFKA_ADMIN_CLIENT_CONFIG_NAME)
                        .addNewItem()
                        .withKey(Constants.CONFIG_PROP_FILE_NAME)
                        .withPath(Constants.CONFIG_PROP_FILE_NAME)
                        .endItem()
                        .build())
                .build();

        VolumeMount configMount = new VolumeMountBuilder()
                .withName(Constants.KAFKA_ADMIN_CLIENT_CONFIG_NAME)
                .withMountPath(Constants.CONFIG_PROP_TEMP_DIR)
                .build();
        volumeMounts.add(configMount);
        volumes.add(configVolume);

        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withVolumes(volumes)
                .withContainers(ContainerTemplates.baseImageBuilder("admin", Environment.TEST_CLIENTS_IMAGE)
                        .withCommand("admin-client")
                        .withArgs(args)
                        .withVolumeMounts(volumeMounts)
                        .build())
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
     * @param messageKey the message key
     * @param additionalConfig the additional config
     * @return  the job builder
     */
    public static JobBuilder defaultTestClientProducerJob(String jobName, String bootstrap, String topicName, int numOfMessages, String message,
                                                          @Nullable String messageKey, Map<String, String> additionalConfig) {
        return newJobForContainer(jobName,
                "test-client-producer",
                Environment.TEST_CLIENTS_IMAGE,
                testClientsProducerEnvVars(bootstrap, topicName, numOfMessages, message, messageKey, additionalConfig));
    }

    private static JobBuilder newJobForContainer(String jobName, String containerName, String image, List<EnvVar> envVars) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(ContainerTemplates.baseImageBuilder(containerName, image)
                        .withEnv(envVars)
                        .build())
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
     * @param additionalKafkaProps the additional kafka props
     * @return  the job builder
     */
    public static JobBuilder defaultTestClientConsumerJob(String jobName, String bootstrap, String topicName, int numOfMessages,
                                                          Map<String, String> additionalKafkaProps) {
        return newJobForContainer(jobName,
                "test-client-consumer",
                Environment.TEST_CLIENTS_IMAGE,
                testClientsConsumerEnvVars(bootstrap, topicName, numOfMessages, additionalKafkaProps));
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
                .withBackoffLimit(3)
                .editTemplate()
                .editSpec()
                .withRestartPolicy(Constants.RESTART_POLICY_ON_FAILURE)
                .withContainers(ContainerTemplates.baseImageBuilder("kcat", Constants.KCAT_CLIENT_IMAGE)
                        .withArgs(args)
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    /**
     * Default python job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return the job builder
     */
    public static JobBuilder defaultPythonJob(String jobName, List<String> args) {
        return baseClientJob(jobName)
                .editSpec()
                .withBackoffLimit(3)
                .editTemplate()
                .editSpec()
                .withRestartPolicy(Constants.RESTART_POLICY_ON_FAILURE)
                .withContainers(ContainerTemplates.baseImageBuilder("python", Constants.PYTHON_CLIENT_IMAGE)
                        .withArgs(args)
                        .build())
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
                .withRestartPolicy(Constants.RESTART_POLICY_ON_FAILURE)
                .withContainers(ContainerTemplates.baseImageBuilder("kafka-go-consumer", Constants.KAF_CLIENT_IMAGE)
                        .withArgs(args)
                        .build())
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

    private static List<EnvVar> testClientsProducerEnvVars(String bootstrap, String topicName, int numOfMessages, String message,
                                                           @Nullable String messageKey, Map<String, String> additionalKafkaProps) {
        List<String> additionalConfig = new ArrayList<>();
        additionalKafkaProps.forEach((key, value) -> additionalConfig.add(key + "=" + value));

        List<EnvVar> envVars = new ArrayList<>(List.of(
                envVar(BOOTSTRAP_VAR, bootstrap),
                envVar(DELAY_MS_VAR, "200"),
                envVar(TOPIC_VAR, topicName),
                envVar(MESSAGE_COUNT_VAR, String.valueOf(numOfMessages)),
                envVar(MESSAGE_VAR, message),
                envVar(PRODUCER_ACKS_VAR, "all"),
                envVar(LOG_LEVEL_VAR, "INFO"),
                envVar(CLIENT_TYPE_VAR, "KafkaProducer"),
                envVar(ADDITIONAL_CONFIG_VAR, String.join("\n", additionalConfig))));
        if (messageKey != null) {
            envVars.add(envVar(MESSAGE_KEY_VAR, messageKey));
        }

        return envVars;
    }

    private static List<EnvVar> testClientsConsumerEnvVars(String bootstrap, String topicName, int numOfMessages, Map<String, String> additionalKafkaProps) {
        List<String> additionalConfig = new ArrayList<>();
        additionalKafkaProps.forEach((key, value) -> additionalConfig.add(key + "=" + value));
        return List.of(
                envVar(BOOTSTRAP_VAR, bootstrap),
                envVar(TOPIC_VAR, topicName),
                envVar(MESSAGE_COUNT_VAR, String.valueOf(numOfMessages)),
                envVar(GROUP_ID_VAR, "my-group"),
                envVar(LOG_LEVEL_VAR, "INFO"),
                envVar(CLIENT_TYPE_VAR, "KafkaConsumer"),
                envVar(OUTPUT_FORMAT_VAR, "json"),
                envVar(ADDITIONAL_CONFIG_VAR, String.join("\n", additionalConfig)));
    }
}
