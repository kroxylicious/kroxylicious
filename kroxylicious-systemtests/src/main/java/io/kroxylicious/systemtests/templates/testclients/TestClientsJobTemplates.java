/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.CapabilitiesBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Test Clients job templates.
 */
public class TestClientsJobTemplates {
    private static final String BOOTSTRAP_VAR = "BOOTSTRAP_SERVERS";
    private static final String TOPIC_VAR = "TOPIC";
    private static final String MESSAGE_VAR = "MESSAGE";
    private static final String MESSAGE_COUNT_VAR = "MESSAGE_COUNT";
    private static final String GROUP_ID_VAR = "GROUP_ID";
    private static final String LOG_LEVEL_VAR = "LOG_LEVEL";
    private static final String CLIENT_TYPE_VAR = "CLIENT_TYPE";
    private static final String PRODUCER_ACKS_VAR = "PRODUCER_ACKS";
    private static final String DELAY_MS_VAR = "DELAY_MS";

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
                .withRestartPolicy("Never")
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
                .withContainers(new ContainerBuilder()
                        .withName("admin")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withCommand("admin-client")
                        .withArgs(args)
                        .withSecurityContext(jobsSecurityContext())
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
     * @return the job builder
     */
    public static JobBuilder defaultTestClientProducerJob(String jobName, String bootstrap, String topicName, int numOfMessages, String message) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("test-client-producer")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withEnv(testClientsProducerEnvVars(bootstrap, topicName, numOfMessages, message))
                        .withSecurityContext(jobsSecurityContext())
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
     * @return the job builder
     */
    public static JobBuilder defaultTestClientConsumerJob(String jobName, String bootstrap, String topicName, int numOfMessages) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("test-client-consumer")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withEnv(testClientsConsumerEnvVars(bootstrap, topicName, numOfMessages))
                        .withSecurityContext(jobsSecurityContext())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
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
                .withContainers(new ContainerBuilder()
                        .withName("kcat")
                        .withImage("edenhill/kcat:1.7.1")
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withArgs(args)
                        .withSecurityContext(jobsSecurityContext())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    /**
     * Default kafka go producer job builder.
     *
     * @param jobName the job name
     * @param bootstrap the bootstrap
     * @param topicName the topic name
     * @return the job builder
     */
    public static JobBuilder defaultKafkaGoProducerJob(String jobName, String bootstrap, String topicName) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kafka-go-producer")
                        .withImage("ppatierno/kafka-go-producer:latest")
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withEnv(kafkaGoProducerEnvVars(bootstrap, topicName))
                        .withSecurityContext(jobsSecurityContext())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    /**
     * Default sarama consumer job builder.
     *
     * @param jobName the job name
     * @param bootstrap the bootstrap
     * @param topicName the topic name
     * @return the job builder
     */
    public static JobBuilder defaultKafkaGoConsumerJob(String jobName, String bootstrap, String topicName) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kafka-go-consumer")
                        .withImage("ppatierno/kafka-go-consumer:latest")
                        .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                        .withEnv(kafkaGoConsumerEnvVars(bootstrap, topicName))
                        .withSecurityContext(jobsSecurityContext())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    private static List<EnvVar> testClientsProducerEnvVars(String bootstrap, String topicName, int numOfMessages, String message) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName(BOOTSTRAP_VAR)
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(DELAY_MS_VAR)
                .withValue("1000")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(TOPIC_VAR)
                .withValue(topicName)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(MESSAGE_COUNT_VAR)
                .withValue(String.valueOf(numOfMessages))
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(MESSAGE_VAR)
                .withValue(message)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(PRODUCER_ACKS_VAR)
                .withValue("all")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(LOG_LEVEL_VAR)
                .withValue("INFO")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(CLIENT_TYPE_VAR)
                .withValue("KafkaProducer")
                .build());

        return envVarList;
    }

    private static List<EnvVar> testClientsConsumerEnvVars(String bootstrap, String topicName, int numOfMessages) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName(BOOTSTRAP_VAR)
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(TOPIC_VAR)
                .withValue(topicName)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(MESSAGE_COUNT_VAR)
                .withValue(String.valueOf(numOfMessages))
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(GROUP_ID_VAR)
                .withValue("my-group")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(LOG_LEVEL_VAR)
                .withValue("INFO")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(CLIENT_TYPE_VAR)
                .withValue("KafkaConsumer")
                .build());

        return envVarList;
    }

    private static List<EnvVar> kafkaGoProducerEnvVars(String bootstrap, String topicName) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName(BOOTSTRAP_VAR)
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName(TOPIC_VAR)
                .withValue(topicName)
                .build());

        return envVarList;
    }

    private static List<EnvVar> kafkaGoConsumerEnvVars(String bootstrap, String topicName) {
        List<EnvVar> envVarList = kafkaGoProducerEnvVars(bootstrap, topicName);
        envVarList.add(new EnvVarBuilder()
                .withName(GROUP_ID_VAR)
                .withValue("my-kafka-go-group")
                .build());

        return envVarList;
    }

    private static SecurityContext jobsSecurityContext() {
        return new SecurityContextBuilder()
                .withAllowPrivilegeEscalation(false)
                .withSeccompProfile(new SeccompProfileBuilder()
                        .withType("RuntimeDefault")
                        .build())
                .withCapabilities(new CapabilitiesBuilder()
                        .withDrop("ALL")
                        .build())
                .build();
    }
}
