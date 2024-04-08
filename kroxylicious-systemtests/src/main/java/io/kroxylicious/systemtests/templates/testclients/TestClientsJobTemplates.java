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
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Test Clients job templates.
 */
public class TestClientsJobTemplates {

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
                        .withImagePullPolicy("IfNotPresent")
                        .withCommand("admin-client")
                        .withArgs(args)
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    public static JobBuilder defaultTestClientProducerJob(String jobName, String bootstrap, String topicName, int numOfMessages, String message) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("admin")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy("IfNotPresent")
                        .withEnv(testClientsProducerEnvVars(bootstrap, topicName, numOfMessages, message))
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    public static JobBuilder defaultTestClientConsumerJob(String jobName, String bootstrap, String topicName, int numOfMessages) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("admin")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy("IfNotPresent")
                        .withEnv(testClientsConsumerEnvVars(bootstrap, topicName, numOfMessages))
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    public static JobBuilder defaultKcatJob(String jobName, List<String> args) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kcat")
                        .withImage("edenhill/kcat:1.7.1")
                        .withImagePullPolicy("IfNotPresent")
                        .withArgs(args)
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    public static JobBuilder defaultSaramaProducerJob(String jobName, String bootstrap, String topicName) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kafka-go-producer")
                        .withImage("ppatierno/kafka-go-producer:latest")
                        .withImagePullPolicy("IfNotPresent")
                        .withEnv(saramaProducerEnvVars(bootstrap, topicName))
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    public static JobBuilder defaultSaramaConsumerJob(String jobName, String bootstrap, String topicName) {
        return baseClientJob(jobName)
                .editSpec()
                .editTemplate()
                .editSpec()
                .withContainers(new ContainerBuilder()
                        .withName("kafka-go-consumer")
                        .withImage("ppatierno/kafka-go-consumer:latest")
                        .withImagePullPolicy("IfNotPresent")
                        .withEnv(saramaConsumerEnvVars(bootstrap, topicName))
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec();
    }

    private static List<EnvVar> testClientsProducerEnvVars(String bootstrap, String topicName, int numOfMessages, String message) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName("BOOTSTRAP_SERVERS")
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("DELAY_MS")
                .withValue("1000")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("TOPIC")
                .withValue(topicName)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("MESSAGE_COUNT")
                .withValue(String.valueOf(numOfMessages))
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("MESSAGE")
                .withValue(message)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("PRODUCER_ACKS")
                .withValue("all")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("LOG_LEVEL")
                .withValue("INFO")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("CLIENT_TYPE")
                .withValue("KafkaProducer")
                .build());

        return envVarList;
    }

    private static List<EnvVar> testClientsConsumerEnvVars(String bootstrap, String topicName, int numOfMessages) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName("BOOTSTRAP_SERVERS")
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("TOPIC")
                .withValue(topicName)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("MESSAGE_COUNT")
                .withValue(String.valueOf(numOfMessages))
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("GROUP_ID")
                .withValue("my-group")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("LOG_LEVEL")
                .withValue("INFO")
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("CLIENT_TYPE")
                .withValue("KafkaConsumer")
                .build());

        return envVarList;
    }

    private static List<EnvVar> saramaProducerEnvVars(String bootstrap, String topicName) {
        List<EnvVar> envVarList = new ArrayList<>();
        envVarList.add(new EnvVarBuilder()
                .withName("BOOTSTRAP_SERVERS")
                .withValue(bootstrap)
                .build());
        envVarList.add(new EnvVarBuilder()
                .withName("TOPIC")
                .withValue(topicName)
                .build());

        return envVarList;
    }

    private static List<EnvVar> saramaConsumerEnvVars(String bootstrap, String topicName) {
        List<EnvVar> envVarList = saramaProducerEnvVars(bootstrap, topicName);
        envVarList.add(new EnvVarBuilder()
                .withName("GROUP_ID")
                .withValue("my-kafka-go-group")
                .build());

        return envVarList;
    }
}
