/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.steps;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTopicTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * The type Kafka steps.
 */
public class KafkaSteps {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSteps.class);

    private KafkaSteps() {
    }

    /**
     * Create topic.
     *
     * @param clusterName the cluster name
     * @param topicName the topic name
     * @param namespace the namespace
     * @param partitions the partitions
     * @param replicas the replicas
     * @param minIsr the min isr
     */
    public static void createTopic(String clusterName, String topicName, String namespace,
                                   int partitions, int replicas, int minIsr) {
        resourceManager.createResourceWithWait(
                KafkaTopicTemplates.defaultTopic(namespace, clusterName, topicName, partitions, replicas, minIsr).build());
    }

    /**
     * Create topic using test clients.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     * @param minIsr the min isr
     */
    public static void createTopicTestClient(String deployNamespace, String topicName, String bootstrap, int partitions, int replicas, int minIsr) {
        String podName = KafkaUtils.AdminTestClient(deployNamespace, bootstrap);
        String command = "admin-client topic create --bootstrap-server=" + bootstrap + " --topic=" + topicName + " --topic-partitions=" + partitions +
                " --topic-rep-factor=" + replicas;
        // CompletableFuture<String> data = new CompletableFuture<>();
        // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // kubeClient().getClient().pods().inNamespace(deployNamespace).withName(podName)
        // .writingOutput(baos)
        // .writingError(baos)
        // .usingListener(new SimpleListener(data, baos))
        // .exec(command);
        //
        // LOGGER.info(baos.toString());

        ExecResult result = cmdKubeClient(deployNamespace).execInPod(podName, false, "admin-client", "topic", "create", "--bootstrap-server=" + bootstrap,
                "--topic=" + topicName, "--topic-partitions=" + partitions, "--topic-rep-factor=" + replicas);
        if (!result.isSuccess()) {
            LOGGER.error(result.err());
        }
        LOGGER.info(result.out());
    }

    /**
     * Create compacted topic with compression.
     *
     * @param topicName the topic name
     * @param bootstrap the bootstrap
     * @param partitions the partitions
     * @param replicas the replicas
     * @param minIsr the min isr
     * @param compressionType the compression type
     * @throws Exception the exception
     */
    public static void createCompactedTopicWithCompression(String topicName, String bootstrap, int partitions, int replicas, int minIsr, String compressionType)
            throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (Admin admin = Admin.create(props)) {

            // Create a compacted topic with compression codec
            Map<String, String> newTopicConfig = new HashMap<>();
            newTopicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(minIsr));
            newTopicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
            newTopicConfig.put(TopicConfig.COMPRESSION_TYPE_CONFIG, compressionType);
            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicas).configs(newTopicConfig);

            CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));

            KafkaFuture<Void> future = result.values().get(topicName);
            future.get();
        }
    }

    /**
     * Restart kafka broker.
     *
     * @param clusterName the cluster name
     */
    public static void restartKafkaBroker(String clusterName) {
        clusterName = clusterName + "-kafka";
        assertThat("Broker has not been restarted successfully!", KafkaUtils.restartBroker(Constants.KROXY_DEFAULT_NAMESPACE, clusterName));
    }
}
