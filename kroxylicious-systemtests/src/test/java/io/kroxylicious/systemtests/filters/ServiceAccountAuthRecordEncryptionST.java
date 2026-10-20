/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.kms.ServiceAccountAuthKmsFacadeFactory;
import io.kroxylicious.systemtests.resources.kms.ServiceAccountAuthTestKmsFacade;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Covers record encryption where the proxy authenticates with the KMS as its own Kubernetes
 * {@code ServiceAccount}, rather than with a static credential embedded in the filter configuration.
 * <p>
 * The {@link ServiceAccountAuthTestKmsFacade} binds the KMS role only to the ServiceAccount the proxy runs as,
 * so a successful encrypt/decrypt proves the proxy pod authenticated as that ServiceAccount.
 */
@ParameterizedClass(autoCloseArguments = true)
@MethodSource("facadesSource")
class ServiceAccountAuthRecordEncryptionST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceAccountAuthRecordEncryptionST.class);
    private static final String MESSAGE = "Hello-world";
    private static final String KEK_PREFIX = "KEK-";
    private static final String SERVICE_ACCOUNT_NAME = "service-account-auth-proxy";
    private static final String RUNNING_PHASE = "Running";
    private static final Duration ASSERTION_DURATION = Duration.ofSeconds(90);
    private final String clusterName = "service-account-auth-encryption-cluster";
    private String bootstrap;
    private TestKekManager testKekManager;
    private KroxyliciousOperator kroxyliciousOperator;
    private Kroxylicious kroxylicious;

    static Stream<ServiceAccountAuthTestKmsFacade<?, ?, ?>> facadesSource() {
        // We rely on the fact that streams are lazy so the facade isn't built
        // or started until the first test needs it.
        // ServiceAccount-auth facades only. Static-credential facades are covered by RecordEncryptionST.
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(ServiceAccountAuthKmsFacadeFactory.class::isInstance)
                // Cast is safe: the filter guarantees a ServiceAccountAuthKmsFacadeFactory, whose build() returns a ServiceAccountAuthTestKmsFacade.
                .<ServiceAccountAuthTestKmsFacade<?, ?, ?>> map(f -> ((ServiceAccountAuthKmsFacadeFactory<?, ?, ?>) f).build())
                .filter(TestKmsFacade::isAvailable)
                .peek(TestKmsFacade::start);
    }

    @Parameter
    ServiceAccountAuthTestKmsFacade<?, ?, ?> testKmsFacade;

    @BeforeAll
    void setUp() {
        KafkaClients.getKafkaClient().preloadImage();
        List<Pod> kafkaPods = kubeClient().listPodsByPrefixInName(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName);
        if (!kafkaPods.isEmpty()) {
            LOGGER.atInfo().setMessage("Skipping kafka deployment. It is already deployed!").log();
        }
        else {
            LOGGER.atInfo().setMessage("Deploying Kafka in {} namespace").addArgument(Constants.KAFKA_DEFAULT_NAMESPACE).log();

            int kafkaReplicas = 1;
            resourceManager.createOrUpdateResourceFromBuilderWithWait(
                    KafkaNodePoolTemplates.poolWithDualRoleAndPersistentStorage(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas),
                    KafkaTemplates.defaultKafka(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();
    }

    @BeforeEach
    void beforeEach() {
        bootstrap = null;
        testKekManager = null;
    }

    @AfterAll
    void cleanUp() {
        kroxyliciousOperator.delete();
    }

    @AfterEach
    void afterEach(String namespace) {
        try {
            if (testKekManager != null) {
                LOGGER.atInfo().log("Deleting KEK...");
                testKekManager.deleteKek(KEK_PREFIX + topicName);
            }
        }
        catch (KubeClusterException e) {
            LOGGER.atError().setMessage("KEK deletion has not been successfully done: {}").addArgument(e).log();
            throw e;
        }
        finally {
            if (bootstrap != null) {
                KafkaSteps.deleteTopic(namespace, topicName, bootstrap);
            }
        }
    }

    @Test
    void ensureClusterHasEncryptedMessage(String namespace) {
        int numberOfMessages = 1;
        deployProxyAuthenticatingWithServiceAccount(namespace);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the message read straight from Kafka is encrypted");
        List<ConsumerRecord> resultEncrypted = KroxyliciousSteps.consumeMessageFromKafkaCluster(namespace, topicName, clusterName,
                Constants.KAFKA_DEFAULT_NAMESPACE, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received from Kafka: {}", resultEncrypted);
        assertThat(resultEncrypted.stream())
                .withFailMessage("expected encryption header has not been received!")
                .allMatch(r -> r.getRecordHeaders().containsKey("kroxylicious.io/encryption"));
        assertThat(resultEncrypted.stream())
                .withFailMessage("Encrypted message still includes the original one!")
                .allMatch(r -> !r.getPayload().contains(MESSAGE));
    }

    @Test
    void produceAndConsumeMessage(String namespace) {
        int numberOfMessages = 1;
        deployProxyAuthenticatingWithServiceAccount(namespace);

        LOGGER.info("And a kafka Topic named {}", topicName);
        KafkaSteps.createTopic(namespace, topicName, bootstrap, 1, 1);

        LOGGER.info("When {} messages '{}' are sent to the topic '{}'", numberOfMessages, MESSAGE, topicName);
        ExecResult produceResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages);
        assertThat(produceResult.isSuccess()).withFailMessage("Unable to produce messages! " + produceResult.err()).isTrue();

        LOGGER.info("Then the message read through the proxy is decrypted");
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2));
        LOGGER.info("Received through proxy: {}", result);
        assertThat(result)
                .withFailMessage("expected decrypted messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(payload -> assertThat(payload).contains(MESSAGE));
    }

    private void deployProxyAuthenticatingWithServiceAccount(String namespace) {
        // The ServiceAccount is user-managed, so it must exist before the proxy is deployed, otherwise the rollout stalls.
        createServiceAccount(namespace, SERVICE_ACCOUNT_NAME);

        // Grant the KMS role to this ServiceAccount, so a proxy pod running as it can authenticate.
        testKmsFacade.bindToServiceAccount(namespace, SERVICE_ACCOUNT_NAME);

        testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(KEK_PREFIX + topicName);

        LOGGER.info("Given Kroxylicious in {} namespace running as ServiceAccount {}", namespace, SERVICE_ACCOUNT_NAME);
        deployProxyWithServiceAccountAuth(namespace, SERVICE_ACCOUNT_NAME);
        bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.info("Then the single proxy pod runs as ServiceAccount {}", SERVICE_ACCOUNT_NAME);
        assertProxyPodServiceAccount(namespace, SERVICE_ACCOUNT_NAME);
    }

    private void deployProxyWithServiceAccountAuth(String namespace, String serviceAccountName) {
        // @formatter:off
        KafkaProxy kafkaProxy = KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(1)
                .editSpec()
                    .withNewInfrastructure()
                        .withServiceAccountName(serviceAccountName)
                    .endInfrastructure()
                .endSpec()
                .build();
        // @formatter:on
        kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(namespace, clusterName, 1)
                .withKafkaProxy(kafkaProxy)
                .addKafkaProtocolFilter(
                        KroxyliciousFilterTemplates.kroxyliciousRecordEncryptionFilter(namespace, testKmsFacade, null).build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(clusterName,
                        Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP, List.of(Constants.KROXYLICIOUS_ENCRYPTION_FILTER_NAME)).build())
                .build();
        kroxylicious.createOrUpdateResources();
    }

    private static void createServiceAccount(String namespace, String serviceAccountName) {
        kubeClient(namespace).getClient()
                .resource(new ServiceAccountBuilder()
                        .withNewMetadata()
                        .withName(serviceAccountName)
                        .withNamespace(namespace)
                        .endMetadata()
                        .build())
                .create();
        LOGGER.info("ServiceAccount {}/{} created", namespace, serviceAccountName);
    }

    private static void assertProxyPodServiceAccount(String namespace, String expectedServiceAccountName) {
        await().atMost(ASSERTION_DURATION).untilAsserted(() -> assertThat(KroxyliciousSteps.getProxyPods(namespace))
                .as("proxy pods running under ServiceAccount '%s'", expectedServiceAccountName)
                .singleElement()
                .satisfies(pod -> {
                    assertThat(pod.getStatus()).extracting(PodStatus::getPhase).isEqualTo(RUNNING_PHASE);
                    assertThat(pod.getSpec()).extracting(PodSpec::getServiceAccountName).isEqualTo(expectedServiceAccountName);
                }));
    }
}
