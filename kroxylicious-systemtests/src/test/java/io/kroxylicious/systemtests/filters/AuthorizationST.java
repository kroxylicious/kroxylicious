/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
import io.kroxylicious.systemtests.enums.KafkaClientType;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kroxylicious.CertManager;
import io.kroxylicious.systemtests.installation.kroxylicious.Kroxylicious;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousBuilder;
import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;
import io.kroxylicious.systemtests.steps.KafkaSteps;
import io.kroxylicious.systemtests.steps.KroxyliciousSteps;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousFilterTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaNodePoolTemplates;
import io.kroxylicious.systemtests.templates.strimzi.KafkaTemplates;
import io.kroxylicious.systemtests.utils.KafkaUtils;
import io.kroxylicious.systemtests.utils.KroxyliciousUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertAll;

class AuthorizationST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(AuthorizationST.class);
    private static final String MESSAGE = "Hello-world";
    private static final String TLS_CA_SECRET_NAME = "my-ca";

    private final String clusterName = "authorization-st-cluster";
    private KroxyliciousOperator kroxyliciousOperator;
    private CertManager certManager;
    private final Map<String, String> usernamePasswords = new HashMap<>();
    List<String> aclRules;

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
                    KafkaTemplates.kafkaWithAuthentication(Constants.KAFKA_DEFAULT_NAMESPACE, clusterName, kafkaReplicas));
        }

        kroxyliciousOperator = new KroxyliciousOperator(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE);
        kroxyliciousOperator.deploy();

        generatePasswordForNewUser(Constants.KROXYLICIOUS_ADMIN_USER);

        if (KafkaClients.isStrimziTestClient()) {
            certManager = new CertManager();
            certManager.deploy();
        }
    }

    @BeforeEach
    void beforeEach() {
        aclRules = new ArrayList<>();
        aclRules.add(generateAllowAclRule(Constants.KROXYLICIOUS_ADMIN_USER, "*"));
    }

    @AfterAll
    void cleanUp() {
        if (kroxyliciousOperator != null) {
            kroxyliciousOperator.delete();
        }
        if (certManager != null) {
            certManager.delete();
        }
    }

    private void deployAuthorizationResources(String namespace, Map<String, String> usernamePassword, List<String> aclRules) {
        LOGGER.info("Deploy Kroxylicious with authorization filter in {} namespace", namespace);
        KafkaUtils.createKafkaUsers(clusterName, usernamePassword);

        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                KroxyliciousConfigMapTemplates.getAclRulesConfigMap(namespace, "acl-rules", aclRules));
    }

    /**
     * Deploys the proxy into the test's own namespace. The ACL rules are read by the authorization filter once,
     * when the proxy starts, so the rules must be in place before the proxy pod is created. Deploying per test
     * (rather than sharing a class-scoped proxy) guarantees that, because the pod is always created afresh.
     */
    private Kroxylicious deployPortIdentifiesNodeWithAuthorizationFilter(String namespace, Map<String, String> usernamePasswords, List<String> aclRules) {
        deployAuthorizationResources(namespace, usernamePasswords, aclRules);
        var kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(namespace, clusterName, 1)
                .addKafkaProtocolFilter(KroxyliciousFilterTemplates.kroxyliciousSaslInspectorFilter(namespace).build())
                .addKafkaProtocolFilter(
                        KroxyliciousFilterTemplates.kroxyliciousAuthorizationFilter(namespace, "${configmap:acl-rules:acl-rules}").build())
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithFilterCR(clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        List.of(Constants.KROXYLICIOUS_SASL_INSPECTOR_FILTER_NAME, Constants.KROXYLICIOUS_AUTHORIZATION_FILTER_NAME)).build())
                .build();
        kroxylicious.createOrUpdateResources();
        return kroxylicious;
    }

    @Test
    void testScramSha512AuthenticationAllowProduceAndConsume(String namespace) {
        // kcat does not support scram-sha-512 authentication: https://github.com/edenhill/kcat/issues/462
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;
        String user = "bob"; // name shall be always lowercase
        generatePasswordForNewUser(user);
        aclRules.add(generateAllowAclRule(user, topicName));

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        var kroxylicious = deployPortIdentifiesNodeWithAuthorizationFilter(namespace, usernamePasswords, aclRules);
        String bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopicWithAuthentication(namespace, topicName, bootstrap, 1, 1, usernamePasswords);

        Map<String, String> additionalKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, user, usernamePasswords.get(user));
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, additionalKafkaProps);

        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), additionalKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    @Test
    void testScramSha512AuthenticationDenyProduceAllowConsume(String namespace) {
        // kcat does not support scram-sha-512 authentication: https://github.com/edenhill/kcat/issues/462
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;
        String userBob = "bob"; // name shall be always lowercase
        generatePasswordForNewUser(userBob);
        String userAlice = "alice";
        generatePasswordForNewUser(userAlice);
        aclRules.add(generateDenyAclRule(userBob, topicName));
        aclRules.add(generateAllowAclRule(userAlice, topicName));

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        var kroxylicious = deployPortIdentifiesNodeWithAuthorizationFilter(namespace, usernamePasswords, aclRules);
        String bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopicWithAuthentication(namespace, topicName, bootstrap, 1, 1, usernamePasswords);

        Map<String, String> bobKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userBob, usernamePasswords.get(userBob));
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        ExecResult producerResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, bobKafkaProps);
        // depending on the client, error is written on stdout instead of stderr
        String logError = producerResult.out().concat(producerResult.err());

        assertAll(() -> {
            assertThat(producerResult.isSuccess() && producerResult.err().isEmpty()).isFalse();
            assertThat(logError).containsAnyOf("Not authorized to access topics: [" + topicName + "]",
                    "The client is not authorized to access this topic",
                    "Topic authorization failed");
        });

        LOGGER.atInfo().setMessage("Then aren't any messages to be consumed").log();
        Map<String, String> aliceKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userAlice, usernamePasswords.get(userAlice));
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofSeconds(10), aliceKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .isEmpty();
    }

    @Test
    void testScramSha512AuthenticationAllowProduceDenyConsume(String namespace) {
        // kcat does not support scram-sha-512 authentication: https://github.com/edenhill/kcat/issues/462
        assumeThat(Environment.KAFKA_CLIENT).isNotEqualToIgnoringCase(KafkaClientType.KCAT.name());

        int numberOfMessages = 1;
        String userBob = "bob"; // name shall be always lowercase
        generatePasswordForNewUser(userBob);
        String userAlice = "alice";
        generatePasswordForNewUser(userAlice);
        aclRules.add(generateAllowAclRule(userBob, topicName));
        aclRules.add(generateDenyAclRule(userAlice, topicName));

        // start Kroxylicious
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with {} replicas").addArgument(namespace).addArgument(1).log();
        var kroxylicious = deployPortIdentifiesNodeWithAuthorizationFilter(namespace, usernamePasswords, aclRules);
        String bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopicWithAuthentication(namespace, topicName, bootstrap, 1, 1, usernamePasswords);

        Map<String, String> bobKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userBob, usernamePasswords.get(userBob));
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, bobKafkaProps);

        LOGGER.atInfo().setMessage("Then aren't any messages to be consumed").log();
        Map<String, String> aliceKafkaProps = KroxyliciousSteps.getAdditionalSaslProps(namespace, userAlice, usernamePasswords.get(userAlice));
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofSeconds(10), aliceKafkaProps);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        String log = KroxyliciousSteps.getConsumerLog(namespace);
        assertAll(() -> {
            assertThat(log).containsAnyOf("Not authorized to access topics: [" + topicName + "]",
                    "The client is not authorized to access this topic",
                    "Topic authorization failed");
            assertThat(result).withFailMessage("expected messages have not been received!")
                    .isEmpty();
        });
    }

    /**
     * When alice's TLS client certificate is presented, her CN is extracted and mapped to
     * {@code User[name=alice]}. The ACL allows alice on her topic, so produce and consume both succeed.
     */
    @Test
    void testAliceTlsClientAuthAllowProduceAndConsume(String namespace) {
        assumeThat(KafkaClients.isStrimziTestClient()).as("client TLS requires the Strimzi test client").isTrue();

        int numberOfMessages = 1;
        var tlsAclRules = new ArrayList<>(List.of(generateAllowAclRule("alice", topicName)));

        // Given
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with mTLS and authorization filter").addArgument(namespace).log();
        var kroxylicious = deployTlsAuthProxy(namespace, tlsAclRules);
        String bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, kafkaAnonymousBootstrap(), 1, 1);

        var aliceTlsVars = KroxyliciousSteps.clientTlsEnvVars("alice-cert");

        // When
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}'").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName).log();
        KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, aliceTlsVars);

        // Then
        LOGGER.atInfo().setMessage("Then the messages are consumed").log();
        List<ConsumerRecord> result = KroxyliciousSteps.consumeMessages(namespace, topicName, bootstrap, numberOfMessages, Duration.ofMinutes(2), aliceTlsVars);
        LOGGER.atInfo().setMessage("Received: {}").addArgument(result).log();

        assertThat(result).withFailMessage("expected messages have not been received!")
                .extracting(ConsumerRecord::getPayload)
                .hasSize(numberOfMessages)
                .allSatisfy(v -> assertThat(v).contains(MESSAGE));
    }

    /**
     * When bob's TLS client certificate is presented, his CN is extracted and mapped to
     * {@code User[name=bob]}. The ACL denies bob on the topic, so produce fails with an authorization error.
     */
    @Test
    void testBobTlsClientAuthDenyProduce(String namespace) {
        assumeThat(KafkaClients.isStrimziTestClient()).as("client TLS requires the Strimzi test client").isTrue();

        int numberOfMessages = 1;
        var tlsAclRules = new ArrayList<>(List.of(generateDenyAclRule("bob", topicName)));

        // Given
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with mTLS and authorization filter").addArgument(namespace).log();
        var kroxylicious = deployTlsAuthProxy(namespace, tlsAclRules);
        String bootstrap = kroxylicious.getBootstrap(namespace, clusterName);

        LOGGER.atInfo().setMessage("And a kafka Topic named {}").addArgument(topicName).log();
        KafkaSteps.createTopic(namespace, topicName, kafkaAnonymousBootstrap(), 1, 1);

        var bobTlsVars = KroxyliciousSteps.clientTlsEnvVars("bob-cert");

        // When
        LOGGER.atInfo().setMessage("When {} messages '{}' are sent to the topic '{}' as bob").addArgument(numberOfMessages).addArgument(MESSAGE).addArgument(topicName)
                .log();
        ExecResult producerResult = KroxyliciousSteps.produceMessages(namespace, topicName, bootstrap, MESSAGE, numberOfMessages, bobTlsVars);

        // Then
        String logError = producerResult.out().concat(producerResult.err());
        assertAll(() -> {
            assertThat(producerResult.isSuccess() && producerResult.err().isEmpty()).isFalse();
            assertThat(logError).containsAnyOf("Not authorized to access topics: [" + topicName + "]",
                    "The client is not authorized to access this topic",
                    "Topic authorization failed");
        });
    }

    /**
     * Deploys the proxy with downstream mTLS, the CN-extracting {@code tlsSubjectBuilder}, and
     * the authorization filter. The proxy→Kafka upstream uses Kafka's anonymous listener so no
     * SASL credentials are required.
     */
    private Kroxylicious deployTlsAuthProxy(String namespace, List<String> tlsAclRules) {
        LOGGER.atInfo().setMessage("Deploy cert chain in {} namespace").addArgument(namespace).log();

        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.issuer(namespace),
                certManager.caCertificate(namespace, TLS_CA_SECRET_NAME));

        String caIssuerName = TLS_CA_SECRET_NAME + "-issuer";
        resourceManager.createOrUpdateResourceFromBuilderWithWait(certManager.caIssuer(namespace, TLS_CA_SECRET_NAME));

        // Server certificate: short CN (<=64 bytes, cert-manager limit); hostname verification
        // uses SANs. The wildcard covers bootstrap + all per-broker cluster-ip services.
        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.certForWithIssuer(namespace, caIssuerName, "proxy", "*." + namespace + ".svc.cluster.local"));

        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.clientCertFor(namespace, caIssuerName, "alice"),
                certManager.clientCertFor(namespace, caIssuerName, "bob"));

        LOGGER.atInfo().setMessage("Deploy Kroxylicious with TLS client auth and authorization filter in {} namespace").addArgument(namespace).log();

        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                KroxyliciousConfigMapTemplates.getAclRulesConfigMap(namespace, "acl-rules", tlsAclRules));

        // TLS: server cert from Secret "server-certificate"; trust anchor points at the CA Secret
        // directly (storeType PEM required so the operator accepts a non-.pem key name).
        var tls = new TlsBuilder(KroxyliciousUtils.tlsConfigFromCert("server-certificate"))
                .withNewTrustAnchorRef()
                .withNewRef()
                .withName(TLS_CA_SECRET_NAME)
                .withKind("Secret")
                .endRef()
                .withKey("ca.crt")
                .withStoreType("PEM")
                .endTrustAnchorRef()
                .build();

        var kroxylicious = KroxyliciousBuilder.singleNodeBaseBuilder(namespace, clusterName, 1)
                // Use the anonymous listener so the proxy→Kafka upstream needs no SASL credentials
                .withKafkaService(KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(clusterName)
                        .editSpec().withBootstrapServers(kafkaAnonymousBootstrap()).endSpec().build())
                .addKafkaProtocolFilter(
                        KroxyliciousFilterTemplates.kroxyliciousAuthorizationFilter(namespace, "${configmap:acl-rules:acl-rules}").build())
                .withDownstreamTls(tls)
                .withVirtualKafkaCluster(KroxyliciousVirtualKafkaClusterTemplates.virtualKafkaClusterWithTlsSubjectBuilder(
                        clusterName, Constants.KROXYLICIOUS_INGRESS_CLUSTER_IP,
                        List.of(Constants.KROXYLICIOUS_AUTHORIZATION_FILTER_NAME)).build())
                .build();
        kroxylicious.createOrUpdateResources();
        return kroxylicious;
    }

    private String kafkaAnonymousBootstrap() {
        return KafkaUtils.getKafkaListenerStatus(Constants.ANONYMOUS_LISTENER_NAME)
                .map(ListenerStatus::getBootstrapServers)
                .orElseThrow(() -> new IllegalStateException("No anonymous listener found on " + clusterName));
    }

    private String generateAllowAclRule(String userName, String topicName) {
        return generateAclRule(Decision.ALLOW, userName, topicName);
    }

    private String generateDenyAclRule(String userName, String topicName) {
        return generateAclRule(Decision.DENY, userName, topicName);
    }

    private String generateAclRule(Decision decision, String userName, String topicName) {
        String comparison = "=";
        if (topicName.contains("*")) {
            comparison = "like";
        }
        return decision.name().toLowerCase(Locale.ROOT) + " User with name = \"" + userName.toLowerCase(Locale.ROOT) + "\" to * Topic with name " + comparison + " \""
                + topicName + "\";";
    }

    private void generatePasswordForNewUser(String user) {
        String password = UUID.randomUUID().toString().replace("-", "");
        usernamePasswords.putIfAbsent(user, password);
    }
}
