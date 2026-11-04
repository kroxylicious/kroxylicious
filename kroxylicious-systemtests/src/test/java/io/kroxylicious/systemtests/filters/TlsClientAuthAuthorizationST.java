/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.filters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Pod;

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.systemtests.AbstractSystemTests;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.clients.KafkaClients;
import io.kroxylicious.systemtests.clients.records.ConsumerRecord;
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
import static org.junit.jupiter.api.Assertions.assertAll;

/**
 * System test proving that a subject built from a TLS client certificate flows all the way through
 * to authorization decisions when configured through the Operator.
 *
 * <p>The proxy is configured with:
 * <ul>
 *   <li>Downstream TLS with mTLS (trust anchor = CA)</li>
 *   <li>{@code tlsSubjectBuilder} extracting the CN from the client certificate subject</li>
 *   <li>Authorization filter applying ACL rules to the extracted {@code User} principal</li>
 * </ul>
 *
 * <p>Topic creation goes directly against Kafka's anonymous listener (port 9094, no auth),
 * bypassing the proxy. This avoids needing SASL through the proxy and keeps the test focused
 * on TLS client auth and authorization.
 *
 * <p>Both test methods use {@code StrimziTestClient} explicitly because the TLS client certificate
 * env vars ({@code CA_CRT}, {@code USER_CRT}, {@code USER_KEY}) are only supported by that client.
 *
 * @see <a href="https://github.com/kroxylicious/kroxylicious/issues/5271">Issue #5271</a>
 */
class TlsClientAuthAuthorizationST extends AbstractSystemTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(TlsClientAuthAuthorizationST.class);
    private static final String MESSAGE = "Hello-world";
    private static final String CA_SECRET_NAME = "my-ca";

    private final String clusterName = "authorization-st-cluster";
    private KroxyliciousOperator kroxyliciousOperator;
    private CertManager certManager;

    @BeforeAll
    void setUp() {
        KafkaClients.strimziTestClient().preloadImage();
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

        certManager = new CertManager();
        certManager.deploy();
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

    /**
     * When alice's TLS client certificate is presented, her CN is extracted and mapped to
     * {@code User[name=alice]}. The ACL allows alice on her topic, so produce and consume both succeed.
     */
    @Test
    void testAliceTlsClientAuthAllowProduceAndConsume(String namespace) {
        int numberOfMessages = 1;
        var aclRules = new ArrayList<>(List.of("allow User with name = \"alice\" to * Topic with name = \"" + topicName + "\";"));

        // Given
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with mTLS and authorization filter").addArgument(namespace).log();
        var kroxylicious = deployTlsAuthProxy(namespace, aclRules);
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
        int numberOfMessages = 1;
        var aclRules = new ArrayList<>(List.of("deny User with name = \"bob\" to * Topic with name = \"" + topicName + "\";"));

        // Given
        LOGGER.atInfo().setMessage("Given Kroxylicious in {} namespace with mTLS and authorization filter").addArgument(namespace).log();
        var kroxylicious = deployTlsAuthProxy(namespace, aclRules);
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
     * the authorization filter. No SASL filter is used; the proxy→Kafka upstream uses the
     * anonymous listener (no auth required).
     */
    private Kroxylicious deployTlsAuthProxy(String namespace, List<String> aclRules) {
        LOGGER.atInfo().setMessage("Deploy cert chain in {} namespace").addArgument(namespace).log();

        // Self-signed issuer + CA certificate
        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.issuer(namespace),
                certManager.caCertificate(namespace, CA_SECRET_NAME));

        // CA Issuer backed by the CA Secret
        String caIssuerName = CA_SECRET_NAME + "-issuer";
        resourceManager.createOrUpdateResourceFromBuilderWithWait(certManager.caIssuer(namespace, CA_SECRET_NAME));

        // Server certificate: short CN (<=64 bytes, cert-manager limit); hostname verification
        // uses SANs. The wildcard covers bootstrap + all per-broker cluster-ip services.
        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.certForWithIssuer(namespace, caIssuerName, "proxy", "*." + namespace + ".svc.cluster.local"));

        // Client certificates for alice and bob
        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                certManager.clientCertFor(namespace, caIssuerName, "alice"),
                certManager.clientCertFor(namespace, caIssuerName, "bob"));

        LOGGER.atInfo().setMessage("Deploy Kroxylicious with TLS client auth and authorization filter in {} namespace").addArgument(namespace).log();

        // ACL rules ConfigMap
        resourceManager.createOrUpdateResourceFromBuilderWithWait(
                KroxyliciousConfigMapTemplates.getAclRulesConfigMap(namespace, "acl-rules", aclRules));

        // TLS: server cert from Secret "server-certificate"; trust anchor points at the CA Secret
        // directly — no extra ConfigMap needed.
        var tls = new TlsBuilder(KroxyliciousUtils.tlsConfigFromCert("server-certificate"))
                .withNewTrustAnchorRef()
                .withNewRef()
                .withName(CA_SECRET_NAME)
                .withKind("Secret")
                .endRef()
                .withKey("ca.crt")
                .withStoreType("PEM")
                .endTrustAnchorRef()
                .build();

        // @formatter:off
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
        // @formatter:on
        kroxylicious.createOrUpdateResources();
        return kroxylicious;
    }

    private String kafkaAnonymousBootstrap() {
        return KafkaUtils.getKafkaListenerStatus(Constants.ANONYMOUS_LISTENER_NAME)
                .map(io.strimzi.api.kafka.model.kafka.listener.ListenerStatus::getBootstrapServers)
                .orElseThrow(() -> new IllegalStateException("No anonymous listener found on " + clusterName));
    }
}
