/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.KafkaUtils;

public class KroxyliciousKafkaClusterRefTemplates {

    private KroxyliciousKafkaClusterRefTemplates() {
    }

    /**
     * Default kafka cluster ref CR.
     *
     * @param namespaceName the namespace name
     * @param clusterRefName the cluster ref name
     * @return the kafka service builder
     */
    public static KafkaServiceBuilder defaultKafkaClusterRefCR(String namespaceName, String clusterRefName) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(clusterRefName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers(getKafkaBootstrap("plain", clusterRefName))
                .endSpec();
        // @formatter:on
    }

    /**
     * Kafka cluster ref CR with tls.
     *
     * @param namespaceName the namespace name
     * @param clusterRefName the cluster ref name
     * @return the kafka service builder
     */
    public static KafkaServiceBuilder kafkaClusterRefCRWithTls(String namespaceName, String clusterRefName) {
        final TrustAnchorRef trustAnchorRef = new TrustAnchorRefBuilder()
                .withNewRef()
                .withName(Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT)
                .withKind(Constants.CONFIG_MAP)
                .endRef()
                .withKey(Constants.KROXYLICIOUS_TLS_CA_NAME)
                .build();
        return kafkaClusterRefCRWithTls(namespaceName, clusterRefName, new TlsBuilder()
                .withTrustAnchorRef(trustAnchorRef)
                .build());
    }

    public static KafkaServiceBuilder kafkaClusterRefCRWithTls(String namespaceName, String clusterRefName, Tls tls) {
        // @formatter:off
        return defaultKafkaClusterRefCR(namespaceName, clusterRefName)
                .editSpec()
                    .withBootstrapServers(getKafkaBootstrap("tls", clusterRefName))
                    .withTls(tls)
                .endSpec();
        // @formatter:on
    }

    private static String getKafkaBootstrap(String listenerStatusName, String clusterRefName) {
        // wait for listeners to contain data
        if (KafkaUtils.isKafkaUp(clusterRefName)) {
            var kafkaListenerStatus = KafkaUtils.getKafkaListenerStatus(listenerStatusName);

            return kafkaListenerStatus.stream()
                    .map(ListenerStatus::getBootstrapServers)
                    .findFirst().orElseThrow();
        }
        else {
            // Some operator tests do not need kafka running so we can set a default value
            return String.format("%s-kafka-bootstrap.%s.svc.cluster.local:9092".formatted(clusterRefName, Constants.KAFKA_DEFAULT_NAMESPACE));
        }
    }
}
