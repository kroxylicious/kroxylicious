/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.tls.TrustAnchorRefBuilder;
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
                    .withBootstrapServers(getKafkaBootstrap("plain"))
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
        // @formatter:off
        return defaultKafkaClusterRefCR(namespaceName, clusterRefName)
                .editSpec()
                    .withBootstrapServers(getKafkaBootstrap("tls"))
                    .withTls(new TlsBuilder()
                            .withTrustAnchorRef(new TrustAnchorRefBuilder()
                                    .withName(Constants.KROXYLICIOUS_TLS_CLIENT_CA_CERT)
                                    .withKind(Constants.CONFIG_MAP)
                                    .withKey(Constants.KROXYLICIOUS_TLS_CA_NAME)
                                    .build())
                            .build())
                .endSpec();
        // @formatter:on
    }

    private static String getKafkaBootstrap(String listenerStatusName) {
        // wait for listeners to contain data
        var kafkaListenerStatus = KafkaUtils.getKafkaListenerStatus(listenerStatusName);

        return kafkaListenerStatus.stream()
                .map(ListenerStatus::getBootstrapServers)
                .findFirst().orElseThrow();
    }
}
