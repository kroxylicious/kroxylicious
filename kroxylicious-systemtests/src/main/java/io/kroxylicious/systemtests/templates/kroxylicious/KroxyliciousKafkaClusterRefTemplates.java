/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.TlsBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.tls.TrustAnchorRefBuilder;
import io.kroxylicious.systemtests.Constants;

public class KroxyliciousKafkaClusterRefTemplates {

    private KroxyliciousKafkaClusterRefTemplates() {
    }

    /**
     * Default kafka cluster ref deployment.
     *
     * @param namespaceName the namespace name
     * @param clusterRefName the cluster ref name
     * @return the kafka service builder
     */
    public static KafkaServiceBuilder defaultKafkaClusterRefDeployment(String namespaceName, String clusterRefName) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(clusterRefName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                    .withBootstrapServers("%s-kafka-bootstrap.%s.svc.cluster.local:9092".formatted(clusterRefName, Constants.KAFKA_DEFAULT_NAMESPACE))
                .endSpec();
        // @formatter:on
    }

    public static KafkaServiceBuilder kafkaClusterRefDeploymentWithTls(String namespaceName, String clusterRefName) {
        // @formatter:off
        return defaultKafkaClusterRefDeployment(namespaceName, clusterRefName)
                .editSpec()
                    .withBootstrapServers("%s-kafka-bootstrap.%s.svc.cluster.local:9093".formatted(clusterRefName, Constants.KAFKA_DEFAULT_NAMESPACE))
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
}
