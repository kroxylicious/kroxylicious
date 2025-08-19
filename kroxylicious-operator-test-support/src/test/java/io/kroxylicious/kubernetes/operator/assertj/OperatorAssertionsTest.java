/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.Assert;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatusBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.ClustersBuilder;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.BDDAssertions.then;

class OperatorAssertionsTest {

    public static final String ANNOTATION_A = "AnnotationA";
    public static final long OBSERVED_GENERATION = 2L;

    @Test
    void shouldReturnKafkaProxyStatusAssert() {
        // Given
        var kafkaProxyStatus = new KafkaProxyStatusBuilder().withObservedGeneration(OBSERVED_GENERATION).build();

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(kafkaProxyStatus);

        // Then
        then(actualAssertion).isInstanceOf(KafkaProxyStatusAssert.class);
    }

    @Test
    void shouldReturnKafkaServiceStatusAssert() {
        // Given
        var kafkaProxyStatus = new KafkaServiceStatusBuilder().withObservedGeneration(OBSERVED_GENERATION).build();

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(kafkaProxyStatus);

        // Then
        then(actualAssertion).isInstanceOf(KafkaServiceStatusAssert.class);
    }

    @Test
    void shouldReturnClusterAssert() {
        // Given
        var clusters = new ClustersBuilder().withName("wibble").build();

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(clusters);

        // Then
        then(actualAssertion).isInstanceOf(ClusterAssert.class);
    }

    @Test
    void shouldReturnConditionAssert() {
        // Given
        var conditions = new ConditionBuilder()
                .withType(Condition.Type.Ready)
                .withObservedGeneration(OBSERVED_GENERATION)
                .withLastTransitionTime(Instant.now())
                .withMessage("message")
                .withReason("Reason")
                .build();

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(conditions);

        // Then
        then(actualAssertion).isInstanceOf(ConditionAssert.class);
    }

    @Test
    void shouldReturnConfigurationAssert() {
        // Given
        var configurations = new Configuration(null,
                List.of(),
                List.of(),
                List.of(new VirtualCluster("Bob",
                        new TargetCluster("", Optional.empty()),
                        List.of(new VirtualClusterGateway("gateway",
                                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9090), null, null, List.of()),
                                null,
                                Optional.empty())),
                        false,
                        false,
                        List.of(), null)),
                List.of(),
                false,
                Optional.empty());

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(configurations);

        // Then
        then(actualAssertion).isInstanceOf(ProxyConfigAssert.class);
    }

    @Test
    void shouldReturnMetadataAssert() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName("thing")
                .withAnnotations(Map.of(ANNOTATION_A, "VALUE_1"))
                .endMetadata()
                .build();

        // When
        Assert<?, ?> actualAssertion = OperatorAssertions.assertThat(thingWithMetadata);

        // Then
        then(actualAssertion).isInstanceOf(MetadataAssert.class);
    }
}