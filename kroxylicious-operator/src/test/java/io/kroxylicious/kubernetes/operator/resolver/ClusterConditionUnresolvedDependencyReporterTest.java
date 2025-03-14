/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions;
import io.kroxylicious.kubernetes.operator.ClusterCondition;
import io.kroxylicious.kubernetes.operator.ConditionType;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedDependency;

import static io.kroxylicious.kubernetes.operator.resolver.Dependency.FILTER;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_CLUSTER_REF;
import static io.kroxylicious.kubernetes.operator.resolver.Dependency.KAFKA_PROXY_INGRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClusterConditionUnresolvedDependencyReporterTest {

    private static final VirtualKafkaCluster CLUSTER = new VirtualKafkaClusterBuilder().editMetadata().withName("cluster").endMetadata().build();

    @Mock(strictness = LENIENT)
    Context<KafkaProxy> mockContext;

    @Mock(strictness = LENIENT)
    ManagedWorkflowAndDependentResourceContext mockResourceContext;

    UnresolvedDependencyReporter reporter;

    @BeforeEach
    void beforeEach() {
        when(mockContext.managedWorkflowAndDependentResourceContext()).thenReturn(mockResourceContext);
        when(mockResourceContext.get(any(), eq(Map.class))).thenReturn(Optional.empty());
        reporter = new ClusterConditionUnresolvedDependencyReporter(mockContext);
    }

    @Test
    void emptyUnresolvedDependenciesIsInvalidState() {
        Set<UnresolvedDependency> unresolvedDependencies = Set.of();
        Assertions.assertThatThrownBy(() -> reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies)).isInstanceOf(IllegalStateException.class)
                .hasMessage("reporter should not be invoked if there are no unresolved dependencies");
    }

    @Test
    void deterministicallyReportsOneUnresolvedDependency() {
        HashSet<UnresolvedDependency> unresolvedDependencies = new HashSet<>();
        unresolvedDependencies.add(new UnresolvedDependency(KAFKA_CLUSTER_REF, "a"));
        unresolvedDependencies.add(new UnresolvedDependency(KAFKA_CLUSTER_REF, "b"));
        unresolvedDependencies.add(new UnresolvedDependency(FILTER, "a"));
        unresolvedDependencies.add(new UnresolvedDependency(FILTER, "b"));
        unresolvedDependencies.add(new UnresolvedDependency(KAFKA_PROXY_INGRESS, "a"));
        unresolvedDependencies.add(new UnresolvedDependency(KAFKA_PROXY_INGRESS, "b"));

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", ConditionType.Accepted, Conditions.Status.FALSE, "Invalid",
                        "KafkaProxyIngress \"a\" does not exist.")));
            });
        }));
    }

    @Test
    void clusterRefUnresolved() {
        UnresolvedDependency unresolved = new UnresolvedDependency(KAFKA_CLUSTER_REF, "a");
        Set<UnresolvedDependency> unresolvedDependencies = Set.of(unresolved);
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().editMetadata().withName("cluster").endMetadata()
                .withNewSpec().withNewTargetCluster().withNewClusterRef().withName("name").endClusterRef().endTargetCluster()
                .endSpec().build();

        // when
        reporter.reportUnresolvedDependencies(cluster, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", ConditionType.Accepted, Conditions.Status.FALSE, "Invalid",
                        "Target Cluster \"kafkaclusterref.kroxylicious.io/name\" does not exist.")));
            });
        }));
    }

    @Test
    void ingressRefUnresolved() {
        UnresolvedDependency unresolved = new UnresolvedDependency(KAFKA_PROXY_INGRESS, "a");
        Set<UnresolvedDependency> unresolvedDependencies = Set.of(unresolved);

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", ConditionType.Accepted, Conditions.Status.FALSE, "Invalid",
                        "KafkaProxyIngress \"a\" does not exist.")));
            });
        }));
    }

    @Test
    void filterUnresolved() {
        UnresolvedDependency unresolved = new UnresolvedDependency(FILTER, "a");
        Set<UnresolvedDependency> unresolvedDependencies = Set.of(unresolved);

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", ConditionType.Accepted, Conditions.Status.FALSE, "Invalid",
                        "Filter \"a\" does not exist.")));
            });
        }));
    }

}
