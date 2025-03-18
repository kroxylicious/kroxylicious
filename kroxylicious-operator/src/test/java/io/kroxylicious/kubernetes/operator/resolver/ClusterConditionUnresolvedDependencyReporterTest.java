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

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.ClusterCondition;

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
        Set<LocalRef<?>> unresolvedDependencies = Set.of();
        Assertions.assertThatThrownBy(() -> reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies)).isInstanceOf(IllegalStateException.class)
                .hasMessage("reporter should not be invoked if there are no unresolved dependencies");
    }

    @Test
    void deterministicallyReportsOneUnresolvedDependency() {
        HashSet<LocalRef<?>> unresolvedDependencies = new HashSet<>();
        unresolvedDependencies.add(new KafkaServiceRefBuilder().withName("a").build());
        unresolvedDependencies.add(new KafkaServiceRefBuilder().withName("b").build());
        unresolvedDependencies.add(new FilterRefBuilder().withName("a").build());
        unresolvedDependencies.add(new FilterRefBuilder().withName("b").build());
        unresolvedDependencies.add(new IngressRefBuilder().withName("a").build());
        unresolvedDependencies.add(new IngressRefBuilder().withName("b").build());

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", Condition.Type.ResolvedRefs, Condition.Status.FALSE, "Invalid",
                        "Resource of kind \"KafkaProtocolFilter\" in group \"filter.kroxylicious.io\" named \"a\" does not exist.")));
            });
        }));
    }

    @Test
    void clusterRefUnresolved() {
        var unresolved = new KafkaServiceRefBuilder().withName("a").build();
        Set<LocalRef<?>> unresolvedDependencies = Set.of(unresolved);
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().editMetadata().withName("cluster").endMetadata()
                .withNewSpec().withNewTargetKafkaServiceRef().withName("name").endTargetKafkaServiceRef()
                .endSpec().build();

        // when
        reporter.reportUnresolvedDependencies(cluster, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", Condition.Type.ResolvedRefs, Condition.Status.FALSE, "Invalid",
                        "Resource of kind \"KafkaService\" in group \"kroxylicious.io\" named \"a\" does not exist.")));
            });
        }));
    }

    @Test
    void ingressRefUnresolved() {
        var unresolved = new IngressRefBuilder().withName("a").build();
        Set<LocalRef<?>> unresolvedDependencies = Set.of(unresolved);

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", Condition.Type.ResolvedRefs, Condition.Status.FALSE, "Invalid",
                        "Resource of kind \"KafkaProxyIngress\" in group \"kroxylicious.io\" named \"a\" does not exist.")));
            });
        }));
    }

    @Test
    void filterUnresolved() {
        var unresolved = new FilterRefBuilder().withName("a").build();
        Set<LocalRef<?>> unresolvedDependencies = Set.of(unresolved);

        // when
        reporter.reportUnresolvedDependencies(CLUSTER, unresolvedDependencies);

        // then
        verify(mockResourceContext).put(eq("cluster_conditions"), assertArg(a -> {
            assertThat(a).isInstanceOfSatisfying(Map.class, map -> {
                assertThat(map).containsExactlyEntriesOf(Map.of("cluster", new ClusterCondition("cluster", Condition.Type.ResolvedRefs, Condition.Status.FALSE, "Invalid",
                        "Resource of kind \"KafkaProtocolFilter\" in group \"filter.kroxylicious.io\" named \"a\" does not exist.")));
            });
        }));
    }

}
