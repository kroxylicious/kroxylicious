/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload.operations;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.reload.ChangeResult;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OperationsPlannerTest {

    private final VirtualClusterRegistry vcr = mock(VirtualClusterRegistry.class);
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);

    @Test
    void shouldPlanRemovesBeforeAdds() {
        var planner = plannerWithModels(modelFor("cluster-add"));
        var changes = new ChangeResult(Set.of("cluster-add"), Set.of("cluster-remove"), Set.of());

        var ops = planner.plan(changes, configWith("cluster-add"));

        assertThat(ops).hasSize(2);
        assertThat(ops.get(0)).isInstanceOf(RemoveCluster.class);
        assertThat(ops.get(0).clusterName()).isEqualTo("cluster-remove");
        assertThat(ops.get(1)).isInstanceOf(AddCluster.class);
        assertThat(ops.get(1).clusterName()).isEqualTo("cluster-add");
    }

    @Test
    void shouldReturnEmptyListWhenNoOperationsRequired() {
        var planner = plannerWithModels();

        var ops = planner.plan(new ChangeResult(Set.of(), Set.of(), Set.of()), configWith("placeholder"));

        assertThat(ops).isEmpty();
    }

    @Test
    void shouldNotResolveModelsWhenOnlyRemovesArePresent() {
        // Remove operations don't need model resolution from the new config (they look up
        // the original model from VCR's virtualClusterModels). The planner shouldn't waste
        // work resolving when there are no adds.
        var resolverCalls = new int[1];
        Function<Configuration, List<VirtualClusterModel>> resolver = config -> {
            resolverCalls[0]++;
            return List.of();
        };
        var planner = new OperationsPlanner(vcr, endpointRegistry, resolver);

        planner.plan(new ChangeResult(Set.of(), Set.of("cluster-remove"), Set.of()), configWith("placeholder"));

        assertThat(resolverCalls[0]).as("modelResolver must not be called for remove-only plans").isEqualTo(0);
    }

    @Test
    void shouldThrowIllegalStateWhenChangeDetectorReportsAddForClusterNotInNewConfig() {
        // Phantom-add — a buggy ChangeDetector reported a cluster as added that isn't in
        // the submitted config. Planner surfaces this as IllegalStateException so the
        // failure is loud-and-clear at the framework layer, not a per-cluster error or a
        // generic NPE deep in AddCluster.
        var planner = plannerWithModels(); // resolves no models from any config

        assertThatThrownBy(() -> planner.plan(
                new ChangeResult(Set.of("phantom-cluster"), Set.of(), Set.of()),
                configWith("placeholder")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("phantom-cluster")
                .hasMessageContaining("ChangeDetector contract violation");
    }

    private OperationsPlanner plannerWithModels(VirtualClusterModel... resolvedModels) {
        return new OperationsPlanner(vcr, endpointRegistry, config -> List.of(resolvedModels));
    }

    private static VirtualClusterModel modelFor(String name) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        return model;
    }

    private static Configuration configWith(String... clusterNames) {
        var clusters = Arrays.stream(clusterNames).map(OperationsPlannerTest::vc).toList();
        return new Configuration(null, null, null, clusters, null, false,
                Optional.empty(), null, null);
    }

    private static VirtualCluster vc(String name) {
        var gateway = new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", 9192), null, null, null),
                null,
                Optional.empty());
        return new VirtualCluster(name,
                new TargetCluster("kafka:9092", Optional.empty()),
                List.of(gateway),
                false, false, List.of());
    }
}
