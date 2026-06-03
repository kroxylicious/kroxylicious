/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

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
        var removeModel = modelFor("cluster-remove");
        var addModel = modelFor("cluster-add");
        when(vcr.virtualClusterModels()).thenReturn(List.of(removeModel));
        var planner = plannerWithModels(addModel);
        var changes = new ChangeResult(Set.of("cluster-add"), Set.of("cluster-remove"), Set.of());

        var ops = planner.plan(changes, configWith("cluster-add"));

        assertThat(ops).hasSize(2);
        assertThat(ops.get(0)).isInstanceOf(RemoveCluster.class);
        assertThat(ops.get(0).clusterName()).isEqualTo("cluster-remove");
        assertThat(ops.get(1)).isInstanceOf(AddCluster.class);
        assertThat(ops.get(1).clusterName()).isEqualTo("cluster-add");
    }

    @Test
    void shouldHandRemoveClusterTheRegistryOwnedModel() {
        // Reference-identity gotcha: EndpointRegistry's binding map is keyed on the
        // EndpointGateway reference itself. RemoveCluster must be given the *registry's*
        // model, not a freshly-resolved one whose gateways have different identities, or
        // the deregister becomes a silent no-op and the binding leaks. We assert that the
        // model instance the planner hands to RemoveCluster is the one returned by
        // vcr.virtualClusterModels().
        var registryModel = modelFor("cluster-remove");
        when(vcr.virtualClusterModels()).thenReturn(List.of(registryModel));
        var planner = plannerWithModels(); // no adds; resolver returns nothing
        var changes = new ChangeResult(Set.of(), Set.of("cluster-remove"), Set.of());

        var ops = planner.plan(changes, configWith("placeholder"));

        assertThat(ops).singleElement()
                .isInstanceOfSatisfying(RemoveCluster.class,
                        op -> assertThat(op.clusterName()).isEqualTo(registryModel.getClusterName()));
    }

    @Test
    void shouldReturnEmptyListWhenNoOperationsRequired() {
        var planner = plannerWithModels();

        var ops = planner.plan(new ChangeResult(Set.of(), Set.of(), Set.of()), configWith("placeholder"));

        assertThat(ops).isEmpty();
    }

    @Test
    void shouldNotResolveModelsWhenOnlyRemovesArePresent() {
        // Remove operations resolve their model from the registry (vcr.virtualClusterModels),
        // not the submitted Configuration. The modelResolver (which builds models for the new
        // config) must not be invoked when there are no adds.
        var removeModel = modelFor("cluster-remove");
        when(vcr.virtualClusterModels()).thenReturn(List.of(removeModel));
        var resolverCalls = new int[1];
        Function<Configuration, List<VirtualClusterModel>> resolver = config -> {
            resolverCalls[0]++;
            return List.of();
        };
        var planner = new OperationsPlanner(vcr, endpointRegistry, resolver);

        planner.plan(new ChangeResult(Set.of(), Set.of("cluster-remove"), Set.of()), configWith("placeholder"));

        assertThat(resolverCalls[0]).as("modelResolver must not be called for remove-only plans").isZero();
    }

    @Test
    void shouldThrowIllegalStateWhenChangeDetectorReportsAddForClusterNotInNewConfig() {
        // Phantom-add — a buggy ChangeDetector reported a cluster as added that isn't in
        // the submitted config. Planner surfaces this as IllegalStateException so the
        // failure is loud-and-clear at the framework layer, not a per-cluster error or a
        // generic NPE deep in AddCluster.
        var planner = plannerWithModels(); // resolves no models from any config
        var changes = new ChangeResult(Set.of("phantom-cluster"), Set.of(), Set.of());
        var config = configWith("placeholder");

        assertThatThrownBy(() -> planner.plan(changes, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("phantom-cluster")
                .hasMessageContaining("ChangeDetector contract violation");
    }

    @Test
    void shouldThrowIllegalStateWhenChangeDetectorReportsRemoveForClusterNotInRegistry() {
        // Phantom-remove — symmetric to phantom-add. The registry has no model for the
        // named cluster, so the planner can't build a RemoveCluster for it. Surfacing this
        // at the framework layer (rather than deeper in RemoveCluster) keeps the operation
        // free of "couldn't resolve gateways" guards.
        when(vcr.virtualClusterModels()).thenReturn(List.of());
        var planner = plannerWithModels();
        var changes = new ChangeResult(Set.of(), Set.of("phantom-cluster"), Set.of());
        var config = configWith("placeholder");

        assertThatThrownBy(() -> planner.plan(changes, config))
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
        return new Configuration(null, null, null, null, null, clusters, null, false,
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
