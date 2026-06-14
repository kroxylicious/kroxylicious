/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

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
        when(vcr.modelFor("cluster-remove")).thenReturn(removeModel);
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
    void shouldOrderPureRemovesThenModifiesThenPureAdds() {
        // The full three-bucket ordering. A same-port modify wants its remove and add tight
        // together — emitting one ReplaceCluster per modify achieves that structurally rather
        // than by the planner remembering to pair them.
        var pureRemoveModel = modelFor("cluster-pure-remove");
        var oldModifyModel = modelFor("cluster-modify");
        var newModifyModel = modelFor("cluster-modify");
        var pureAddModel = modelFor("cluster-pure-add");
        when(vcr.modelFor("cluster-pure-remove")).thenReturn(pureRemoveModel);
        when(vcr.modelFor("cluster-modify")).thenReturn(oldModifyModel);
        var planner = plannerWithModels(newModifyModel, pureAddModel);
        var changes = new ChangeResult(
                Set.of("cluster-pure-add"),
                Set.of("cluster-pure-remove"),
                Set.of("cluster-modify"));

        var ops = planner.plan(changes, configWith("cluster-modify", "cluster-pure-add"));

        assertThat(ops).hasSize(3);
        assertThat(ops.get(0)).isInstanceOf(RemoveCluster.class);
        assertThat(ops.get(0).clusterName()).isEqualTo("cluster-pure-remove");
        assertThat(ops.get(1)).isInstanceOf(ReplaceCluster.class);
        assertThat(ops.get(1).clusterName()).isEqualTo("cluster-modify");
        assertThat(ops.get(2)).isInstanceOf(AddCluster.class);
        assertThat(ops.get(2).clusterName()).isEqualTo("cluster-pure-add");
    }

    @Test
    void shouldPlanModifyAsSingleReplaceClusterOp() {
        // Modify produces ONE op, not two. The remove-then-add internals are encapsulated
        // inside ReplaceCluster — the orchestrator sees a single intent.
        var oldModel = modelFor("cluster-modify");
        var newModel = modelFor("cluster-modify");
        when(vcr.modelFor("cluster-modify")).thenReturn(oldModel);
        var planner = plannerWithModels(newModel);
        var changes = new ChangeResult(Set.of(), Set.of(), Set.of("cluster-modify"));

        var ops = planner.plan(changes, configWith("cluster-modify"));

        assertThat(ops).singleElement()
                .isInstanceOfSatisfying(ReplaceCluster.class,
                        op -> assertThat(op.clusterName()).isEqualTo("cluster-modify"));
    }

    @Test
    void shouldThrowIllegalStateWhenChangeDetectorReportsModifyForClusterNotInRegistry() {
        // Phantom-modify, old-side variant. The change detector reported a modify for a name
        // the registry doesn't have an entry for — the planner can't build a ReplaceCluster
        // without both halves. Same shape of failure as phantom-add and phantom-remove.
        // vcr.modelFor("cluster-modify") returns null by Mockito default — no stub needed.
        var newModel = modelFor("cluster-modify");
        var planner = plannerWithModels(newModel);
        var changes = new ChangeResult(Set.of(), Set.of(), Set.of("cluster-modify"));
        var config = configWith("cluster-modify");

        assertThatThrownBy(() -> planner.plan(changes, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster-modify")
                .hasMessageContaining("absent from the registry")
                .hasMessageContaining("ChangeDetector contract violation");
    }

    @Test
    void shouldThrowIllegalStateWhenChangeDetectorReportsModifyForClusterNotInNewConfig() {
        // Phantom-modify, new-side variant. The registry has the old model but the submitted
        // configuration doesn't include the cluster — internally inconsistent.
        var oldModel = modelFor("cluster-modify");
        when(vcr.modelFor("cluster-modify")).thenReturn(oldModel);
        var planner = plannerWithModels(); // resolver returns no models
        var changes = new ChangeResult(Set.of(), Set.of(), Set.of("cluster-modify"));
        var config = configWith("placeholder");

        assertThatThrownBy(() -> planner.plan(changes, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cluster-modify")
                .hasMessageContaining("absent from the submitted configuration")
                .hasMessageContaining("ChangeDetector contract violation");
    }

    @Test
    void shouldHandRemoveClusterTheRegistryOwnedModel() {
        // Reference-identity gotcha: EndpointRegistry's binding map is keyed on the
        // EndpointGateway reference itself. RemoveCluster must be given the *registry's*
        // model, not a freshly-resolved one whose gateways have different identities, or
        // the deregister becomes a silent no-op and the binding leaks. We assert that the
        // model instance the planner hands to RemoveCluster is the one returned by
        // vcr.modelFor(name).
        var registryModel = modelFor("cluster-remove");
        when(vcr.modelFor("cluster-remove")).thenReturn(registryModel);
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
        // Remove operations resolve their model from the registry (vcr.modelFor(name)),
        // not the submitted Configuration. The modelResolver (which builds models for the new
        // config) must not be invoked when there are no adds.
        var removeModel = modelFor("cluster-remove");
        when(vcr.modelFor("cluster-remove")).thenReturn(removeModel);
        var resolverCalls = new int[1];
        BiFunction<Configuration, String, VirtualClusterModel> resolver = (config, clusterName) -> {
            resolverCalls[0]++;
            throw new IllegalStateException("resolver should not be called for remove-only plans");
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
        // vcr.modelFor("phantom-cluster") returns null by Mockito default — no stub needed.
        var planner = plannerWithModels();
        var changes = new ChangeResult(Set.of(), Set.of("phantom-cluster"), Set.of());
        var config = configWith("placeholder");

        assertThatThrownBy(() -> planner.plan(changes, config))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("phantom-cluster")
                .hasMessageContaining("ChangeDetector contract violation");
    }

    private OperationsPlanner plannerWithModels(VirtualClusterModel... resolvedModels) {
        Map<String, VirtualClusterModel> byName = Arrays.stream(resolvedModels)
                .collect(Collectors.toMap(VirtualClusterModel::getClusterName, m -> m));
        return new OperationsPlanner(vcr, endpointRegistry, (config, clusterName) -> {
            var resolved = byName.get(clusterName);
            if (resolved == null) {
                // Simulate the production behaviour where Configuration#virtualClusterModel(pfr, name)
                // throws IllegalArgumentException for unknown clusters. Some tests rely on this
                // behaviour to surface phantom-add bugs.
                throw new IllegalArgumentException("No virtual cluster named '" + clusterName + "' in this configuration");
            }
            return resolved;
        });
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
