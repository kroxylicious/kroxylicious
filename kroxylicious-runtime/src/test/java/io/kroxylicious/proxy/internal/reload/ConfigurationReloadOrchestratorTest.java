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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.ProxyProtocolConfig;
import io.kroxylicious.proxy.config.ProxyProtocolMode;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ConcurrentReconfigureException;
import io.kroxylicious.proxy.reload.StaticConfigurationChangedException;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConfigurationReloadOrchestratorTest {

    @Test
    void preFlightRejectsStaticSectionDiff() {
        // given
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = withDifferentUseIoUring(oldConfig);
        var registry = mock(VirtualClusterRegistry.class);
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause().isInstanceOf(StaticConfigurationChangedException.class);
        // No registry interactions — pre-flight rejected before the pipeline ran.
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).addVirtualCluster(any());
    }

    @Test
    void identicalConfigsCompleteSuccessfullyAsNoOp() throws Exception {
        // given — currentConfig == newConfig, no clusters differ, no static-section diff.
        // Change detection returns an empty ChangeResult; the orchestrator takes the
        // no-op early-return path and reports a clean ReconfigureResult.
        var config = configWith(vc("cluster-a"));
        var registry = mock(VirtualClusterRegistry.class);
        var orchestrator = newOrchestrator(config, registry);

        // when
        var future = orchestrator.reconfigure(config);

        // then
        assertThat(future).isCompletedWithValueMatching(result -> !result.hasErrors());
        // No registry interactions — there was nothing to do.
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).addVirtualCluster(any());
    }

    @Test
    void shouldTransitionAddedClusterToServing() {
        // given — old has cluster-a (baseline), new has cluster-a + cluster-add.
        // The orchestrator:
        // 1. vcr.addVirtualCluster(newModel) — bookkeeping (lifecycle in INITIALIZING)
        // 2. endpointRegistry.registerVirtualCluster — bind every gateway
        // 3. vcr.initializationSucceeded(name) — transition to SERVING
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-add"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then — successful with no errors.
        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());

        // Step 1: addVirtualCluster invoked for the new cluster only.
        var captor = ArgumentCaptor.forClass(VirtualClusterModel.class);
        verify(registry).addVirtualCluster(captor.capture());
        assertThat(captor.getValue().getClusterName()).isEqualTo("cluster-add");

        // Step 2: at least one gateway was registered. (cluster-add's vc() helper builds a
        // single "default" gateway, so we expect exactly one register call for this test.)
        verify(endpointRegistry).registerVirtualCluster(any(EndpointGateway.class));

        // Step 3: lifecycle transitioned to SERVING.
        verify(registry).initializationSucceeded("cluster-add");

        // Negative assertions: no remove/replace happened, no rollback deregister fired.
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).initializationFailed(anyString(), any());
        verify(endpointRegistry, never()).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void shouldSurfaceErrorAndRollbackWhenGatewayBindFails() {
        // given — pure-add reconfigure where endpointRegistry.registerVirtualCluster fails.
        // The orchestrator must:
        // - call vcr.addVirtualCluster (bookkeeping always runs first)
        // - observe the bind failure
        // - call vcr.initializationFailed to drive the lifecycle Stopped via Failed
        // - issue a best-effort deregisterVirtualCluster for each gateway (rollback)
        // - surface a per-cluster ReconfigureError in the result (not an exceptional future)
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-add"));
        var registry = stubbedRegistry();

        // Override the shared endpointRegistry to fail every register call.
        var bindFailure = new IllegalStateException("simulated bind failure");
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(bindFailure));

        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then — completes successfully with one error for cluster-add.
        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-add")
                && r.errors().iterator().next().cause() == bindFailure);

        // Verify the orchestrator drove the full rollback sequence.
        verify(registry).addVirtualCluster(argThat(m -> m != null && "cluster-add".equals(m.getClusterName())));
        verify(registry).initializationFailed("cluster-add", bindFailure);
        verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
        // Critically: initializationSucceeded must NOT have fired.
        verify(registry, never()).initializationSucceeded("cluster-add");
    }

    @Test
    void shouldNotPropagateRollbackDeregisterFailureWhenBindAlsoFailed() {
        // The bind failed (primary reason the add is being rolled back). The subsequent
        // deregister rollback ALSO fails. The orchestrator must:
        // - log the deregister failure (operator-visible) but not propagate it;
        // - keep the per-cluster ReconfigureError carrying the original bind cause,
        // not the deregister cause — the deregister is a side-effect, not the trigger.
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-add"));
        var registry = stubbedRegistry();

        var bindFailure = new IllegalStateException("simulated bind failure");
        var rollbackFailure = new IllegalStateException("simulated deregister failure during rollback");
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(bindFailure));
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(rollbackFailure));

        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        // The reconfigure future is still non-exceptional; the ReconfigureError carries the
        // BIND cause, not the deregister cause.
        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-add")
                && r.errors().iterator().next().cause() == bindFailure);

        // Deregister was attempted.
        verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void pureRemoveCompletesSuccessfully() {
        // given — old has cluster-a (baseline) + cluster-remove; new has only cluster-a.
        // Drives lifecycle through removeVirtualCluster and frees the gateway binding via
        // deregisterVirtualCluster (the path that closes the port for port-addressed VCs).
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-remove"));
        var newConfig = configWith(vc("cluster-a"));
        var registry = stubbedRegistry("cluster-a", "cluster-remove");
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then
        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
        var inOrder = inOrder(registry, endpointRegistry);
        inOrder.verify(registry).removeVirtualCluster("cluster-remove");
        inOrder.verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
        verify(registry, never()).addVirtualCluster(any());
    }

    @Test
    void shouldSurfaceErrorWhenGatewayDeregisterFailsOnRemove() {
        // Remove drove the lifecycle to Stopped, but freeing the gateway binding then
        // failed (e.g. the unbind future never completes cleanly). A failed deregister
        // means the port may still be held, which is a real operator-visible problem —
        // surface as a per-cluster ReconfigureError (symmetric with the add side's
        // bind-failure handling).
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-remove"));
        var newConfig = configWith(vc("cluster-a"));
        var registry = stubbedRegistry("cluster-a", "cluster-remove");

        var deregisterFailure = new IllegalStateException("simulated deregister failure");
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(deregisterFailure));

        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-remove")
                && r.errors().iterator().next().cause() == deregisterFailure);

        // Both steps were still attempted.
        verify(registry).removeVirtualCluster("cluster-remove");
        verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void lockIsReleasedOnExceptionSoSubsequentCallsCanProceed() {
        // After the first reconfigure throws (planner-level IllegalStateException, e.g.
        // phantom-add from a buggy change detector), a second call must not be rejected with
        // ConcurrentReconfigureException — the lock should be released even on exception via
        // the finally block. We inject a detector that reports a phantom add for a cluster
        // that's NOT present in the submitted configuration; the planner's guard fires with
        // IllegalStateException, which propagates out of doReconfigure.
        var config = configWith(vc("cluster-a"));
        var phantomAddDetector = mock(ChangeDetector.class);
        when(phantomAddDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of("phantom-cluster"), Set.of(), Set.of()));
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, stubbedRegistry(), endpointRegistry, mock(PluginFactoryRegistry.class), List.of(phantomAddDetector));

        var first = orchestrator.reconfigure(config);
        assertThat(first).isCompletedExceptionally();
        assertThatThrownBy(first::join).cause().isInstanceOf(IllegalStateException.class);

        var second = orchestrator.reconfigure(config);
        assertThat(second).isCompletedExceptionally();
        // Second call should also throw the planner's IllegalStateException (not
        // ConcurrentReconfigureException) — proving the lock was released between calls.
        assertThatThrownBy(second::join).cause().isInstanceOf(IllegalStateException.class);
    }

    @Test
    void concurrentReconfigureIsRejected() throws Exception {
        // Hold the reconfigure lock by blocking inside a registry no-op invocation. While the
        // first reconfigure is parked there, a second call from another thread must fail fast
        // with ConcurrentReconfigureException.
        var oldConfig = configWith(vc("cluster-remove"), vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"));
        var registry = mock(VirtualClusterRegistry.class);

        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        // removeVirtualCluster will be called for cluster-remove — block there.
        when(registry.removeVirtualCluster(anyString())).thenAnswer(inv -> {
            entered.countDown();
            release.await();
            return CompletableFuture.completedFuture(null);
        });
        // Planner resolves the model from the registry before handing it to RemoveCluster.
        var removeModel = mock(VirtualClusterModel.class);
        when(removeModel.getClusterName()).thenReturn("cluster-remove");
        when(removeModel.gateways()).thenReturn(Map.of("default", mock(EndpointGateway.class)));
        when(registry.modelFor("cluster-remove")).thenReturn(removeModel);

        var orchestrator = newOrchestrator(oldConfig, registry);

        var firstResult = new AtomicReference<Throwable>();
        var thread = new Thread(() -> {
            try {
                orchestrator.reconfigure(newConfig).join();
            }
            catch (Throwable t) {
                firstResult.set(t);
            }
        }, "first-reconfigure");
        // Daemon so a bug in this test (e.g. release.countDown() never reached) can't
        // keep the JVM alive after the test framework has otherwise finished.
        thread.setDaemon(true);
        thread.start();
        // Wait until thread-1 has acquired the lock and is parked in the registry stub.
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        // Now invoke from the main thread; tryLock should fail.
        var contendingFuture = orchestrator.reconfigure(newConfig);

        // then
        assertThat(contendingFuture).isCompletedExceptionally();
        assertThatThrownBy(contendingFuture::join).cause().isInstanceOf(ConcurrentReconfigureException.class);

        // Cleanup: unblock thread-1 so it can finish.
        release.countDown();
        thread.join(5_000);
    }

    @Test
    void nullNewConfigThrowsNullPointerException() {
        var config = configWith(vc("cluster-a"));
        var orchestrator = newOrchestrator(config, stubbedRegistry());
        assertThatThrownBy(() -> orchestrator.reconfigure(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("newConfig");
    }

    @Test
    void noOpSuccessUpdatesCurrentConfigurationSoSubsequentChangeDetectionUsesNewBaseline() {
        // After a no-op reconfigure succeeds, currentConfiguration must advance to the
        // submitted config — otherwise subsequent change detection would compare against
        // a stale baseline.
        //
        // Capture the ConfigurationChangeContext passed to the detector on each call and
        // verify the second call's oldConfig is the config the first call SUBMITTED, not
        // the orchestrator's original constructor-supplied config. Both configs here are
        // structurally identical at the static-section level (so the static-section diff
        // returns empty and the pipeline reaches change detection), but they are distinct
        // object instances, so isSameAs distinguishes them.
        var initialConfig = configWith(vc("cluster-a"));
        var firstSubmittedConfig = configWith(vc("cluster-a")); // distinct instance, structurally identical
        var capturingDetector = mock(ChangeDetector.class);
        when(capturingDetector.detect(any())).thenReturn(ChangeResult.EMPTY);

        var orchestrator = new ConfigurationReloadOrchestrator(
                initialConfig, mock(VirtualClusterRegistry.class), endpointRegistry,
                mock(PluginFactoryRegistry.class), List.of(capturingDetector));

        orchestrator.reconfigure(firstSubmittedConfig).join();
        orchestrator.reconfigure(firstSubmittedConfig).join();

        var captor = ArgumentCaptor.forClass(ConfigurationChangeContext.class);
        verify(capturingDetector, times(2)).detect(captor.capture());
        var contexts = captor.getAllValues();
        assertThat(contexts.get(0).oldConfig()).isSameAs(initialConfig);
        assertThat(contexts.get(1).oldConfig()).isSameAs(firstSubmittedConfig);
    }

    @Test
    void orchestratorDrivesRegistryFromInjectedDetectorOutput() {
        // given — identical old/new configs. The production detectors would return EMPTY here
        // (nothing changed). We inject a custom detector that returns a non-empty ChangeResult
        // anyway, so the only way the orchestrator can drive the registry is if it is in fact
        // consulting the injected detector rather than synthesising its own diff.
        var config = configWith(vc("cluster-remove"));
        var customDetector = mock(ChangeDetector.class);
        when(customDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of(), // clustersToAdd
                Set.of("cluster-remove"), // clustersToRemove
                Set.of())); // clustersToModify

        var registry = stubbedRegistry("cluster-remove");
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, endpointRegistry, mock(PluginFactoryRegistry.class), List.of(customDetector));

        // when
        var future = orchestrator.reconfigure(config);

        // then — the orchestrator consulted the injected detector and acted on its verdict.
        verify(customDetector).detect(any());
        verify(registry).removeVirtualCluster("cluster-remove");
        verify(registry, never()).addVirtualCluster(any());

        // Pure-remove reconfigure: orchestrator completes successfully.
        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
    }

    @Test
    void shouldExecuteRemovesBeforeAddsInMixedReconfigure() {
        // A reconfigure that BOTH removes and adds clusters is supported under step 2. Removes
        // are executed BEFORE adds so swap-style edits (e.g. moving an endpoint between VCs)
        // resolve cleanly without overlapping binding conflicts.
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-remove"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-add")); // removes cluster-remove, adds cluster-add
        var registry = stubbedRegistry("cluster-a", "cluster-remove");
        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
        var inOrder = inOrder(registry);
        inOrder.verify(registry).removeVirtualCluster("cluster-remove");
        inOrder.verify(registry).addVirtualCluster(argThat(m -> m.getClusterName().equals("cluster-add")));
    }

    @Test
    void modifyOnlyReconfigureExecutesViaReplaceCluster() {
        // A modify-only reconfigure now runs a ReplaceCluster operation, which internally
        // composes RemoveCluster + AddCluster. The orchestrator-visible behaviour is the same
        // shape as add/remove: the future completes successfully, ReconfigureResult has no
        // errors, and the registry sees remove + add for the same cluster name.
        var config = configWith(vc("cluster-a"));
        var modifyDetector = mock(ChangeDetector.class);
        when(modifyDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of(), Set.of(), Set.of("cluster-a")));

        var registry = stubbedRegistry("cluster-a");
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, endpointRegistry, mock(PluginFactoryRegistry.class), List.of(modifyDetector));

        var future = orchestrator.reconfigure(config);

        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
        // Both halves of the replace executed on the registry side. Ordering is enforced by
        // ReplaceCluster — the orchestrator just dispatches.
        var inOrder = inOrder(registry);
        inOrder.verify(registry).removeVirtualCluster("cluster-a");
        inOrder.verify(registry).addVirtualCluster(argThat(m -> m.getClusterName().equals("cluster-a")));
    }

    @Test
    void shouldExecuteAddRemoveAndModifyInSingleReconfigure() {
        // End-to-end: a reconfigure that touches every bucket. Ordering invariant (pure
        // removes → modifies → pure adds) is asserted via mockito's inOrder verification on
        // the registry's per-name calls. The submitted configuration mentions both the
        // modified cluster (so the planner can resolve newModel) and the pure-add cluster
        // (so the planner can resolve its addModel); cluster-pure-remove is absent because
        // it's being removed.
        var config = configWith(vc("cluster-modify"), vc("cluster-pure-add"));
        var multiBucketDetector = mock(ChangeDetector.class);
        when(multiBucketDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of("cluster-pure-add"),
                Set.of("cluster-pure-remove"),
                Set.of("cluster-modify")));

        var registry = stubbedRegistry("cluster-pure-remove", "cluster-modify");
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, endpointRegistry, mock(PluginFactoryRegistry.class), List.of(multiBucketDetector));

        var future = orchestrator.reconfigure(config);

        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
        var inOrder = inOrder(registry);
        // Pure remove first.
        inOrder.verify(registry).removeVirtualCluster("cluster-pure-remove");
        // Then the modify pair (encapsulated inside ReplaceCluster — both calls happen).
        inOrder.verify(registry).removeVirtualCluster("cluster-modify");
        inOrder.verify(registry).addVirtualCluster(argThat(m -> m.getClusterName().equals("cluster-modify")));
        // Then the pure add last.
        inOrder.verify(registry).addVirtualCluster(argThat(m -> m.getClusterName().equals("cluster-pure-add")));
        // We made two adds across the reconfigure (one for the modify, one for the pure-add).
        verify(registry, times(2)).addVirtualCluster(any());
        verify(registry, times(2)).removeVirtualCluster(anyString());
    }

    @Test
    void shouldFailFastWhenChangeDetectorReportsAddForClusterMissingFromNewConfig() {
        // Programming-error guard: if a ChangeDetector lies and reports a cluster as added
        // when that cluster isn't present in the submitted newConfig's models, the orchestrator
        // must fail loud (IllegalStateException via addCluster's null-model guard) rather than
        // NPE deep inside VCR or surface it as a per-cluster ReconfigureError (the cluster
        // isn't the cause of the failure; the detector contract is). We inject a custom
        // detector that reports a phantom add.
        var config = configWith(vc("cluster-a"));
        var phantomDetector = mock(ChangeDetector.class);
        when(phantomDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of("phantom-cluster"), // detector lies: this cluster is not in any config
                Set.of(),
                Set.of()));

        var registry = stubbedRegistry();
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, endpointRegistry, mock(PluginFactoryRegistry.class), List.of(phantomDetector));

        var future = orchestrator.reconfigure(config);

        // Future fails with IllegalStateException naming the phantom cluster and pointing
        // at the ChangeDetector contract as the diagnostic origin.
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join)
                .cause()
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("phantom-cluster")
                .hasMessageContaining("ChangeDetector contract violation");

        // The planner's guard fires BEFORE the orchestrator even constructs an AddCluster,
        // so the registry's addVirtualCluster must not have been invoked.
        verify(registry, never()).addVirtualCluster(any());
    }

    @Test
    void shouldContinueAddingClustersAfterPerClusterFailure() {
        // Two clusters are being added. The registry's addVirtualCluster (bookkeeping) for
        // one of them returns a failed future; the other returns a completed future.
        // Required orchestrator behaviour:
        // - still attempt the second add (failure of one does not abort the loop);
        // - surface the failure as a per-cluster ReconfigureError in the result;
        // - return a successful future overall (ReconfigureResult conveys partial failure).
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-add-fails"), vc("cluster-add-succeeds"));

        var registry = mock(VirtualClusterRegistry.class);
        var failsSpecificFailure = new IllegalStateException("simulated bookkeeping failure on cluster-add-fails");
        when(registry.addVirtualCluster(argThat(m -> m != null && "cluster-add-fails".equals(m.getClusterName()))))
                .thenReturn(CompletableFuture.failedFuture(failsSpecificFailure));
        when(registry.addVirtualCluster(argThat(m -> m != null && "cluster-add-succeeds".equals(m.getClusterName()))))
                .thenReturn(CompletableFuture.completedFuture(null));

        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        // Both adds attempted.
        verify(registry).addVirtualCluster(argThat(m -> m != null && "cluster-add-fails".equals(m.getClusterName())));
        verify(registry).addVirtualCluster(argThat(m -> m != null && "cluster-add-succeeds".equals(m.getClusterName())));

        // Future succeeds (with errors inside the result), not exceptional.
        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-add-fails")
                && r.errors().iterator().next().cause() == failsSpecificFailure);
    }

    @Test
    void shouldContinueRemovingClustersAfterPerClusterFailure() {
        // Two clusters are being removed. The registry's removeVirtualCluster for one of them
        // returns a failed future; the other returns a completed future.
        // Required orchestrator behaviour:
        // - still attempt the second removal (failure of one does not abort the loop);
        // - surface the failure as a per-cluster ReconfigureError in the result;
        // - return a successful future overall (ReconfigureResult conveys partial failure).
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-remove-fails"), vc("cluster-remove-succeeds"));
        var newConfig = configWith(vc("cluster-a")); // removes cluster-remove-fails AND cluster-remove-succeeds

        var registry = mock(VirtualClusterRegistry.class);
        var failsSpecificFailure = new IllegalStateException("simulated drain failure on cluster-remove-fails");
        when(registry.removeVirtualCluster("cluster-remove-fails"))
                .thenReturn(CompletableFuture.failedFuture(failsSpecificFailure));
        when(registry.removeVirtualCluster("cluster-remove-succeeds"))
                .thenReturn(CompletableFuture.completedFuture(null));
        // Stub the model lookup so the planner can resolve models for BOTH removes (the
        // planner refuses to build a RemoveCluster for a cluster the registry doesn't
        // know about — see OperationsPlannerTest#shouldThrowIllegalStateWhenChangeDetectorReportsRemoveForClusterNotInRegistry).
        var failsModel = mock(VirtualClusterModel.class);
        when(failsModel.getClusterName()).thenReturn("cluster-remove-fails");
        when(failsModel.gateways()).thenReturn(Map.of("default", mock(EndpointGateway.class)));
        var succeedsModel = mock(VirtualClusterModel.class);
        when(succeedsModel.getClusterName()).thenReturn("cluster-remove-succeeds");
        when(succeedsModel.gateways()).thenReturn(Map.of("default", mock(EndpointGateway.class)));
        when(registry.modelFor("cluster-remove-fails")).thenReturn(failsModel);
        when(registry.modelFor("cluster-remove-succeeds")).thenReturn(succeedsModel);

        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        // Both removals attempted.
        verify(registry).removeVirtualCluster("cluster-remove-fails");
        verify(registry).removeVirtualCluster("cluster-remove-succeeds");

        // Future succeeds (with errors inside the result), not exceptional.
        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-remove-fails")
                && r.errors().iterator().next().cause() == failsSpecificFailure);
    }

    @Test
    void pureRemoveAdvancesCurrentConfigurationOnSuccess() {
        // After a successful pure-remove, currentConfiguration should advance to the submitted
        // value so subsequent reconfigures use the new baseline. Verified via the captured
        // ConfigurationChangeContext on the detector's subsequent invocation.
        var initialConfig = configWith(vc("cluster-a"), vc("cluster-remove"));
        var afterRemove = configWith(vc("cluster-a")); // removes cluster-remove
        var capturingDetector = mock(ChangeDetector.class);
        // First call: report cluster-remove as removed.
        // Second call: report no changes (we're submitting afterRemove again).
        when(capturingDetector.detect(any())).thenReturn(
                new ChangeResult(Set.of(), Set.of("cluster-remove"), Set.of()),
                ChangeResult.EMPTY);

        var orchestrator = new ConfigurationReloadOrchestrator(
                initialConfig, stubbedRegistry("cluster-a", "cluster-remove"), endpointRegistry, mock(PluginFactoryRegistry.class), List.of(capturingDetector));

        orchestrator.reconfigure(afterRemove).join();
        orchestrator.reconfigure(afterRemove).join();

        var captor = ArgumentCaptor.forClass(ConfigurationChangeContext.class);
        verify(capturingDetector, times(2)).detect(captor.capture());
        var contexts = captor.getAllValues();
        // First call's oldConfig is the constructor-supplied one.
        assertThat(contexts.get(0).oldConfig()).isSameAs(initialConfig);
        // Second call's oldConfig must be the previously-submitted config, proving the
        // baseline advanced through the pure-remove path (not just the no-op early return).
        assertThat(contexts.get(1).oldConfig()).isSameAs(afterRemove);
    }

    // -------- fixture helpers --------

    /**
     * A single shared {@link EndpointRegistry} mock per test, stubbed to succeed for every
     * {@code registerVirtualCluster} call. Tests that need bind failure override this
     * per-test (see {@link #shouldSurfaceErrorAndRollbackWhenGatewayBindFails()}).
     */
    private final EndpointRegistry endpointRegistry = stubbedEndpointRegistry();

    private ConfigurationReloadOrchestrator newOrchestrator(Configuration initial, VirtualClusterRegistry registry) {
        return new ConfigurationReloadOrchestrator(initial, registry, endpointRegistry, mock(PluginFactoryRegistry.class),
                ConfigurationReloadOrchestrator.defaultDetectors());
    }

    /**
     * A {@link VirtualClusterRegistry} mock where the reconfigure operations are stubbed to
     * return a completed future, mirroring the production stub behaviour.
     */
    private static VirtualClusterRegistry stubbedRegistry(String... clustersInOldConfig) {
        var registry = mock(VirtualClusterRegistry.class);
        when(registry.removeVirtualCluster(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(registry.addVirtualCluster(any())).thenReturn(CompletableFuture.completedFuture(null));
        var models = Arrays.stream(clustersInOldConfig).map(name -> {
            var model = mock(VirtualClusterModel.class);
            when(model.getClusterName()).thenReturn(name);
            when(model.gateways()).thenReturn(Map.of("default", mock(EndpointGateway.class)));
            return model;
        }).toList();
        when(registry.virtualClusterModels()).thenReturn(models);
        // The planner uses modelFor(name) for registry-side resolution; stub each known name.
        // Unknown names fall through to Mockito's default (null), which the planner surfaces
        // as a phantom-remove/phantom-modify IllegalStateException.
        for (var model : models) {
            when(registry.modelFor(model.getClusterName())).thenReturn(model);
        }
        return registry;
    }

    private static EndpointRegistry stubbedEndpointRegistry() {
        var registry = mock(EndpointRegistry.class);
        when(registry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
        when(registry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
        return registry;
    }

    private static Configuration configWith(VirtualCluster... clusters) {
        return new Configuration(null, null, null, null, null, List.of(clusters), null, false, Optional.empty(), null, null);
    }

    private static Configuration withDifferentUseIoUring(Configuration base) {
        return new Configuration(base.management(), base.clusterDefinitions(), base.filterDefinitions(), base.defaultFilters(),
                base.routerDefinitions(), base.virtualClusters(), base.micrometer(),
                !base.useIoUring(),
                base.development(), base.network(),
                // also vary proxyProtocol just to make the diff non-empty even if useIoUring matches
                new ProxyProtocolConfig(ProxyProtocolMode.REQUIRED));
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
