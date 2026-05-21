/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
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
import io.kroxylicious.proxy.reload.ConcurrentReconfigureException;
import io.kroxylicious.proxy.reload.StaticConfigurationChangedException;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
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
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
        verify(registry, never()).addVirtualCluster(any());
    }

    @Test
    void clusterAdditionInvokesAddNoOpThenThrows() {
        // given — old has cluster-a, new has cluster-a + cluster-b → addVirtualCluster is called
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause().isInstanceOf(UnsupportedOperationException.class);

        verify(registry, never()).addVirtualCluster(any());
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
    }

    @Test
    void pureRemoveCompletesSuccessfully() {
        // given — old has cluster-a + cluster-b, new has only cluster-a → cluster-b removed.
        // This is a pure-remove reconfigure: step 1 of the hot-reload staircase supports it
        // end-to-end and the orchestrator returns a clean ReconfigureResult rather than
        // throwing the placeholder UOE.
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var newConfig = configWith(vc("cluster-a"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then
        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
        verify(registry).removeVirtualCluster("cluster-b");
        verify(registry, never()).addVirtualCluster(any());
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
    }

    @Test
    void lockIsReleasedOnExceptionSoSubsequentCallsCanProceed() {
        // After the first reconfigure throws UnsupportedOperationException, a second call
        // must not be rejected with ConcurrentReconfigureException — the lock should be
        // released even on exception via the finally block. Use a config with a real
        // cluster change so the orchestrator reaches the placeholder UOE rather than
        // the no-op early-return.
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        var first = orchestrator.reconfigure(newConfig);
        assertThat(first).isCompletedExceptionally();
        assertThatThrownBy(first::join).cause().isInstanceOf(UnsupportedOperationException.class);

        var second = orchestrator.reconfigure(newConfig);
        assertThat(second).isCompletedExceptionally();
        // Second call should also throw UOE (not ConcurrentReconfigureException).
        assertThatThrownBy(second::join).cause().isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void concurrentReconfigureIsRejected() throws Exception {
        // Hold the reconfigure lock by blocking inside a registry no-op invocation. While the
        // first reconfigure is parked there, a second call from another thread must fail fast
        // with ConcurrentReconfigureException.
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var newConfig = configWith(vc("cluster-b"));
        var registry = mock(VirtualClusterRegistry.class);

        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        // removeVirtualCluster will be called for cluster-b — block there.
        when(registry.removeVirtualCluster(anyString())).thenAnswer(inv -> {
            entered.countDown();
            release.await();
            return CompletableFuture.completedFuture(null);
        });

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
                initialConfig, mock(VirtualClusterRegistry.class),
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
        var config = configWith(vc("cluster-a"));
        var customDetector = mock(ChangeDetector.class);
        when(customDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of(), // clustersToAdd
                Set.of("cluster-a"), // clustersToRemove
                Set.of())); // clustersToModify

        var registry = stubbedRegistry();
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, mock(PluginFactoryRegistry.class), List.of(customDetector));

        // when
        var future = orchestrator.reconfigure(config);

        // then — the orchestrator consulted the injected detector and acted on its verdict.
        verify(customDetector).detect(any());
        verify(registry).removeVirtualCluster("cluster-a");
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
        verify(registry, never()).addVirtualCluster(any());

        // Pure-remove reconfigure: orchestrator completes successfully.
        assertThat(future).isCompletedWithValueMatching(r -> !r.hasErrors());
    }

    @Test
    void mixedReconfigureWithRemoveAndAddIsRejectedUpfront() {
        // A reconfigure that BOTH removes and adds clusters is a mixed result. Step 1 of the
        // staircase rejects this upfront with UOE rather than processing the removes and then
        // throwing for the adds (which would leave the proxy in a partial-state outcome).
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var newConfig = configWith(vc("cluster-a"), vc("cluster-c")); // removes cluster-b, adds cluster-c
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause().isInstanceOf(UnsupportedOperationException.class);
        // No registry methods invoked — rejection happens before the per-VC loop.
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).addVirtualCluster(any());
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
    }

    @Test
    void perClusterRemoveFailureSurfacesAsReconfigureErrorAndOthersStillRun() {
        // Two clusters are being removed. The registry's removeVirtualCluster for one of them
        // returns a failed future; the other returns a completed future. The orchestrator must:
        // (a) still attempt the second removal (failure of one does not abort the loop);
        // (b) surface the failure as a per-cluster ReconfigureError in the result;
        // (c) return a successful future overall (ReconfigureResult conveys partial failure).
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-b"), vc("cluster-c"));
        var newConfig = configWith(vc("cluster-a")); // removes cluster-b AND cluster-c

        var registry = mock(VirtualClusterRegistry.class);
        var bSpecificFailure = new IllegalStateException("simulated drain failure on cluster-b");
        when(registry.removeVirtualCluster("cluster-b"))
                .thenReturn(CompletableFuture.failedFuture(bSpecificFailure));
        when(registry.removeVirtualCluster("cluster-c"))
                .thenReturn(CompletableFuture.completedFuture(null));

        var orchestrator = newOrchestrator(oldConfig, registry);

        var future = orchestrator.reconfigure(newConfig);

        // Both removals attempted.
        verify(registry).removeVirtualCluster("cluster-b");
        verify(registry).removeVirtualCluster("cluster-c");

        // Future succeeds (with errors inside the result), not exceptional.
        assertThat(future).isCompletedWithValueMatching(r -> r.hasErrors()
                && r.errors().size() == 1
                && r.errors().iterator().next().humanReadableIdentifier().equals("cluster-b")
                && r.errors().iterator().next().cause() == bSpecificFailure);
    }

    @Test
    void pureRemoveAdvancesCurrentConfigurationOnSuccess() {
        // After a successful pure-remove, currentConfiguration should advance to the submitted
        // value so subsequent reconfigures use the new baseline. Verified via the captured
        // ConfigurationChangeContext on the detector's subsequent invocation.
        var initialConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var afterRemove = configWith(vc("cluster-a")); // removes cluster-b
        var capturingDetector = mock(ChangeDetector.class);
        // First call: report cluster-b as removed.
        // Second call: report no changes (we're submitting afterRemove again).
        when(capturingDetector.detect(any())).thenReturn(
                new ChangeResult(Set.of(), Set.of("cluster-b"), Set.of()),
                ChangeResult.EMPTY);

        var orchestrator = new ConfigurationReloadOrchestrator(
                initialConfig, stubbedRegistry(), mock(PluginFactoryRegistry.class), List.of(capturingDetector));

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

    private ConfigurationReloadOrchestrator newOrchestrator(Configuration initial, VirtualClusterRegistry registry) {
        return new ConfigurationReloadOrchestrator(initial, registry, mock(PluginFactoryRegistry.class),
                ConfigurationReloadOrchestrator.defaultDetectors());
    }

    /**
     * A {@link VirtualClusterRegistry} mock where the three reconfigure operations are all
     * stubbed to return a completed future, mirroring the production stub behaviour. Used in
     * tests that don't need per-call observation of the registry.
     */
    private static VirtualClusterRegistry stubbedRegistry() {
        var registry = mock(VirtualClusterRegistry.class);
        when(registry.removeVirtualCluster(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        when(registry.replaceVirtualCluster(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        when(registry.addVirtualCluster(any())).thenReturn(CompletableFuture.completedFuture(null));
        return registry;
    }

    private static Configuration configWith(VirtualCluster... clusters) {
        return new Configuration(null, null, null, List.of(clusters), null, false,
                Optional.empty(), null, null);
    }

    private static Configuration withDifferentUseIoUring(Configuration base) {
        return new Configuration(base.management(), base.filterDefinitions(), base.defaultFilters(),
                base.virtualClusters(), base.micrometer(),
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
