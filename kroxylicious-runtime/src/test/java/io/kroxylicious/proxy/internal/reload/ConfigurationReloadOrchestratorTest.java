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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
    void identicalConfigsThrowUnsupportedOperationAtSwapPoint() {
        // given — currentConfig == newConfig, no clusters differ, no static-section diff
        var config = configWith(vc("cluster-a"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(config, registry);

        // when
        var future = orchestrator.reconfigure(config);

        // then — pipeline reaches the placeholder throw
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause()
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("per-VC mechanics not yet implemented");
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
        // The orchestrator invoked the addVirtualCluster stub for the new cluster before the
        // swap-point throw. The other two ops are not called when there's only an addition.
        verify(registry).addVirtualCluster(any());
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
    }

    @Test
    void clusterRemovalInvokesRemoveNoOpThenThrows() {
        // given — old has cluster-a + cluster-b, new has only cluster-a → cluster-b removed
        var oldConfig = configWith(vc("cluster-a"), vc("cluster-b"));
        var newConfig = configWith(vc("cluster-a"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(oldConfig, registry);

        // when
        var future = orchestrator.reconfigure(newConfig);

        // then
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause().isInstanceOf(UnsupportedOperationException.class);
        verify(registry).removeVirtualCluster("cluster-b");
        verify(registry, never()).addVirtualCluster(any());
        verify(registry, never()).replaceVirtualCluster(anyString(), any());
    }

    @Test
    void lockIsReleasedOnExceptionSoSubsequentCallsCanProceed() {
        // After the first reconfigure throws UnsupportedOperationException, a second call
        // must not be rejected with ConcurrentReconfigureException — the lock should be
        // released even on exception via the finally block.
        var config = configWith(vc("cluster-a"));
        var registry = stubbedRegistry();
        var orchestrator = newOrchestrator(config, registry);

        var first = orchestrator.reconfigure(config);
        assertThat(first).isCompletedExceptionally();
        assertThatThrownBy(first::join).cause().isInstanceOf(UnsupportedOperationException.class);

        var second = orchestrator.reconfigure(config);
        assertThat(second).isCompletedExceptionally();
        // Second call should also throw UOE (not ConcurrentReconfigureException).
        assertThatThrownBy(second::join).cause().isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void concurrentReconfigureIsRejected() throws Exception {
        // Hold the reconfigure lock by blocking inside a registry no-op invocation. While the
        // first reconfigure is parked there, a second call from another thread must fail fast
        // with ConcurrentReconfigureException.
        var oldConfig = configWith(vc("cluster-a"));
        var newConfig = configWith(vc("cluster-b"));
        var registry = mock(VirtualClusterRegistry.class);

        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        // removeVirtualCluster will be called for cluster-a — block there.
        when(registry.removeVirtualCluster(anyString())).thenAnswer(inv -> {
            entered.countDown();
            release.await();
            return CompletableFuture.completedFuture(null);
        });
        when(registry.addVirtualCluster(any())).thenReturn(CompletableFuture.completedFuture(null));

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
    void orchestratorDrivesRegistryFromInjectedDetectorOutput() {
        // given — identical old/new configs. The production detectors would return EMPTY here
        // (nothing changed). We inject a custom detector that returns a non-empty ChangeResult
        // anyway, so the only way the orchestrator can drive the registry is if it is in fact
        // consulting the injected detector rather than synthesising its own diff.
        var config = configWith(vc("cluster-a"));
        var customDetector = mock(ChangeDetector.class);
        when(customDetector.detect(any())).thenReturn(new ChangeResult(
                Set.of(), // clustersToAdd
                Set.of(), // clustersToRemove
                Set.of("cluster-a"))); // clustersToModify

        var registry = stubbedRegistry();
        var orchestrator = new ConfigurationReloadOrchestrator(
                config, registry, mock(PluginFactoryRegistry.class), List.of(customDetector));

        // when
        var future = orchestrator.reconfigure(config);

        // then — the orchestrator consulted the injected detector and acted on its verdict.
        verify(customDetector).detect(any());
        verify(registry).replaceVirtualCluster(eq("cluster-a"), any());
        verify(registry, never()).removeVirtualCluster(anyString());
        verify(registry, never()).addVirtualCluster(any());

        // The pipeline still reaches the swap-point placeholder.
        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::join).cause().isInstanceOf(UnsupportedOperationException.class);
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
