/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ConcurrentReconfigureException;
import io.kroxylicious.proxy.reload.ReconfigureError;
import io.kroxylicious.proxy.reload.ReconfigureResult;
import io.kroxylicious.proxy.reload.StaticConfigurationChangedException;

/**
 * Internal class that owns the {@code KafkaProxy.reconfigure(Configuration)} pipeline.
 * Not part of any public API; embedders interact with the proxy only via
 * {@code KafkaProxy.reconfigure()}, which delegates to this class privately.
 *
 * <p>The orchestrator's responsibilities (per the hot-reload design):
 * <ul>
 *   <li><b>Pre-flight validation</b> &mdash; reject submissions that differ in any static
 *       configuration section, via {@link StaticSectionDiffer}. The result is an exceptional
 *       completion with {@link StaticConfigurationChangedException}; the proxy's running
 *       state is unchanged.</li>
 *   <li><b>Concurrency control</b> &mdash; serialise overlapping reconfigure calls. A second
 *       call arriving while one is in flight completes exceptionally with
 *       {@link ConcurrentReconfigureException}; the trigger is expected to retry.</li>
 *   <li><b>Change detection</b> &mdash; delegate to the {@link ChangeDetector} pipeline
 *       (see {@link VirtualClusterChangeDetector}, {@link FilterChangeDetector}) to identify
 *       added/removed/modified clusters.</li>
 *   <li><b>Per-VC change execution</b> &mdash; drive {@link VirtualClusterRegistry} to apply
 *       the detected changes in {@code remove &rarr; replace &rarr; add} order via its
 *       per-VC lifecycle transitions.</li>
 *   <li><b>Result construction</b> &mdash; accumulate per-component outcomes into a
 *       {@link ReconfigureResult} and complete the returned future.</li>
 * </ul>
 *
 */
public class ConfigurationReloadOrchestrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationReloadOrchestrator.class);

    private final ReentrantLock reconfigureLock = new ReentrantLock();
    private final List<ChangeDetector> detectors;
    private final StaticSectionDiffer staticSectionDiffer;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;
    private final PluginFactoryRegistry pfr;

    /**
     * The configuration currently applied to the running proxy. Either the initial
     * constructor-supplied configuration, or the result of the most recent successful
     * reconfigure. Both reads and writes happen while {@link #reconfigureLock} is held,
     * which makes this field effectively single-writer / single-reader at any instant —
     * no additional synchronisation is required on top of the lock.
     */
    private Configuration currentConfiguration;

    /**
     * Constructs an orchestrator wired into a running proxy.
     *
     * @param initialConfiguration   the configuration applied at proxy startup
     * @param virtualClusterRegistry the registry holding per-VC lifecycles; the orchestrator
     *                               drives its {@code removeVirtualCluster} /
     *                               {@code replaceVirtualCluster} / {@code addVirtualCluster}
     *                               methods (currently no-ops; see class-level Javadoc)
     * @param endpointRegistry       the proxy's endpoint registry; the orchestrator calls
     *                               {@code registerVirtualCluster} / {@code deregisterVirtualCluster}
     *                               on it when applying per-VC add/remove operations
     * @param pfr                    plugin factory registry, used by change detectors and
     *                               by filter-chain reconciliation
     * @param detectors              the change-detector pipeline to drive; production wiring
     *                               passes {@link #defaultDetectors()}, tests can pass stubs
     *                               to drive the orchestrator with a controlled
     *                               {@link ChangeResult}
     */
    public ConfigurationReloadOrchestrator(Configuration initialConfiguration,
                                           VirtualClusterRegistry virtualClusterRegistry,
                                           EndpointRegistry endpointRegistry,
                                           PluginFactoryRegistry pfr,
                                           List<ChangeDetector> detectors) {
        this.currentConfiguration = Objects.requireNonNull(initialConfiguration, "initialConfiguration");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        this.pfr = Objects.requireNonNull(pfr, "pfr");
        this.staticSectionDiffer = new StaticSectionDiffer();
        this.detectors = List.copyOf(Objects.requireNonNull(detectors, "detectors"));
    }

    /**
     * The production-default set of change detectors:
     * {@link VirtualClusterChangeDetector} and {@link FilterChangeDetector}.
     */
    public static List<ChangeDetector> defaultDetectors() {
        return List.of(new VirtualClusterChangeDetector(), new FilterChangeDetector());
    }

    /**
     * Apply {@code newConfig} to the running proxy. See the class-level Javadoc for the
     * full pipeline shape and the current incomplete-implementation status.
     *
     * @param newConfig the desired configuration; must not be null
     * @return a future that completes:
     *         <ul>
     *           <li>successfully with an empty-errors {@link ReconfigureResult} when the
     *               submitted configuration produces no changes (a no-op reconfigure);
     *               {@code currentConfiguration} is updated to the submitted value</li>
     *           <li>successfully with a {@link ReconfigureResult} (possibly with per-cluster
     *               errors) when the submitted configuration only removes and/or adds
     *               virtual clusters (no modifies); {@code currentConfiguration} is updated
     *               to the submitted value</li>
     *           <li>exceptionally with {@link StaticConfigurationChangedException} when the
     *               submitted configuration differs from the current one in any static
     *               section</li>
     *           <li>exceptionally with {@link ConcurrentReconfigureException} when another
     *               reconfigure is already in progress</li>
     *           <li>exceptionally with {@link UnsupportedOperationException} when the
     *               submitted configuration would modify any virtual cluster </li>
     *         </ul>
     */
    public CompletableFuture<ReconfigureResult> reconfigure(Configuration newConfig) {
        Objects.requireNonNull(newConfig, "newConfig");

        // 1. Concurrency control: tryLock rather than block, so a second concurrent caller
        // gets a fast rejection rather than queueing
        if (!reconfigureLock.tryLock()) {
            LOGGER.atWarn().log("reconfigure rejected: another reconfigure is already in progress");
            return CompletableFuture.failedFuture(new ConcurrentReconfigureException());
        }
        try {
            // 2. Pre-flight: reject submissions that differ in any static section.
            var staticDiffs = staticSectionDiffer.diff(currentConfiguration, newConfig);
            if (!staticDiffs.isEmpty()) {
                LOGGER.atWarn()
                        .addKeyValue("differingSections", staticDiffs)
                        .log("reconfigure rejected: static configuration sections differ");
                return CompletableFuture.failedFuture(new StaticConfigurationChangedException(staticDiffs));
            }

            // 3. Aggregate change-detector results.
            var changeResult = aggregateChanges(currentConfiguration, newConfig);

            // 4. No-op early return: if no clusters were added, removed, or modified, record
            // the submitted configuration as currently-applied and return a clean result.
            // This is the operator-observable "reload triggered, nothing to do" outcome — a
            // reload tool can round-trip a no-op reconfigure cleanly without provoking the
            // per-VC placeholder below.
            if (changeResult.isEmpty()) {
                this.currentConfiguration = newConfig;
                return CompletableFuture.completedFuture(ReconfigureResult.of(List.of()));
            }

            // 5. Mixed-reconfigure guard. Modify operations would land at the per-VC modify
            // placeholder below, which is still a no-op stub. Reject submissions that require
            // any modify rather than partially-applying the adds/removes around it.
            if (!changeResult.clustersToModify().isEmpty()) {
                throw new UnsupportedOperationException(
                        "KafkaProxy.reconfigure() does not yet support modify operations. "
                                + "Pre-flight, concurrency, validation, and change detection have completed; "
                                + "this reconfigure was rejected because it would have required "
                                + changeResult.clustersToModify().size() + " cluster modify operation(s).");
            }

            // 6. Per-VC remove. SEQUENTIAL. Errors are accumulated into a per-cluster list
            // and surfaced via the ReconfigureResult; a failed cluster does not prevent
            // subsequent removes from being attempted.
            var errors = new ArrayList<ReconfigureError>();
            for (String name : changeResult.clustersToRemove()) {
                removeCluster(name, errors);
            }

            // 7. Per-VC add. SEQUENTIAL, after removes. Same error-accumulation contract:
            // a failed add does not prevent subsequent adds from being attempted. Adds run
            // after removes so that endpoint conflicts produced by swap-style edits resolve
            // in the right order.
            if (!changeResult.clustersToAdd().isEmpty()) {
                var newModelsByName = resolveModelsByName(newConfig);
                for (String name : changeResult.clustersToAdd()) {
                    addCluster(name, newModelsByName.get(name), errors);
                }
            }

            // 8. Commit. currentConfiguration advances to the submitted value
            this.currentConfiguration = newConfig;
            return CompletableFuture.completedFuture(ReconfigureResult.of(errors));
        }
        catch (RuntimeException e) {
            LOGGER.atError()
                    .setCause(e)
                    .addKeyValue("error", e.getMessage())
                    .log("reconfigure failed");
            return CompletableFuture.failedFuture(e);
        }
        finally {
            reconfigureLock.unlock();
        }
    }

    private ChangeResult aggregateChanges(Configuration oldConfig, Configuration newConfig) {
        var context = new ConfigurationChangeContext(oldConfig, newConfig);
        return detectors.stream()
                .map(d -> d.detect(context))
                .reduce(ChangeResult.EMPTY, ChangeResult::merge);
    }

    /**
     * Attempt to remove a single virtual cluster.
     */
    private void removeCluster(String clusterName, List<ReconfigureError> errors) {
        try {
            virtualClusterRegistry.removeVirtualCluster(clusterName).join();
        }
        catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            LOGGER.atWarn()
                    .setCause(cause)
                    .addKeyValue("virtualCluster", clusterName)
                    .addKeyValue("error", cause.getMessage())
                    .log("reconfigure: failed to remove virtual cluster");
            errors.add(new ReconfigureError(clusterName, cause));
        }
    }

    /**
     * Attempt to add a single virtual cluster. Mirrors {@code KafkaProxy.start()}'s wiring:
     * <ol>
     *   <li>Ask the registry to create the lifecycle in {@code INITIALIZING}.</li>
     *   <li>Bind each gateway via {@link EndpointRegistry#registerVirtualCluster}.</li>
     *   <li>On success, transition the lifecycle to {@code SERVING} via
     *       {@link VirtualClusterRegistry#initializationSucceeded}.</li>
     *   <li>On bind failure, transition to {@code STOPPED} via
     *       {@link VirtualClusterRegistry#initializationFailed} and best-effort deregister each
     *       gateway to roll back any partial registration.</li>
     * </ol>
     * Mirrors {@link #removeCluster}'s error-accumulation contract: failures are recorded
     * but do not stop subsequent adds.
     */
    private void addCluster(String clusterName, VirtualClusterModel newModel, List<ReconfigureError> errors) {
        try {
            // Step 1: pure bookkeeping — creates the lifecycle in INITIALIZING.
            virtualClusterRegistry.addVirtualCluster(newModel).join();
            // Step 2 (+ rollback on failure): bind gateways and transition the lifecycle.
            bindGatewaysAndTransitionToServing(clusterName, newModel, errors);
        }
        catch (RuntimeException e) {
            Throwable cause = unwrap(e);
            LOGGER.atWarn()
                    .setCause(cause)
                    .addKeyValue("virtualCluster", clusterName)
                    .addKeyValue("error", cause.getMessage())
                    .log("reconfigure: failed to create lifecycle for virtual cluster");
            errors.add(new ReconfigureError(clusterName, cause));
        }
    }

    /**
     * Register every gateway for {@code newModel}. On success, transitions the cluster's
     * lifecycle to {@code SERVING}. On failure, drives the lifecycle to {@code STOPPED}
     * via {@code FAILED}, issues best-effort deregister for every gateway (some may not
     * have been registered yet — that's fine, {@code deregisterVirtualCluster} is idempotent),
     * and adds a {@link ReconfigureError} to {@code errors}.
     */
    private void bindGatewaysAndTransitionToServing(String clusterName, VirtualClusterModel newModel, List<ReconfigureError> errors) {
        List<EndpointGateway> gateways = List.copyOf(newModel.gateways().values());
        var bindFutures = gateways.stream()
                .map(g -> endpointRegistry.registerVirtualCluster(g).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(bindFutures).join();
        }
        catch (RuntimeException bindError) {
            Throwable cause = unwrap(bindError);
            LOGGER.atWarn()
                    .setCause(cause)
                    .addKeyValue("virtualCluster", clusterName)
                    .addKeyValue("error", cause.getMessage())
                    .log("reconfigure: gateway registration failed; rolling back");
            virtualClusterRegistry.initializationFailed(clusterName, cause);
            for (var g : gateways) {
                endpointRegistry.deregisterVirtualCluster(g);
            }
            errors.add(new ReconfigureError(clusterName, cause));
            return;
        }
        virtualClusterRegistry.initializationSucceeded(clusterName);
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException ce && ce.getCause() != null ? ce.getCause() : t;
    }

    private Map<String, VirtualClusterModel> resolveModelsByName(Configuration newConfig) {
        return newConfig.virtualClusterModel(pfr).stream()
                .collect(Collectors.toUnmodifiableMap(VirtualClusterModel::getClusterName, m -> m));
    }
}
