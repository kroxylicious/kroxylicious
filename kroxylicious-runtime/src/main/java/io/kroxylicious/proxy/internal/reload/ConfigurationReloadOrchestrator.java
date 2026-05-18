/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.bootstrap.FilterChainFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ConcurrentReconfigureException;
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
 *   <li><b>FilterChainFactory hot-swap</b> &mdash; atomically swap the
 *       {@link FilterChainFactory} reference held by {@code KafkaProxy} on success.</li>
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
    private final PluginFactoryRegistry pfr;
    private final Consumer<FilterChainFactory> filterChainFactorySwap;

    /**
     * The configuration currently applied to the running proxy. Either the initial
     * constructor-supplied configuration, or the result of the most recent successful
     * reconfigure. Read under {@link #reconfigureLock} during the pipeline; the lock also
     * guards updates to this field (none today since the swap step throws).
     */
    private final Configuration currentConfiguration;

    /**
     * Constructs an orchestrator wired into a running proxy.
     *
     * @param initialConfiguration   the configuration applied at proxy startup
     * @param virtualClusterRegistry the registry holding per-VC lifecycles; the orchestrator
     *                               drives its {@code removeVirtualCluster} /
     *                               {@code replaceVirtualCluster} / {@code addVirtualCluster}
     *                               methods (currently no-ops; see class-level Javadoc)
     * @param filterChainFactorySwap a consumer that atomically swaps the proxy's installed
     *                               {@link FilterChainFactory}; not invoked by this PR's
     *                               implementation (the orchestrator throws before reaching
     *                               the swap step)
     * @param pfr                    plugin factory registry used to build new
     *                               {@link FilterChainFactory} instances from filter
     *                               definitions in the submitted configuration
     */
    public ConfigurationReloadOrchestrator(Configuration initialConfiguration,
                                           VirtualClusterRegistry virtualClusterRegistry,
                                           Consumer<FilterChainFactory> filterChainFactorySwap,
                                           PluginFactoryRegistry pfr) {
        this.currentConfiguration = Objects.requireNonNull(initialConfiguration, "initialConfiguration");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.filterChainFactorySwap = Objects.requireNonNull(filterChainFactorySwap, "filterChainFactorySwap");
        this.pfr = Objects.requireNonNull(pfr, "pfr");
        this.staticSectionDiffer = new StaticSectionDiffer();
        this.detectors = List.of(new VirtualClusterChangeDetector(), new FilterChangeDetector());
    }

    /**
     * Apply {@code newConfig} to the running proxy. See the class-level Javadoc for the
     * full pipeline shape and the current incomplete-implementation status.
     *
     * @param newConfig the desired configuration; must not be null
     * @return a future that completes either with a {@link ReconfigureResult} (success path,
     *         not reachable in this PR) or exceptionally with one of:
     *         {@link StaticConfigurationChangedException} (static-section diff),
     *         {@link ConcurrentReconfigureException} (a reconfigure is already in progress)
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

            // 3. Build the new FilterChainFactory. Constructing here exercises plugin
            // initialisation; a thrown exception propagates as a future-exceptional-completion
            // via the catch block below.
            FilterChainFactory newFactory = new FilterChainFactory(pfr, newConfig.filterDefinitions());

            // 4. Aggregate change-detector results.
            var changeResult = aggregateChanges(currentConfiguration, newConfig);

            // 5. Per-VC operations in order: remove -> replace -> add. These are no-ops in
            // this PR — see VirtualClusterRegistry's stub methods and class-level Javadoc.
            var newModelsByName = newConfig.virtualClusterModel(pfr).stream()
                    .collect(Collectors.toMap(VirtualClusterModel::getClusterName, m -> m));

            // TODO (follow-up PR): when the registry methods become real:
            // 1. wrap each .join() in try/catch to accumulate ReconfigureError into a list
            // rather than aborting on the first failure — a failed cluster shouldn't
            // prevent the others from being attempted.
            // 2. parallelise within each phase via CompletableFuture.allOf — operations
            // within a phase are independent; only the phase boundaries (remove →
            // replace → add) must remain ordered.
            // 3. on success: filterChainFactorySwap.accept(newFactory); update
            // currentConfiguration; return ReconfigureResult.of(errors). The throw
            // at step 6 below is what's keeping this PR honest about the missing work.
            for (String name : changeResult.clustersToRemove()) {
                virtualClusterRegistry.removeVirtualCluster(name).join();
            }
            for (String name : changeResult.clustersToModify()) {
                VirtualClusterModel newModel = requireModel(newModelsByName, name);
                virtualClusterRegistry.replaceVirtualCluster(name, newModel).join();
            }
            for (String name : changeResult.clustersToAdd()) {
                VirtualClusterModel newModel = requireModel(newModelsByName, name);
                virtualClusterRegistry.addVirtualCluster(newModel).join();
            }

            // 6. PLACEHOLDER: throw until the follow-up PR implements the swap + result
            // construction. The throw lands here intentionally — every phase above is real
            // and will run on a real call to reconfigure(), but the proxy's state has not
            // yet been mutated (per-VC ops are no-ops; FilterChainFactory is not swapped).
            // Embedders calling reconfigure() therefore see a clear "feature not done"
            // signal rather than a misleading "successful no-op" outcome.
            newFactory.close(); // we built one but won't install it; release plugin resources
            throw new UnsupportedOperationException(
                    "KafkaProxy.reconfigure() per-VC mechanics not yet implemented; coming in follow-up PR. "
                            + "Pre-flight, concurrency, validation, and change detection have completed.");

            // 7. (Follow-up PR) On success:
            // filterChainFactorySwap.accept(newFactory);
            // currentConfiguration = newConfig;
            // return CompletableFuture.completedFuture(ReconfigureResult.of(errors));
        }
        catch (RuntimeException e) {
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

    private static VirtualClusterModel requireModel(Map<String, VirtualClusterModel> modelsByName, String name) {
        VirtualClusterModel model = modelsByName.get(name);
        if (model == null) {
            throw new IllegalStateException("ChangeResult referenced cluster '" + name
                    + "' but no matching model was built from the submitted configuration");
        }
        return model;
    }
}
