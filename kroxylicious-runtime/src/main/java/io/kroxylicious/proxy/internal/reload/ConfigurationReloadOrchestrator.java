/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.reload.ConcurrentReconfigureException;
import io.kroxylicious.proxy.reload.ReconfigureError;
import io.kroxylicious.proxy.reload.ReconfigureResult;
import io.kroxylicious.proxy.reload.StaticConfigurationChangedException;
import io.kroxylicious.proxy.tag.VisibleForTesting;

/**
 * Internal class that owns the {@code KafkaProxy.reconfigure(Configuration)} pipeline.
 * Not part of any public API; embedders interact with the proxy only via
 * {@code KafkaProxy.reconfigure()}, which delegates here privately.
 *
 * <p>This class is a coordinator. The mechanics of applying changes to individual virtual
 * clusters live in {@link ClusterOperation} implementations under the {@code operations}
 * subpackage. The orchestrator's job is sequencing:
 * <ul>
 *   <li><b>Concurrency control</b> — serialise overlapping reconfigure calls via
 *       {@link ReentrantLock#tryLock()}; a second concurrent call gets
 *       {@link ConcurrentReconfigureException} immediately.</li>
 *   <li><b>Pre-flight validation</b> — reject submissions that differ in any static section
 *       via {@link StaticSectionDiffer}, surfacing {@link StaticConfigurationChangedException}.</li>
 *   <li><b>Change detection</b> — delegate to the {@link ChangeDetector} pipeline to compute
 *       the set of per-VC adds/removes/modifies.</li>
 *   <li><b>Planning</b> — delegate to {@link OperationsPlanner} for the ordered list of
 *       per-VC operations.</li>
 *   <li><b>Execution</b> — apply each operation, aggregating per-cluster errors into the
 *       {@link ReconfigureResult}.</li>
 *   <li><b>Commit</b> — advance {@code currentConfiguration} to the submitted value.</li>
 * </ul>
 *
 */
public class ConfigurationReloadOrchestrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationReloadOrchestrator.class);

    private static final String LOG_KEY_ERROR = "error";

    private static final String OUTCOME_SUCCESS = "success";
    private static final String OUTCOME_PARTIAL_FAILURE = "partial_failure";
    private static final String OUTCOME_CATASTROPHIC = "catastrophic";
    private static final String OUTCOME_FAILURE = "failure";

    private final ReentrantLock reconfigureLock = new ReentrantLock();
    private final List<ChangeDetector> detectors;
    private final StaticSectionDiffer staticSectionDiffer = new StaticSectionDiffer();
    private final OperationsPlanner planner;

    /**
     * The configuration currently applied to the running proxy. Reads and writes happen
     * under {@link #reconfigureLock}, making this field effectively single-writer /
     * single-reader at any instant — no additional synchronisation needed.
     */
    private Configuration currentConfiguration;

    public ConfigurationReloadOrchestrator(Configuration initialConfiguration,
                                           VirtualClusterRegistry virtualClusterRegistry,
                                           EndpointRegistry endpointRegistry,
                                           List<ChangeDetector> detectors) {
        this(initialConfiguration, detectors,
                new OperationsPlanner(virtualClusterRegistry, endpointRegistry, virtualClusterRegistry::resolveModel));
    }

    /**
     * Test seam: lets unit tests inject a pre-configured {@link OperationsPlanner} (or a
     * mock) without resolving plugin-factory-backed models.
     */
    @VisibleForTesting
    ConfigurationReloadOrchestrator(Configuration initialConfiguration,
                                    List<ChangeDetector> detectors,
                                    OperationsPlanner planner) {
        this.currentConfiguration = Objects.requireNonNull(initialConfiguration, "initialConfiguration");
        this.detectors = List.copyOf(Objects.requireNonNull(detectors, "detectors"));
        this.planner = Objects.requireNonNull(planner, "planner");
    }

    /**
     * The production-default set of change detectors:
     * {@link VirtualClusterChangeDetector} and {@link FilterChangeDetector}.
     */
    public static List<ChangeDetector> defaultDetectors() {
        return List.of(new VirtualClusterChangeDetector(), new FilterChangeDetector(), new RouterChangeDetector(), new ClusterDefinitionChangeDetector());
    }

    /**
     * Apply {@code newConfig} to the running proxy.
     *
     * @return a future that completes:
     *         <ul>
     *           <li>successfully with an empty-errors {@link ReconfigureResult} on a no-op
     *               reconfigure ({@code currentConfiguration} still advances)</li>
     *           <li>successfully with a {@link ReconfigureResult} (possibly with per-cluster
     *               errors) on any reconfigure ({@code currentConfiguration} advances
     *               unconditionally — see {@link #commit} for rationale)</li>
     *           <li>exceptionally with {@link StaticConfigurationChangedException} on
     *               static-section diff</li>
     *           <li>exceptionally with {@link ConcurrentReconfigureException} on
     *               concurrent submission</li>
     *         </ul>
     */
    public CompletableFuture<ReconfigureResult> reconfigure(Configuration newConfig) {
        Objects.requireNonNull(newConfig, "newConfig");

        if (!reconfigureLock.tryLock()) {
            LOGGER.atWarn().log("reconfigure rejected: another reconfigure is already in progress");
            return CompletableFuture.failedFuture(new ConcurrentReconfigureException());
        }
        try {
            return doReconfigure(newConfig);
        }
        catch (RuntimeException e) {
            LOGGER.atError()
                    .setCause(e)
                    .addKeyValue(LOG_KEY_ERROR, e.getMessage())
                    .log("reconfigure failed");
            Metrics.reconfigureCounter(OUTCOME_CATASTROPHIC).increment();
            return CompletableFuture.failedFuture(e);
        }
        finally {
            reconfigureLock.unlock();
        }
    }

    private CompletableFuture<ReconfigureResult> doReconfigure(Configuration newConfig) {
        var staticDiffs = staticSectionDiffer.diff(currentConfiguration, newConfig);
        if (!staticDiffs.isEmpty()) {
            LOGGER.atWarn()
                    .addKeyValue("differingSections", staticDiffs)
                    .log("reconfigure rejected: static configuration sections differ");
            return CompletableFuture.failedFuture(new StaticConfigurationChangedException(staticDiffs));
        }

        // Time the attempted reconfigure end-to-end. The body is synchronous (each operation
        // blocks internally), so the sample captures real wall-clock duration; the finally
        // records it even when planning/applying throws (the catastrophic path).
        var sample = Timer.start();
        try {
            var changes = aggregateChanges(currentConfiguration, newConfig);
            if (changes.isEmpty()) {
                Metrics.reconfigureCounter(OUTCOME_SUCCESS).increment();
                return commit(newConfig, List.of());
            }

            var errors = planner.plan(changes, newConfig).stream()
                    .map(this::applyAndCount)
                    .flatMap(Optional::stream)
                    .toList();

            Metrics.reconfigureCounter(errors.isEmpty() ? OUTCOME_SUCCESS : OUTCOME_PARTIAL_FAILURE).increment();
            return commit(newConfig, errors);
        }
        finally {
            sample.stop(Metrics.reconfigureDurationTimer());
        }
    }

    /**
     * Applies one operation and records the {@code clusters_affected} counter for it, tagged by
     * operation kind and per-cluster outcome.
     */
    private Optional<ReconfigureError> applyAndCount(ClusterOperation operation) {
        var error = operation.apply();
        Metrics.reconfigureClustersAffectedCounter(operation.operation().label(),
                error.isPresent() ? OUTCOME_FAILURE : OUTCOME_SUCCESS).increment();
        return error;
    }

    /**
     * Commits {@code newConfig} as the currently-applied configuration and returns a
     * result-future. The commit happens unconditionally on any non-exceptional completion,
     * including results that carry per-cluster errors — the proxy's view of what was
     * <em>attempted</em> always advances. Tests / operators inspect {@link ReconfigureResult}
     * to learn whether every individual operation succeeded.
     */
    private CompletableFuture<ReconfigureResult> commit(Configuration newConfig, List<ReconfigureError> errors) {
        this.currentConfiguration = newConfig;
        return CompletableFuture.completedFuture(ReconfigureResult.of(errors));
    }

    private ChangeResult aggregateChanges(Configuration oldConfig, Configuration newConfig) {
        var context = new ConfigurationChangeContext(oldConfig, newConfig);
        return detectors.stream()
                .map(d -> d.detect(context))
                .reduce(ChangeResult.EMPTY, ChangeResult::merge);
    }
}
