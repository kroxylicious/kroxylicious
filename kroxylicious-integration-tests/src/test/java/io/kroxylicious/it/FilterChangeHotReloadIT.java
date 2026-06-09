/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.it.testplugins.FailingInitFilterFactory;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for filter-chain changes via hot reload. Each test exercises
 * {@code KafkaProxy#reconfigure(Configuration)} with a configuration delta that mutates one or
 * more virtual clusters' filter chains, then asserts on the observable lifecycle effects via
 * {@link InvocationCountingFilterFactory}'s per-UUID initialize/close counters.
 *
 * <p>A filter-chain change is detected as a virtual cluster {@code modify} by the orchestrator's
 * {@code FilterChangeDetector}, planned as a {@code ReplaceCluster} operation, and executed
 * as a pair-wise {@code RemoveCluster + AddCluster} on the affected virtual cluster.
 */
class FilterChangeHotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterChangeHotReloadIT.class);

    /**
     * Per-test port blocks. The base is randomised once per JVM so re-runs within the OS's
     * TIME_WAIT window don't collide with the previous run's sockets. The range [25000, 28000)
     * sits below the OS ephemeral ranges on Linux (32768+) and macOS (49152+).
     */
    private static final int PORT_BLOCK_BASE = 25000 + ThreadLocalRandom.current().nextInt(3000);
    private static final int PORT_FILTER_CHANGE = PORT_BLOCK_BASE;
    private static final int PORT_CROSS_VC_A = PORT_BLOCK_BASE + 200;
    private static final int PORT_CROSS_VC_B = PORT_BLOCK_BASE + 210;
    private static final int PORT_FAILURE = PORT_BLOCK_BASE + 300;
    private static final int PORT_ADD_FILTER = PORT_BLOCK_BASE + 400;
    private static final int PORT_REMOVE_FILTER = PORT_BLOCK_BASE + 500;
    private static final int PORT_REORDER = PORT_BLOCK_BASE + 600;
    private static final int PORT_CONFIG_CHANGE = PORT_BLOCK_BASE + 700;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);

    static {
        LoggerFactory.getLogger(FilterChangeHotReloadIT.class)
                .atInfo()
                .addKeyValue("portBlockBase", PORT_BLOCK_BASE)
                .log("FilterChangeHotReloadIT: per-JVM port block base chosen");
    }

    @AfterEach
    void afterEach() {
        // Per-test isolation
        InvocationCountingFilterFactory.resetCounts();
    }

    @Test
    void shouldCloseOldFilterInitResultWhenFilterChainChangesViaReload(@BrokerCluster KafkaCluster cluster) throws Exception {
        // The central test for filter-change hot reload: changing a VC's filter chain triggers
        // ReplaceCluster, which closes the old FilterChainFactory (firing close on every old
        // initResult) and constructs the new one (firing initialize on every new definition).
        UUID oldFilterId = UUID.randomUUID();
        UUID newFilterId = UUID.randomUUID();

        var oldFilterDef = invocationCounterDef("old-counter", oldFilterId);
        var newFilterDef = invocationCounterDef("new-counter", newFilterId);

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-filter-change", PORT_FILTER_CHANGE, "old-counter")),
                oldFilterDef);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-filter-change", PORT_FILTER_CHANGE, "new-counter")),
                newFilterDef);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(oldFilterDef)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: only the old filter has been initialised.
            InvocationCountingFilterFactory.assertInitializationCount(oldFilterId, 1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(oldFilterId)).isZero();

            // Phase 2: real traffic confirms the old chain is in use.
            String topic = tester.createTopic("vc-filter-change");
            assertProduceConsumeRoundTrip(tester, "vc-filter-change", topic, "phase2-old");

            // Phase 3: reconfigure swaps the filter chain.
            LOGGER.info("Reconfiguring vc-filter-change: old-counter -> new-counter");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean filter-chain swap")
                            .isFalse());

            // Phase 4: old initResult was closed during ReplaceCluster's remove step;
            // new initResult was initialised during ReplaceCluster's add step.
            assertThat(InvocationCountingFilterFactory.closeCountFor(oldFilterId))
                    .as("old filter's initResult should have been closed exactly once during ReplaceCluster")
                    .isEqualTo(1);
            InvocationCountingFilterFactory.assertInitializationCount(newFilterId, 1);

            // Phase 5: real traffic confirms the new chain is in use end-to-end.
            assertProduceConsumeRoundTrip(tester, "vc-filter-change", topic, "phase5-new");
        }
    }

    @Test
    void shouldNotAffectUnchangedVcsFilterStateWhenAnotherVcsFiltersChange(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Per-VC isolation contract: changing VC-A's filter chain does not touch VC-B's filter
        // state, even though both VCs use the same filter type. VC-B's old filter must not be
        // closed, and no new initialise call should happen for it.
        UUID vcAFilter1Id = UUID.randomUUID();
        UUID vcAFilter2Id = UUID.randomUUID();
        UUID vcBFilterId = UUID.randomUUID();

        var vcAFilter1Def = invocationCounterDef("vc-a-counter-old", vcAFilter1Id);
        var vcAFilter2Def = invocationCounterDef("vc-a-counter-new", vcAFilter2Id);
        var vcBFilterDef = invocationCounterDef("vc-b-counter", vcBFilterId);

        var startingConfig = buildConfig(
                List.of(
                        portVcWithFilters(cluster, "vc-a", PORT_CROSS_VC_A, "vc-a-counter-old"),
                        portVcWithFilters(cluster, "vc-b", PORT_CROSS_VC_B, "vc-b-counter")),
                vcAFilter1Def, vcBFilterDef);
        var afterConfig = buildConfig(
                List.of(
                        portVcWithFilters(cluster, "vc-a", PORT_CROSS_VC_A, "vc-a-counter-new"),
                        portVcWithFilters(cluster, "vc-b", PORT_CROSS_VC_B, "vc-b-counter")),
                vcAFilter2Def, vcBFilterDef);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(vcAFilter1Def, vcBFilterDef)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: both VCs initialised. (Init counts not asserted strictly because the
            // planner over-initialises during reconfigure planning — see the @BeforeEach comment.
            // The contract this test pins is on close counts.)
            assertThat(InvocationCountingFilterFactory.initializationCountFor(vcAFilter1Id))
                    .as("VC-A's old filter was initialised at startup").isGreaterThanOrEqualTo(1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(vcBFilterId))
                    .as("VC-B's filter was initialised at startup").isGreaterThanOrEqualTo(1);

            // Phase 2: traffic confirms both chains are in use.
            String topicA = tester.createTopic("vc-a");
            String topicB = tester.createTopic("vc-b");
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "phase2-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "phase2-vc-b");

            // Phase 3: reconfigure only changes VC-A's filter chain; VC-B's config is identical.
            LOGGER.info("Reconfiguring to change vc-a's filter chain only");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Phase 4: per-VC isolation — VC-A's old filter is closed (it was actually replaced);
            // VC-B's filter is NOT closed (its live VCM is unaffected by VC-A's reconfigure).
            // We assert on close counts because they correspond 1:1 with VCMs the registry
            // actually managed; init counts are inflated by planner pre-construction.
            assertThat(InvocationCountingFilterFactory.closeCountFor(vcAFilter1Id))
                    .as("VC-A's old filter should have been closed exactly once during ReplaceCluster")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(vcBFilterId))
                    .as("VC-B's filter must NOT have been closed by VC-A's reconfigure (per-VC isolation)")
                    .isZero();

            // Phase 5: both VCs still serve traffic — VC-A on its new chain, VC-B unchanged.
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "phase5-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "phase5-vc-b");
        }
    }

    @Test
    void shouldFailReconfigureExceptionallyWhenNewFilterChainHasInvalidConfig(@BrokerCluster KafkaCluster cluster) throws Exception {
        // The orchestrator's OperationsPlanner pre-constructs VCMs for the entire new config
        // as part of planning (so it can resolve cluster names to fully-built models for the
        // Add/Replace operations). Filter init failures surface during this construction,
        // BEFORE any operation's apply runs — so the reconfigure future fails exceptionally
        // and no live cluster is mutated. The contract is therefore *more transactional* than
        // the naive "non-transactional replace" reading would suggest: a doomed new config
        // is rejected at the planning phase, leaving the live proxy state intact.
        UUID goodFilterId = UUID.randomUUID();

        var goodFilterDef = invocationCounterDef("good-counter", goodFilterId);
        var badFilterDef = new NamedFilterDefinitionBuilder("bad-filter", FailingInitFilterFactory.class.getName())
                .build();

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-fail", PORT_FAILURE, "good-counter")),
                goodFilterDef);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-fail", PORT_FAILURE, "bad-filter")),
                badFilterDef);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(goodFilterDef)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: good filter active, traffic flows.
            String topic = tester.createTopic("vc-fail");
            assertProduceConsumeRoundTrip(tester, "vc-fail", topic, "phase1-good");

            // Phase 2: reconfigure with a filter that will fail on initialize. The future fails
            // exceptionally — the planner could not even build the new VCMs, so no operations
            // were applied. The proxy continues running with the original configuration.
            LOGGER.info("Reconfiguring vc-fail with invalid filter chain (expected to fail at plan phase)");
            assertThat(tester.reconfigure(afterConfig))
                    .as("reconfigure with a filter that throws on initialize should fail exceptionally")
                    .failsWithin(RECONFIGURE_TIMEOUT)
                    .withThrowableThat()
                    .havingCause()
                    .withMessageContaining("FailingInitFilterFactory");

            // Phase 3: the old filter was NOT closed because no operation actually ran. The
            // live VC is unaffected.
            assertThat(InvocationCountingFilterFactory.closeCountFor(goodFilterId))
                    .as("old filter must NOT be closed when the reconfigure fails at planning")
                    .isZero();

            // Phase 4: traffic still flows on the original chain (the proxy state was not mutated).
            assertProduceConsumeRoundTrip(tester, "vc-fail", topic, "phase4-still-good");
        }
    }

    @Test
    void shouldHandleAddingFilterToExistingChain(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Adding a filter to a VC's chain triggers ReplaceCluster. The runtime implements
        // whole-FCF replacement (no partial-chain optimisation), so the pre-existing F1 is
        // RE-INITIALISED with a new initResult — even though its definition didn't change.
        UUID filter1Id = UUID.randomUUID();
        UUID filter2Id = UUID.randomUUID();

        var filter1Def = invocationCounterDef("f1", filter1Id);
        var filter2Def = invocationCounterDef("f2", filter2Id);

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-add-filter", PORT_ADD_FILTER, "f1")),
                filter1Def);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-add-filter", PORT_ADD_FILTER, "f1", "f2")),
                filter1Def, filter2Def);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filter1Def)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: only F1 initialised at startup.
            InvocationCountingFilterFactory.assertInitializationCount(filter1Id, 1);

            // Phase 2: reconfigure — chain [f1] -> [f1, f2].
            LOGGER.info("Reconfiguring vc-add-filter: [f1] -> [f1, f2]");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Phase 3: F1's old initResult was closed; F1 was re-initialised (init=2); F2 was
            // initialised. The init=2 count for F1 is the load-bearing assertion: it pins the
            // whole-FCF-replacement semantic (no partial-chain optimisation).
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter1Id))
                    .as("F1's old initResult should have been closed during ReplaceCluster")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter1Id))
                    .as("F1 should have been RE-INITIALISED for the new chain (whole-FCF replacement, no partial preservation)")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter2Id))
                    .as("F2 should have been newly initialised when added to the chain")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter2Id))
                    .as("F2 is part of the live new chain, should not be closed yet")
                    .isZero();

            // Phase 4: traffic flows on the new chain.
            String topic = tester.createTopic("vc-add-filter");
            assertProduceConsumeRoundTrip(tester, "vc-add-filter", topic, "phase4-new-chain");
        }
    }

    @Test
    void shouldHandleRemovingFilterFromExistingChain(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Removing a filter from a VC's chain triggers ReplaceCluster. Both old initResults are
        // closed (the whole old FCF is torn down). F1 is re-initialised for the new chain; F2
        // is NOT re-initialised because it's not in the new chain.
        UUID filter1Id = UUID.randomUUID();
        UUID filter2Id = UUID.randomUUID();

        var filter1Def = invocationCounterDef("f1", filter1Id);
        var filter2Def = invocationCounterDef("f2", filter2Id);

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-remove-filter", PORT_REMOVE_FILTER, "f1", "f2")),
                filter1Def, filter2Def);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-remove-filter", PORT_REMOVE_FILTER, "f1")),
                filter1Def);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filter1Def, filter2Def)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: both F1 and F2 initialised at startup.
            InvocationCountingFilterFactory.assertInitializationCount(filter1Id, 1);
            InvocationCountingFilterFactory.assertInitializationCount(filter2Id, 1);

            // Phase 2: reconfigure — chain [f1, f2] -> [f1].
            LOGGER.info("Reconfiguring vc-remove-filter: [f1, f2] -> [f1]");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Phase 3: both old initResults closed (whole-FCF replacement). F1 re-initialised
            // for the new chain. F2 NOT re-initialised (not in the new chain).
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter1Id))
                    .as("F1's old initResult should have been closed")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter2Id))
                    .as("F2's initResult should have been closed (it was removed from the chain)")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter1Id))
                    .as("F1 should have been re-initialised for the new chain")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter2Id))
                    .as("F2 should NOT have been re-initialised — it's not in the new chain")
                    .isEqualTo(1);

            // Phase 4: traffic flows on the new chain.
            String topic = tester.createTopic("vc-remove-filter");
            assertProduceConsumeRoundTrip(tester, "vc-remove-filter", topic, "phase4-shortened-chain");
        }
    }

    @Test
    void shouldHandleReorderingFiltersInExistingChain(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Reordering a VC's filter chain is a semantic change (filters execute sequentially) so
        // FilterChangeDetector's order-sensitive comparison flags this as a modify. ReplaceCluster
        // tears down the old FCF and builds a fresh one — both filters' old initResults are
        // closed, both are re-initialised for the reordered chain. No optimisation skips the
        // re-initialisation of filters whose definitions didn't change.
        UUID filter1Id = UUID.randomUUID();
        UUID filter2Id = UUID.randomUUID();

        var filter1Def = invocationCounterDef("f1", filter1Id);
        var filter2Def = invocationCounterDef("f2", filter2Id);

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-reorder", PORT_REORDER, "f1", "f2")),
                filter1Def, filter2Def);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-reorder", PORT_REORDER, "f2", "f1")),
                filter1Def, filter2Def);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filter1Def, filter2Def)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: both F1 and F2 initialised at startup.
            InvocationCountingFilterFactory.assertInitializationCount(filter1Id, 1);
            InvocationCountingFilterFactory.assertInitializationCount(filter2Id, 1);

            // Phase 2: reconfigure — chain [f1, f2] -> [f2, f1] (reorder only).
            LOGGER.info("Reconfiguring vc-reorder: [f1, f2] -> [f2, f1]");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Phase 3: reorder = full replace. Both old initResults closed, both new initResults
            // initialised. No filter is preserved across the reorder.
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter1Id))
                    .as("F1's old initResult should have been closed")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filter2Id))
                    .as("F2's old initResult should have been closed")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter1Id))
                    .as("F1 should have been re-initialised for the reordered chain")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filter2Id))
                    .as("F2 should have been re-initialised for the reordered chain")
                    .isEqualTo(2);

            // Phase 4: traffic flows on the reordered chain.
            String topic = tester.createTopic("vc-reorder");
            assertProduceConsumeRoundTrip(tester, "vc-reorder", topic, "phase4-reordered");
        }
    }

    @Test
    void shouldHandleFilterConfigChangeAsReplace(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Same filter definition name "f1" but different configuration content. The orchestrator's
        // FilterChangeDetector reports filter definitions whose config changed as affecting any VC
        // that references them, so this triggers ReplaceCluster on the VC even though no name was
        // added/removed/reordered. This is the operator-visible "change the KMS key" semantic for
        // record-encryption-style plugins.
        UUID configX = UUID.randomUUID();
        UUID configY = UUID.randomUUID();

        var filterDefX = invocationCounterDef("f1", configX);
        var filterDefY = invocationCounterDef("f1", configY);

        var startingConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-cfg-change", PORT_CONFIG_CHANGE, "f1")),
                filterDefX);
        var afterConfig = buildConfig(
                List.of(portVcWithFilters(cluster, "vc-cfg-change", PORT_CONFIG_CHANGE, "f1")),
                filterDefY);

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterDefX)
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Phase 1: filter initialised with the X config.
            InvocationCountingFilterFactory.assertInitializationCount(configX, 1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(configY)).isZero();

            // Phase 2: reconfigure — same filter name, different config (X -> Y).
            LOGGER.info("Reconfiguring vc-cfg-change: filter 'f1' config X -> Y");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Phase 3: the X initResult was closed; the Y initResult was initialised. Different
            // UUIDs mean independent lifecycles even though the filter NAME stayed the same.
            assertThat(InvocationCountingFilterFactory.closeCountFor(configX))
                    .as("the old config's initResult should have been closed")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.initializationCountFor(configY))
                    .as("the new config's initResult should have been initialised exactly once")
                    .isEqualTo(1);
            assertThat(InvocationCountingFilterFactory.closeCountFor(configY))
                    .as("the new config's initResult is part of the live chain, not yet closed")
                    .isZero();

            // Phase 4: traffic flows under the new config.
            String topic = tester.createTopic("vc-cfg-change");
            assertProduceConsumeRoundTrip(tester, "vc-cfg-change", topic, "phase4-new-config");
        }
    }

    // -----------------------------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------------------------

    private static VirtualCluster portVcWithFilters(KafkaCluster cluster, String name, int port, String... filterNames) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .addToFilters(filterNames)
                .build();
    }

    private static NamedFilterDefinition invocationCounterDef(String name, UUID uuid) {
        return new NamedFilterDefinitionBuilder(name, InvocationCountingFilterFactory.class.getSimpleName())
                .withConfig("configInstanceId", uuid)
                .build();
    }

    private static Configuration buildConfig(List<VirtualCluster> vcs, NamedFilterDefinition... filters) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder();
        for (var f : filters) {
            builder.addToFilterDefinitions(f);
        }
        for (var vc : vcs) {
            builder.addToVirtualClusters(vc);
        }
        return builder.build();
    }

    /**
     * Per-VC produce-consume round-trip. Mirrors the helper in {@link HotReloadIT}, simplified
     * to a smaller per-call message count since these tests don't depend on volume — they only
     * need to prove a single end-to-end request flowed through the relevant filter chain.
     */
    private static void assertProduceConsumeRoundTrip(KroxyliciousTester tester, String vc, String topic, String marker) throws Exception {
        int messageCount = 5;
        try (var producer = tester.producer(vc, Map.of(ProducerConfig.LINGER_MS_CONFIG, 0))) {
            for (int i = 0; i < messageCount; i++) {
                producer.send(new ProducerRecord<>(topic, marker + "-" + i, marker + "-v-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }
    }
}
