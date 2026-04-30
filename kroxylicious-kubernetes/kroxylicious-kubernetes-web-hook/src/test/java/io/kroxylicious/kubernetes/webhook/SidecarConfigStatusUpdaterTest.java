/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfig;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KroxyliciousSidecarConfigStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SidecarConfigStatusUpdaterTest {

    private static final Instant NOW = Instant.parse("2026-04-30T12:00:00Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final String NAMESPACE = "test-ns";
    private static final String NAME = "test-config";

    @Mock
    private KubernetesClient client;

    @Mock
    @SuppressWarnings("rawtypes")
    private MixedOperation mixedOp;

    @Mock
    @SuppressWarnings("rawtypes")
    private Resource resource;

    private SidecarConfigStatusUpdater updater;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        updater = new SidecarConfigStatusUpdater(client, FIXED_CLOCK);
    }

    // --- isAlreadyReady tests ---

    @Test
    void isAlreadyReadyReturnsFalseWhenStatusIsNull() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyReturnsFalseWhenConditionsAreNull() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        config.setStatus(new KroxyliciousSidecarConfigStatus());
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyReturnsFalseWhenConditionsAreEmpty() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of());
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyReturnsFalseWhenReadyIsFalse() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.FALSE, 1L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyReturnsFalseWhenObservedGenerationIsOlder() {
        KroxyliciousSidecarConfig config = createConfig(3L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.TRUE, 2L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyReturnsTrueWhenReadyTrueWithMatchingGeneration() {
        KroxyliciousSidecarConfig config = createConfig(5L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.TRUE, 5L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isTrue();
    }

    @Test
    void isAlreadyReadyReturnsTrueWhenObservedGenerationIsNewer() {
        KroxyliciousSidecarConfig config = createConfig(3L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.TRUE, 5L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isTrue();
    }

    @Test
    void isAlreadyReadyReturnsFalseWhenOnlyOtherConditionTypes() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Accepted, Condition.Status.TRUE, 1L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isFalse();
    }

    @Test
    void isAlreadyReadyHandlesNullMetadataGeneration() {
        KroxyliciousSidecarConfig config = createConfig(null);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.TRUE, 0L)));
        config.setStatus(status);
        assertThat(SidecarConfigStatusUpdater.isAlreadyReady(config)).isTrue();
    }

    // --- setReady tests ---

    @Test
    @SuppressWarnings("unchecked")
    void setReadyUpdatesStatusWhenNotAlreadyReady() {
        KroxyliciousSidecarConfig config = createConfig(2L);
        stubClientChain();
        KroxyliciousSidecarConfig serverState = createConfig(2L);
        when(resource.editStatus(any(UnaryOperator.class))).thenAnswer(invocation -> {
            UnaryOperator<KroxyliciousSidecarConfig> op = invocation.getArgument(0);
            return op.apply(serverState);
        });

        updater.setReady(config);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<UnaryOperator<KroxyliciousSidecarConfig>> captor = ArgumentCaptor.forClass(UnaryOperator.class);
        verify(resource).editStatus(captor.capture());

        KroxyliciousSidecarConfig result = captor.getValue().apply(createConfig(2L));
        assertThat(result.getStatus()).isNotNull();
        assertThat(result.getStatus().getObservedGeneration()).isEqualTo(2L);
        assertThat(result.getStatus().getConditions()).hasSize(1);

        Condition condition = result.getStatus().getConditions().get(0);
        assertThat(condition.getType()).isEqualTo(Condition.Type.Ready);
        assertThat(condition.getStatus()).isEqualTo(Condition.Status.TRUE);
        assertThat(condition.getReason()).isEqualTo(SidecarConfigStatusUpdater.REASON_ACCEPTED);
        assertThat(condition.getMessage()).isEmpty();
        assertThat(condition.getLastTransitionTime()).isEqualTo(NOW);
        assertThat(condition.getObservedGeneration()).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void setReadySkipsUpdateWhenAlreadyReady() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        KroxyliciousSidecarConfigStatus status = new KroxyliciousSidecarConfigStatus();
        status.setConditions(List.of(buildCondition(Condition.Type.Ready, Condition.Status.TRUE, 1L)));
        config.setStatus(status);

        updater.setReady(config);

        verify(client, never()).resources(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void setReadySwallowsExceptions() {
        KroxyliciousSidecarConfig config = createConfig(1L);
        stubClientChain();
        when(resource.editStatus(any(UnaryOperator.class)))
                .thenThrow(new RuntimeException("API server error"));

        assertThatCode(() -> updater.setReady(config)).doesNotThrowAnyException();
    }

    // --- helpers ---

    @SuppressWarnings("unchecked")
    private void stubClientChain() {
        when(client.resources(KroxyliciousSidecarConfig.class)).thenReturn(mixedOp);
        when(mixedOp.inNamespace(eq(NAMESPACE))).thenReturn(mixedOp);
        when(mixedOp.withName(eq(NAME))).thenReturn(resource);
    }

    private static KroxyliciousSidecarConfig createConfig(Long generation) {
        KroxyliciousSidecarConfig config = new KroxyliciousSidecarConfig();
        ObjectMeta meta = new ObjectMeta();
        meta.setNamespace(NAMESPACE);
        meta.setName(NAME);
        meta.setGeneration(generation);
        config.setMetadata(meta);
        KroxyliciousSidecarConfigSpec spec = new KroxyliciousSidecarConfigSpec();
        spec.setTargetBootstrapServers("kafka.example.com:9092");
        config.setSpec(spec);
        return config;
    }

    private static Condition buildCondition(
                                            Condition.Type type,
                                            Condition.Status status,
                                            Long observedGeneration) {
        return new ConditionBuilder()
                .withType(type)
                .withStatus(status)
                .withObservedGeneration(observedGeneration)
                .withLastTransitionTime(Instant.now())
                .withReason("Test")
                .withMessage("")
                .build();
    }
}
