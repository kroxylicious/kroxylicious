/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;
import io.kroxylicious.kubernetes.operator.assertj.AssertFactory;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

class ProxyReconcilerTest {

    @Mock
    Context<KafkaProxy> context;

    @Mock
    ManagedDependentResourceContext mdrc;

    private AutoCloseable closeable;

    @BeforeEach
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    void successfulInitialReconciliationShouldResultInReadyTrueCondition() {
        // Given
        // @formatter:off
        long generation = 42L;
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotNull();
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void failedInitialReconciliationShouldResultInReadyTrueCondition() {
        // Given
        // @formatter:off
        long generation = 42L;
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(generation)
                .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotNull();
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo("InvalidResourceException");
    }

    @Test
    void remainInReadyTrueShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void transitionToReadyFalseShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull().isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo(InvalidResourceException.class.getSimpleName());
    }

    @Test
    void remainInReadyFalseShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                 .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.FALSE)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).updateErrorStatus(primary, context, new InvalidResourceException("Resource was terrible"));

        // Then
        assertThat(updateControl.isPatch()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull().isPresent().get()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.FALSE);
        first.extracting(Conditions::getMessage).isEqualTo("Resource was terrible");
        first.extracting(Conditions::getReason).isEqualTo(InvalidResourceException.class.getSimpleName());
    }

    @Test
    void transitionToReadyTrueShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.FALSE)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource()).isNotNull()
                .extracting(KafkaProxy::getStatus);
        statusAssert.extracting(KafkaProxyStatus::getObservedGeneration).isEqualTo(generation);
        ObjectAssert<Conditions> first = statusAssert.extracting(KafkaProxyStatus::getConditions, InstanceOfAssertFactories.list(Conditions.class))
                .first();
        first.extracting(Conditions::getObservedGeneration).isEqualTo(generation);
        first.extracting(Conditions::getLastTransitionTime).isNotEqualTo(time);
        first.extracting(Conditions::getType).isEqualTo("Ready");
        first.extracting(Conditions::getStatus).isEqualTo(Conditions.Status.TRUE);
        first.extracting(Conditions::getMessage).isEqualTo("");
        first.extracting(Conditions::getReason).isEqualTo("");
    }

    @Test
    void transitionToReadyFalseShouldChangeTransitionTime2() {
        // Given
        long generation = 42L;
        var time = ZonedDateTime.now(ZoneId.of("Z"));
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                .endMetadata()
                .withNewSpec()
                    .addNewCluster()
                        .withName("my-cluster")
                    .endCluster()
                .endSpec()
                .withNewStatus()
                    .addNewCondition()
                        .withType("Ready")
                        .withStatus(Conditions.Status.TRUE)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(time)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        doReturn(mdrc).when(context).managedDependentResourceContext();
        doReturn(Optional.of(Map.of("my-cluster", ClusterCondition.filterNotExists("my-cluster", "MissingFilter")))).when(mdrc).get(SharedKafkaProxyContext.ERROR_KEY,
                Map.class);
        doReturn(new RuntimeDecl(List.of())).when(mdrc).getMandatory(SharedKafkaProxyContext.RUNTIME_DECL_KEY, Map.class);

        // When
        var updateControl = new ProxyReconciler(new RuntimeDecl(List.of())).reconcile(primary, context);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotNull()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.observedGeneration().isEqualTo(generation);
        statusAssert.singleCondition()
                .isReady()
                .hasObservedGeneration(generation)
                .lastTransitionTimeIsEqualTo(time);
        statusAssert.singleCluster()
                .nameIsEqualTo("my-cluster")
                .singleCondition()
                .isAcceptedFalse("Invalid", "Filter \"MissingFilter\" does not exist.")
                .hasObservedGeneration(generation);
        // TODO .lastTransitionTimeIsEqualTo(time);

    }

}
