/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.InvalidResourceException;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.StaleReferentStatusException;
import io.kroxylicious.kubernetes.operator.assertj.AssertFactory;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class KafkaProxyReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    @Mock(strictness = Mock.Strictness.LENIENT)
    Context<KafkaProxy> reconcilerContext;

    @Mock
    ManagedWorkflowAndDependentResourceContext workdflowContext;

    private AutoCloseable closeable;

    @BeforeEach
    void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
        when(reconcilerContext.managedWorkflowAndDependentResourceContext()).thenReturn(workdflowContext);
    }

    @AfterEach
    void releaseMocks() throws Exception {
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
                    .withUid(UUID.randomUUID().toString())
                    .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = newKafkaProxyReconciler(TEST_CLOCK).reconcile(primary, reconcilerContext);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .conditionList()
                .containsOnlyTypes(Condition.Type.Ready)
                .singleOfType(Condition.Type.Ready)
                .isReadyTrue();
        assertThat(updateControl.isPatchResource()).isFalse();
        assertThat(updateControl.getResource().get())
                .satisfies(kp -> OperatorAssertions.assertThat(kp).doesNotHaveAnnotation(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY));
    }

    @Test
    void failedInitialReconciliationShouldResultInReadyTrueCondition() {
        // Given
        // @formatter:off
        long generation = 42L;
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                .withGeneration(generation)
                .withName("my-proxy")
                .endMetadata()
                .build();
        // @formatter:on

        // When
        var updateControl = newKafkaProxyReconciler(TEST_CLOCK)
                .updateErrorStatus(proxy, reconcilerContext, new InvalidResourceException("Resource was terrible"));

        // Then
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .singleCondition()
                .hasObservedGeneration(generation)
                .hasLastTransitionTime(TEST_CLOCK.instant())
                .hasType(Condition.Type.Ready)
                .hasStatus(Condition.Status.UNKNOWN)
                .hasReason(InvalidResourceException.class.getName())
                .hasMessage("Resource was terrible");

    }

    @Test
    void remainInReadyTrueShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withUid(UUID.randomUUID().toString())
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.TRUE)
                        .withObservedGeneration(generation)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(TEST_CLOCK.instant())
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on

        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime).reconcile(primary, reconcilerContext);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .conditionList()
                .containsOnlyTypes(Condition.Type.Ready)
                .singleOfType(Condition.Type.Ready)
                .isReadyTrue();
    }

    @Test
    void transitionToReadyUnknownShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withObservedGeneration(generation)
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.TRUE)
                        .withObservedGeneration(generation)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(TEST_CLOCK.instant())
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime)
                .updateErrorStatus(primary, reconcilerContext, new InvalidResourceException("Resource was terrible"));

        // Then
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .singleCondition()
                .hasObservedGeneration(generation)
                .hasLastTransitionTime(reconciliationTime.instant())
                .hasType(Condition.Type.Ready)
                .hasStatus(Condition.Status.UNKNOWN)
                .hasReason(InvalidResourceException.class.getName())
                .hasMessage("Resource was terrible");
    }

    @Test
    void remainInReadyUnknownShouldRetainTransitionTime() {
        // Given
        long generation = 42L;
        // @formatter:off
        Instant originalInstant = TEST_CLOCK.instant();
        var primary = new KafkaProxyBuilder()
                 .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.UNKNOWN)
                        .withObservedGeneration(generation)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(originalInstant)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime)
                .updateErrorStatus(primary, reconcilerContext, new InvalidResourceException("Resource was terrible"));

        // Then
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());

        statusAssert.hasObservedGeneration(generation)
                .singleCondition()
                .hasObservedGeneration(generation)
                .hasLastTransitionTime(originalInstant)
                .hasType(Condition.Type.Ready)
                .hasStatus(Condition.Status.UNKNOWN)
                .hasReason(InvalidResourceException.class.getName())
                .hasMessage("Resource was terrible");
    }

    @Test
    void staleReferentStatusExceptionResultsInNoStatusUpdate() {
        // Given
        long generation = 42L;
        // @formatter:off
        Instant originalInstant = TEST_CLOCK.instant();
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.UNKNOWN)
                        .withObservedGeneration(generation)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(originalInstant)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime)
                .updateErrorStatus(primary, reconcilerContext, new StaleReferentStatusException("stale virtualcluster status"));

        // Then
        assertThat(updateControl.getResource()).isEmpty();
    }

    @Test
    void staleReferentStatusExceptionCausingOperatorExceptionResultsInNoStatusUpdate() {
        // Given
        long generation = 42L;
        // @formatter:off
        Instant originalInstant = TEST_CLOCK.instant();
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.UNKNOWN)
                        .withObservedGeneration(generation)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withLastTransitionTime(originalInstant)
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime)
                .updateErrorStatus(primary, reconcilerContext, new OperatorException(new StaleReferentStatusException("stale virtualcluster status")));

        // Then
        assertThat(updateControl.getResource()).isEmpty();
    }

    @Test
    void transitionToReadyTrueShouldChangeTransitionTime() {
        // Given
        long generation = 42L;
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withUid(UUID.randomUUID().toString())
                    .withName("my-proxy")
                .endMetadata()
                .withNewStatus()
                    .addNewCondition()
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.FALSE)
                        .withMessage("Resource was terrible")
                        .withReason(InvalidResourceException.class.getSimpleName())
                        .withObservedGeneration(generation)
                        .withLastTransitionTime(TEST_CLOCK.instant())
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime).reconcile(proxy, reconcilerContext);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .conditionList()
                .containsOnlyTypes(Condition.Type.Ready)
                .singleOfType(Condition.Type.Ready)
                .isReadyTrue();
    }

    @Test
    void transitionToReadyFalseShouldChangeTransitionTime2() {
        // Given
        long generation = 42L;
        // @formatter:off
        var primary = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withUid(UUID.randomUUID().toString())
                    .withName("my-proxy")
                    .withNamespace("my-ns")
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                    .addNewCondition()
                        .withObservedGeneration(generation)
                        .withType(Condition.Type.Ready)
                        .withStatus(Condition.Status.TRUE)
                        .withObservedGeneration(generation)
                        .withMessage("")
                        .withReason("")
                        .withLastTransitionTime(TEST_CLOCK.instant())
                    .endCondition()
                .endStatus()
                .build();
        // @formatter:on
        Clock reconciliationTime = Clock.offset(TEST_CLOCK, Duration.ofSeconds(1));
        doReturn(workdflowContext).when(reconcilerContext).managedWorkflowAndDependentResourceContext();
        doReturn(Set.of(new VirtualKafkaClusterBuilder().withNewMetadata().withName("my-cluster").withNamespace("my-ns").endMetadata().withNewSpec().withNewProxyRef()
                .withName("my-proxy").endProxyRef().endSpec().build())).when(reconcilerContext).getSecondaryResources(VirtualKafkaCluster.class);

        // When
        var updateControl = newKafkaProxyReconciler(reconciliationTime).reconcile(primary, reconcilerContext);

        // Then
        assertThat(updateControl.isPatchStatus()).isTrue();
        var statusAssert = assertThat(updateControl.getResource())
                .isNotEmpty().get()
                .extracting(KafkaProxy::getStatus, AssertFactory.status());
        statusAssert.hasObservedGeneration(generation)
                .conditionList()
                .containsOnlyTypes(Condition.Type.Ready)
                .singleOfType(Condition.Type.Ready)
                .isReadyTrue();
    }

    @NonNull
    private static KafkaProxyReconciler newKafkaProxyReconciler(Clock reconciliationTime) {
        return new KafkaProxyReconciler(reconciliationTime, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);
    }

    @Test
    void shouldIncludeReadyReplicaCountInStatus() {
        // Given
        //@formatter:off
        KafkaProxy proxy = proxyBuilder("proxy")
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();
        //@formatter:on

        //@formatter:off
        var deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName("deployment")
                .endMetadata()
                .withNewStatus()
                    .withReplicas(3)
                    .withReadyReplicas(2)
                .endStatus()
                .build();
        //@formatter:on

        when(reconcilerContext.getSecondaryResource(Deployment.class, KafkaProxyReconciler.DEPLOYMENT_DEP)).thenReturn(Optional.of(deployment));

        // When
        UpdateControl<KafkaProxy> updateControl = newKafkaProxyReconciler(TEST_CLOCK).reconcile(proxy, reconcilerContext);

        // Then
        assertThat(updateControl).isNotNull()
                .extracting(UpdateControl::getResource, InstanceOfAssertFactories.OPTIONAL)
                .isPresent()
                .get(InstanceOfAssertFactories.type(KafkaProxy.class))
                .satisfies(kp -> {
                    assertThat(kp).isNotNull();
                    assertThat(kp.getStatus())
                            .isNotNull()
                            .extracting(KafkaProxyStatus::getReplicas)
                            .isEqualTo(2);
                });
    }

    @Test
    void shouldHandleDeploymentWithoutStatus() {
        // Given
        //@formatter:off
        KafkaProxy proxy = proxyBuilder("proxy")
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();
        //@formatter:on

        //@formatter:off
        var deployment = new DeploymentBuilder()
                .withNewMetadata()
                    .withName("deployment")
                .endMetadata()
                .build();
        //@formatter:on

        when(reconcilerContext.getSecondaryResource(Deployment.class, KafkaProxyReconciler.DEPLOYMENT_DEP)).thenReturn(Optional.of(deployment));

        // When
        UpdateControl<KafkaProxy> updateControl = newKafkaProxyReconciler(TEST_CLOCK).reconcile(proxy, reconcilerContext);

        // Then
        assertThat(updateControl).isNotNull()
                .extracting(UpdateControl::getResource, InstanceOfAssertFactories.OPTIONAL)
                .isPresent()
                .get(InstanceOfAssertFactories.type(KafkaProxy.class))
                .satisfies(kp -> {
                    assertThat(kp).isNotNull();
                    assertThat(kp.getStatus())
                            .isNotNull()
                            .extracting(KafkaProxyStatus::getReplicas)
                            .isEqualTo(0); // No status implies no replicas running
                });
    }

    @Test
    void shouldHandleMissingDeployment() {
        // Given
        //@formatter:off
        KafkaProxy proxy = proxyBuilder("proxy")
                .withNewSpec()
                    .withReplicas(3)
                .endSpec()
                .build();
        //@formatter:on

        when(reconcilerContext.getSecondaryResource(Deployment.class, KafkaProxyReconciler.DEPLOYMENT_DEP)).thenReturn(Optional.empty());

        // When
        UpdateControl<KafkaProxy> updateControl = newKafkaProxyReconciler(TEST_CLOCK).reconcile(proxy, reconcilerContext);

        // Then
        assertThat(updateControl).isNotNull()
                .extracting(UpdateControl::getResource, InstanceOfAssertFactories.OPTIONAL)
                .isPresent()
                .get(InstanceOfAssertFactories.type(KafkaProxy.class))
                .satisfies(kp -> {
                    assertThat(kp).isNotNull();
                    assertThat(kp.getStatus())
                            .isNotNull()
                            .extracting(KafkaProxyStatus::getReplicas)
                            .isEqualTo(0); // No status implies no replicas running
                });
    }

    @Test
    void testDeriveStoreTypeFromKeySuffix() {
        TrustAnchorRef trustAnchorRef = new TrustAnchorRefBuilder()
                .withKey("key.pem")
                .withRef(new AnyLocalRefBuilder()
                        .withName("test")
                        .withKind("ConfigMap")
                        .build())
                .build();

        String storeType = KafkaProxyReconciler.deriveStoreTypeFromKeySuffix(trustAnchorRef);
        assertThat(storeType).isEqualTo("PEM");
    }

    private KafkaProxyBuilder proxyBuilder(String name) {
        return new KafkaProxyBuilder().withNewMetadata().withName(name).endMetadata();
    }

}
