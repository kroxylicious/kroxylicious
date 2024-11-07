/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;

class DeploymentReadyConditionTest {

    @Mock
    DependentResource<Deployment, io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy> dependentResource;
    @Mock
    KafkaProxy primary;
    @Mock
    Context<KafkaProxy> context;

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
    void conditionShouldBeMetByReadyDeployment() {
        // Given
        doReturn(Optional.of(new DeploymentBuilder().withNewStatus().withReadyReplicas(1).withReplicas(1).endStatus().build()))
                .when(dependentResource).getSecondaryResource(primary, context);

        // When
        var met = new DeploymentReadyCondition().isMet(dependentResource, primary, context);

        // Then
        assertThat(met).isTrue();
    }

    @Test
    void conditionShouldNotBeMetByUnreadyDeployment() {
        // Given
        doReturn(Optional.of(new DeploymentBuilder().withNewStatus().withReadyReplicas(0).withReplicas(1).endStatus().build()))
                .when(dependentResource).getSecondaryResource(primary, context);

        // When
        var met = new DeploymentReadyCondition().isMet(dependentResource, primary, context);

        // Then
        assertThat(met).isFalse();
    }

    @Test
    void conditionShouldNotBeMetByMissingDeployment() {
        // Given
        doReturn(Optional.empty()).when(dependentResource).getSecondaryResource(primary, context);

        // When
        var met = new DeploymentReadyCondition().isMet(dependentResource, primary, context);

        // Then
        assertThat(met).isFalse();
    }

}