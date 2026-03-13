/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxyingress;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class IsOpenshiftRouteSupportedActivationConditionTest {

    @Mock
    DependentResource<Route, KafkaProxy> dependentResource;
    @Mock
    KafkaProxy primary;
    @Mock
    Context<KafkaProxy> context;
    @Mock
    KubernetesClient kubernetesClient;

    private AutoCloseable closeable;

    @BeforeEach
    void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    void conditionShouldBeMetByKubernetesServerSupportingRoutes() {
        // Given
        when(context.getClient()).thenReturn(kubernetesClient);
        when(kubernetesClient.supports(Route.class)).thenReturn(true);

        // When
        var met = new IsOpenshiftRouteSupportedActivationCondition().isMet(dependentResource, primary, context);

        // Then
        assertThat(met).isTrue();
    }

    @Test
    void conditionShouldNotBeMetByKubernetesServerNotSupportingRoutes() {
        // Given
        when(context.getClient()).thenReturn(kubernetesClient);
        when(kubernetesClient.supports(Route.class)).thenReturn(false);

        // When
        var met = new IsOpenshiftRouteSupportedActivationCondition().isMet(dependentResource, primary, context);

        // Then
        assertThat(met).isFalse();
    }

}