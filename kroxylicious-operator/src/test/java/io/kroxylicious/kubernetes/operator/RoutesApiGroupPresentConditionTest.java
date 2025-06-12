/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.APIGroup;
import io.fabric8.kubernetes.api.model.APIGroupBuilder;
import io.fabric8.kubernetes.api.model.GroupVersionForDiscovery;
import io.fabric8.kubernetes.api.model.GroupVersionForDiscoveryBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RoutesApiGroupPresentConditionTest {

    @Mock
    Context<KafkaProxy> context;
    @Mock
    KubernetesClient client;

    RoutesApiGroupPresentCondition<Route, KafkaProxy> routesApiGroupPresentCondition = new RoutesApiGroupPresentCondition<>();

    @BeforeEach
    void setup() {
        RoutesApiGroupPresentCondition.reset();
        when(context.getClient()).thenReturn(client);
    }

    @Test
    public void routesApiGroupNotPresentInCluster() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(null);
        // when
        boolean met = isMet();
        // then
        assertThat(met).isFalse();
    }

    @Test
    public void negativeResultIsCached() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(null);
        isMet();
        boolean secondCall = isMet();
        // then
        assertThat(secondCall).isFalse();
        verify(client, times(1)).getApiGroup("route.openshift.io");
    }

    @Test
    public void positiveResultIsCached() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(apiGroupWithVersions("v1"));
        isMet();
        boolean secondCall = isMet();
        // then
        assertThat(secondCall).isTrue();
        verify(client, times(1)).getApiGroup("route.openshift.io");
    }

    @Test
    public void canRecoverFromInitialClientException() {
        // given
        RuntimeException exception = new RuntimeException("boom!");
        when(client.getApiGroup("route.openshift.io")).thenThrow(exception);
        assertThatThrownBy(this::isMet).isEqualTo(exception);
        doReturn(apiGroupWithVersions("v1")).when(client).getApiGroup("route.openshift.io");

        // when
        boolean secondCall = isMet();
        // then
        assertThat(secondCall).isTrue();
    }

    @Test
    public void routesApiGroupPresentInClusterWithMismatchedVersion() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(apiGroupWithVersions("v2"));
        // when
        boolean met = isMet();
        // then
        assertThat(met).isFalse();
    }

    @Test
    public void routesApiGroupPresentInClusterWithExpectedVersion() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(apiGroupWithVersions("v1"));
        // when
        boolean met = isMet();
        // then
        assertThat(met).isTrue();
    }

    @Test
    public void routesApiGroupPresentInClusterWithMultipleVersions() {
        // given
        when(client.getApiGroup("route.openshift.io")).thenReturn(apiGroupWithVersions("v0", "v1"));
        // when
        boolean met = isMet();
        // then
        assertThat(met).isTrue();
    }

    private static APIGroup apiGroupWithVersions(String... versions) {
        List<GroupVersionForDiscovery> groupVersions = Arrays.stream(versions).map(v -> new GroupVersionForDiscoveryBuilder().withVersion(v).build()).toList();
        return new APIGroupBuilder().withVersions(groupVersions).build();
    }

    private boolean isMet() {
        return routesApiGroupPresentCondition.isMet(new OpenShiftRouteDependentResource(), new KafkaProxy(), context);
    }

}