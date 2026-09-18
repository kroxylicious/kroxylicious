/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.bootstrap.RandomBootstrapSelectionStrategy;
import io.kroxylicious.proxy.bootstrap.RoundRobinBootstrapSelectionStrategy;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.internal.routing.UpstreamClusterModel;
import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ClusterDefinitionTest {

    @Mock
    Tls tls;

    @Test
    void shouldCreateValidDefinition() {
        var def = new ClusterDefinition("my-cluster", "broker1:9092,broker2:9092", null);

        assertThat(def.name()).isEqualTo("my-cluster");
        assertThat(def.bootstrapServers()).isEqualTo("broker1:9092,broker2:9092");
        assertThat(def.tls()).isNull();
    }

    @Test
    void shouldStripWhitespaceFromBootstrapServers() {
        var def = new ClusterDefinition("c1", "broker1:9092 , broker2:9092", null);

        assertThat(def.bootstrapServers()).isEqualTo("broker1:9092,broker2:9092");
    }

    @Test
    void shouldRejectNullName() {
        assertThatThrownBy(() -> new ClusterDefinition(null, "broker:9092", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
    }

    @Test
    void shouldRejectNullBootstrapServers() {
        assertThatThrownBy(() -> new ClusterDefinition("c1", null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("bootstrapServers");
    }

    @Test
    void toTargetClusterWithoutTls() {
        var def = new ClusterDefinition("c1", "broker:9092", null);

        var target = def.toTargetCluster();

        assertThat(target.bootstrapServers()).isEqualTo("broker:9092");
        assertThat(target.tls()).isEmpty();
    }

    @Test
    void toTargetClusterWithTls() {
        var def = new ClusterDefinition("c1", "broker:9092", tls);

        var target = def.toTargetCluster();

        assertThat(target.bootstrapServers()).isEqualTo("broker:9092");
        assertThat(target.tls()).contains(tls);
    }

    @Test
    void toTargetClusterPassesSelectionStrategyThrough() {
        // Given
        var strategy = new RandomBootstrapSelectionStrategy();
        var def = new ClusterDefinition("c1", "broker:9092", null, strategy);

        // When
        var target = def.toTargetCluster();

        // Then
        assertThat(target.selectionStrategy()).isEqualTo(strategy);
    }

    @Test
    void toTargetClusterWithoutSelectionStrategy() {
        // Given
        var def = new ClusterDefinition("c1", "broker:9092", null);

        // When
        var target = def.toTargetCluster();

        // Then
        assertThat(target.selectionStrategy()).isNull();
        assertThat(target.effectiveSelectionStrategy()).isInstanceOf(RoundRobinBootstrapSelectionStrategy.class);
    }

    @Test
    void upstreamClusterModelsDerivedFromTheSameDefinitionShouldHaveIndependentBootstrapSelectionState() {
        // Given
        var def = new ClusterDefinition("c1", "broker1:9092,broker2:9092", null, new RoundRobinBootstrapSelectionStrategy());
        var first = UpstreamClusterModel.build(def.toTargetCluster(), null);
        var second = UpstreamClusterModel.build(def.toTargetCluster(), null);
        first.bootstrapServer();

        // When
        var secondSelection = second.bootstrapServer();

        // Then
        assertThat(secondSelection).isEqualTo(new HostPort("broker1", 9092));
    }
}
