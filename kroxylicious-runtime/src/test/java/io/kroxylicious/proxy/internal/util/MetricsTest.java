/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static io.micrometer.core.instrument.Metrics.globalRegistry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricsTest {

    private SimpleMeterRegistry simpleMeterRegistry;

    @BeforeEach
    void setUp() {
        simpleMeterRegistry = new SimpleMeterRegistry();
        globalRegistry.add(simpleMeterRegistry);
    }

    @AfterEach
    void tearDown() {
        if (simpleMeterRegistry != null) {
            simpleMeterRegistry.getMeters().forEach(globalRegistry::remove);
            globalRegistry.remove(simpleMeterRegistry);
        }
    }

    @Test
    void shouldBuildTagList() {
        // Given

        // When
        List<Tag> tags = Metrics.tags();

        // Then
        assertThat(tags).isEmpty();
    }

    @Test
    void shouldBuildTagListWithValue() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1");

        // Then
        assertThat(tags).containsExactly(Tag.of("TagA", "value1"));
    }

    @Test
    void shouldBuildTagListWithTwoTags() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1",
                "TagB", "value2");

        // Then
        assertThat(tags).containsExactly(
                Tag.of("TagA", "value1"),
                Tag.of("TagB", "value2"));
    }

    @Test
    void shouldBuildTagListWithMultipleTags() {
        // Given

        // When
        List<Tag> tags = Metrics.tags("TagA", "value1",
                "TagB", "value2",
                "TagC", "value3");

        // Then
        assertThat(tags).containsExactly(
                Tag.of("TagA", "value1"),
                Tag.of("TagB", "value2"),
                Tag.of("TagC", "value3"));
    }

    @Test
    void shouldThrowIfTagNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfTwoTagNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfSeveralTagsNameHasNoValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", "value2", "TagC", "value3", "TagD"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfTagNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", " "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfTwoTagNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", ""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIfOneOfSeveralTagsNameHasEmptyValue() {
        // Given

        // When
        // Then
        assertThatThrownBy(() -> Metrics.tags("TagA", "value1", "TagB", "value2", "TagC", "value3", "TagD", "   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void disconnectsCounterShouldIncludeCauseTag() {
        // Given
        var meterProvider = Metrics.clientToProxyDisconnectsCounter("test-cluster", 1, "idle_timeout");
        var counter = meterProvider.withTags();

        // When
        counter.increment();

        // Then
        assertThat(counter.getId().getTag("cause")).isEqualTo("idle_timeout");
        assertThat(counter.getId().getTag("virtual_cluster")).isEqualTo("test-cluster");
        assertThat(counter.getId().getTag("node_id")).isEqualTo("1");
        assertThat(counter.count()).isEqualTo(1.0);
    }

    @Test
    void disconnectsCounterShouldSupportAllCauses() {
        // Given
        var clusterName = "cluster";
        // When
        var idleCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, null, "idle_timeout").withTags();
        var clientClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, null, "client_closed").withTags();
        var serverClosedCounter = Metrics.clientToProxyDisconnectsCounter(clusterName, null, "server_closed").withTags();

        idleCounter.increment();
        clientClosedCounter.increment();
        serverClosedCounter.increment();

        // Then
        assertThat(simpleMeterRegistry.get("kroxylicious_client_to_proxy_disconnects")
                .tag("virtual_cluster", clusterName)
                .tag("node_id", "bootstrap")
                .tag("cause", "idle_timeout")
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
        assertThat(simpleMeterRegistry.get("kroxylicious_client_to_proxy_disconnects")
                .tag("virtual_cluster", clusterName)
                .tag("node_id", "bootstrap")
                .tag("cause", "client_closed")
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
        assertThat(simpleMeterRegistry.get("kroxylicious_client_to_proxy_disconnects")
                .tag("virtual_cluster", clusterName)
                .tag("node_id", "bootstrap")
                .tag("cause", "server_closed")
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
    }

    @Test
    void clientAuthCounterShouldIncludeAllTags() {
        // Given
        var clusterName = "my-cluster";
        var mechanism = "SCRAM-SHA-512";
        var outcome = "success";
        var counter = Metrics.clientAuthCounter(clusterName, mechanism, outcome);

        // When
        counter.increment();

        // Then
        assertThat(simpleMeterRegistry.get("kroxylicious_client_auth_total")
                .tag("virtual_cluster", clusterName)
                .tag("mechanism", mechanism)
                .tag("outcome", outcome)
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
    }

    @Test
    void clientAuthCounterShouldDistinguishOutcomes() {
        // Given
        var clusterName = "cluster";
        var mechanism = "SCRAM-SHA-256";
        var successCounter = Metrics.clientAuthCounter(clusterName, mechanism, "success");
        var failureCounter = Metrics.clientAuthCounter(clusterName, mechanism, "failure");

        // When
        successCounter.increment();
        failureCounter.increment();

        // Then
        assertThat(simpleMeterRegistry.get("kroxylicious_client_auth_total")
                .tag("virtual_cluster", clusterName)
                .tag("mechanism", mechanism)
                .tag("outcome", "success")
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
        assertThat(simpleMeterRegistry.get("kroxylicious_client_auth_total")
                .tag("virtual_cluster", clusterName)
                .tag("mechanism", mechanism)
                .tag("outcome", "failure")
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
    }

    @Test
    void clientAuthCounterShouldDistinguishMechanisms() {
        // Given
        var clusterName = "cluster";
        var outcome = "success";
        var scramCounter = Metrics.clientAuthCounter(clusterName, "SCRAM-SHA-256", outcome);
        var oauthCounter = Metrics.clientAuthCounter(clusterName, "OAUTHBEARER", outcome);

        // When
        scramCounter.increment();
        oauthCounter.increment();
        oauthCounter.increment();

        // Then
        assertThat(simpleMeterRegistry.get("kroxylicious_client_auth_total")
                .tag("virtual_cluster", clusterName)
                .tag("mechanism", "SCRAM-SHA-256")
                .tag("outcome", outcome)
                .counter())
                .extracting(Counter::count)
                .isEqualTo(1.0);
        assertThat(simpleMeterRegistry.get("kroxylicious_client_auth_total")
                .tag("virtual_cluster", clusterName)
                .tag("mechanism", "OAUTHBEARER")
                .tag("outcome", outcome)
                .counter())
                .extracting(Counter::count)
                .isEqualTo(2.0);
    }
}
