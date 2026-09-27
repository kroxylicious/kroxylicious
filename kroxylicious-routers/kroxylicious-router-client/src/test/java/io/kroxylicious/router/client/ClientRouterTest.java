/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class ClientRouterTest {

    @Mock
    private RouterFactoryContext context;

    private final ClientRouter router = new ClientRouter();

    private static Rule clientIdRule(String clientId, String route) {
        return new Rule(clientId, route);
    }

    @Test
    void initializeReturnsValidInitializedDataForWellFormedConfig() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        Config config = new Config(List.of(clientIdRule("alice", "team-a"), clientIdRule("carol", "team-b")));

        // When
        ClientRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).hasSize(2);
        assertThat(initialized.rules().get(0).hasClientId()).isEqualTo("alice");
        assertThat(initialized.rules().get(0).useRoute()).isEqualTo("team-a");
        assertThat(initialized.rules().get(1).hasClientId()).isEqualTo("carol");
        assertThat(initialized.rules().get(1).useRoute()).isEqualTo("team-b");
        verify(context).allowSharedClusterTargets();
    }

    @Test
    void initializeSupportsClientIdOnlyRule() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        Config config = new Config(List.of(clientIdRule("demo-app", "team-a")));

        // When
        ClientRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).singleElement().satisfies(compiled -> {
            assertThat(compiled.hasClientId()).isEqualTo("demo-app");
            assertThat(compiled.useRoute()).isEqualTo("team-a");
        });
    }

    @Test
    void initializeThrowsWhenRuleHasRouteButNoClientId() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        Rule rule = new Rule(null, "team-a");
        Config config = new Config(List.of(rule));

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("routingRules entry needs hasClientId");
    }

    @Test
    void initializeThrowsWhenTwoClientIdOnlyRulesHaveIdenticalPredicates() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        Config config = new Config(List.of(clientIdRule("demo-app", "team-a"), clientIdRule("demo-app", "team-b")));

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("more than one rule");
    }

    @Test
    void initializeThrowsWhenRuleReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        Config config = new Config(List.of(clientIdRule("alice", "team-x")));

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }

}
