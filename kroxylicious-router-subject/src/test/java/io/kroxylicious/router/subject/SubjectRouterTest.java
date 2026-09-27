/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubjectRouterTest {

    @Mock
    private RouterFactoryContext context;

    @Mock
    private RouteSelector<Object> selector;

    private final SubjectRouter router = new SubjectRouter();

    @Test
    void initializeReturnsValidInitializedDataWhenSelectorRoutesAreDeclared() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        when(context.pluginInstance(eq(RouteSelector.class), eq("UserNameMatch"))).thenReturn(selector);
        when(selector.referencedRoutes()).thenReturn(CompletableFuture.completedFuture(Optional.of(Set.of("team-a"))));
        SubjectRouter.Config config = new SubjectRouter.Config("UserNameMatch", null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.selector()).isSameAs(selector);
        assertThat(initialized.routeNames()).containsExactlyInAnyOrder("team-a", "team-b");
        verify(context).allowSharedClusterTargets();
        verify(selector).initialize(null);
    }

    @Test
    void initializeThrowsWhenSelectorReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        when(context.pluginInstance(eq(RouteSelector.class), eq("UserNameMatch"))).thenReturn(selector);
        when(selector.referencedRoutes()).thenReturn(CompletableFuture.completedFuture(Optional.of(Set.of("team-a", "team-x"))));
        SubjectRouter.Config config = new SubjectRouter.Config("UserNameMatch", null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }
}
