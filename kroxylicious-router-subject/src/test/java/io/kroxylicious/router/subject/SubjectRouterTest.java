/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class SubjectRouterTest {

    @Mock
    private RouterFactoryContext context;

    private final SubjectRouter router = new SubjectRouter();

    private static SubjectRouter.Config config(String type, List<SubjectRouter.RouteMappingEntry> routeMapping, String defaultRoute) {
        return new SubjectRouter.Config(new SubjectRouter.SingularPrincipalMatch(type, routeMapping, defaultRoute));
    }

    @Test
    void initializeReturnsValidInitializedDataForWellFormedConfig() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Config config = config(User.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a"), new SubjectRouter.RouteMappingEntry("carol", "team-b")), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.principalType()).isEqualTo(User.class);
        assertThat(initialized.principalToRoute()).containsOnly(entry("alice", "team-a"), entry("carol", "team-b"));
        assertThat(initialized.defaultRoute()).isNull();
        verify(context).allowSharedClusterTargets();
    }

    @Test
    void initializeThrowsWhenTypeIsNotSingular() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(NonSingularPrincipal.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("is not a singular principal type");
    }

    @Test
    void initializeThrowsWhenTypeIsNotAPrincipal() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(String.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("does not implement");
    }

    @Test
    void initializeThrowsWhenTypeIsUnknown() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config("not.a.Real.Class",
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void initializeThrowsWhenRouteMappingReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(User.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-x")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }

    @Test
    void initializeThrowsWhenDefaultRouteReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(User.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a")), "team-x");

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }

    @Test
    void initializeThrowsWhenPrincipalNameAppearsMoreThanOnce() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Config config = config(User.class.getName(),
                List.of(new SubjectRouter.RouteMappingEntry("alice", "team-a"), new SubjectRouter.RouteMappingEntry("alice", "team-b")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("alice");
    }

    /**
     * Not annotated {@code @Unique}/{@code @SingularPrincipal}, for {@link #initializeThrowsWhenTypeIsNotSingular()}.
     */
    private record NonSingularPrincipal(String name) implements Principal {}
}
