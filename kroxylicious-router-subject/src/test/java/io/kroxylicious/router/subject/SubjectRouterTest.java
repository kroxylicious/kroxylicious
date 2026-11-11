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

import io.kroxylicious.identity.SingularPrincipal;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.router.RouterFactoryContext;
import io.kroxylicious.router.subject.SubjectRouter.PrincipalRequirement;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@SuppressWarnings({ "java:S5738", "removal" })
class SubjectRouterTest {

    @Mock
    private RouterFactoryContext context;

    private final SubjectRouter router = new SubjectRouter();

    private static PrincipalRequirement userRequirement(String principalName) {
        return new PrincipalRequirement(User.class.getName(), principalName);
    }

    private static SubjectRouter.Rule rule(String principalName, String route) {
        return new SubjectRouter.Rule(List.of(userRequirement(principalName)), null, route);
    }

    private static SubjectRouter.Rule clientIdRule(String clientId, String route) {
        return new SubjectRouter.Rule(null, clientId, route);
    }

    private static SubjectRouter.Config config(List<SubjectRouter.Rule> rules, @Nullable String defaultRoute) {
        return new SubjectRouter.Config(new SubjectRouter.RuleMatch(rules, defaultRoute));
    }

    @Test
    void initializeReturnsValidInitializedDataForWellFormedConfig() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Config config = config(List.of(rule("alice", "team-a"), rule("carol", "team-b")), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).hasSize(2);
        assertThat(initialized.rules().get(0).requiredPrincipals())
                .containsExactly(new SubjectRouter.CompiledPrincipalRequirement(User.class, "alice"));
        assertThat(initialized.rules().get(0).requiredClientId()).isNull();
        assertThat(initialized.rules().get(0).route()).isEqualTo("team-a");
        assertThat(initialized.rules().get(1).requiredPrincipals())
                .containsExactly(new SubjectRouter.CompiledPrincipalRequirement(User.class, "carol"));
        assertThat(initialized.rules().get(1).route()).isEqualTo("team-b");
        assertThat(initialized.defaultRoute()).isNull();
        verify(context).allowSharedClusterTargets();
    }

    @Test
    void initializeSupportsRuleRequiringMultiplePrincipalTypes() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(
                List.of(userRequirement("alice"), new PrincipalRequirement(OtherSingularPrincipal.class.getName(), "x")), null, "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).singleElement().satisfies(compiled -> assertThat(compiled.requiredPrincipals())
                .containsExactlyInAnyOrder(
                        new SubjectRouter.CompiledPrincipalRequirement(User.class, "alice"),
                        new SubjectRouter.CompiledPrincipalRequirement(OtherSingularPrincipal.class, "x")));
    }

    @Test
    void initializeSupportsRuleRequiringTwoPrincipalsOfTheSameNonSingularType() {
        // Given: two Role requirements of the same type, expressible only as a list, not a map
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(
                List.of(new PrincipalRequirement(Role.class.getName(), "developer"), new PrincipalRequirement(Role.class.getName(), "team-a")), null, "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).singleElement().satisfies(compiled -> assertThat(compiled.requiredPrincipals())
                .containsExactlyInAnyOrder(
                        new SubjectRouter.CompiledPrincipalRequirement(Role.class, "developer"),
                        new SubjectRouter.CompiledPrincipalRequirement(Role.class, "team-a")));
    }

    @Test
    void initializeSupportsClientIdOnlyRule() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(List.of(clientIdRule("demo-app", "team-a")), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).singleElement().satisfies(compiled -> {
            assertThat(compiled.requiredPrincipals()).isEmpty();
            assertThat(compiled.requiredClientId()).isEqualTo("demo-app");
            assertThat(compiled.route()).isEqualTo("team-a");
        });
    }

    @Test
    void initializeSupportsRuleCombiningHasPrincipalsAndHasClientId() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(List.of(userRequirement("alice")), "alice-app", "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When
        SubjectRouter.Initialized initialized = router.initialize(context, config);

        // Then
        assertThat(initialized.rules()).singleElement().satisfies(compiled -> {
            assertThat(compiled.requiredPrincipals()).containsExactly(new SubjectRouter.CompiledPrincipalRequirement(User.class, "alice"));
            assertThat(compiled.requiredClientId()).isEqualTo("alice-app");
        });
    }

    @Test
    void initializeThrowsWhenRuleHasNeitherHasPrincipalsNorHasClientId() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(List.of(), null, "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("needs at least one of hasPrincipals or hasClientId");
    }

    @Test
    void initializeThrowsWhenTwoRulesHaveIdenticalPredicates() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Config config = config(List.of(rule("alice", "team-a"), rule("alice", "team-b")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("more than one rule");
    }

    @Test
    void initializeThrowsWhenTwoRulesHaveIdenticalPredicatesInDifferentOrder() {
        // Given: predicate identity is order-insensitive over hasPrincipals
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Rule ruleA = new SubjectRouter.Rule(
                List.of(new PrincipalRequirement(Role.class.getName(), "developer"), new PrincipalRequirement(Role.class.getName(), "team-a")), null, "team-a");
        SubjectRouter.Rule ruleB = new SubjectRouter.Rule(
                List.of(new PrincipalRequirement(Role.class.getName(), "team-a"), new PrincipalRequirement(Role.class.getName(), "developer")), null, "team-b");
        SubjectRouter.Config config = config(List.of(ruleA, ruleB), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("more than one rule");
    }

    @Test
    void initializeThrowsWhenTwoClientIdOnlyRulesHaveIdenticalPredicates() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a", "team-b"));
        SubjectRouter.Config config = config(List.of(clientIdRule("demo-app", "team-a"), clientIdRule("demo-app", "team-b")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("more than one rule");
    }

    @Test
    void initializeThrowsWhenTypeIsNotAPrincipal() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(List.of(new PrincipalRequirement(String.class.getName(), "alice")), null, "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("does not implement");
    }

    @Test
    void initializeThrowsWhenTypeIsUnknown() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Rule rule = new SubjectRouter.Rule(List.of(new PrincipalRequirement("not.a.Real.Class", "alice")), null, "team-a");
        SubjectRouter.Config config = config(List.of(rule), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void initializeThrowsWhenRuleReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(List.of(rule("alice", "team-x")), null);

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }

    @Test
    void initializeThrowsWhenDefaultRouteReferencesUnknownRoute() {
        // Given
        when(context.routeNames()).thenReturn(Set.of("team-a"));
        SubjectRouter.Config config = config(List.of(rule("alice", "team-a")), "team-x");

        // When / Then
        assertThatThrownBy(() -> router.initialize(context, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("team-x");
    }

    /**
     * A second principal type, distinct from {@link User}, for a rule requiring multiple types.
     */
    @SingularPrincipal
    private record OtherSingularPrincipal(String name) implements Principal {}

    /**
     * A non-singular principal type: a subject may carry more than one, for a rule requiring two
     * principals of the same type.
     */
    private record Role(String name) implements Principal {}
}
