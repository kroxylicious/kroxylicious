/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings({ "java:S5738", "removal" })
class UserNameMatchTest {

    private static final RouteSelectorContext CONTEXT = new RouteSelectorContextImpl(Set.of("team-a", "team-b"));

    private static UserNameMatch selector(UserNameMatch.Config config) {
        UserNameMatch selector = new UserNameMatch();
        selector.initialize(config);
        return selector;
    }

    @Test
    void anonymousSubjectRejected() {
        // Given
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), null));

        // When
        Optional<String> route = selector.selectRoute(Subject.anonymous(), CONTEXT).toCompletableFuture().join();

        // Then
        assertThat(route).isEmpty();
    }

    @Test
    void mappedPrincipalRoutesToItsRoute() {
        // Given
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice", "CN=bob")),
                        new UserNameMatch.Mapping("team-b", List.of("CN=carol"))),
                null));

        // When
        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=carol"))), CONTEXT).toCompletableFuture().join();

        // Then
        assertThat(route).contains("team-b");
    }

    @Test
    void unmappedPrincipalWithoutDefaultRejected() {
        // Given
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), null));

        // When
        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=eve"))), CONTEXT).toCompletableFuture().join();

        // Then
        assertThat(route).isEmpty();
    }

    @Test
    void unmappedPrincipalWithDefaultRoutesToDefault() {
        // Given
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), "team-b"));

        // When
        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=eve"))), CONTEXT).toCompletableFuture().join();

        // Then
        assertThat(route).contains("team-b");
    }

    @Test
    void duplicatePrincipalAcrossMappingsRejected() {
        // Given
        UserNameMatch.Config config = new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice")),
                        new UserNameMatch.Mapping("team-b", List.of("CN=alice"))),
                null);

        // When / Then
        assertThatThrownBy(() -> selector(config)).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void referencedRoutesIncludesMappingsAndDefault() {
        // Given
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), "team-b"));

        // When
        Optional<Set<String>> referenced = selector.referencedRoutes().toCompletableFuture().join();

        // Then
        assertThat(referenced).contains(Set.of("team-a", "team-b"));
    }
}
