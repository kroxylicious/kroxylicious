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
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), null));

        Optional<String> route = selector.selectRoute(Subject.anonymous(), CONTEXT).toCompletableFuture().join();

        assertThat(route).isEmpty();
    }

    @Test
    void mappedPrincipalRoutesToItsRoute() {
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice", "CN=bob")),
                        new UserNameMatch.Mapping("team-b", List.of("CN=carol"))),
                null));

        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=carol"))), CONTEXT).toCompletableFuture().join();

        assertThat(route).contains("team-b");
    }

    @Test
    void unmappedPrincipalWithoutDefaultRejected() {
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), null));

        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=eve"))), CONTEXT).toCompletableFuture().join();

        assertThat(route).isEmpty();
    }

    @Test
    void unmappedPrincipalWithDefaultRoutesToDefault() {
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), "team-b"));

        Optional<String> route = selector.selectRoute(new Subject(Set.of(new User("CN=eve"))), CONTEXT).toCompletableFuture().join();

        assertThat(route).contains("team-b");
    }

    @Test
    void duplicatePrincipalAcrossMappingsRejected() {
        UserNameMatch.Config config = new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice")),
                        new UserNameMatch.Mapping("team-b", List.of("CN=alice"))),
                null);

        assertThatThrownBy(() -> selector(config)).isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void referencedRoutesIncludesMappingsAndDefault() {
        UserNameMatch selector = selector(new UserNameMatch.Config(
                List.of(new UserNameMatch.Mapping("team-a", List.of("CN=alice"))), "team-b"));

        Optional<Set<String>> referenced = selector.referencedRoutes().toCompletableFuture().join();

        assertThat(referenced).contains(Set.of("team-a", "team-b"));
    }
}
