/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.antlr.v4.runtime.CharStreams;
import org.junit.jupiter.api.Test;

import io.kroxylicious.authorizer.provider.acl.allow.FakeClusterResource;
import io.kroxylicious.authorizer.provider.acl.allow.FakeTopicResource;
import io.kroxylicious.authorizer.provider.acl.allow.ServiceAccount;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AclAuthorizer#resourceSelectors} must select exactly the names {@link AclAuthorizer#authorize}
 * allows, for every subject, operation and name.
 */
class ResourceSelectorsTest {

    private static final String RULES = """
            from io.kroxylicious.authorizer.provider.acl.allow import FakeTopicResource as Topic, FakeClusterResource as Cluster;
            from io.kroxylicious.authorizer.provider.acl.allow import ServiceAccount;
            deny User with name = "bob" to * Topic with name = "team-a-secret";
            deny User with name = "carol" to READ Topic with name matching /orders-9.*/;
            deny User with name = "dan" to * Topic with name *;
            deny anonymous User to READ Topic with name = "public-secret";
            allow User with name = "bob" to WRITE Topic with name like "team-a-*";
            allow User with name = "alice" to READ Topic with name in {"x", "y"};
            allow User with name = "carol" to READ Topic with name matching /orders-[0-9]+/;
            allow User with name like "temp-*" to READ Topic with name like "scratch-*";
            allow User with name * to DESCRIBE Topic with name *;
            allow ServiceAccount with name = "svc" to {READ, WRITE} Topic with name like "svc-*";
            allow User with name = "alice" to CONNECT Cluster with name = "c1";
            allow anonymous User to READ Topic with name like "public-*";
            otherwise deny;
            """;

    private static final AclAuthorizer AUTHORIZER = AclAuthorizerService.parse(CharStreams.fromString(RULES));

    private static final List<Subject> SUBJECTS = List.of(
            subject(new User("bob")),
            subject(new User("alice")),
            subject(new User("carol")),
            subject(new User("dan")),
            subject(new User("temp-joe")),
            subject(new User("temp")),
            subject(new User("mallory"), new ServiceAccount("svc")),
            subject(new User("svc")),
            Subject.anonymous());

    private static final List<String> NAMES = List.of(
            "team-a-secret", "team-a-public", "team-a-", "team-a", "team-b-x", "x", "y", "z", "orders-1", "orders-9",
            "orders-91", "orders-x", "scratch-1", "scratch", "svc-1", "svc", "c1", "c2", "public-a", "public-secret", "");

    private static Subject subject(Principal... principals) {
        return new Subject(Set.of(principals));
    }

    private static boolean authorized(Subject subject, ResourceType<?> operation, String name) {
        return AUTHORIZER.authorize(subject, List.of(new Action(operation, name))).toCompletableFuture().join()
                .decision(operation, name) == Decision.ALLOW;
    }

    @Test
    void selectorsAllowExactlyWhatAuthorizeAllows() {
        List<String> mismatches = new ArrayList<>();
        int allowed = 0;
        int checked = 0;
        var operations = Stream.concat(Stream.of(FakeTopicResource.values()), Stream.of(FakeClusterResource.values()))
                .toList();
        for (Subject subject : SUBJECTS) {
            for (ResourceType<?> operation : operations) {
                ResourceSelectors selectors = AUTHORIZER.resourceSelectors(subject, operation);
                for (String name : NAMES) {
                    boolean expected = authorized(subject, operation, name);
                    allowed += expected ? 1 : 0;
                    checked++;
                    if (selectors.allows(name) != expected) {
                        mismatches.add(subject.principals() + " " + operation + " '" + name + "': authorize " + expected);
                    }
                }
            }
        }
        assertThat(mismatches).isEmpty();
        // Not vacuous: plenty of combinations are allowed, and plenty denied
        assertThat(allowed).isGreaterThan(50);
        assertThat(checked - allowed).isGreaterThan(50);
    }

    @Test
    void selectorsReportTheRulesForms() {
        ResourceSelectors bob = AUTHORIZER.resourceSelectors(subject(new User("bob")), FakeTopicResource.READ);
        // WRITE implies DESCRIBE but not READ in these resource types, so the WRITE rule is not a READ rule
        assertThat(bob.allowed()).isEqualTo(ResourceNameSelectors.NONE);

        ResourceSelectors bobWrite = AUTHORIZER.resourceSelectors(subject(new User("bob")), FakeTopicResource.WRITE);
        assertThat(bobWrite.allowed()).isEqualTo(new ResourceNameSelectors(false, Set.of(), Set.of("team-a-"), Set.of()));
        assertThat(bobWrite.denied()).isEqualTo(new ResourceNameSelectors(false, Set.of("team-a-secret"), Set.of(), Set.of()));

        ResourceSelectors carol = AUTHORIZER.resourceSelectors(subject(new User("carol")), FakeTopicResource.READ);
        assertThat(carol.allowed().patterns()).containsExactly("orders-[0-9]+");
        assertThat(carol.denied().patterns()).containsExactly("orders-9.*");

        ResourceSelectors describe = AUTHORIZER.resourceSelectors(subject(new User("zed")), FakeTopicResource.DESCRIBE);
        assertThat(describe.allowed().anyName()).isTrue();

        ResourceSelectors dan = AUTHORIZER.resourceSelectors(subject(new User("dan")), FakeTopicResource.DESCRIBE);
        assertThat(dan.denied().anyName()).isTrue();
        assertThat(dan.allows("anything")).isFalse();

        // A rule for another principal class does not apply to a User with the same name
        assertThat(AUTHORIZER.resourceSelectors(subject(new User("svc")), FakeTopicResource.READ).allowed())
                .isEqualTo(ResourceNameSelectors.NONE);
        // Another resource type's rules do not apply
        assertThat(AUTHORIZER.resourceSelectors(subject(new User("alice")), FakeClusterResource.CONNECT).allowed().names())
                .containsExactly("c1");
    }
}
