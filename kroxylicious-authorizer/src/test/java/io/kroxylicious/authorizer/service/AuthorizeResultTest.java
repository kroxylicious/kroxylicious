/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.List;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

class AuthorizeResultTest {

    enum TestResource implements ResourceType<TestResource> {
        SQUIDGE,
        TESSELLATE,
        AETHERIFY
    }

    Subject subject = new Subject(new User("alice"));
    Action foo = new Action(TestResource.SQUIDGE, "foo");
    Action bar = new Action(TestResource.SQUIDGE, "bar");
    Action baz = new Action(TestResource.TESSELLATE, "baz");

    AuthorizeResult result = new AuthorizeResult(subject, List.of(foo, baz), List.of(bar));

    @Test
    void shouldReturnSameSubject() {
        Assertions.assertThat(result.subject()).isSameAs(subject);
    }

    @Test
    void shouldReturnAllowedAndDenied() {
        Assertions.assertThat(result.allowed()).isEqualTo(List.of(foo, baz));
        Assertions.assertThat(result.denied()).isEqualTo(List.of(bar));
    }

    @Test
    void shouldReturnAllowedAndDeniedNarroedByResource() {
        Assertions.assertThat(result.allowed(TestResource.SQUIDGE)).isEqualTo(List.of(foo.resourceName()));
        Assertions.assertThat(result.allowed(TestResource.TESSELLATE)).isEqualTo(List.of(baz.resourceName()));
        Assertions.assertThat(result.allowed(TestResource.AETHERIFY)).isEmpty();

        Assertions.assertThat(result.denied(TestResource.SQUIDGE)).isEqualTo(List.of(bar.resourceName()));
        Assertions.assertThat(result.denied(TestResource.TESSELLATE)).isEmpty();
        Assertions.assertThat(result.denied(TestResource.AETHERIFY)).isEmpty();
    }

    @Test
    void shouldReturnDecisions() {
        Assertions.assertThat(result.decision(TestResource.SQUIDGE, "foo")).isEqualTo(Decision.ALLOW);
        Assertions.assertThat(result.decision(TestResource.SQUIDGE, "bar")).isEqualTo(Decision.DENY);
        Assertions.assertThat(result.decision(TestResource.TESSELLATE, "baz")).isEqualTo(Decision.ALLOW);
        Assertions.assertThat(result.decision(TestResource.AETHERIFY, "quux")).isEqualTo(Decision.DENY);
    }

    record Widget(String name) {}

    @Test
    void shouldPartition() {
        Assertions.assertThat(result.partition(List.of(new Widget("foo"), new Widget("bar"), new Widget("baz")),
                TestResource.SQUIDGE,
                Widget::name)).isEqualTo(Map.of(Decision.ALLOW, List.of(new Widget("foo")),
                Decision.DENY, List.of(new Widget("bar"), new Widget("baz"))));
    }



}