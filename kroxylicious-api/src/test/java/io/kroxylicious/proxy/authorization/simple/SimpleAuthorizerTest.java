/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.simple;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authorization.Action;
import io.kroxylicious.proxy.authorization.Authorization;
import io.kroxylicious.proxy.authorization.Authorizer;
import io.kroxylicious.proxy.authorization.ClusterResource;
import io.kroxylicious.proxy.authorization.Decision;
import io.kroxylicious.proxy.authorization.Subject;
import io.kroxylicious.proxy.authorization.TopicResource;

import static org.assertj.core.api.Assertions.assertThat;

class SimpleAuthorizerTest {

    @Test
    void builderAllOperationsAndResourceNameEqualAndPrincipalNameEqual() {
        // Given
        EnumSet<TopicResource> shouldBeAllowed = EnumSet.allOf(TopicResource.class);
        EnumSet<TopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        var authz = SimpleAuthorizer.builder()
                .grant()
                .allOperations(TopicResource.class)
                .forResourceWithNameEqualTo("my-topic")
                .toSubjectsHavingPrincipal(UserPrincipal.class)
                .withNameEqualTo("bob")
                .build();

        Subject alice = new Subject(Set.of(new UserPrincipal("alice")));
        Subject bob = new Subject(Set.of(new UserPrincipal("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : shouldBeDenied) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderOneOperationAndResourceNameEqualAndPrincipalNameEqual() {
        // Given
        EnumSet<TopicResource> shouldBeAllowed = EnumSet.of(TopicResource.CREATE);
        EnumSet<TopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        var authz = SimpleAuthorizer.builder()
                .grant()
                .operations(shouldBeAllowed)
                .forResourceWithNameEqualTo("my-topic")
                .toSubjectsHavingPrincipal(UserPrincipal.class)
                .withNameEqualTo("bob")
                .build();

        Subject alice = new Subject(Set.of(new UserPrincipal("alice")));
        Subject bob = new Subject(Set.of(new UserPrincipal("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : shouldBeDenied) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNamePrefixAndPrincipalNameEqual() {
        // Given
        EnumSet<TopicResource> shouldBeAllowed = EnumSet.allOf(TopicResource.class);
        var authz = SimpleAuthorizer.builder()
                .grant()
                .allOperations(TopicResource.class)
                .forResourcesWithNameStartingWith("my-")
                .toSubjectsHavingPrincipal(UserPrincipal.class)
                .withNameEqualTo("bob")
                .build();

        Subject alice = new Subject(Set.of(new UserPrincipal("alice")));
        Subject bob = new Subject(Set.of(new UserPrincipal("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic"),
                            new Action(op, "my-thingy")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "my-thingy")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNameAnyAndPrincipalNameEqual() {
        // Given
        EnumSet<TopicResource> shouldBeAllowed = EnumSet.allOf(TopicResource.class);
        var authz = SimpleAuthorizer.builder()
                .grant()
                .allOperations(TopicResource.class)
                .forAllResources()
                .toSubjectsHavingPrincipal(UserPrincipal.class)
                .withNameEqualTo("bob")
                .build();

        Subject alice = new Subject(Set.of(new UserPrincipal("alice")));
        Subject bob = new Subject(Set.of(new UserPrincipal("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic"),
                            new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNameMatchingAndPrincipalNameEqual() {
        // Given
        EnumSet<TopicResource> shouldBeAllowed = EnumSet.allOf(TopicResource.class);
        var authz = SimpleAuthorizer.builder()
                .grant()
                .allOperations(TopicResource.class)
                .forResourcesWithNameMatching("(my|your)-topic+")
                .toSubjectsHavingPrincipal(UserPrincipal.class)
                .withNameEqualTo("bob")
                .build();

        Subject alice = new Subject(Set.of(new UserPrincipal("alice")));
        Subject bob = new Subject(Set.of(new UserPrincipal("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            Authorization authorize = authz.authorize(bob,
                    List.of(new Action(op, "my-topic"),
                            new Action(op, "your-topic"),
                            new Action(op, "my-topiccccc"),
                            new Action(op, "your-topiccccc")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic"),
                    new Action(op, "my-topiccccc"),
                    new Action(op, "your-topiccccc")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "their-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "their-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : TopicResource.values()) {
            Authorization authorize = authz.authorize(alice,
                    List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    // Prove we can build a SimpleAuthorizer
    // Prove that the aggregate method matches the single method
    // Test the single method
    // For principal with equals, any and prefix
    // For resource with equals, any and prefix

    @Test
    void t() {

        SimpleAuthorizer simple = new SimpleAuthorizer();
        UserPrincipal alice = new UserPrincipal("alice");
        UserPrincipal bob = new UserPrincipal("bob");
        UserPrincipal carol = new UserPrincipal("carol");
        UserPrincipal dan = new UserPrincipal("dan");
        UserPrincipal eve = new UserPrincipal("eve");

        RolePrincipal admins = new RolePrincipal("admins");
        var anon = new Subject(Set.of());
        var alices = new Subject(Set.of(alice, admins));
        var bobs = new Subject(Set.of(bob));
        var carols = new Subject(Set.of(carol));
        var dans = new Subject(Set.of(dan));
        var eves = new Subject(Set.of(eve));

        // Everyone who is allowed to authorize is allowed to connect
        simple.grantToAllPrincipalsOfType(EnumSet.of(ClusterResource.CONNECT), "", UserPrincipal.class);
        String bobOnly = "my-topic";
        simple.grant(Set.of(TopicResource.READ, TopicResource.WRITE), bobOnly, Set.of(bob));
        String aliceAndBobWriteOnlyCarolReadOnly = "your";
        simple.grant(Set.of(TopicResource.WRITE), aliceAndBobWriteOnlyCarolReadOnly, Set.of(bob, alice));
        simple.grant(Set.of(TopicResource.READ), aliceAndBobWriteOnlyCarolReadOnly, Set.of(carol));
        String adminsOnly = "admins-only";
        simple.grant(EnumSet.allOf(TopicResource.class), adminsOnly, Set.of(admins));

        Authorizer a = simple;

        assertThat(a.decision(anon, ClusterResource.CONNECT, "")).isEqualTo(Decision.DENY);
        for (var s: List.of(alices, bobs, carols, dans, eves)) {
            assertThat(a.decision(s, ClusterResource.CONNECT, "")).
                    as("Expected %s to be ALLOWed", s).isEqualTo(Decision.ALLOW);
        }

        assertThat(a.decision(alices, TopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(alices, TopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(alices, TopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(alices, TopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(alices, TopicResource.READ, adminsOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(alices, TopicResource.WRITE, adminsOnly)).isEqualTo(Decision.ALLOW);

        assertThat(a.decision(bobs, TopicResource.READ, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(bobs, TopicResource.WRITE, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(bobs, TopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(bobs, TopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(bobs, TopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(bobs, TopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

        assertThat(a.decision(carols, TopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(carols, TopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(carols, TopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(a.decision(carols, TopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(carols, TopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(a.decision(carols, TopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

    }

}