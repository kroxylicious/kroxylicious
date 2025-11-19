/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.authorizer.provider.acl.allow.FakeClusterResource;
import io.kroxylicious.authorizer.provider.acl.allow.FakeTopicResource;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class AclAuthorizerTest {

    @Test
    void builderAllOperationsAndResourceNameEqualAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.allOf(FakeTopicResource.class);
        EnumSet<FakeTopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onResourceWithNameEqualTo("my-topic")
                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).as("Expect allow %s", op).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).as("Expect deny %s", op).isEmpty();
        }

        for (var op : shouldBeDenied) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    // Check READ => DESCRIBE, as described by
    // `org.apache.kafka.common.acl.AclOperation`.
    @Test
    void builderAllOperationsAndResourceNameEqualAndPrincipalNameEqualWithImplication() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.of(FakeTopicResource.READ, FakeTopicResource.DESCRIBE);
        EnumSet<FakeTopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .operations(Set.of(FakeTopicResource.READ))
                .onResourceWithNameEqualTo("my-topic")
                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).as("Expect allow %s", op).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).as("Expect deny %s", op).isEmpty();
        }

        for (var op : shouldBeDenied) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderOneOperationAndResourceNameEqualAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.of(FakeTopicResource.CREATE);
        EnumSet<FakeTopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .operations(shouldBeAllowed)
                .onResourceWithNameEqualTo("my-topic")
                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : shouldBeDenied) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNamePrefixAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.allOf(FakeTopicResource.class);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onResourcesWithNameStartingWith("my-")
                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic"),
                    new Action(op, "my-thingy")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "my-thingy")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNameAnyAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.allOf(FakeTopicResource.class);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onAllResources()

                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    @Test
    void builderAllOperationsAndResourceNameMatchingAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.allOf(FakeTopicResource.class);
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onResourcesWithNameMatching("(my|your)-topic+")
                .build();

        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic"),
                    new Action(op, "my-topiccccc"),
                    new Action(op, "your-topiccccc")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "your-topic"),
                    new Action(op, "my-topiccccc"),
                    new Action(op, "your-topiccccc")));
            assertThat(authorize.denied()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "their-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "their-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "their-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "their-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }

        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, alice, List.of(new Action(op, "my-topic")));
            assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "my-topic")));
            assertThat(authorize.allowed()).isEmpty();
        }
    }

    private static AuthorizeResult getAuthorization(AclAuthorizer authz, Subject alice, List<Action> op) {
        CompletionStage<AuthorizeResult> authorizationStage = authz.authorize(alice,
                op);
        assertThat(authorizationStage).isCompleted();
        return authorizationStage.toCompletableFuture().join();
    }

    // Prove we can build a SimpleAuthorizer
    // Prove that the aggregate method matches the single method
    // Test the single method
    // For principal with equals, any and prefix
    // For resource with equals, any and prefix

    @Test
    void t() {

        AclAuthorizer simple = new AclAuthorizer();
        User alice = new User("alice");
        User bob = new User("bob");
        User carol = new User("carol");
        User dan = new User("dan");
        User eve = new User("eve");

        RolePrincipal admins = new RolePrincipal("admins");
        var anon = new Subject(Set.of());
        var alices = new Subject(Set.of(alice, admins));
        var bobs = new Subject(Set.of(bob));
        var carols = new Subject(Set.of(carol));
        var dans = new Subject(Set.of(dan));
        var eves = new Subject(Set.of(eve));

        // Everyone who is allowed to authorize is allowed to connect

        simple.internalAllowOrDeny(simple.allowPerPrincipal, (Class<? extends Principal>) User.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, null,
                FakeClusterResource.class, AclAuthorizer.Pred.EQ, "", EnumSet.of(FakeClusterResource.CONNECT));

        String bobOnly = "my-topic";
        for (var p3 : Set.of(bob)) {
            String principalName = p3.name();
            simple.internalAllowOrDeny(simple.allowPerPrincipal, p3.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalName,
                    FakeTopicResource.class, AclAuthorizer.Pred.EQ, bobOnly, Set.of(FakeTopicResource.READ, FakeTopicResource.WRITE));
        }
        String aliceAndBobWriteOnlyCarolReadOnly = "your";
        for (var p2 : Set.of(bob, alice)) {
            String principalName = p2.name();
            simple.internalAllowOrDeny(simple.allowPerPrincipal, p2.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalName,
                    FakeTopicResource.class, AclAuthorizer.Pred.EQ, aliceAndBobWriteOnlyCarolReadOnly, Set.of(FakeTopicResource.WRITE));
        }
        for (var p1 : Set.of(carol)) {
            String principalName = p1.name();
            simple.internalAllowOrDeny(simple.allowPerPrincipal, p1.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalName,
                    FakeTopicResource.class, AclAuthorizer.Pred.EQ, aliceAndBobWriteOnlyCarolReadOnly, Set.of(FakeTopicResource.READ));
        }
        String adminsOnly = "admins-only";
        for (var p : Set.<Principal> of(admins)) {
            String principalName = p.name();
            simple.internalAllowOrDeny(simple.allowPerPrincipal, p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalName,
                    FakeTopicResource.class, AclAuthorizer.Pred.EQ, adminsOnly, EnumSet.allOf(FakeTopicResource.class));
        }

        Authorizer a = simple;

        assertThat(getDecision(a, anon, FakeClusterResource.CONNECT, "")).isEqualTo(Decision.DENY);
        for (var s : List.of(alices, bobs, carols, dans, eves)) {
            assertThat(getDecision(a, s, FakeClusterResource.CONNECT, "")).as("Expected %s to be ALLOWed", s).isEqualTo(Decision.ALLOW);
        }

        assertThat(getDecision(a, alices, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, alices, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, alices, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, alices, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, alices, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, alices, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.ALLOW);

        assertThat(getDecision(a, bobs, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, bobs, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, bobs, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, bobs, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, bobs, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, bobs, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

        assertThat(getDecision(a, carols, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, carols, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, carols, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(getDecision(a, carols, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, carols, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(getDecision(a, carols, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

    }

    @NonNull
    private static Decision getDecision(Authorizer authorizer, Subject subject, ResourceType<?> resourceType, String resourceName) {
        CompletionStage<AuthorizeResult> authorize = authorizer.authorize(subject, List.of(new Action(resourceType, resourceName)));
        assertThat(authorize).isCompleted();
        return authorize.toCompletableFuture().join().decision(resourceType, resourceName);
    }

}