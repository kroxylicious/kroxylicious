/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.time.Duration;
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

@SuppressWarnings("removal") // Principal, Subject scheduled for removal
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

    @Test
    void builderOperationsCorrectlyAuthorizesWhenEnumConstantsHaveAnonymousBodies() {
        // Given
        // READ (ordinal 0) has an anonymous body, so without fixes both getClass() calls — in the
        // builder and in Action.resourceTypeClass() — return anonymous subclasses, causing key
        // mismatches in the rule tree and silently denying all operations.
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .operations(Set.of(FakeResourceWithAnonymousConstantBody.WITH_BODY, FakeResourceWithAnonymousConstantBody.WITHOUT_BODY))
                .onResourceWithNameEqualTo("my-topic")
                .build();

        Subject bob = new Subject(Set.of(new User("bob")));

        // When
        AuthorizeResult result = getAuthorization(authz, bob, List.of(
                new Action(FakeResourceWithAnonymousConstantBody.WITH_BODY, "my-topic"),
                new Action(FakeResourceWithAnonymousConstantBody.WITHOUT_BODY, "my-topic")));

        // Then
        assertThat(result.allowed()).isEqualTo(List.of(
                new Action(FakeResourceWithAnonymousConstantBody.WITH_BODY, "my-topic"),
                new Action(FakeResourceWithAnonymousConstantBody.WITHOUT_BODY, "my-topic")));
        assertThat(result.denied()).isEmpty();
    }

    private static Decision decide(AclAuthorizer authz, Subject subject, FakeTopicResource op, String name) {
        return getAuthorization(authz, subject, List.of(new Action(op, name))).decision(op, name);
    }

    /**
     * A prefix rule for one principal type (say {@code RolePrincipal}) must not grant access to a subject that holds a
     * principal of a different type (say {@code User}) whose name starts with the same prefix.
     */
    @Test
    void prefixRuleForOnePrincipalClassDoesNotMatchAnotherClass() {
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(RolePrincipal.class)
                .withNameStartingWith("reader-")
                .allOperations(FakeTopicResource.class)
                .onAllResources()
                .build();

        assertThat(decide(authz, new Subject(Set.of(new User("reader-bob"))), FakeTopicResource.READ, "t"))
                .isEqualTo(Decision.DENY);
        assertThat(decide(authz, new Subject(Set.of(new User("someone"), new RolePrincipal("reader-bob"))), FakeTopicResource.READ, "t"))
                .isEqualTo(Decision.ALLOW);
    }

    /**
     * A broader deny on {@code team-b*} must not be bypassed just because a narrower deny on
     * {@code team-b-internal*} also exists. Both matching prefix rules apply; the broader one's
     * operation set still contributes.
     */
    @Test
    void broaderDenyOnResourceNotBypassedByNarrowerOverlappingDeny() {
        var authz = AclAuthorizer.builder()
                .deny()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onResourcesWithNameStartingWith("team-b")
                .deny()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .operations(Set.of(FakeTopicResource.READ))
                .onResourcesWithNameStartingWith("team-b-internal")
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("bob")
                .allOperations(FakeTopicResource.class)
                .onAllResources()
                .build();
        Subject bob = new Subject(Set.of(new User("bob")));

        assertThat(decide(authz, bob, FakeTopicResource.WRITE, "team-b-internal-x")).isEqualTo(Decision.DENY);
        assertThat(decide(authz, bob, FakeTopicResource.READ, "team-b-internal-x")).isEqualTo(Decision.DENY);
        assertThat(decide(authz, bob, FakeTopicResource.WRITE, "team-b-public")).isEqualTo(Decision.DENY);
        assertThat(decide(authz, bob, FakeTopicResource.WRITE, "team-c")).isEqualTo(Decision.ALLOW);
    }

    /**
     * A broader allow on {@code team-a*} must not be lost just because a narrower allow on
     * {@code team-a-internal*} also exists. Both matching prefix rules apply; each one's
     * operation set contributes.
     */
    @Test
    void broaderAllowOnResourceNotLostByNarrowerOverlappingAllow() {
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("alice")
                .operations(Set.of(FakeTopicResource.READ))
                .onResourcesWithNameStartingWith("team-a")
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameEqualTo("alice")
                .operations(Set.of(FakeTopicResource.WRITE))
                .onResourcesWithNameStartingWith("team-a-internal")
                .build();
        Subject alice = new Subject(Set.of(new User("alice")));

        assertThat(decide(authz, alice, FakeTopicResource.READ, "team-a-internal-x")).isEqualTo(Decision.ALLOW);
        assertThat(decide(authz, alice, FakeTopicResource.WRITE, "team-a-internal-x")).isEqualTo(Decision.ALLOW);
        assertThat(decide(authz, alice, FakeTopicResource.WRITE, "team-a-public")).isEqualTo(Decision.DENY);
    }

    /**
     * A broader deny on principals matching {@code a*} must not be bypassed just because a narrower
     * deny on {@code alic*} also exists. Both matching principal prefix rules apply; the broader
     * one's operation set still contributes.
     */
    @Test
    void broaderDenyOnPrincipalNotBypassedByNarrowerOverlappingDeny() {
        var authz = AclAuthorizer.builder()
                .deny()
                .subjectsHavingPrincipal(User.class)
                .withNameStartingWith("a")
                .operations(Set.of(FakeTopicResource.WRITE))
                .onResourceWithNameEqualTo("t")
                .deny()
                .subjectsHavingPrincipal(User.class)
                .withNameStartingWith("alic")
                .operations(Set.of(FakeTopicResource.ALTER))
                .onResourceWithNameEqualTo("t")
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withAnyName()
                .allOperations(FakeTopicResource.class)
                .onResourceWithNameEqualTo("t")
                .build();
        Subject alice = new Subject(Set.of(new User("alice")));

        assertThat(decide(authz, alice, FakeTopicResource.WRITE, "t")).isEqualTo(Decision.DENY);
        assertThat(decide(authz, alice, FakeTopicResource.ALTER, "t")).isEqualTo(Decision.DENY);
        assertThat(decide(authz, alice, FakeTopicResource.CREATE, "t")).isEqualTo(Decision.ALLOW);
    }

    /**
     * A broader allow on principals matching {@code al*} must not be lost just because a narrower
     * allow on {@code ali*} also exists. Both matching principal prefix rules apply; each one's
     * operation set contributes.
     */
    @Test
    void broaderAllowOnPrincipalNotLostByNarrowerOverlappingAllow() {
        var authz = AclAuthorizer.builder()
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameStartingWith("al")
                .operations(Set.of(FakeTopicResource.READ))
                .onResourceWithNameEqualTo("t")
                .allow()
                .subjectsHavingPrincipal(User.class)
                .withNameStartingWith("ali")
                .operations(Set.of(FakeTopicResource.WRITE))
                .onResourceWithNameEqualTo("t")
                .build();
        Subject alice = new Subject(Set.of(new User("alice")));

        assertThat(decide(authz, alice, FakeTopicResource.READ, "t")).isEqualTo(Decision.ALLOW);
        assertThat(decide(authz, alice, FakeTopicResource.WRITE, "t")).isEqualTo(Decision.ALLOW);
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

        var builder = AclAuthorizer.builder().allow()
                .subjectsHavingPrincipal(User.class)
                .withAnyName()
                .operations(Set.of(FakeClusterResource.CONNECT))
                .onResourceWithNameEqualTo("");

        String bobOnly = "my-topic";
        for (var p3 : Set.of(bob)) {
            String principalName = p3.name();
            builder.allow()
                    .subjectsHavingPrincipal(p3.getClass())
                    .withNameEqualTo(principalName)
                    .operations(Set.of(FakeTopicResource.READ, FakeTopicResource.WRITE))
                    .onResourceWithNameEqualTo(bobOnly);
        }

        String aliceAndBobWriteOnlyCarolReadOnly = "your";
        for (var p2 : Set.of(bob, alice)) {
            String principalName = p2.name();
            builder.allow()
                    .subjectsHavingPrincipal(p2.getClass())
                    .withNameEqualTo(principalName)
                    .operations(Set.of(FakeTopicResource.WRITE))
                    .onResourceWithNameEqualTo(aliceAndBobWriteOnlyCarolReadOnly);
        }

        for (var p1 : Set.of(carol)) {
            String principalName = p1.name();
            builder.allow()
                    .subjectsHavingPrincipal(p1.getClass())
                    .withNameEqualTo(principalName)
                    .operations(Set.of(FakeTopicResource.READ))
                    .onResourceWithNameEqualTo(aliceAndBobWriteOnlyCarolReadOnly);
        }

        String adminsOnly = "admins-only";
        for (var p : Set.<Principal> of(admins)) {
            String principalName = p.name();
            builder.allow()
                    .subjectsHavingPrincipal(p.getClass())
                    .withNameEqualTo(principalName)
                    .allOperations(FakeTopicResource.class)
                    .onResourceWithNameEqualTo(adminsOnly);
        }

        Authorizer a = builder.build();

        assertThat(decision(a, anon, FakeClusterResource.CONNECT, "")).isEqualTo(Decision.DENY);
        for (var s : List.of(alices, bobs, carols, dans, eves)) {
            assertThat(decision(a, s, FakeClusterResource.CONNECT, "")).as("Expected %s to be ALLOWed", s).isEqualTo(Decision.ALLOW);
        }

        assertThat(decision(a, alices, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, alices, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, alices, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, alices, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, alices, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, alices, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.ALLOW);

        assertThat(decision(a, bobs, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, bobs, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, bobs, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, bobs, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, bobs, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, bobs, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

        assertThat(decision(a, carols, FakeTopicResource.READ, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, carols, FakeTopicResource.WRITE, bobOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, carols, FakeTopicResource.READ, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.ALLOW);
        assertThat(decision(a, carols, FakeTopicResource.WRITE, aliceAndBobWriteOnlyCarolReadOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, carols, FakeTopicResource.READ, adminsOnly)).isEqualTo(Decision.DENY);
        assertThat(decision(a, carols, FakeTopicResource.WRITE, adminsOnly)).isEqualTo(Decision.DENY);

    }

    @NonNull
    private static Decision decision(Authorizer authorizer, Subject subject, ResourceType<?> resourceType, String resourceName) {
        CompletionStage<AuthorizeResult> authorize = authorizer.authorize(subject, List.of(new Action(resourceType, resourceName)));
        assertThat(authorize).isCompleted();
        return assertThat(authorize).succeedsWithin(Duration.ZERO).actual().decision(resourceType, resourceName);
    }

    private enum FakeResourceWithAnonymousConstantBody implements ResourceType<FakeResourceWithAnonymousConstantBody> {
        WITH_BODY {
            @Override
            public Set<FakeResourceWithAnonymousConstantBody> implies() {
                return Set.of(OTHER);
            }
        },
        WITHOUT_BODY,
        OTHER;
    }

}