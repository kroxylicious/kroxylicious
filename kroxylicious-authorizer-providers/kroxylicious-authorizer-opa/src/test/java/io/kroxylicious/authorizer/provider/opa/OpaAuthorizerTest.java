/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.authorizer.provider.opa.allow.FakeTopicResource;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

public class OpaAuthorizerTest {

    @Test
    void builderAllOperationsAndResourceNameEqualAndPrincipalNameEqual() {
        // Given
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.allOf(FakeTopicResource.class);
        EnumSet<FakeTopicResource> shouldBeDenied = EnumSet.complementOf(shouldBeAllowed);
        Subject alice = new Subject(Set.of(new User("alice")));
        // Use admin user who is both admin (for CREATE) and owner (for READ, WRITE, DESCRIBE, ALTER)
        Subject bob = new Subject(Set.of(new User("admin")));

        // Create test data: admin owns "my-topic", alice owns "your-topic"
        String testDataJson = """
                {
                    "topics": {
                        "my-topic": {
                            "owner": "admin",
                            "subscribers": []
                        },
                        "your-topic": {
                            "owner": "alice",
                            "subscribers": []
                        }
                    }
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/base/policy.wasm"))
                .withData(testDataJson)
                .build();

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

        // Admin can CREATE on any topic, but other operations are denied on "your-topic" (not owned by admin)
        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            if (op == FakeTopicResource.CREATE) {
                assertThat(authorize.allowed()).as("Admin should be able to CREATE on any topic").isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.denied()).isEmpty();
            } else {
                assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.allowed()).isEmpty();
            }
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
        EnumSet<FakeTopicResource> shouldBeAllowed = EnumSet.of(FakeTopicResource.READ, FakeTopicResource.WRITE, FakeTopicResource.DESCRIBE, FakeTopicResource.ALTER);
        EnumSet<FakeTopicResource> shouldBeDenied = EnumSet.of(FakeTopicResource.CREATE);
        Subject alice = new Subject(Set.of(new User("alice")));
        Subject bob = new Subject(Set.of(new User("bob")));

        // Create test data: bob owns "my-topic", alice owns "your-topic"
        String testDataJson = """
                {
                    "topics": {
                        "my-topic": {
                            "owner": "bob",
                            "subscribers": []
                        },
                        "your-topic": {
                            "owner": "alice",
                            "subscribers": []
                        }
                    }
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/base/policy.wasm"))
                .withData(testDataJson)
                .build();

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

        // bob is not admin and not owner of "your-topic", so all operations should be denied
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
        Subject alice = new Subject(Set.of(new User("alice")));
        // Use admin user to allow CREATE operation
        Subject bob = new Subject(Set.of(new User("admin")));

        // Create test data: "my-topic" has no owner (bob is not owner, only admin)
        // This ensures only admin operations (CREATE) are allowed for bob
        String testDataJson = """
                {
                    "topics": {
                        "my-topic": {
                            "owner": "nobody",
                            "subscribers": []
                        },
                        "your-topic": {
                            "owner": "nobody",
                            "subscribers": []
                        }
                    }
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/base/policy.wasm"))
                .withData(testDataJson)
                .build();

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

        // Admin can CREATE on any topic, but other operations are denied on "your-topic" (not owned by admin)
        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            if (op == FakeTopicResource.CREATE) {
                assertThat(authorize.allowed()).as("Admin should be able to CREATE on any topic").isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.denied()).isEmpty();
            } else {
                assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.allowed()).isEmpty();
            }
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
        Subject alice = new Subject(Set.of(new User("alice")));
        // Use admin user who is both admin (for CREATE) and owner (for READ, WRITE, DESCRIBE, ALTER)
        Subject bob = new Subject(Set.of(new User("admin")));

        // Create test data: admin owns resources starting with "my-"
        String testDataJson = """
                {
                    "prefixes": [
                        {
                            "prefix": "my-",
                            "owner": "admin",
                            "subscribers": []
                        }
                    ]
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/prefix/policy.wasm"))
                .withData(testDataJson)
                .build();

        // Then
        for (var op : shouldBeAllowed) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "my-topic"),
                    new Action(op, "my-thingy")));
            assertThat(authorize.allowed()).isEqualTo(List.of(new Action(op, "my-topic"),
                    new Action(op, "my-thingy")));
            assertThat(authorize.denied()).isEmpty();
        }

        // Admin can CREATE on any topic, but other operations are denied on "your-topic" (not owned by admin)
        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "your-topic")));
            if (op == FakeTopicResource.CREATE) {
                assertThat(authorize.allowed()).as("Admin should be able to CREATE on any topic").isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.denied()).isEmpty();
            } else {
                assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "your-topic")));
                assertThat(authorize.allowed()).isEmpty();
            }
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
        Subject alice = new Subject(Set.of(new User("alice")));
        // Use admin user who is both admin (for CREATE) and can access any resource (for READ, WRITE, DESCRIBE, ALTER)
        Subject bob = new Subject(Set.of(new User("admin")));

        // Create test data: admin can access any resource
        String testDataJson = """
                {
                    "anyResourceUsers": ["admin"]
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/any/policy.wasm"))
                .withData(testDataJson)
                .build();

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
        Subject alice = new Subject(Set.of(new User("alice")));
        // Use admin user who is both admin (for CREATE) and owner (for READ, WRITE, DESCRIBE, ALTER)
        Subject bob = new Subject(Set.of(new User("admin")));

        // Create test data: admin owns resources matching pattern "(my|your)-topic+"
        String testDataJson = """
                {
                    "patterns": [
                        {
                            "pattern": "(my|your)-topic+",
                            "owner": "admin",
                            "subscribers": []
                        }
                    ]
                }
                """;

        var authz = OpaAuthorizer.builder()
                .withOpaPolicy(OpaAuthorizerTest.class.getResourceAsStream("/policies/regex/policy.wasm"))
                .withData(testDataJson)
                .build();

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

        // Admin can CREATE on any topic, but other operations are denied on "their-topic" (not matching pattern)
        for (var op : FakeTopicResource.values()) {
            AuthorizeResult authorize = getAuthorization(authz, bob, List.of(new Action(op, "their-topic")));
            if (op == FakeTopicResource.CREATE) {
                assertThat(authorize.allowed()).as("Admin should be able to CREATE on any topic").isEqualTo(List.of(new Action(op, "their-topic")));
                assertThat(authorize.denied()).isEmpty();
            } else {
                assertThat(authorize.denied()).isEqualTo(List.of(new Action(op, "their-topic")));
                assertThat(authorize.allowed()).isEmpty();
            }
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

    private static AuthorizeResult getAuthorization(OpaAuthorizer authz, Subject alice, List<Action> op) {
        CompletionStage<AuthorizeResult> authorizationStage = authz.authorize(alice,
                op);
        assertThat(authorizationStage).isCompleted();
        return authorizationStage.toCompletableFuture().join();
    }

    @NonNull
    private static Decision decision(Authorizer authorizer, Subject subject, ResourceType<?> resourceType, String resourceName) {
        CompletionStage<AuthorizeResult> authorize = authorizer.authorize(subject, List.of(new Action(resourceType, resourceName)));
        assertThat(authorize).isCompleted();
        return assertThat(authorize).succeedsWithin(Duration.ZERO).actual().decision(resourceType, resourceName);
    }
}
