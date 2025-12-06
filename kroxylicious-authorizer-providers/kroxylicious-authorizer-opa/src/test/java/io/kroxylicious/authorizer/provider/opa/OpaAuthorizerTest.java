/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.authorizer.provider.opa.allow.FakeTopicResource;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;

public class OpaAuthorizerTest {

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

    private static AuthorizeResult getAuthorization(OpaAuthorizer authz, Subject alice, List<Action> op) {
        CompletionStage<AuthorizeResult> authorizationStage = authz.authorize(alice,
                op);
        assertThat(authorizationStage).isCompleted();
        return authorizationStage.toCompletableFuture().join();
    }
}
