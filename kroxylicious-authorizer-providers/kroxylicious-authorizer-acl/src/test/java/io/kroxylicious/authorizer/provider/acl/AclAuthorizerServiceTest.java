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
import java.util.regex.Pattern;

import org.antlr.v4.runtime.CharStreams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.authorizer.provider.acl.allow.FakeClusterResource;
import io.kroxylicious.authorizer.provider.acl.allow.FakeTopicResource;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AclAuthorizerServiceTest {

    static Decision decision(AclAuthorizer authz,
                             Principal principal,
                             ResourceType<?> resourceType,
                             String resourceName) {
        CompletionStage<AuthorizeResult> authorize = authz.authorize(
                new Subject(principal instanceof User ? Set.of(principal) : Set.of(new User("Mallory"), principal)),
                List.of(new Action(resourceType, resourceName)));
        assertThat(authorize).isCompleted();
        return authorize.toCompletableFuture().join()
                .decision(resourceType, resourceName);
    }

    static List<Arguments> parserErrors() {
        return List.of(
                Arguments.argumentSet("Missing version",
                        """
                                //version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "2:0: mismatched input 'import' expecting 'version'."),
                Arguments.argumentSet("Missing final 'otherwise deny'",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                //otherwise deny;
                                """,
                        "8:0: extraneous input '<EOF>' expecting {'allow', 'otherwise'}."),
                Arguments.argumentSet("Bad keyword",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                frobnicate User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "5:0: extraneous input 'frobnicate' expecting {'deny', 'allow', 'otherwise', 'import'}."),
                Arguments.argumentSet("Allow before deny",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";
                                deny User with name = "Eve" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "6:0: extraneous input 'deny' expecting {'allow', 'otherwise'}."),
                Arguments.argumentSet("Using matching with principal",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name matching /Ali(ce)?/ to READ Topic with name = "foo";

                                otherwise deny;""",
                        "5:21: mismatched input 'matching' expecting {'*', '=', 'in', 'like'}."));
    }

    @ParameterizedTest
    @MethodSource
    void parserErrors(String src, String expectedError) {
        var exasserrt = assertThatThrownBy(() -> AclAuthorizerService.parse(CharStreams.fromString(src)));
        exasserrt.isExactlyInstanceOf(InvalidRulesFileException.class)
                .hasMessageMatching(Pattern.compile("Found [1-9][0-9]* syntax errors.*", Pattern.DOTALL));
        assertThat(((InvalidRulesFileException) exasserrt.actual()).errors()).contains(expectedError);
    }

    static List<Arguments> checkErrors() {
        return List.of(
                Arguments.argumentSet("Unsupported version",
                        """
                                version 12;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "1:0: Unsupported version: Only version 1 is supported."),
                Arguments.argumentSet("Missing import",
                        """
                                version 1;
                                //import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "5:6: Principal class with name 'User' has not been imported."),
                Arguments.argumentSet("Colliding import",
                        """
                                version 1;
                                import User as Collide from io.kroxylicious.authorizer.provider.acl;
                                import FakeTopicResource as Collide from io.kroxylicious.authorizer.provider.acl.allow;

                                otherwise deny;""",
                        "3:28: Local name 'Collide' is already being used for class io.kroxylicious.authorizer.provider.acl.User."),
                Arguments.argumentSet(
                        "Missing principal class",
                        """
                                version 1;
                                import DoesNotExist as User from io.kroxylicious.authorizer.provider.acl;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "5:6: Principal class 'io.kroxylicious.authorizer.provider.acl.DoesNotExist' was not found."),
                Arguments.argumentSet(
                        "Principal class not a subclass",
                        """
                                version 1;
                                import FakeTopicResource as User from io.kroxylicious.authorizer.provider.acl.allow;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name = "foo";

                                otherwise deny;""",
                        "5:6: Principal class 'User' is not a subclass of interface io.kroxylicious.proxy.authentication.Principal."),
                Arguments.argumentSet(
                        "Invalid like",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name like "Foo*Bar";

                                otherwise deny;""",
                        """
                                5:60: Wildcard '*' only supported as last character in 'like'."""),
                Arguments.argumentSet(
                        "Invalid regex",
                        """
                                version 1;
                                import User as User from io.kroxylicious.proxy.authentication;
                                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                                allow User with name = "Alice" to READ Topic with name matching /**/;

                                otherwise deny;""",
                        """
                                5:64: Regex provided for 'matching' operation is not valid: error parsing regexp: missing argument to repetition operator: `*`."""));
    }

    @ParameterizedTest
    @MethodSource
    void checkErrors(String src, String expectedError) {
        Assertions.setMaxStackTraceElementsDisplayed(Integer.MAX_VALUE);
        var exasserrt = assertThatThrownBy(() -> AclAuthorizerService.parse(CharStreams.fromString(src)));
        exasserrt.isExactlyInstanceOf(InvalidRulesFileException.class)
                .hasMessageMatching(Pattern.compile("Found [1-9][0-9]* errors.*", Pattern.DOTALL));
        assertThat(((InvalidRulesFileException) exasserrt.actual()).errors()).contains(expectedError);
    }

    @Test
    void testPrincipalEqResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name = "foo";

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.WRITE, "foo"))
                .as("Mismatching operation name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.CREATE, "foo"))
                .as("Mismatching operation name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "bar"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alicea"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalInResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name in {"Alice", "Bob"} to READ Topic with name = "foo";

                otherwise deny;"""));
        for (String allowedPrincipal : Set.of("Alice", "Bob")) {
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .isEqualTo(Decision.ALLOW);
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "bar"))
                    .as("Mismatching resource name")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new User(allowedPrincipal), FakeClusterResource.CONNECT, "foo"))
                    .as("Mismatching resource type")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new RolePrincipal(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal type")
                    .isEqualTo(Decision.DENY);
        }
        for (String deniedPrincipal : Set.of("Eve", "Alic")) {
            assertThat(decision(authz, new User(deniedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal name")
                    .isEqualTo(Decision.DENY);
        }
    }

    @Test
    void testPrincipalStartsWithResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name like "Alice*" to READ Topic with name = "foo";

                otherwise deny;"""));
        for (String allowedPrincipal : Set.of("Alice", "Alicea")) {
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .isEqualTo(Decision.ALLOW);
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "bar"))
                    .as("Mismatching resource name")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new User(allowedPrincipal), FakeClusterResource.CONNECT, "foo"))
                    .as("Mismatching resource type")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new RolePrincipal(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal type")
                    .isEqualTo(Decision.DENY);
        }
        for (String deniedPrincipal : Set.of("Eve", "Alic")) {
            assertThat(decision(authz, new User(deniedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal name")
                    .isEqualTo(Decision.DENY);
        }
    }

    @Test
    void testPrincipalStartsWithAnyResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name like "*" to READ Topic with name = "foo";

                otherwise deny;"""));
        for (String allowedPrincipal : Set.of("Alice", "Alicea", "Eve", "Alic")) {
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .isEqualTo(Decision.ALLOW);
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "bar"))
                    .as("Mismatching resource name")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new User(allowedPrincipal), FakeClusterResource.CONNECT, "foo"))
                    .as("Mismatching resource type")
                    .isEqualTo(Decision.DENY);
            // assertThat(decision(authz, new RolePrincipal(allowedPrincipal), FakeTopicResource.READ, "foo"))
            // .as("Mismatching principal type")
            // .isEqualTo(Decision.DENY);
        }
    }

    @Test
    void testPrincipalStartsWithAndEqResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name like "Alice" to READ Topic with name = "foo";

                otherwise deny;"""));
        for (String allowedPrincipal : Set.of("Alice")) {
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .isEqualTo(Decision.ALLOW);
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "bar"))
                    .as("Mismatching resource name")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new User(allowedPrincipal), FakeClusterResource.CONNECT, "foo"))
                    .as("Mismatching resource type")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new RolePrincipal(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal type")
                    .isEqualTo(Decision.DENY);
        }
        for (String deniedPrincipal : Set.of("Eve", "Alic", "Alicea")) {
            assertThat(decision(authz, new User(deniedPrincipal), FakeTopicResource.READ, "foo"))
                    .as("Mismatching principal name")
                    .isEqualTo(Decision.DENY);
        }
    }

    @Test
    void testPrincipalAnyResourceEq() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name * to READ Topic with name = "foo";

                otherwise deny;"""));
        for (String allowedPrincipal : Set.of("Alice", "Bob", "Eve", "Alic")) {
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "foo"))
                    .isEqualTo(Decision.ALLOW);
            assertThat(decision(authz, new User(allowedPrincipal), FakeTopicResource.READ, "bar"))
                    .as("Mismatching resource name")
                    .isEqualTo(Decision.DENY);
            assertThat(decision(authz, new User(allowedPrincipal), FakeClusterResource.CONNECT, "foo"))
                    .as("Mismatching resource type")
                    .isEqualTo(Decision.DENY);
            // assertThat(decision(authz, new RolePrincipal(allowedPrincipal), FakeTopicResource.READ, "foo"))
            // .as("Mismatching principal type")
            // .isEqualTo(Decision.DENY);
        }
    }

    @Test
    void testPrincipalEqResourceIn() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name in {"foo", "bar"};

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "bar"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "baz"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalEqResourceStartsWith() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name like "foo*";

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foobar"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "fo"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalEqResourceAny() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name *;

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foobar"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "fo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalEqResourceMatching() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name matching /(foo+|bar)/;

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "bar"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "fooo"))
                .as("Matching resource name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "fo"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "Foo"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foobar"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalEqResourceEqOpsIn() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to {READ, WRITE} Topic with name = "foo";

                otherwise deny;"""));
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.WRITE, "foo"))
                .as("Matching operation name")
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.CREATE, "foo"))
                .as("Mismatching operation name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "bar"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alicea"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testPrincipalEqResourceEqOpsAny() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to * Topic with name = "foo";

                otherwise deny;"""));
        for (var op : FakeTopicResource.values()) {
            assertThat(decision(authz, new User("Alice"), op, "foo"))
                    .isEqualTo(Decision.ALLOW);
        }
        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "bar"))
                .as("Mismatching resource name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alicea"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testGrantsAreAdditive() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name = "foo";
                allow User with name = "Alice" to WRITE Topic with name = "foo";
                allow User with name = "Alice" to * Topic with name = "foo";

                allow User with name = "Alice" to READ Topic with name = "bar";
                allow User with name = "Alice" to * Topic with name = "bar";
                allow User with name = "Alice" to WRITE Topic with name = "bar";

                allow User with name = "Alice" to READ Topic with name = "baz";
                allow User with name = "Alice" to WRITE Topic with name = "baz";

                otherwise deny;"""));
        for (var op : FakeTopicResource.values()) {
            assertThat(decision(authz, new User("Alice"), op, "foo"))
                    .as("Matching operation %s", op)
                    .isEqualTo(Decision.ALLOW);
        }
        for (var op : FakeTopicResource.values()) {
            assertThat(decision(authz, new User("Alice"), op, "bar"))
                    .as("Matching operation %s", op)
                    .isEqualTo(Decision.ALLOW);
        }
        EnumSet<FakeTopicResource> expectedBazOps = EnumSet.of(
                FakeTopicResource.READ,
                FakeTopicResource.WRITE,
                FakeTopicResource.DESCRIBE // by implication rules
        );
        for (var op : FakeTopicResource.values()) {
            assertThat(decision(authz, new User("Alice"), op, "baz"))
                    .as("Matching operation %s", op)
                    .isEqualTo(expectedBazOps.contains(op) ? Decision.ALLOW : Decision.DENY);
        }

        assertThat(decision(authz, new User("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new User("Alicea"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal name")
                .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

    @Test
    void testDenyOverrulesAllow() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                deny User with name = "Eve" to READ Topic with name = "foo";
                allow User with name * to READ Topic with name = "foo";
                otherwise deny;"""));

        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Eve"), FakeTopicResource.READ, "foo"))
                .isEqualTo(Decision.DENY);

    }

    @Test
    void testStringQuoting() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "NameWithA\\"" to READ Topic with name = "foo\\\\";
                allow User with name = "Carol" to READ Topic with name in {"bar\\\\", "baz"};
                otherwise deny;"""));

        assertThat(decision(authz, new User("NameWithA\""), FakeTopicResource.READ, "foo\\"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Carol"), FakeTopicResource.READ, "bar\\"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Carol"), FakeTopicResource.READ, "baz"))
                .isEqualTo(Decision.ALLOW);
    }

    @Test
    void testRegexQuoting() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name matching /\\//;
                allow User with name = "Bob" to READ Topic with name matching /\\\\\\\\/;
                // The \\\\\\\\ is a single backslash quoted:
                // 1. backslash in a regex is the quote character, so needs to be doubled to match a real backslash
                // 2. but each of those needs to be doubled because of the lexer rule for quoting.
                // 3. but this is all within this Java triple-quoted string, so all each backslash
                //    again needs to be doubled.
                otherwise deny;"""));

        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "/"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "\\"))
                .isEqualTo(Decision.ALLOW);
    }

    @Test
    void testLikeQuoting() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import User as User from io.kroxylicious.proxy.authentication;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl.allow;

                allow User with name = "Alice" to READ Topic with name like "foo\\*bar*";
                allow User with name = "Bob" to READ Topic with name like "foo\\\\\\*bar*";
                otherwise deny;"""));

        assertThat(decision(authz, new User("Alice"), FakeTopicResource.READ, "foo*bary"))
                .isEqualTo(Decision.ALLOW);
        assertThat(decision(authz, new User("Bob"), FakeTopicResource.READ, "foo\\*bary"))
                .isEqualTo(Decision.ALLOW);
    }
}