/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.antlr.v4.runtime.CharStreams;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Authorization;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.Operation;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AclAuthorizerServiceTest {

    static Decision decision(AclAuthorizer authz,
                      Principal principal,
                      Operation<?> operation,
                      String resourceName) {
        CompletionStage<Authorization> authorize = authz.authorize(
                new Subject(Set.of(principal)),
                List.of(new Action(operation, resourceName)));
        assertThat(authorize).isCompleted();
        return authorize.toCompletableFuture().join()
                .decision(operation, resourceName);
    }

    static List<Arguments> parserErrors() {
        return List.of(
                Arguments.argumentSet("Missing version",
                """
                //version 1;
                import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                
                allow User with name = "Alice" to READ Topic with name = "foo";
                
                otherwise deny;""",
                        "2:0: mismatched input 'import' expecting 'version'."),
                Arguments.argumentSet("Unsupported version",
                        """
                        version 12;
                        import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        allow User with name = "Alice" to READ Topic with name = "foo";
                        
                        otherwise deny;""",
                        "1:9: token recognition error at: '2'."),
                Arguments.argumentSet("Missing final 'otherwise deny'",
                        """
                        version 1;
                        import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        allow User with name = "Alice" to READ Topic with name = "foo";
                        
                        //otherwise deny;
                        """,
                        "8:0: extraneous input '<EOF>' expecting {'allow', 'otherwise'}."),
                Arguments.argumentSet("Bad keyword",
                        """
                        version 1;
                        import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        frobnicate User with name = "Alice" to READ Topic with name = "foo";
                        
                        otherwise deny;""",
                        "5:0: extraneous input 'frobnicate' expecting {'deny', 'allow', 'otherwise', 'import'}.")
        );
    }

    @ParameterizedTest
    @MethodSource
    void parserErrors(String src, String expectedError) {
        var exasserrt = assertThatThrownBy(() -> AclAuthorizerService.parse(CharStreams.fromString(src)));
        exasserrt.isExactlyInstanceOf(InvalidRulesFileException.class)
                .hasMessageMatching("Found [1-9][0-9]* syntax errors");
        assertThat(((InvalidRulesFileException) exasserrt.actual()).errors()).contains(expectedError);
    }

    static List<Arguments> checkErrors() {
        return List.of(
                Arguments.argumentSet("Missing import",
                        """
                        version 1;
                        //import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        allow User with name = "Alice" to READ Topic with name = "foo";
                        
                        otherwise deny;""",
                        "5:6: Principal class with name 'User' has not been imported."),
                Arguments.argumentSet(
                        "Missing principal class",
                        """
                        version 1;
                        import DoesNotExist as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        allow User with name = "Alice" to READ Topic with name = "foo";
                        
                        otherwise deny;""",
                        "5:6: Principal class 'io.kroxylicious.authorizer.provider.acl.DoesNotExist' was not found."),
                Arguments.argumentSet(
                        "Principal class not a subclass",
                        """
                        version 1;
                        import FakeTopicResource as User from io.kroxylicious.authorizer.provider.acl;
                        import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                        
                        allow User with name = "Alice" to READ Topic with name = "foo";
                        
                        otherwise deny;""",
                        "5:6: Principal class 'User' is not a subclass of interface io.kroxylicious.proxy.authentication.Principal.")
        );
    }

    @ParameterizedTest
    @MethodSource
    void checkErrors(String src, String expectedError) {
        var exasserrt = assertThatThrownBy(() -> AclAuthorizerService.parse(CharStreams.fromString(src)));
        exasserrt.isExactlyInstanceOf(InvalidRulesFileException.class)
                .hasMessageMatching("Found [1-9][0-9]* errors");
        assertThat(((InvalidRulesFileException) exasserrt.actual()).errors()).contains(expectedError);
    }

    @Test
    void test() {
        var authz = AclAuthorizerService.parse(CharStreams.fromString("""
                version 1;
                import UserPrincipal as User from io.kroxylicious.authorizer.provider.acl;
                import FakeTopicResource as Topic from io.kroxylicious.authorizer.provider.acl;
                
                allow User with name = "Alice" to READ Topic with name = "foo";
                
                otherwise deny;"""));
          assertThat(decision(authz, new UserPrincipal("Alice"), FakeTopicResource.READ, "foo"))
                  .isEqualTo(Decision.ALLOW);
          assertThat(decision(authz, new UserPrincipal("Alice"), FakeTopicResource.READ, "bar"))
                  .as("Mismatching resource name")
                  .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new UserPrincipal("Alice"), FakeClusterResource.CONNECT, "foo"))
                .as("Mismatching resource type")
                .isEqualTo(Decision.DENY);
          assertThat(decision(authz, new UserPrincipal("Bob"), FakeTopicResource.READ, "foo"))
                  .as("Mismatching principal name")
                  .isEqualTo(Decision.DENY);
        assertThat(decision(authz, new RolePrincipal("Alice"), FakeTopicResource.READ, "foo"))
                .as("Mismatching principal type")
                .isEqualTo(Decision.DENY);
    }

}