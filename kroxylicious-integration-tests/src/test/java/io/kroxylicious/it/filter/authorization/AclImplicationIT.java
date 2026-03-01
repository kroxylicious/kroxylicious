/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerConfig;
import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.filter.authorization.GroupResource;
import io.kroxylicious.filter.authorization.TopicResource;
import io.kroxylicious.filter.authorization.TransactionalIdResource;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

/**
 * Verifies quickly that the Resource Type implications declared by the Authorization Filter are honoured
 * by the ACL Authorizer. We have unit tests showing that the ACL Authorizer can apply implications in general
 * and unit tests showing that we set up our implications in the resources. But it is nice to have further confidence
 * they are plugged together correctly without having to test it via an RPC authorization.
 */
public class AclImplicationIT {

    public static final String ALICE = "alice";
    public static final Subject ALICE_SUBJECT = new Subject(new User(ALICE));
    public static final String RESOURCE_NAME = "my-resource";

    public static Stream<Arguments> implication() {
        return Stream.of(argumentSet("transactionalId write implies describe", "WRITE", TransactionalIdResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("group read implies describe", "READ", GroupResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("group delete implies describe", "DELETE", GroupResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("group alter configs implies describe configs", "ALTER_CONFIGS", GroupResource.DESCRIBE_CONFIGS, RESOURCE_NAME),
                argumentSet("topic write implies describe", "WRITE", TopicResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("topic read implies describe", "READ", TopicResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("topic delete implies describe", "DELETE", TopicResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("topic alter implies describe", "ALTER", TopicResource.DESCRIBE, RESOURCE_NAME),
                argumentSet("topic alter configs implies describe configs", "ALTER_CONFIGS", TopicResource.DESCRIBE_CONFIGS, RESOURCE_NAME));
    }

    @MethodSource
    @ParameterizedTest
    void implication(String allowedResource, ResourceType<?> authorizeResource, String resourceName) throws IOException {
        var rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import %s as Resource;
                allow User with name = "%s" to %s Resource with name = "%s";
                otherwise deny;
                """.formatted(authorizeResource.getClass().getSimpleName(), ALICE, allowedResource, RESOURCE_NAME));
        AclAuthorizerService aclAuthorizerService = new AclAuthorizerService();
        aclAuthorizerService.initialize(new AclAuthorizerConfig(rulesFile.toString()));
        Authorizer build = aclAuthorizerService.build();
        Action action = new Action(authorizeResource, resourceName);
        CompletionStage<AuthorizeResult> result = build.authorize(ALICE_SUBJECT, List.of(action));
        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(authorizeResult -> {
            assertThat(authorizeResult.allowed()).contains(action);
        });
    }
}
