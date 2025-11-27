/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.styra.opa.wasm.OpaBuiltin;
import com.styra.opa.wasm.OpaPolicy;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Subject;

public class OpaAuthorizer implements Authorizer {
    private final OpaPolicy policy;

    // TODO: should we inject it? or even use plain strings?
    private final ObjectMapper mapper = new ObjectMapper();

    private OpaAuthorizer(OpaPolicy policy) {
        this.policy = policy;
    }

    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions) {
        // TODO: just demo purposes for now, let see how to use multiple subjects
        var inputSubject = subject.principals().stream().findFirst().get();
        var inputSubjectName = inputSubject.name();
        // TODO: check what would be the real logic here
        var inputActions = actions.stream()
                .map(action -> new OpaInput.OpaAction(operationName(action), action.resourceName()))
                .toArray(OpaInput.OpaAction[]::new);
        var input = new OpaInput(inputSubjectName, inputActions);
        var resultStr = policy.evaluate(mapper.valueToTree(input));
        try {
            var results = mapper.readValue(resultStr, OpaResult[].class);

            // TODO: verify that this is the correct semantics
            var allowed = Arrays.stream(results[0].result.allowed)
                    .map(String::toLowerCase)
                    .collect(Collectors.toUnmodifiableSet());
            var denied = Arrays.stream(results[0].result.denied)
                    .map(String::toLowerCase)
                    .collect(Collectors.toUnmodifiableSet());

            var allowedOutput = actions.stream()
                    .filter(action -> allowed.contains(operationName(action)))
                    .collect(Collectors.toUnmodifiableList());
            var deniedOutput = actions.stream()
                    .filter(action -> denied.contains(operationName(action)))
                    .collect(Collectors.toUnmodifiableList());

            return CompletableFuture.completedStage(new AuthorizeResult(new Subject(inputSubject), allowedOutput, deniedOutput));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse result: " + resultStr, e);
        }
    }

    private static String operationName(Action action) {
        return action.operation().toString().toLowerCase(Locale.ROOT);
    }

    @Override
    public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
        return Optional.empty();
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        OpaPolicy.Builder opaPolicyBuilder;

        public Builder withOpaPolicy(Path policyPath) {
            this.opaPolicyBuilder = OpaPolicy.builder()
                    .withPolicy(policyPath);
            return this;
        }

        public Builder withOpaPolicy(InputStream policyStream) {
            this.opaPolicyBuilder = OpaPolicy.builder()
                    .withPolicy(policyStream);
            return this;
        }

        public Builder addBuiltins(OpaBuiltin.Builtin... builtins) {
            this.opaPolicyBuilder.addBuiltins(builtins);
            return this;
        }

        public OpaAuthorizer build() {
            return new OpaAuthorizer(opaPolicyBuilder.build());
        }
    }
}
