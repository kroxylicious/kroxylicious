/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.io.InputStream;
import java.nio.file.Path;
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
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.Nullable;

public class OpaAuthorizer implements Authorizer {
    private final OpaPolicy policy;

    // TODO: should we inject it? or even use plain strings?
    private final ObjectMapper mapper = new ObjectMapper();

    private OpaAuthorizer(OpaPolicy policy, String dataJson) {
        this.policy = policy.data(dataJson);
    }

    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions) {
        if (actions.isEmpty()) {
            return CompletableFuture.completedStage(
                    new AuthorizeResult(subject, List.of(), List.of()));
        }

        var principals = subject.principals().stream()
                .map(p -> new OpaInput.OpaPrincipal(p.name(), getPrincipalType(p)))
                .toArray(OpaInput.OpaPrincipal[]::new);
        var opaSubject = new OpaInput.OpaSubject(principals);
        var opaActions = actions.stream()
                .map(action -> new OpaInput.OpaAction(operationName(action), action.resourceName()))
                .toArray(OpaInput.OpaAction[]::new);

        var input = new OpaInput(opaActions, opaSubject);

        var inputNode = mapper.valueToTree(input);

        var resultStr = policy.evaluate(inputNode);

        try {
            var results = mapper.readValue(resultStr, OpaResult[].class);
            var result = results.length > 0 && results[0].result() != null ? results[0].result() : new OpaResult.OpaResultData();

            var allowedResults = result.allowed() != null
                    ? java.util.Arrays.stream(result.allowed())
                            .filter(java.util.Objects::nonNull)
                            .collect(Collectors.toSet())
                    : Set.<OpaResult.OpaActionResult> of();
            var deniedResults = result.denied() != null
                    ? java.util.Arrays.stream(result.denied())
                            .filter(java.util.Objects::nonNull)
                            .collect(Collectors.toSet())
                    : Set.<OpaResult.OpaActionResult> of();

            var allowedActions = actions.stream()
                    .filter(action -> allowedResults.stream()
                            .anyMatch(r -> operationName(action).equals(r.action())
                                    && action.resourceName().equals(r.resourceName())))
                    .collect(Collectors.toUnmodifiableList());
            var deniedActions = actions.stream()
                    .filter(action -> deniedResults.stream()
                            .anyMatch(r -> operationName(action).equals(r.action())
                                    && action.resourceName().equals(r.resourceName())))
                    .collect(Collectors.toUnmodifiableList());

            return CompletableFuture.completedStage(
                    new AuthorizeResult(subject, allowedActions, deniedActions));
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse result: " + resultStr, e);
        }
    }

    private static String getPrincipalType(Principal principal) {
        if (principal instanceof User) {
            return "User";
        }
        // TODO: handle other principal types (Role, etc.)
        return principal.getClass().getSimpleName();
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
        @Nullable
        OpaPolicy.Builder opaPolicyBuilder;
        @Nullable
        String dataJson;

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

        public Builder withData(String dataJson) {
            this.dataJson = dataJson;
            return this;
        }

        public OpaAuthorizer build() {
            if (dataJson == null) {
                throw new IllegalStateException("Data must be provided via withData() method");
            }
            var policy = opaPolicyBuilder.build();
            return new OpaAuthorizer(policy, dataJson);
        }
    }
}
