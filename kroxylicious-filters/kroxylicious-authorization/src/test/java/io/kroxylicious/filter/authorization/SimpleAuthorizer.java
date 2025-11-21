/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.NonNull;

class SimpleAuthorizer implements Authorizer {

    private final Set<AllowedOperation> allowedOperations;

    SimpleAuthorizer(Config config) {
        this.allowedOperations = config.allowed().stream().map(allowedActionDef -> {
            ResourceType<?> resourceType = allowedActionDef.resourceClass().toResourceType(allowedActionDef.resourceType);
            return new AllowedOperation(allowedActionDef.subject, allowedActionDef.resourceName, resourceType);
        }).collect(Collectors.toSet());
    }

    @NonNull
    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, @NonNull List<io.kroxylicious.authorizer.service.Action> actions) {
        Set<Principal> principals = subject.principals();
        if (principals.size() != 1) {
            throw new IllegalStateException("Subject must have exactly one principal");
        }
        String principal = principals.stream().findFirst().map(Principal::name).orElseThrow();
        Map<Boolean, List<Action>> collect = actions.stream().collect(Collectors.partitioningBy(action -> {
            AllowedOperation operation = new AllowedOperation(principal, action.resourceName(), action.operation());
            return allowedOperations.contains(operation);
        }));
        return CompletableFuture.completedFuture(new AuthorizeResult(subject, collect.get(true), collect.get(false)));
    }

    @NonNull
    @Override
    public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
        return Optional.of(Set.of(TopicResource.class, ClusterResource.class));
    }

    record Config(List<AllowedActionDef> allowed) {

    }

    enum TargetResourceType {
        TOPIC(TopicResource::valueOf),
        CLUSTER(ClusterResource::valueOf);

        private final Function<String, ResourceType<?>> resourceTypeFunction;

        TargetResourceType(Function<String, ResourceType<?>> resourceTypeFunction) {
            this.resourceTypeFunction = resourceTypeFunction;
        }

        public ResourceType<?> toResourceType(String resourceName) {
            return resourceTypeFunction.apply(resourceName);
        }
    }

    record AllowedOperation(String subject, String resourceName, ResourceType<?> operation) {

    }

    record AllowedActionDef(String subject, String resourceName, TargetResourceType resourceClass, String resourceType) {

    }
}
