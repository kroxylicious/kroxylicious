/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.google.re2j.Pattern;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>An implementation of {@link Authorizer} in terms of a list of rules defining <em>who</em> can do <em>what</em>
 * that is evaluated in-process.</p>
 *
 * <p>The rules are deny-by-default, meaning that subjects need to be <strong>explicitly granted</strong> permission to be able to perform any action.
 * There is no "super user", so <strong>every subject</strong> needs to be granted access to every resource.
 * However, to avoid needing a vast number of rules there are mechanisms to
 * grant access by subjects to resources <i>en-masse</i> using prefixes and regular expressions.
 * Also, it is possible to explicitly deny actions which are otherwise allowed by a "wider" pattern; this allows
 * granting access broadly while removing it in some specific cases, which is simpler than having to enumerate
 * every case where access should be allowed.
 * </p>
 *
 * <p>The set of rules can be defined:</p>
 * <ul>
 *     <li>programmatically, using the fluent API exposed by {@link #builder()},</li>
 *     <li>or externally, from a file which expresses the rules using naturalish language,
 *     according to a simple grammar (see {@code src/main/antlr4/io/kroxylicious/authorizer/provider/acl/parser/AclRules.g4}).</li>
 * </ul>
 */
public class AclAuthorizer implements Authorizer {

    record ResourceGrants(
                           @Nullable TypeNameMap<ResourceType<?>, EnumSet<? extends ResourceType<?>>> nameMatches,
                           @Nullable TypePatternMatch patternMatches) {
    }

    private final TypeNameMap<Principal, ResourceGrants> denyPerPrincipal = new TypeNameMap<>();

    private final TypeNameMap<Principal, ResourceGrants> allowPerPrincipal = new TypeNameMap<>();

    private final Set<Class<? extends ResourceType<?>>> usedResourceTypes = new HashSet<>();

    static Builder builder() {
        return new Builder();
    }

    public static class PrincipalSelectorBuilder {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final boolean allow;

        public PrincipalSelectorBuilder(Builder builder,
                                        boolean allow,
                                        Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.allow = allow;
            this.principalClass = principalClass;
        }

        public OperationsBuilder withNameEqualTo(String principalName) {
            return new OperationsBuilder(builder,
                    allow,
                    Set.of(new ResourceMatcherNameEquals<>(principalClass, principalName)));
        }

        public OperationsBuilder withNameIn(Set<String> principalNames) {
            return new OperationsBuilder(builder,
                    allow,
                    principalNames.stream()
                            .map(principalName -> (OrderedKey<Principal>) new ResourceMatcherNameEquals<>(principalClass, principalName))
                            .collect(Collectors.toSet()));
        }

        public OperationsBuilder withNameStartingWith(String principalNamePrefix) {
            return new OperationsBuilder(builder, allow, Set.of(new ResourceMatcherNameStarts<>(principalClass, principalNamePrefix)));
        }

        public OperationsBuilder withAnyName() {
            return new OperationsBuilder(builder, allow, Set.of(new ResourceMatcherAnyOfType<>(principalClass)));
        }
    }

    public static class SubjectSelectorBuilder {

        private final Builder builder;

        private final boolean allow;

        private SubjectSelectorBuilder(Builder builder, boolean allow) {
            this.builder = builder;
            this.allow = allow;
        }

        public PrincipalSelectorBuilder subjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder(builder, allow,
                    userPrincipalClass);
        }
    }

    public static class ResourceBuilder<O extends Enum<O> & ResourceType<O>> {
        private final Builder builder;
        private final Set<? extends OrderedKey<Principal>> principalMatchers;
        private final Class<O> operationsClass;
        private final Set<O> operations;
        private final boolean allow;

        public ResourceBuilder(Builder builder,
                               boolean allow,
                               Set<? extends OrderedKey<Principal>> principalMatchers,
                               Class<O> operationsClass,
                               Set<O> operations) {
            this.builder = Objects.requireNonNull(builder);
            this.allow = allow;
            this.principalMatchers = Objects.requireNonNull(principalMatchers);
            this.operationsClass = Objects.requireNonNull(operationsClass);
            this.operations = Objects.requireNonNull(operations);
        }

        public Builder onResourceWithNameEqualTo(String resourceName) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalMatcher,
                        new ResourceMatcherNameEquals<>(operationsClass, resourceName),
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameIn(Set<String> resourceNames) {
            for (var principalMatcher : principalMatchers) {
                for (String resourceName : resourceNames) {
                    builder.aclAuthorizer.internalAllowOrDeny(allow,
                            principalMatcher,
                            new ResourceMatcherNameEquals<>(operationsClass, resourceName),
                            operations);
                }
            }
            return builder;
        }

        public Builder onResourcesWithNameStartingWith(String resourceNamePrefix) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalMatcher,
                        new ResourceMatcherNameStarts<>(operationsClass, resourceNamePrefix),
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameMatching(String resourceNameRegex) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalMatcher,
                        new ResourceMatcherNameMatches<>(operationsClass, Pattern.compile(resourceNameRegex)),
                        operations);
            }
            return builder;
        }

        public Builder onAllResources() {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalMatcher,
                        new ResourceMatcherAnyOfType<>(operationsClass),
                        operations);
            }
            return builder;
        }
    }

    public static class OperationsBuilder {

        private final Builder builder;
        private final boolean allow;
        private final Set<? extends OrderedKey<Principal>> principalMatchers;

        private OperationsBuilder(Builder builder,
                                  boolean allow,
                                  Set<OrderedKey<Principal>> principalMatchers) {
            this.builder = builder;
            this.allow = allow;
            this.principalMatchers = Objects.requireNonNull(principalMatchers);
        }

        <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> allOperations(Class<O> cls) {
            return new ResourceBuilder<>(builder,
                    allow,
                    this.principalMatchers,
                    cls,
                    EnumSet.allOf(cls));
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> operations(Set<O> operations) {
            EnumSet<O> os = EnumSet.copyOf(operations);
            return new ResourceBuilder<>(builder,
                    allow,
                    this.principalMatchers,
                    (Class) operations.iterator().next().getClass(),
                    os);
        }

    }

    public static class Builder {
        AclAuthorizer aclAuthorizer = new AclAuthorizer();

        public SubjectSelectorBuilder allow() {
            return new SubjectSelectorBuilder(this, true);
        }

        public SubjectSelectorBuilder deny() {
            return new SubjectSelectorBuilder(this, false);
        }

        public AclAuthorizer build() {
            return aclAuthorizer;
        }
    }

    private <O extends Enum<O> & ResourceType<O>> void internalAllowOrDeny(boolean allow,
                                                                           OrderedKey<Principal> principalMatcher,
                                                                           Key<O> resourceMatcher,
                                                                           Set<O> operations) {
        usedResourceTypes.add(resourceMatcher.type());
        internalAllowOrDeny(allow ? allowPerPrincipal : denyPerPrincipal,
                principalMatcher,
                resourceMatcher,
                operations);
    }

    @SuppressWarnings("rawtypes")
    @VisibleForTesting
    <O extends Enum<O> & ResourceType<O>> void internalAllowOrDeny(
                                                                   TypeNameMap<Principal, ResourceGrants> allowPerPrincipal,
                                                                   OrderedKey<Principal> principalMatcher,
                                                                   Key<O> resourceMatcher,
                                                                   Set<O> operations) {
        var es = EnumSet.copyOf(operations);
        for (var op : es) {
            es.addAll(op.implies());
        }
        ResourceGrants compute = allowPerPrincipal.addApply(principalMatcher,
                g -> {
                    if (g == null) {
                        return new ResourceGrants(resourceMatcher instanceof ResourceMatcherNameMatches ? null : new TypeNameMap<>(),
                                resourceMatcher instanceof ResourceMatcherNameMatches ? new TypePatternMatch() : null);
                    }
                    else if (g.patternMatches() == null && resourceMatcher instanceof ResourceMatcherNameMatches) {
                        return new ResourceGrants(g.nameMatches(), new TypePatternMatch());
                    }
                    else if (g.nameMatches() == null && !(resourceMatcher instanceof ResourceMatcherNameMatches)) {
                        return new ResourceGrants(new TypeNameMap<>(), g.patternMatches());
                    }
                    return g;
                });

        if (resourceMatcher instanceof ResourceMatcherNameMatches resourceNameMatch) {
            Objects.requireNonNull(compute.patternMatches()).add(resourceNameMatch,
                    es);
        }
        else if (resourceMatcher instanceof OrderedKey orderedKey) {
            Objects.requireNonNull(compute.nameMatches()).addApply(orderedKey,
                    v -> {
                        if (v == null) {
                            return es;
                        }
                        es.addAll((EnumSet) v);
                        return es;
                    });
        }
    }

    private static @Nullable Decision authorizeInternal(
                                                        TypeNameMap<Principal, ResourceGrants> allowPerPrincipal,
                                                        Subject subject,
                                                        Action action,
                                                        Decision whenFound,
                                                        @Nullable Decision whenNotFound) {
        assert (whenFound != whenNotFound);
        for (var p : subject.principals()) {

            Decision foundDecision;
            var grant = allowPerPrincipal.matchingOperations(new ResourceMatcherNameEquals<>(p.getClass(), p.name()));
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
            grant = allowPerPrincipal.matchingOperations(new ResourceMatcherAnyOfType<>(p.getClass()));
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
            grant = allowPerPrincipal.matchingOperations(new ResourceMatcherNameStarts<>(p.getClass(), p.name()));
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
        }
        return whenNotFound;
    }

    @Nullable
    private static Decision getDecision(Action action,
                                        ResourceGrants grants,
                                        Decision whenFound) {
        Set<? extends ResourceType<?>> operations;
        var typeNameMap = grants.nameMatches();
        ResourceType<?> resourceType = action.operation();
        if (typeNameMap != null) {
            operations = typeNameMap.matchingOperations(new ResourceMatcherNameEquals<>(action.resourceTypeClass(),
                    action.resourceName()));
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
            operations = typeNameMap.matchingOperations(new ResourceMatcherAnyOfType<>(action.resourceTypeClass()));
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
            operations = typeNameMap.matchingOperations(new ResourceMatcherNameStarts<>(action.resourceTypeClass(),
                    action.resourceName()));
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
        }
        var patternMatch = grants.patternMatches();
        if (patternMatch != null) {
            operations = patternMatch.matchingOperations((Class) action.resourceTypeClass(), action.resourceName());
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
        }
        return null;
    }

    /**
     * Return true iff the given set of operations allow the given action.
     */
    private static boolean isFound(@Nullable Set<? extends ResourceType<?>> operations,
                                   ResourceType<?> op) {
        return operations != null && operations.contains(op);
    }

    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions) {
        List<Action> allowedActions = new ArrayList<>();
        List<Action> deniedActions = new ArrayList<>();
        for (var action : actions) {
            @Nullable
            Decision decision = authorizeInternal(this.denyPerPrincipal, subject, action, Decision.DENY, null);
            if (decision == Decision.DENY) {
                deniedActions.add(action);
            }
            else {
                decision = authorizeInternal(this.allowPerPrincipal, subject, action, Decision.ALLOW, Decision.DENY);
                if (decision == Decision.DENY) {
                    deniedActions.add(action);
                }
                else if (decision == Decision.ALLOW) {
                    allowedActions.add(action);
                }
                else {
                    throw new IllegalStateException();
                }
            }
        }
        return CompletableFuture.completedStage(new AuthorizeResult(subject, allowedActions, deniedActions));
    }

    @Override
    public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
        return Optional.of(Set.copyOf(usedResourceTypes));
    }

    @Override
    public String toString() {
        return "AclAuthorizer{" +
                "perPrincipal=" + allowPerPrincipal +
                '}';
    }
}
