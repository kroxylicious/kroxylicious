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
import io.kroxylicious.identity.Identity;
import io.kroxylicious.identity.Principal;
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
                          @Nullable TypePatternMatch patternMatches) {}

    private final TypeNameMap<Principal, ResourceGrants> denyPerPrincipal = new TypeNameMap<>();

    private final TypeNameMap<Principal, ResourceGrants> allowPerPrincipal = new TypeNameMap<>();

    private final Set<Class<? extends ResourceType<?>>> usedResourceTypes = new HashSet<>();

    /**
     * Constructs an authorizer with an initially empty set of rules, which denies every action.
     * Use {@link #builder()} to construct an authorizer with rules.
     */
    public AclAuthorizer() {
        // explicit ctor needed for javadoc
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Fluent builder stage for selecting the principals to which an allow or deny rule applies.
     */
    public static class PrincipalSelectorBuilder {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final boolean isAllowRule;

        /**
         * Constructs a PrincipalSelectorBuilder.
         * @param builder the top-level builder.
         * @param isAllowRule true if building an allow rule, false if building a deny rule.
         * @param principalClass the type of principal to which the rule applies.
         */
        public PrincipalSelectorBuilder(Builder builder,
                                        boolean isAllowRule,
                                        Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.isAllowRule = isAllowRule;
            this.principalClass = principalClass;
        }

        /**
         * Selects principals whose name is equal to the given name.
         * @param principalName the principal name.
         * @return the next stage in the fluent builder.
         */
        public OperationsBuilder withNameEqualTo(String principalName) {
            return new OperationsBuilder(builder,
                    isAllowRule,
                    Set.of(new ResourceMatcherNameEquals<>(principalClass, principalName)));
        }

        /**
         * Selects principals whose name is any of the given names.
         * @param principalNames the principal names.
         * @return the next stage in the fluent builder.
         */
        public OperationsBuilder withNameIn(Set<String> principalNames) {
            return new OperationsBuilder(builder,
                    isAllowRule,
                    principalNames.stream()
                            .map(principalName -> (OrderedKey<Principal>) new ResourceMatcherNameEquals<>(principalClass, principalName))
                            .collect(Collectors.toSet()));
        }

        /**
         * Selects principals whose name starts with the given prefix.
         * @param principalNamePrefix the principal name prefix.
         * @return the next stage in the fluent builder.
         */
        public OperationsBuilder withNameStartingWith(String principalNamePrefix) {
            return new OperationsBuilder(builder, isAllowRule, Set.of(new ResourceMatcherNameStarts<>(principalClass, principalNamePrefix)));
        }

        /**
         * Selects all principals of the selected type, whatever their name.
         * @return the next stage in the fluent builder.
         */
        public OperationsBuilder withAnyName() {
            return new OperationsBuilder(builder, isAllowRule, Set.of(new ResourceMatcherAnyOfType<>(principalClass)));
        }
    }

    /**
     * Fluent builder stage for selecting the subjects to which an allow or deny rule applies.
     */
    public static class SubjectSelectorBuilder {

        private final Builder builder;

        private final boolean allow;

        private SubjectSelectorBuilder(Builder builder, boolean allow) {
            this.builder = builder;
            this.allow = allow;
        }

        /**
         * Selects subjects having a principal of the given type.
         * @param userPrincipalClass the type of principal to which the rule applies.
         * @return the next stage in the fluent builder.
         */
        public PrincipalSelectorBuilder subjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder(builder, allow,
                    userPrincipalClass);
        }
    }

    /**
     * Fluent builder stage for selecting the resources to which an allow or deny rule applies.
     * @param <O> the type of operation to which the rule applies.
     */
    public static class ResourceBuilder<O extends Enum<O> & ResourceType<O>> {
        private final Builder builder;
        private final Set<? extends OrderedKey<Principal>> principalMatchers;
        private final Class<O> operationsClass;
        private final Set<O> operations;
        private final boolean isAllowRule;

        /**
         * Constructs a ResourceBuilder.
         * @param builder the top-level builder.
         * @param isAllowRule true if building an allow rule, false if building a deny rule.
         * @param principalMatchers the matchers for the principals to which the rule applies.
         * @param operationsClass the type of operation to which the rule applies.
         * @param operations the operations to which the rule applies.
         */
        public ResourceBuilder(Builder builder,
                               boolean isAllowRule,
                               Set<? extends OrderedKey<Principal>> principalMatchers,
                               Class<O> operationsClass,
                               Set<O> operations) {
            this.builder = Objects.requireNonNull(builder);
            this.isAllowRule = isAllowRule;
            this.principalMatchers = Objects.requireNonNull(principalMatchers);
            this.operationsClass = Objects.requireNonNull(operationsClass);
            this.operations = Objects.requireNonNull(operations);
        }

        /**
         * Adds a rule applying to the resource with the given name.
         * @param resourceName the resource name.
         * @return the top-level builder.
         */
        public Builder onResourceWithNameEqualTo(String resourceName) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(isAllowRule,
                        principalMatcher,
                        new ResourceMatcherNameEquals<>(operationsClass, resourceName),
                        operations);
            }
            return builder;
        }

        /**
         * Adds a rule applying to the resources with any of the given names.
         * @param resourceNames the resource names.
         * @return the top-level builder.
         */
        public Builder onResourcesWithNameIn(Set<String> resourceNames) {
            for (var principalMatcher : principalMatchers) {
                for (String resourceName : resourceNames) {
                    builder.aclAuthorizer.internalAllowOrDeny(isAllowRule,
                            principalMatcher,
                            new ResourceMatcherNameEquals<>(operationsClass, resourceName),
                            operations);
                }
            }
            return builder;
        }

        /**
         * Adds a rule applying to the resources whose name starts with the given prefix.
         * @param resourceNamePrefix the resource name prefix.
         * @return the top-level builder.
         */
        public Builder onResourcesWithNameStartingWith(String resourceNamePrefix) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(isAllowRule,
                        principalMatcher,
                        new ResourceMatcherNameStarts<>(operationsClass, resourceNamePrefix),
                        operations);
            }
            return builder;
        }

        /**
         * Adds a rule applying to the resources whose name matches the given regular expression.
         * @param resourceNameRegex the resource name regular expression.
         * @return the top-level builder.
         */
        public Builder onResourcesWithNameMatching(String resourceNameRegex) {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(isAllowRule,
                        principalMatcher,
                        new ResourceMatcherNameMatches<>(operationsClass, Pattern.compile(resourceNameRegex)),
                        operations);
            }
            return builder;
        }

        /**
         * Adds a rule applying to all resources of the selected type, whatever their name.
         * @return the top-level builder.
         */
        public Builder onAllResources() {
            for (var principalMatcher : principalMatchers) {
                builder.aclAuthorizer.internalAllowOrDeny(isAllowRule,
                        principalMatcher,
                        new ResourceMatcherAnyOfType<>(operationsClass),
                        operations);
            }
            return builder;
        }
    }

    /**
     * Fluent builder stage for selecting the operations to which an allow or deny rule applies.
     */
    public static class OperationsBuilder {

        private final Builder builder;
        private final boolean isAllowRule;
        private final Set<? extends OrderedKey<Principal>> principalMatchers;

        private OperationsBuilder(Builder builder,
                                  boolean isAllowRule,
                                  Set<OrderedKey<Principal>> principalMatchers) {
            this.builder = builder;
            this.isAllowRule = isAllowRule;
            this.principalMatchers = Objects.requireNonNull(principalMatchers);
        }

        <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> allOperations(Class<O> cls) {
            return new ResourceBuilder<>(builder,
                    isAllowRule,
                    this.principalMatchers,
                    cls,
                    EnumSet.allOf(cls));
        }

        /**
         * Selects the given operations.
         * @param operations the operations to which the rule applies; must not be empty.
         * @param <O> the type of operation to which the rule applies.
         * @return the next stage in the fluent builder.
         */
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> operations(Set<O> operations) {
            EnumSet<O> os = EnumSet.copyOf(operations);
            return new ResourceBuilder<>(builder,
                    isAllowRule,
                    this.principalMatchers,
                    (Class) operations.iterator().next().getClass(),
                    os);
        }

    }

    /**
     * A fluent builder of {@link AclAuthorizer} instances.
     */
    public static class Builder {
        AclAuthorizer aclAuthorizer = new AclAuthorizer();

        /**
         * Constructs a Builder.
         */
        public Builder() {
            // explicit ctor needed for javadoc
        }

        /**
         * Starts building a rule which allows subjects to perform actions.
         * @return the next stage in the fluent builder.
         */
        public SubjectSelectorBuilder allow() {
            return new SubjectSelectorBuilder(this, true);
        }

        /**
         * Starts building a rule which denies subjects from performing actions.
         * @return the next stage in the fluent builder.
         */
        public SubjectSelectorBuilder deny() {
            return new SubjectSelectorBuilder(this, false);
        }

        /**
         * Builds the authorizer.
         * @return the built authorizer.
         */
        public AclAuthorizer build() {
            return aclAuthorizer;
        }
    }

    private <O extends Enum<O> & ResourceType<O>> void internalAllowOrDeny(boolean isAllowRule,
                                                                           OrderedKey<Principal> principalMatcher,
                                                                           Key<O> resourceMatcher,
                                                                           Set<O> operations) {
        usedResourceTypes.add(resourceMatcher.type());
        internalAllowOrDeny(isAllowRule ? allowPerPrincipal : denyPerPrincipal,
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
                                                        Identity subject,
                                                        Action action,
                                                        Decision whenFound,
                                                        @Nullable Decision whenNotFound) {
        assert whenFound != whenNotFound;
        for (var p : subject.principals()) {
            for (var matcher : principalMatchers(p)) {
                var grant = allowPerPrincipal.matchingOperations(matcher);
                if (grant != null) {
                    Decision foundDecision = decision(action, grant, whenFound);
                    if (foundDecision != null) {
                        return foundDecision;
                    }
                }
            }
        }
        return whenNotFound;
    }

    @SuppressWarnings("unchecked")
    private static List<OrderedKey<Principal>> principalMatchers(Principal p) {
        return List.of(
                (OrderedKey<Principal>) new ResourceMatcherNameEquals<>(p.getClass(), p.name()),
                (OrderedKey<Principal>) new ResourceMatcherAnyOfType<>(p.getClass()),
                (OrderedKey<Principal>) new ResourceMatcherNameStarts<>(p.getClass(), p.name()));
    }

    @SuppressWarnings("unchecked")
    private static List<OrderedKey<?>> resourceMatchers(Action action) {
        return List.of(
                (OrderedKey<?>) new ResourceMatcherNameEquals<>(action.resourceTypeClass(), action.resourceName()),
                (OrderedKey<?>) new ResourceMatcherAnyOfType<>(action.resourceTypeClass()),
                (OrderedKey<?>) new ResourceMatcherNameStarts<>(action.resourceTypeClass(), action.resourceName()));
    }

    @Nullable
    private static Decision decision(Action action,
                                     ResourceGrants grants,
                                     Decision whenFound) {
        Set<? extends ResourceType<?>> operations;
        var typeNameMap = grants.nameMatches();
        ResourceType<?> resourceType = action.operation();
        if (typeNameMap != null) {
            for (var matcher : resourceMatchers(action)) {
                operations = typeNameMap.matchingOperations((OrderedKey) matcher);
                if (isFound(operations, resourceType)) {
                    return whenFound;
                }
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
    public CompletionStage<AuthorizeResult> authorize(Identity subject, List<Action> actions) {
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
                else {
                    allowedActions.add(action);
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
