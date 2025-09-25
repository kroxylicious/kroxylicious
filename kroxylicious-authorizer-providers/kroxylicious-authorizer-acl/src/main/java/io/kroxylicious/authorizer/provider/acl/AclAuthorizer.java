/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Authorization;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.Operation;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

public class AclAuthorizer implements Authorizer {

    enum Pred {
        ANY(TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY),
        EQ(TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL),
        STARTS(TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH),
        MATCH(null);

        final TypeNameMap.Predicate predicate;

        Pred(TypeNameMap.Predicate predicate) {
            this.predicate = predicate;
        }

        public TypeNameMap.Predicate toNameMatchPredicate() {
            return predicate;
        }
    }

    record PrincipalGrants(
                           TypeNameMap<Operation<?>, EnumSet<? extends Operation<?>>> nameMatches,
                           TypePatternMatch patternMatch) {

    }

    TypeNameMap<Principal, PrincipalGrants> perPrincipal = new TypeNameMap<>();

    // TODO allow a way to say "grant * on {T} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} with to {any subject}"

    static Builder builder() {
        return new Builder();
    }

    public static class PrincipalSelectorBuilder<O extends Enum<O> & Operation> {
        private final Builder builder;
        private final Set<O> operations;
        private final Pred resourceNamePredicate;
        private final Set<String> resourceNamesOrPrefixes;
        private final Class<? extends Principal> principalClass;

        public PrincipalSelectorBuilder(Builder builder,
                                        // Class<O> operationType,
                                        Set<O> operations,
                                        Pred resourceNamePredicate,
                                        Set<String> resourceNames,
                                        Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.operations = operations;
            this.resourceNamesOrPrefixes = resourceNames;
            this.resourceNamePredicate = resourceNamePredicate;
            this.principalClass = principalClass;
        }

        public Builder withNameEqualTo(String principalName) {

            if (resourceNamePredicate == Pred.ANY) {
                builder.simpleAuthorizer.internalGrant(principalClass,
                        TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL,
                        principalName,
                        (Class) operations.iterator().next().getClass(),
                        resourceNamePredicate,
                        null,
                        operations);
            }
            else {
                for (String resourceName : resourceNamesOrPrefixes) {
                    builder.simpleAuthorizer.internalGrant(principalClass,
                            TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL,
                            principalName,
                            (Class) operations.iterator().next().getClass(),
                            resourceNamePredicate,
                            resourceName,
                            operations);
                }
            }
            return builder;
        }
    }

    public static class SubjectSelectorBuilder<O extends Enum<O> & Operation> {

        private final Builder builder;
        private final Set<O> operations;
        private final Set<String> resourceNamesOrPrefixes;
        private final Pred resourceNamePredicate;

        private SubjectSelectorBuilder(Builder builder,
                                       Set<O> operations,
                                       Pred resourceNamePredicate,
                                       Set<String> resourceNames) {
            this.builder = builder;
            this.operations = operations;
            this.resourceNamePredicate = resourceNamePredicate;
            this.resourceNamesOrPrefixes = resourceNames;
        }

        public PrincipalSelectorBuilder<O> toSubjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder<>(builder,
                    operations,
                    resourceNamePredicate,
                    resourceNamesOrPrefixes,
                    userPrincipalClass);
        }
    }

    public static class ResourceBuilder<O extends Enum<O> & Operation> {
        private final Builder builder;
        private final Set<O> operations;

        public ResourceBuilder(Builder builder, Set<O> operations) {
            this.builder = Objects.requireNonNull(builder);
            this.operations = Objects.requireNonNull(operations);
        }

        public SubjectSelectorBuilder<O> forResourcesWithNameIn(Set<String> resourceNames) {
            return new SubjectSelectorBuilder<>(builder,
                    operations,
                    Pred.EQ,
                    resourceNames);
        }

        public SubjectSelectorBuilder<O> forResourceWithNameEqualTo(String resourceName) {
            return new SubjectSelectorBuilder<>(builder,
                    operations,
                    Pred.EQ,
                    Set.of(resourceName));
        }

        public SubjectSelectorBuilder<O> forResourcesWithNameStartingWith(String resourceNamePrefix) {
            return new SubjectSelectorBuilder<>(builder,
                    operations,
                    Pred.STARTS,
                    Set.of(resourceNamePrefix));
        }

        public SubjectSelectorBuilder<O> forResourcesWithNameMatching(String resourceNameRegex) {
            return new SubjectSelectorBuilder<>(builder,
                    operations,
                    Pred.MATCH,
                    Set.of(resourceNameRegex));
        }

        public SubjectSelectorBuilder<O> forAllResources() {
            return new SubjectSelectorBuilder<>(builder,
                    operations,
                    Pred.ANY,
                    Set.of());
        }
    }

    public static class GrantBuilder {

        private Builder builder;

        private GrantBuilder(Builder builder) {
            this.builder = builder;
        }

        <O extends Enum<O> & Operation<O>> ResourceBuilder<O> allOperations(Class<O> cls) {
            return new ResourceBuilder<>(builder, EnumSet.allOf(cls));
        }

        public <O extends Enum<O> & Operation<O>> ResourceBuilder<O> operations(Set<O> operations) {
            return new ResourceBuilder<>(builder, EnumSet.copyOf(operations));
        }

    }

    public static class Builder {
        AclAuthorizer simpleAuthorizer = new AclAuthorizer();

        public GrantBuilder grant() {
            return new GrantBuilder(this);
        }

        public AclAuthorizer build() {
            return simpleAuthorizer;
        }
    }

    /**
     * grant * on org.example.MyResource with name=R to com.example.UserPrincipal (bob, sue)
     */
    public <O extends Enum<O> & Operation<O>> void grantAll(Class<O> opClass,
                                                            String resourceName,
                                                            Set<Principal> principals) {
        grant(EnumSet.allOf(opClass), resourceName, principals);
    }

    /**
     * grant(READ, WRITE) on org.example.MyResource with name=R to UserPrincipals (bob, sue)
     */
    public <O extends Enum<O> & Operation<O>> void grant(Set<O> operations,
                                                         String resourceName,
                                                         Set<Principal> principals) {
        for (var p : principals) {
            internalGrant(p.getClass(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, p.name(),
                    (Class) operations.iterator().next().getClass(),
                    Pred.EQ, resourceName,
                    operations);
        }
    }

    public <O extends Enum<O> & Operation<O>> void grantToAllPrincipalsOfType(Set<O> operations,
                                                                              String resourceName,
                                                                              Class<? extends Principal> principalType) {

        internalGrant(principalType,
                TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, null,
                (Class) operations.iterator().next().getClass(),
                Pred.EQ, resourceName,
                operations);

    }

    private <O extends Enum<O> & Operation<O>> void internalGrant(Class<? extends Principal> principalType,
                                                                  TypeNameMap.Predicate principalPredicate,
                                                                  @Nullable String principalName,
                                                                  Class<O> opType,
                                                                  Pred resourceNamePredicate,
                                                                  @Nullable String resourceName,
                                                                  Set<O> operations) {
        // TODO fix the prefix stuff so we don't bother adding redundant prefixes
        var es = EnumSet.copyOf(operations);
        for (var op : es) {
            es.addAll(op.implies());
        }
        PrincipalGrants compute = perPrincipal.compute(principalType, principalName, principalPredicate,
                g -> {
                    if (g == null) {
                        return new PrincipalGrants(resourceNamePredicate == Pred.MATCH ? null : new TypeNameMap<>(),
                                resourceNamePredicate == Pred.MATCH ? new TypePatternMatch() : null);
                    }
                    else if (g.patternMatch() == null && resourceNamePredicate == Pred.MATCH) {
                        return new PrincipalGrants(g.nameMatches(), new TypePatternMatch());
                    }
                    else if (g.nameMatches() == null && resourceNamePredicate != Pred.MATCH) {
                        return new PrincipalGrants(new TypeNameMap<>(), g.patternMatch());
                    }
                    return g;
                });

        if (resourceNamePredicate == Pred.MATCH) {
            compute.patternMatch().compute(opType, Pattern.compile(resourceName), operations);
        }
        else {
            compute.nameMatches().compute(opType, resourceName, resourceNamePredicate.toNameMatchPredicate(),
                    v -> {
                        if (v == null) {
                            return es;
                        }
                        es.addAll((EnumSet) v);
                        return v;
                    });
        }
    }

    private Decision authorize(Subject subject, Action action) {
        for (var p : subject.principals()) {

            Decision allow;
            var grant = perPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, p.name());
            if (grant != null && grant.nameMatches() != null) {
                allow = getDecision(action, grant);
                if (allow != null) {
                    return allow;
                }
            }
            grant = perPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, null);
            if (grant != null) {
                allow = getDecision(action, grant);
                if (allow != null) {
                    return allow;
                }
            }
            grant = perPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, p.name());
            if (grant != null) {
                allow = getDecision(action, grant);
                if (allow != null) {
                    return allow;
                }
            }
        }
        return Decision.DENY;
    }

    @Nullable
    private static Decision getDecision(Action action,
                                        PrincipalGrants grants) {
        Set<? extends Operation<?>> operations;
        var typeNameMap = grants.nameMatches();
        Operation<?> operation = action.operation();
        if (typeNameMap != null) {
            operations = typeNameMap.lookup(action.resourceType(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL,
                    action.resourceName());
            if (allow(operations, operation)) {
                return Decision.ALLOW;
            }
            operations = typeNameMap.lookup(action.resourceType(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY,
                    null);
            if (allow(operations, operation)) {
                return Decision.ALLOW;
            }
            operations = typeNameMap.lookup(action.resourceType(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH,
                    action.resourceName());
            if (allow(operations, operation)) {
                return Decision.ALLOW;
            }
        }
        var patternMatch = grants.patternMatch();
        if (patternMatch != null) {
            operations = patternMatch.lookup(action.resourceType(), action.resourceName());
            if (allow(operations, operation)) {
                return Decision.ALLOW;
            }
        }
        return null;
    }

    /**
     * Return true iff the given set of operations allow the given action.
     */
    private static boolean allow(@Nullable Set<? extends Operation<?>> operations,
                                 Operation<?> op) {
        return operations != null && operations.contains(op);
    }

    @Override
    public CompletionStage<Authorization> authorize(Subject subject, List<Action> actions) {
        List<Action> allowedActions = new ArrayList<>();
        List<Action> deniedActions = new ArrayList<>();
        for (var action : actions) {
            switch (authorize(subject, action)) {
                case DENY -> deniedActions.add(action);
                case ALLOW -> allowedActions.add(action);
            }
            // TODO log it
        }
        return CompletableFuture.completedStage(new Authorization(subject, allowedActions, deniedActions));
    }

    @Override
    public String toString() {
        return "SimpleAuthorizer{" +
                "perPrincipal=" + perPrincipal +
                '}';
    }
}
