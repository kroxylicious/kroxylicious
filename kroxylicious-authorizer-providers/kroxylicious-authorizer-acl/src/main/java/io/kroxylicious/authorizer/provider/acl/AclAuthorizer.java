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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.google.re2j.Pattern;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.Authorization;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.Operation;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>An implementation of {@link Authorizer} in terms of a list of rules defining <em>who</em> can do <em>what</em>
 * that is evaluated in-process.</p>
 *
 * <p>The rules are deny-by-default, meaning that subjects need to be <strong>explicitly granted</strong> permission to be able to perform any action.
 * There is no "super user", so <strong>every subject</strong> needs to be granted access to every resource.
 * However, to avoid needing a vast number of rules there are mechanisms to
 * grant access by subjects to resources <i>en-masse</i> using prefixes and regular expressions.
 * </p>
 *
 * <p>The set of rules can be built:</p>
 * <ul>
 *     <li>programmatically, from the fluent API exposed by {@link #builder()}.</li>
 *     <li>externally, from a file which expresses the rules according to a simple grammar.</li>
 * </ul>
 */
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
                           @Nullable TypeNameMap<Operation<?>, EnumSet<? extends Operation<?>>> nameMatches,
                           @Nullable TypePatternMatch patternMatch) {

    }

    TypeNameMap<Principal, PrincipalGrants> perPrincipal = new TypeNameMap<>();

    // TODO allow a way to say "grant * on {T} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} with to {any subject}"

    static Builder builder() {
        return new Builder();
    }

    public static class PrincipalSelectorBuilder {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;

        public PrincipalSelectorBuilder(Builder builder,

                                        Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.principalClass = principalClass;
        }

        public OperationsBuilder withNameEqualTo(String principalName) {
            return new OperationsBuilder(builder, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, Set.of(principalName));
        }

        public OperationsBuilder withNameIn(Set<String> principalNames) {
            return new OperationsBuilder(builder, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalNames);
        }

        public OperationsBuilder withNameStartingWith(String principalNamePrefix) {
            return new OperationsBuilder(builder, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, Set.of(principalNamePrefix));
        }

        public OperationsBuilder withAnyName() {
            HashSet<String> set = new HashSet<>();
            set.add(null);
            return new OperationsBuilder(builder, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, set);
        }
    }

    public static class SubjectSelectorBuilder {

        private final Builder builder;

        private SubjectSelectorBuilder(Builder builder) {
            this.builder = builder;

        }

        public PrincipalSelectorBuilder subjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder(builder,
                    userPrincipalClass);
        }
    }

    public static class ResourceBuilder<O extends Enum<O> & Operation> {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final TypeNameMap.Predicate principalPred;
        private final Set<String> principalNames;
        private final Class<O> operationsClass;
        private final Set<O> operations;

        public ResourceBuilder(Builder builder,
                               Class<? extends Principal> principalClass,
                               TypeNameMap.Predicate principalPred,
                               Set<String> principalNames,
                               Class<O> operationsClass,
                               Set<O> operations) {
            this.builder = Objects.requireNonNull(builder);
            this.principalClass = Objects.requireNonNull(principalClass);
            this.principalPred = Objects.requireNonNull(principalPred);
            this.principalNames = Objects.requireNonNull(principalNames);
            this.operationsClass = Objects.requireNonNull(operationsClass);
            this.operations = Objects.requireNonNull(operations);
        }

        public Builder onResourceWithNameEqualTo(String resourceName) {
            for (var principalName : principalNames) {
                builder.simpleAuthorizer.internalGrant(principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.EQ,
                        resourceName,
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameIn(Set<String> resourceNames) {
            for (var principalName : principalNames) {
                for (String resourceName : resourceNames) {
                    builder.simpleAuthorizer.internalGrant(principalClass,
                            principalPred,
                            principalName,
                            operationsClass,
                            Pred.EQ,
                            resourceName,
                            operations);
                }
            }
            return builder;
        }

        public Builder onResourcesWithNameStartingWith(String resourceNamePrefix) {
            for (var principalName : principalNames) {
                builder.simpleAuthorizer.internalGrant(principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.STARTS,
                        resourceNamePrefix,
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameMatching(String resourceNameRegex) {
            for (var principalName : principalNames) {
                builder.simpleAuthorizer.internalGrant(principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.MATCH,
                        resourceNameRegex,
                        operations);
            }
            return builder;
        }

        public Builder onAllResources() {
            for (var principalName : principalNames) {
                builder.simpleAuthorizer.internalGrant(principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.ANY,
                        null,
                        operations);
            }
            return builder;
        }
    }

    public static class OperationsBuilder {

        private final Class<? extends Principal> principalClass;
        private final TypeNameMap.Predicate principalPred;
        private final Set<String> principalNames;
        private Builder builder;

        private OperationsBuilder(Builder builder,
                                  Class<? extends Principal> principalClass,
                                  TypeNameMap.Predicate principalPred,
                                  Set<String> principalNames) {
            this.builder = builder;
            this.principalPred = Objects.requireNonNull(principalPred);
            this.principalClass = Objects.requireNonNull(principalClass);
            this.principalNames = principalNames;
        }

        <O extends Enum<O> & Operation<O>> ResourceBuilder<O> allOperations(Class<O> cls) {
            return new ResourceBuilder<>(builder,
                    principalClass,
                    principalPred,
                    principalNames,
                    cls,
                    EnumSet.allOf(cls));
        }

        public <O extends Enum<O> & Operation<O>> ResourceBuilder<O> operations(Set<O> operations) {
            EnumSet<O> os = EnumSet.copyOf(operations);
            return new ResourceBuilder<>(builder,
                    principalClass,
                    principalPred,
                    principalNames,
                    (Class) operations.iterator().next().getClass(),
                    os);
        }

    }

    public static class Builder {
        AclAuthorizer simpleAuthorizer = new AclAuthorizer();

        public SubjectSelectorBuilder grant() {
            return new SubjectSelectorBuilder(this);
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
                        return es;
                    });
        }
    }

    private Decision authorize(Subject subject, Action action) {
        for (var p : subject.principals()) {

            Decision allow;
            var grant = perPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, p.name());
            if (grant != null) {
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
